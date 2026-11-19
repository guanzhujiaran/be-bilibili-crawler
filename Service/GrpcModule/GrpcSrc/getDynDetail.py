"""
通过grpc获取所有的图片动态
"""

from Service.BaseCrawler.plugin.statusPlugin import StatsPlugin


import asyncio
import datetime
import json
import time
from typing import Any
from CONFIG import CONFIG, settings
from Models.base.custom_pydantic import CustomBaseModelHashable
from Service.BaseCrawler.CrawlerType import IncrementalIdCrawler
from Service.BaseCrawler.config import DynDetailScrapyConfig
from Service.BaseCrawler.model.base import WorkerStatus
from Service.BaseCrawler.retry_queue import RetryTaskType
from Service.GrpcModule.Grpc.Bapi.BiliApi import get_lot_notice, reserve_relation_info
from Service.GrpcModule.Grpc.grpc_api import bili_grpc
from Service.GrpcModule.GrpcSrc.DynObjectClass import dynAllDetail
from Service.GrpcModule.GrpcSrc.SQLObject.DynDetailSqlHelperMysqlVer import (
    grpc_sql_helper,
)
from Service.opus新版官方抽奖.Model.BaseLotModel import BaseSuccCounter, BaseStopCounter
from Service.opus新版官方抽奖.预约抽奖.db.sqlHelper import bili_reserve_sqlhelper
from Utils.通用.Common import sem_gen, asyncio_gather
from Utils.通用.dynamic_id_caculate import dynamic_id_2_ts
from Utils.通用.Tool import format_duration
from Utils.推送.PushMe import a_push_error
from bili_common.core.log_mask import (
    format_full_response,
    full_response_log_enabled,
    response_brief,
)


class StopCounter(BaseStopCounter):
    def __init__(self):
        super().__init__(30)


class SuccCounter(BaseSuccCounter):
    first_dyn_id: int = 0
    latest_rid: int = 0  # 最后的rid
    latest_succ_dyn_id: int = 0  # 最后获取成功的动态id

    def __init__(self):
        super().__init__()

    def show_text(self) -> str:
        return f"平均获取速度：{self.show_pace():.2f} s/个动态\t最初的动态：{self.first_dyn_id}\t获取时间：{datetime.datetime.fromtimestamp(self.start_ts).strftime('%Y-%m-%d %H:%M:%S')}"


class DynDetailParams(CustomBaseModelHashable):
    rid: int

    def __hash__(self):
        return hash(self.rid)


class DynDetailScrapy(IncrementalIdCrawler[DynDetailParams]):
    Config = DynDetailScrapyConfig
    status_plugin: StatsPlugin | None = None
    # 至少一次：失败的 rid 必须落 crawl_failed_task，下一轮优先补跑（否则断点会越过它）
    retry_task_type = RetryTaskType.DYN_DETAIL.value
    id_field = "rid"

    def make_params(self, business_id: int) -> DynDetailParams:
        return DynDetailParams(rid=business_id)

    def __init__(self):
        self.offset = 10  # 每次获取rid的数量，数值最好不要超过10，太大的话传输会出问题
        self.BiliGrpc = bili_grpc
        self.succ_times = 0
        self.comm_headers = {
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "content-type": "application/json;charset=UTF-8",
            "dnt": "1",
            "sec-ch-ua": '"Not.A/Brand";v="8", "Chromium";v="114", "Microsoft Edge";v="114"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-site",
            "user-agent": CONFIG.rand_ua,
        }
        self.Sqlhelper = grpc_sql_helper
        self.stop_counter: StopCounter = StopCounter()  # 停止标志
        self.stop_Flag_lock = asyncio.Lock()
        self.scrapy_sem = sem_gen()  # 同时运行的协程数量
        self.stop_limit_time = 2 * 3600  # 提前多少时间停止
        self.succ_counter = SuccCounter()
        self._BiliLotDataPublisher = None
        # 配置（logger / 超时 / 重试 / 插件等）统一由 DynDetailScrapyConfig 控制
        super().__init__()

    async def handle_fetch(self, params: DynDetailParams) -> WorkerStatus:
        detail = (await self.get_grpc_single_dynDetail(params.rid))[0]
        await self.Sqlhelper.upsert_DynDetail(
            doc_id=detail.get("rid"),
            dynamic_id=detail.get("dynamic_id"),
            dynData=detail.get("dynData"),
            lot_id=detail.get("lot_id"),
            dynamic_created_time=detail.get("dynamic_created_time"),
        )
        # 必须显式返回状态：之前隐式返回 None 会被统一当作 complete，
        # 导致"动态不存在"这类空数据也被计入有效成功，统计与连续空数据停止条件都会失真
        if str(detail.get("dynamic_id")) == "-1":
            return WorkerStatus.nullData
        return WorkerStatus.complete

    async def is_stop(self) -> bool:
        return self.stop_counter.stop_flag

    @property
    def BiliLotDataPublisher(self):
        if not self._BiliLotDataPublisher:
            from Service.MQ.base.MQClient.BiliLotDataPublisher import (
                BiliLotDataPublisher,
            )

            self._BiliLotDataPublisher = BiliLotDataPublisher
        return self._BiliLotDataPublisher

    def _timeshift(self, timestamp: int) -> str:
        """
        时间戳转换日期
        :param timestamp:
        :return:
        """
        local_time = time.localtime(timestamp)
        realtime = time.strftime("%Y-%m-%d %H:%M:%S", local_time)
        return realtime

    def _timeunshift(self, datetime_str: str) -> int:
        timeArray = time.strptime(datetime_str, "%Y-%m-%d %H:%M:%S")
        timeStamp = int(time.mktime(timeArray))
        return timeStamp

    # region 从api获取信息操作

    async def _fetch_official_lot_data(
        self,
        business_type: int,
        business_id: int | str,
        origin_dynamic_id: int | str,
    ) -> dict:
        """拉取官方 / 预约 / 充电抽奖的 lottery_notice，返回 data。"""
        lot_notice_res = await get_lot_notice(
            business_type=business_type,
            business_id=business_id,
            origin_dynamic_id=origin_dynamic_id,
        )
        return lot_notice_res.get("data") or {}

    async def _publish_lot_data(self, lot_data: dict) -> None:
        """将抽奖数据投递到落库与大奖判断两条 MQ 链路。"""
        if not lot_data:
            return
        self.log.debug(f"抽奖动态！{lot_data}")
        await self.BiliLotDataPublisher.pub_upsert_official_reserve_charge_lot(
            lot_data,
            extra_routing_key="DynDetailScrapy.resolve_dynamic_details_card",
        )
        await self.BiliLotDataPublisher.pub_prize_extract_from_lot_data(
            lot_data_dict=lot_data,
            extra_routing_key="DynDetailScrapy.resolve_dynamic_details_card",
        )

    async def resolve_dynamic_details_card(self, dynData: dict, is_running_scrapy=True):
        """
        解析grpc返回的data里面的东西
        :param is_running_scrapy:
        :param dynData:
        :return: temp_rid, lot_id, dynamic_id, dynamic_created_time
        """
        try:
            # 安全检查：确保extend字段存在
            extend = dynData.get("extend")
            if not extend:
                self.log.warning(
                    f"动态数据缺少extend字段：{response_brief(dynData)}"
                )
                return None, None, None, None

            temp_rid = extend.get("businessId")
            dynamic_id = extend.get("dynIdStr")
            if not dynamic_id:
                self.log.warning(
                    f"动态缺少dynIdStr字段：{response_brief(dynData)}"
                )
                return None, None, None, None

            try:
                dynamic_calculated_ts = dynamic_id_2_ts(int(dynamic_id))
            except (ValueError, TypeError) as e:
                self.log.warning(f"动态ID格式错误：{dynamic_id}, 错误：{e}")
                return None, None, None, None

            if is_running_scrapy:
                if (
                    time.time() - dynamic_calculated_ts < self.stop_limit_time
                    and time.time() - dynamic_calculated_ts > 0
                ):
                    async with self.stop_Flag_lock:
                        self.log.debug(f"遇到终止动态！连续终止数+1！\n{dynData}")
                        self.stop_counter.cur_stop_continuous_num += 1
            dynamic_created_time = self._timeshift(
                dynamic_calculated_ts
            )  # 通过公式获取大致的时间，误差大概20秒左右
            moduels = dynData.get("modules")
            if not moduels:
                moduels = []

            lot_id = None
            lot_data = {}
            for module in moduels:
                if module.get("moduleAdditional"):
                    moduleAdditional = module.get("moduleAdditional")
                    if moduleAdditional.get("type") == "additional_type_up_reservation":
                        # lot_id不能在这里赋值，需要在底下判断是否为抽奖之后再赋值
                        up = moduleAdditional.get("up")
                        if up:
                            cardType = up.get("cardType")
                            if cardType == "upower_lottery":  # 12是充电抽奖
                                lot_rid = up.get("dynamicId")
                                lot_data = await self._fetch_official_lot_data(
                                    12, lot_rid, dynamic_id
                                )
                                if lot_data:
                                    lot_id = lot_data.get("lottery_id")
                            elif cardType == "reserve":  # 所有的预约
                                # 初始化 lot_rid，无论是否有 lotteryType
                                lot_rid = up.get("rid")
                                if up.get("lotteryType") is not None:  # 10 是预约抽奖
                                    pass  # lot_rid 已经在上面赋值
                                else:
                                    # 非抽奖类型的预约，不需要调用抽奖接口
                                    self.log.info(
                                        f"非抽奖预约卡片，跳过：http://www.bilibili.com/opus/{dynamic_id}"
                                    )
                                    continue  # 跳过本次循环
                                lot_data = await self._fetch_official_lot_data(
                                    10, lot_rid, dynamic_id
                                )
                                has_reserve_relation_ids = (
                                    await bili_reserve_sqlhelper.get_reserve_by_ids(
                                        lot_rid
                                    )
                                )
                                if (
                                    has_reserve_relation_ids
                                    and has_reserve_relation_ids.code == 0
                                    and has_reserve_relation_ids.sid != None
                                ):
                                    req1_dict = has_reserve_relation_ids.raw_JSON
                                else:
                                    req1_dict = await reserve_relation_info(lot_rid)
                                    req1_dict.update({"ids": lot_rid})
                                    await bili_reserve_sqlhelper.add_reserve_info_by_resp_dict(
                                        req1_dict, 1
                                    )  # 添加预约json到数据库
                                lot_id = lot_data.get("lottery_id")
                        else:
                            self.log.error(
                                f"Unknown card type： http://www.bilibili.com/opus/{dynamic_id}\t{json.dumps(moduleAdditional)}"
                            )
                    elif moduleAdditional.get("type") == "additional_type_ugc":
                        self.log.info(
                            f"视频卡片： http://www.bilibili.com/opus/{dynamic_id}\t{json.dumps(moduleAdditional)}"
                        )
                    elif moduleAdditional.get("type") == "additional_type_common":
                        self.log.info(
                            f"游戏/装扮卡片： http://www.bilibili.com/opus/{dynamic_id}\t{json.dumps(moduleAdditional)}"
                        )
                    elif moduleAdditional.get("type") == "additional_type_goods":
                        self.log.info(
                            f"会员购商品卡片： http://www.bilibili.com/opus/{dynamic_id}\t{json.dumps(moduleAdditional)}"
                        )
                    elif moduleAdditional.get("type") == "additional_type_vote":
                        self.log.info(
                            f"投票卡片： http://www.bilibili.com/opus/{dynamic_id}\t{json.dumps(moduleAdditional)}"
                        )
                    elif moduleAdditional.get("type") == "addition_vote_type_word":
                        self.log.info(
                            f"文字投票卡片： http://www.bilibili.com/opus/{dynamic_id}\t{json.dumps(moduleAdditional)}"
                        )
                    elif moduleAdditional.get("type") == "addition_vote_type_default":
                        self.log.info(
                            f"默认投票卡片： http://www.bilibili.com/opus/{dynamic_id}\t{json.dumps(moduleAdditional)}"
                        )
                    elif moduleAdditional.get("type") == "additional_type_esport":
                        self.log.info(
                            f"电子竞技赛事卡片： http://www.bilibili.com/opus/{dynamic_id}\t{json.dumps(moduleAdditional)}"
                        )
                    else:
                        self.log.error(
                            f"未知module： http://www.bilibili.com/opus/{dynamic_id}\t{json.dumps(module)}"
                        )
                if module.get("moduleDesc"):
                    moduleDesc = module.get("moduleDesc")
                    desc = moduleDesc.get("desc")
                    if desc:
                        for descNode in desc:
                            if (
                                descNode.get("type") == "desc_type_lottery"
                            ):  # 获取官方抽奖，这里的比较全
                                lot_id = descNode.get("rid")
                                lot_rid = dynData.get("extend").get("businessId")
                                lot_data = await self._fetch_official_lot_data(
                                    2, lot_rid, dynamic_id
                                )
                                if lot_data:
                                    lot_id = lot_data.get("lottery_id")
                if module.get("moduleItemNull"):
                    self.log.error(
                        f"出错提醒的moduleItem： http://www.bilibili.com/opus/{dynamic_id}\t{json.dumps(module)}"
                    )
            if (
                dynData.get("extend").get("origDesc") and not lot_id
            ):  # 获取官方抽奖，这里的可能会漏掉开头的官方抽奖
                for descNode in dynData.get("extend").get("origDesc"):
                    if descNode.get("type") == "desc_type_lottery":
                        lot_id = descNode.get("rid")
                        lot_rid = dynData.get("extend").get("businessId")
                        lot_data = await self._fetch_official_lot_data(
                            2, lot_rid, dynamic_id
                        )
                        if lot_data:
                            lot_id = lot_data.get("lottery_id")
            await self._publish_lot_data(lot_data)
            return temp_rid, lot_id, dynamic_id, dynamic_created_time
        except Exception as e:
            # 顺序很重要：先打异常（真正的原因），摘要另起一行放 debug。
            # 历史写法把整个 dynData 拼在异常前面，几十 KB 的正文会把「解析动态详情出错：xxx」
            # 挤到日志末尾，看起来像「解析报错但真实原因是取不到 cookie」这种误判。
            self.log.exception(f"解析动态详情出错：{e}")
            self.log.debug(
                f"解析动态详情出错（dynData 摘要）：{response_brief(dynData)}"
            )
            if full_response_log_enabled():
                self.log.debug(f"解析动态详情出错（完整 dynData）：{format_full_response(dynData)}")
            await a_push_error(
                subject="运行异常",
                content=(
                    f"解析动态详情出错：{type(e).__name__}: {e}\n"
                    f"响应摘要：{response_brief(dynData)}"
                ),
            )
            return 0, 0, 0, self._timeshift(0)

    async def _finish_single_detail(
        self,
        resp_list: dict,
        fallback_rid: int | str,
        *,
        mark_latest: bool = False,
        int_first_id: bool = False,
    ) -> list[dict]:
        """把单条 gRPC 响应解析成 dynAllDetail 并更新成功计数。

        两个入口（按 rid / 按 dynamic_id）只有「调哪个 gRPC 方法」和计数细节不同，
        解析与日志统一收敛到这里。
        """
        ret_dict_list: list[dict] = []
        dynamic_created_time = None
        if resp_list.get("item"):
            resp_data = resp_list.get("item")
            (
                temp_rid,
                lot_id,
                dynamic_id,
                dynamic_created_time,
            ) = await self.resolve_dynamic_details_card(resp_data)
            temp_detail_dict = dynAllDetail(
                rid=temp_rid,
                dynamic_id=dynamic_id,
                dynData=resp_data,
                lot_id=lot_id if lot_id else None,
                dynamic_created_time=dynamic_created_time,
            )
            ret_dict_list.append(temp_detail_dict.__dict__)
        else:
            ret_dict_list.append({"rid": fallback_rid, "dynamic_id": "-1"})

        self.succ_times += 1
        dynamic_id = ret_dict_list[0].get("dynamic_id")
        if dynamic_id != "-1":
            self.succ_counter.succ_count += 1
            if mark_latest:
                self.succ_counter.latest_succ_dyn_id = dynamic_id
            if not self.succ_counter.first_dyn_id:
                self.succ_counter.first_dyn_id = (
                    int(dynamic_id) if int_first_id else dynamic_id
                )
            self.log.info(
                f"{self.succ_counter.show_text()}\n总共成功获取{self.succ_times}次\t"
                f"{fallback_rid} 获取单个动态详情成功！http://www.bilibili.com/opus/{dynamic_id} "
                f"{dynamic_created_time if dynamic_created_time else ''}"
            )
        else:
            self.succ_counter.succ_count += 1
            self.log.info(
                f"{self.succ_counter.show_text()}\n总共成功获取{self.succ_times}次\t"
                f"获取单个动态详情成功，但动态被删除了！\t{resp_list}\t"
                f"http://t.bilibili.com/{fallback_rid}?type=2"
            )
        return ret_dict_list

    async def get_grpc_single_dynDetail(
        self, rid: int
    ) -> list[dict[str, Any] | dict[str, int | str]]:
        """
        获取单个grpc的动态详情
        :param rid:
        :return:
        """
        self.log.info(
            f"当前获取rid：{rid}，跳转连接：http://t.bilibili.com/{rid}?type=2"
        )
        resp_list = await self.BiliGrpc.grpc_get_dynamic_detail_by_type_and_rid(
            rid=rid, dynamic_type=2
        )
        return await self._finish_single_detail(
            resp_list, rid, mark_latest=True
        )

    async def get_grpc_single_dynDetail_by_dynamic_id(
        self, dynamic_id: int | str
    ) -> dict:
        """
        获取单个grpc的动态详情
        通过这个获取的rid是负数，防止干扰正常rid
        :param dynamic_id: 动态id
        :return:
        """
        self.log.info(
            f"当前获取dynamic_id：{dynamic_id}，跳转连接：http://t.bilibili.com/{dynamic_id}"
        )
        resp_list = await self.BiliGrpc.grpc_get_dynamic_detail_by_dynamic_id(
            dynamic_id=dynamic_id
        )
        ret = await self._finish_single_detail(
            resp_list, dynamic_id, int_first_id=True
        )
        return ret[0]

    # endregion
    async def get_single_detail_by_rid_list(
        self, rid_list: list[int]
    ) -> list[int] | None:
        """
        通过ridlist直接获取动态详情
        :param rid_list:
        :return:
        """
        async with self.scrapy_sem:
            try:
                for rid in rid_list:
                    self.log.debug(
                        f"当前执行【{rid_list.index(rid) + 1}/{len(rid_list)}】：动态rid列表：{rid_list}\n跳转连接：http://t.bilibili.com/{rid}?type=2"
                    )
                    detail = (await self.get_grpc_single_dynDetail(rid))[0]
                    await self.Sqlhelper.upsert_DynDetail(
                        doc_id=detail.get("rid"),
                        dynamic_id=detail.get("dynamic_id"),
                        dynData=detail.get("dynData"),
                        lot_id=detail.get("lot_id"),
                        dynamic_created_time=detail.get("dynamic_created_time"),
                    )
                return rid_list
            except Exception as e:
                self.log.exception(e)

    async def get_discontious_dynamics_by_single_detail(self):
        """
        通过获取单个rid动态的方式获取不连续的缺失动态
        :return:
        """
        all_rids = await self.Sqlhelper.get_discountious_rids()
        self.log.info(f"共有{len(all_rids)}条缺失动态")
        task_args_list = []  # [[1,2,3,4,5,6,7,8],[9,10,11,12,13,14,15],...]
        for rid_index in range(len(all_rids) // self.offset + 1):
            rids_list = all_rids[
                self.offset * rid_index : self.offset * (rid_index + 1)
            ]
            task_args_list.append(rids_list)
        thread_num = 50
        task_list = []
        for task_index in range(len(task_args_list) // thread_num + 1):
            args_list = task_args_list[
                thread_num * task_index : thread_num * (task_index + 1)
            ]  # 将task切片成[[0...49],[50....99]]
            for args in args_list:
                self.log.info(args)
                task = self.get_single_detail_by_rid_list(args)
                task_list.append(task)
        self.log.info(f"共创建{len(task_list)}个进程！")
        await asyncio_gather(*task_list, log=self.log)
        # for t in thread_list:
        #     t.join()

    async def get_lost_lottery_notice(self, limit_ts: int = 7 * 3600 * 24):
        """
        重新获取有lot_id，但是lotdata没存进去的抽奖
        :return:
        """
        self.log.debug("开始获取缺失的抽奖信息！")

        all_lots = await self.Sqlhelper.get_lost_lots(limit_ts)
        self.log.debug(f"共有{len(all_lots)}条缺失抽奖信息！")
        task_list = []
        for all_detail in all_lots:
            task = asyncio.create_task(
                self.resolve_dynamic_details_card(
                    json.loads(
                        all_detail.dynData if all_detail.dynData else "null",
                        strict=False,
                    )
                )
            )
            task_list.append(task)
        await asyncio_gather(*task_list, log=self.log)

    @property
    def _backfill_days(self) -> int:
        """空库 / 断点过旧时的回补窗口（天）：默认跟随冷归档保留窗口。

        库里本来只保留这么多天的热数据，被清空后回补这一窗口即可；
        `DYN_DETAIL_BACKFILL_DAYS` 可单独调小，不影响 `COLD_ARCHIVE_KEEP_DAYS_DYNDETAIL`
        （后者还负责归档删库，不能为了回补范围去改它）。
        """
        return (
            settings.DYN_DETAIL_BACKFILL_DAYS
            or settings.COLD_ARCHIVE_KEEP_DAYS_DYNDETAIL
        )

    async def _resolve_anchor_rid(self) -> int | None:
        """用「回补窗口起点」那条抽奖动态反查 rid，作为断点失效时的起始位置。

        回补窗口 = 冷归档保留窗口（`COLD_ARCHIVE_KEEP_DAYS_DYNDETAIL`）：库里本来只保留
        这么多天的热数据，被清空后就等于这一窗口全丢，必须从窗口起点重新灌一遍。
        因此锚点取窗口内**最早**一条抽奖动态，而不是最新一条（最新只能补最近几天）。

        `lotdata` 与动态详情在同一个库，且官方 / 充电抽奖的 business_id 就是动态 id，
        通过 gRPC 取该动态详情即可拿到 `extend.businessId`（即 rid）。
        """
        since_ts = int(time.time()) - self._backfill_days * 86400
        # 只试最近几条：一次 gRPC 可能要重试多个代理，条数太多会明显拖慢任务启动
        dynamic_ids = await self.Sqlhelper.get_earliest_lot_dynamic_ids_since(
            since_ts=since_ts, limit=3
        )
        for dynamic_id in dynamic_ids:
            try:
                resp = await self.BiliGrpc.grpc_get_dynamic_detail_by_dynamic_id(
                    dynamic_id=dynamic_id
                )
            except Exception as e:
                self.log.warning(f"锚点动态 {dynamic_id} 反查 rid 失败：{e!r}")
                continue
            item = resp.get("item") if isinstance(resp, dict) else None
            if not item:
                self.log.warning(f"锚点动态 {dynamic_id} 无详情（可能已删除），尝试下一条")
                continue
            rid = (item.get("extend") or {}).get("businessId")
            try:
                return int(rid)
            except (TypeError, ValueError):
                self.log.warning(
                    f"锚点动态 {dynamic_id} 的 extend.businessId 非法：{rid!r}"
                )
                continue
        return None

    async def _resolve_latest_rid(self) -> int | None:
        """确定本轮动态详情爬取的起始 rid。

        - 库里的断点还在保留窗口（`COLD_ARCHIVE_KEEP_DAYS_DYNDETAIL`）内：直接沿用，
          保证增量不断档；
        - 空库 / 断点早于保留窗口：说明本地库被清过或服务长期没跑。库里本来只保留
          最近 keep_days 天的热数据（更早的已冷归档删库），因此等价于「这一窗口全丢」，
          从回补窗口起点那条抽奖动态反查 rid，把整窗口重新灌一遍；
        - 连锚点动态也拿不到（lotdata 也空 / 窗口内没有官方充电动态）：发告警并跳过
          本轮，提示先去同步数据，而不是静默什么都不做。
        """
        now = int(time.time())
        keep_days = self._backfill_days
        # 连续水位优先：只要重试队列里还有没写成功的 rid，本轮就必须从「最小 pending - 1」
        # 开始扫，而不是从「最后成功的 id」开始 —— 后者会直接越过失败 rid（丢数据）。
        pending_min = await self.pending_min_business_id()
        if pending_min is not None:
            self.log.warning(
                f"重试队列还有未写入成功的 rid，本轮从 rid={pending_min - 1} 起重扫"
                f"（最小 pending={pending_min}），不再从「最后成功 id」起跳"
            )
            return pending_min - 1
        breakpoint = await self.Sqlhelper.get_latest_rid_breakpoint()
        if breakpoint is not None and breakpoint.latest_dynamic_ts is not None:
            lag = now - breakpoint.latest_dynamic_ts
            if lag <= keep_days * 86400:
                return breakpoint.rid
            reason = (
                f"断点过旧：最新动态为 {self._timeshift(breakpoint.latest_dynamic_ts)}"
                f"（落后 {format_duration(lag)}，已超出 {keep_days} 天保留窗口）"
            )
        elif breakpoint is None:
            reason = "dyndetail.bilidyndetail 为空（空库或尚未初始化）"
        else:
            # 表里只有 dynamic_id = -1（动态已删除）的记录，没有可用时间参考
            reason = "dyndetail.bilidyndetail 只有无效记录（无可用发布时间）"

        anchor_rid = await self._resolve_anchor_rid()
        if anchor_rid is not None:
            self.log.warning(
                f"{reason}，改用保留窗口（最近 {keep_days} 天）起点那条抽奖动态反查到的 "
                f"rid={anchor_rid} 重新回补；早于窗口的历史不再尝试"
            )
            return anchor_rid

        message = (
            f"{reason}，且未能从保留窗口（最近 {keep_days} 天）内的抽奖动态反查到可用锚点"
            "（lotdata 无该窗口数据，或 gRPC 取详情失败），跳过本次动态详情爬取。"
            "请先从生产同步 dyndetail 数据"
            "（scripts/database/sync_lottery_from_prod），或确认抽奖 / 代理链路是否正常"
        )
        self.log.error(message)
        await a_push_error(subject="运行异常", content=f"GET_DYN：{message}")
        return None

    async def main(self):
        try:
            # 先取「上一轮没写成功」的 rid：它们在任务生成器之前入队，
            # 不依赖游标位置，代理恢复后一定会被重跑（至少一次）。
            retry_params = await self.load_retry_params()
            latest_rid = await self._resolve_latest_rid()
            if latest_rid is None:
                return
            task_list = []
            if latest_rid < settings.DYN_DETAIL_START_RID:
                self.log.info(
                    f"最后 rid 过小（{latest_rid} < {settings.DYN_DETAIL_START_RID}），"
                    "启用默认起始 rid 并补缺动态！"
                )
                latest_rid = settings.DYN_DETAIL_START_RID
                self.log.info("开始重新获取失败的动态！")
                task1 = asyncio.create_task(
                    self.get_discontious_dynamics_by_single_detail()
                )
                task_list.append(task1)
                await asyncio.sleep(30)
                self.log.info("重新获取有lot_id，但是lotdata没存进去的抽奖！")
                task2 = asyncio.create_task(self.get_lost_lottery_notice())
                task_list.append(task2)
            self.succ_counter = SuccCounter()
            self.succ_counter.is_running = True
            self.log.info("开始执行获取动态详情")
            self.log.debug(f"爬虫，启动！最后的rid为：{latest_rid}\t往前回滚500个rid！")
            task3 = asyncio.create_task(
                self.run(DynDetailParams(rid=latest_rid), pre_params=retry_params)
            )
            task_list.append(task3)
            await asyncio_gather(*task_list, log=self.log)
            self.log.error("爬取动态任务全部完成！")
            # 抽奖信息导出不再跟在爬取任务尾部做：它属于「每日 dyn_backup」的一环，
            # 由 DYN_DETAIL_DATABASE_CLEANER 定时任务统一产出 csv.gz
            # （见 scripts/database/clean_backup_outdated_dynamic/cleaner.py）。
        except Exception as e:
            # 必须带堆栈：之前只打 e 的字符串，排查时只能靠猜行号
            self.log.exception(f"爬取动态任务出错：{e}")
            await a_push_error(
                subject="运行异常",
                content=f"爬取动态任务出错：{e}",
            )
        finally:
            self.succ_counter.is_running = False

dyn_detail_scrapy = DynDetailScrapy()
