from Service.BaseCrawler.plugin.statusPlugin import (
    SequentialNullStopPlugin,
    StatsPlugin,
)
import asyncio
import time
from typing import Union, List, AsyncGenerator

from Models.base.custom_pydantic import CustomBaseModelHashable
from Service.BaseCrawler.CrawlerType import UnlimitedCrawler
from Service.BaseCrawler.config import TopicRobotConfig
from Service.BaseCrawler.model.base import WorkerStatus
from Service.BaseCrawler.retry_queue import RetryTaskType

from Service.GrpcModule.Grpc.Bapi.BiliApi import get_web_topic
from Service.opus新版官方抽奖.Model.BaseLotModel import BaseSuccCounter
from Service.opus新版官方抽奖.活动抽奖.话题抽奖.SqlHelper import topic_sqlhelper
from Service.opus新版官方抽奖.活动抽奖.话题抽奖.db.models import (
    TClickAreaCard,
    TTopicCreator,
    TTopicItem,
    TTrafficCard,
    TFunctionalCard,
    TTopDetails,
    TTopic,
    TCapsule,
)
from Utils.推送.PushMe import a_push_error


class TopicParams(CustomBaseModelHashable):
    topic_id: int

    def __hash__(self):
        return hash(self.topic_id)


#: 话题接口的**终态**业务码：该话题不存在 / 已删除，重试也不会变。
#: 只有这些码才记 ``nullData``；其余非 0 码（-352 / -412 / -500 / 超时等）都是
#: 可重试失败，必须返回 fail，否则会被当成「空数据」直接丢掉（2026-11-16 的教训）。
TOPIC_TERMINAL_CODES: frozenset[int] = frozenset({404, -404})


def _model_field_names(model, extra: tuple[str, ...] = ()) -> set[str]:
    """模型列名 + 允许的额外字段（写进 json_data 的嵌套结构）。"""
    return set(model.__table__.columns.keys()) | set(extra)


def _unknown_fields(model, payload: dict, extra: tuple[str, ...] = ()) -> set[str]:
    """payload 中不属于模型列 / 允许额外字段的键。"""
    return set(payload) - _model_field_names(model, extra)


def _filter_model_fields(
    model, payload: dict, extra: tuple[str, ...] = ()
) -> dict:
    """只保留模型认识的字段，避免 B 站新增字段导致构造失败。"""
    allowed = _model_field_names(model, extra)
    return {key: value for key, value in payload.items() if key in allowed}


class SuccCounter(BaseSuccCounter):
    first_topic_id = 0
    latest_succ_topic_id: int = 0  # 最后获取成功的话题id
    latest_topic_id: int = 0  # 最后获取的话题id，不管是否成功

    def __init__(self):
        super().__init__()


class TopicRobot(UnlimitedCrawler[TopicParams]):
    Config = TopicRobotConfig
    stats_plugin: StatsPlugin | None = None
    null_stop_plugin: SequentialNullStopPlugin | None = None
    # 至少一次：失败的话题 id 必须落 crawl_failed_task，下一轮优先补跑
    retry_task_type = RetryTaskType.TOPIC.value

    async def is_stop(self) -> bool:
        return self._cur_stop_times >= self.__max_stop_times

    async def key_params_gen(
        self, params: TopicParams
    ) -> AsyncGenerator[TopicParams, None]:
        if self.has_get_failed_topic_ids:
            topic_id = params.topic_id
            while 1:
                yield TopicParams(topic_id=topic_id)
                topic_id += 1
        else:
            for i in self.get_failed_topic_ids:
                yield TopicParams(topic_id=i)
            return

    async def handle_fetch(self, params: TopicParams) -> WorkerStatus:
        return await self.pipeline(params.topic_id)

    def retry_business_id(self, params: TopicParams | None) -> int | None:
        return params.topic_id if params is not None else None

    def retry_params(self, business_id: int) -> TopicParams:
        return TopicParams(topic_id=business_id)

    def __init__(self):
        self.sem_limit = 1
        self.start_topic_id: int = 1  # 开始的话题id
        self.min_sep_ts = 2 * 3600  # 最小的间隔时间
        self.__max_stop_times = 5  # 遇到超过时间的话题次数
        self._cur_stop_times: int = 0
        self._max_stop_count = 50
        self.null_stop_max_consecutive = self._max_stop_count
        self.sql = topic_sqlhelper
        self._latest_topic_id = 0
        self._traffic_card_lock = asyncio.Lock()  # 活动数据锁
        # 配置（logger / 超时 / 重试 / 插件等）统一由 TopicRobotConfig 控制；
        # 其中的插件会按 PluginConfig.plugin_name 自动绑定到 self（stats_plugin / null_stop_plugin）
        super().__init__()

        self.has_get_failed_topic_ids = False
        self.get_failed_topic_ids = []

    @property
    def cur_stop_times(self):
        return self._cur_stop_times

    async def save_resp(self, topic_id: int, resp: dict) -> WorkerStatus:
        """
        保存话题字典
        :param topic_id:
        :param resp:
        :return:
        """
        tTopic: TTopic = TTopic(topic_id=topic_id, raw_JSON=resp)
        tTopicItem: Union[TTopicItem, None] = None
        tTopicCreator: Union[TTopicCreator, None] = None
        tTopDetails: Union[TTopDetails, None] = None
        tFunctionalCard: Union[TFunctionalCard, None] = None
        tClickAreaCard: Union[TClickAreaCard, None] = None
        tTrafficCard: Union[TTrafficCard, None] = None
        tCapsules: Union[List[TCapsule], None] = None
        if resp.get("code") == 0:
            if topic_id > self._latest_topic_id:
                self._latest_topic_id = topic_id
            da = resp.get("data")
            da_common_keys = {"click_area_card", "functional_card", "top_details"}
            if extra_info := set(da.keys()) - da_common_keys:
                self.log.error(
                    f"data字段不匹配，topic_id:{topic_id}\ndata:{da}\n不匹配字段：{extra_info}"
                )
            top_details = da.get("top_details")
            if top_details:
                if extra_info := _unknown_fields(
                    TTopDetails,
                    top_details,
                    ("topic_item", "topic_creator", "operation_content"),
                ):
                    self.log.error(
                        f"top_details字段不匹配，topic_id:{topic_id}\ntop_details:{top_details}\n不匹配字段：{extra_info}"
                    )
                topic_creator = top_details.get("topic_creator")
                if topic_creator:
                    if extra_info := _unknown_fields(TTopicCreator, topic_creator):
                        self.log.error(
                            f"topic_creator字段不匹配，topic_id:{topic_id}\ntopic_creator:{topic_creator}\n不匹配字段：{extra_info}"
                        )
                    tTopicCreator = TTopicCreator(
                        **_filter_model_fields(TTopicCreator, topic_creator)
                    )
                topic_item = top_details.get("topic_item")
                if topic_item:
                    if extra_info := _unknown_fields(TTopicItem, topic_item):
                        self.log.error(
                            f"topic_item字段不匹配，topic_id:{topic_id}\ntopic_item:{topic_item}\n不匹配字段：{extra_info}"
                        )
                    tTopicItem = TTopicItem(
                        **_filter_model_fields(TTopicItem, topic_item)
                    )
                    if type(topic_item.get("ctime")) is int:
                        if (
                            int(time.time()) - topic_item.get("ctime")
                            <= self.min_sep_ts
                        ):
                            self._cur_stop_times += 1
                            self.log.info("到达最近时间，stop_times+=1！")
                tTopDetails = TTopDetails(
                    close_pub_layer_entry=top_details.get("close_pub_layer_entry"),
                    has_create_jurisdiction=top_details.get("has_create_jurisdiction"),
                    operation_content=top_details.get("operation_content"),
                    word_color=top_details.get("word_color"),
                )
            functional_card = da.get("functional_card")
            if functional_card:
                # game_card 等 B 站新增卡片类型没有独立表列，统一进 json_data 保存，
                # 因此这里必须同步加入白名单，否则每次遇到都会误报“字段不匹配”。
                if extra_info := _unknown_fields(
                    TFunctionalCard,
                    functional_card,
                    ("traffic_card", "capsules", "game_card"),
                ):
                    self.log.error(
                        f"functional_card字段不匹配，topic_id:{topic_id}\nfunctional_card:{functional_card}\n不匹配字段：{extra_info}"
                    )
                tFunctionalCard = TFunctionalCard(json_data=functional_card)
                traffic_card = functional_card.get("traffic_card")
                if traffic_card:
                    if extra_info := _unknown_fields(TTrafficCard, traffic_card):
                        self.log.error(
                            f"traffic_card字段不匹配，topic_id:{topic_id}\ntraffic_card:{traffic_card}\n不匹配字段：{extra_info}"
                        )
                    tTrafficCard = TTrafficCard(
                        **_filter_model_fields(TTrafficCard, traffic_card)
                    )

                capsules = functional_card.get("capsules")
                if capsules:
                    tCapsules = []
                    for capsule in capsules:
                        if extra_info := _unknown_fields(TCapsule, capsule):
                            self.log.error(
                                f"capsules字段不匹配，topic_id:{topic_id}\ncapsule:{capsule}\n不匹配字段：{extra_info}"
                            )
                        tCapsules.append(
                            TCapsule(**_filter_model_fields(TCapsule, capsule))
                        )
            click_area_card = da.get("click_area_card")
            if click_area_card:
                tClickAreaCard = TClickAreaCard(json_data=click_area_card)
        else:
            # 非 0 应答分两类：
            # - 话题确实不存在（终态）→ nullData，否则会永久 pending 在重试队列里；
            # - 其余（风控 / 服务端 5xx / 返回体异常）→ fail，落 crawl_failed_task 重试。
            code = resp.get("code")
            try:
                code_int = int(code)
            except (TypeError, ValueError):
                code_int = None
            if code_int in TOPIC_TERMINAL_CODES:
                self.log.warning(
                    f"话题不存在（code={code} message={resp.get('message')}），"
                    f"按空数据记终态：topic_id:{topic_id}"
                )
                return WorkerStatus.nullData
            self.log.error(
                f"获取话题失败（code={code} message={resp.get('message')}），"
                f"按可重试失败处理（会落 crawl_failed_task）：topic_id:{topic_id}"
            )
            return WorkerStatus.fail
        await self.sql.add_TTopic(
            tTopic,
            tTopicItem,
            tTopicCreator,
            tTopDetails,
            tFunctionalCard,
            tClickAreaCard,
            tTrafficCard,
            tCapsules,
        )
        return WorkerStatus.complete

    async def pipeline(self, topic_id) -> WorkerStatus:
        try:
            resp_dict = await get_web_topic(topic_id)
            self.log.debug(f"topic_id 【{topic_id}】 {resp_dict}")
            async with self._traffic_card_lock:
                return await self.save_resp(topic_id, resp_dict)
        except Exception as e:
            self.log.exception(f"获取话题失败，topic_id:{topic_id}\n{e}")
            raise e

    async def main(self, start_topic_id=0):
        try:
            # 先取「上一轮没写成功」的话题 id：它们在任务生成器之前入队。
            # 没有待补任务时**不再空跑一轮**（历史实现固定连跑两次 run()，第一轮
            # 刚结束立刻开第二轮，把上一轮的失败任务反复重排）。
            retry_params = await self.load_retry_params()
            self.get_failed_topic_ids = await self.sql.get_recent_failed_topic_id(
                self._max_stop_count + self.sem_limit + 5000
            )
            if retry_params or self.get_failed_topic_ids:
                seed_topic_id = (
                    self.get_failed_topic_ids[0] if self.get_failed_topic_ids else 0
                )
                await self.run(
                    TopicParams(topic_id=seed_topic_id),
                    pre_params=retry_params,
                )
                self.has_get_failed_topic_ids = True  # 获取失败的话题完成
                self._cur_stop_times = 0
            else:
                self.log.info("没有上一轮待补跑的话题，跳过补跑轮")

            if start_topic_id:
                self.start_topic_id = start_topic_id
            else:
                self.start_topic_id = await self.sql.get_max_topic_id()
            await self.run(TopicParams(topic_id=self.start_topic_id))
        except Exception as e:
            self.log.error(f"发生异常！{e}")
            await a_push_error(
                subject="运行异常",
                content=f"爬取话题异常\n{str(e)}",
            )


topic_robot = TopicRobot()

if __name__ == "__main__":
    a = TopicRobot()
    asyncio.run(a.main())
