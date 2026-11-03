"""动态详情 → 抽奖信息的解析器（纯解析，不做落盘）。

从 `bilidyndetail.dynData` 原始 JSON 里判断这条动态是不是抽奖动态，
是的话抽取出抽奖类型、抽奖 id、需要携带的词、互动数等业务字段。

消费方：`Service/DataArchive/lot_dyn_info_export.py`（按天导出 csv.gz）。
解析逻辑本身与落盘方式无关，因此独立成模块，避免「解析」和「导出」互相耦合。
"""

import json

from log.base_log import myfastapi_logger
from Service.GetOthersLotDyn import manual_reply_judge
from Service.GrpcModule.GrpcSrc.DynObjectClass import lotDynData
from Utils.通用.CommMethods import methods


class LotDynParser:
    """把单条动态的 `dynData` JSON 文本解析成抽奖信息。"""

    def __init__(self):
        self.BAPI = methods()
        self.manual_reply_judge = manual_reply_judge

    def parse_one(self, dyn_data: str | None) -> lotDynData | None:
        """解析一条动态；不是抽奖动态 / 解析失败时返回 None。"""
        try:
            dynData = json.loads(dyn_data if dyn_data else "null", strict=False)
            if not isinstance(dynData, dict):
                return None
            dynamic_content = ""

            # 安全检查：确保dynData有extend字段
            extend = dynData.get("extend")
            if not extend:
                return None

            # 安全检查：onlyFansProperty可能不存在
            onlyFansProperty = extend.get("onlyFansProperty")
            if onlyFansProperty and onlyFansProperty.get("isOnlyFans"):
                return None

            # 安全检查：opusSummary可能不存在
            opusSummary = extend.get("opusSummary")
            if opusSummary:
                if opusSummary.get("title"):
                    title_text = opusSummary.get("title").get("text")
                    if title_text:
                        nodes = title_text.get("nodes")
                        if nodes:
                            dynamic_content += "".join(
                                [x.get("rawText") for x in nodes]
                            )
                if opusSummary.get("summary"):
                    summary_text = opusSummary.get("summary").get("text")
                    if summary_text:
                        nodes = summary_text.get("nodes")
                        if nodes:
                            dynamic_content += "".join(
                                [x.get("rawText") for x in nodes]
                            )

            author_name = extend.get("origName")
            author_space = f"https://space.bilibili.com/{extend.get('uid')}/dynamic"
            if self.BAPI.daily_choujiangxinxipanduan(dynamic_content):
                # 日常抽奖信息，忽略
                return None
            dyn_url = f"https://t.bilibili.com/{extend.get('dynIdStr')}"
            if self.BAPI.zhuanfapanduan(dynamic_content):
                dyn_url += "?tab=2"
            moduels = dynData.get("modules")
            if not moduels:
                return None
            lot_rid = ""
            lot_type = ""
            forward_count = "0"
            comment_count = "0"
            like_count = "0"
            official_verify_type = ""
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
                                lot_type = "充电抽奖"
                            elif cardType == "reserve":  # 所有的预约
                                if up.get("lotteryType") is not None:  # 10是预约抽奖
                                    lot_rid = up.get("rid")
                                    lot_type = "预约抽奖"
                if module.get("moduleButtom"):
                    moduleState = module.get("moduleButtom").get("moduleStat")
                    if moduleState:
                        forward_count = (
                            moduleState.get("repost")
                            if moduleState.get("repost")
                            else "0"
                        )
                        like_count = (
                            moduleState.get("like") if moduleState.get("like") else "0"
                        )
                        comment_count = (
                            moduleState.get("reply")
                            if moduleState.get("reply")
                            else "0"
                        )
                if module.get("moduleAuthor"):
                    author = module.get("moduleAuthor").get("author")
                    if author:
                        official_verify_type = (
                            str(author.get("official").get("type"))
                            if author.get("official") and author.get("official").get("type")
                            else "0"
                        )
                if module.get("moduleDesc"):
                    moduleDesc = module.get("moduleDesc")
                    desc = moduleDesc.get("desc")
                    if desc:
                        for descNode in desc:
                            if (
                                descNode.get("type") == "desc_type_lottery"
                            ):  # 获取官方抽奖，这里的比较全
                                businessId = extend.get("businessId")
                                if businessId:
                                    lot_rid = businessId
                                    lot_type = "官方抽奖"
            # 安全检查：origDesc可能不存在
            origDesc = extend.get("origDesc")
            if origDesc and not lot_rid:
                for descNode in origDesc:
                    if descNode.get("type") == "desc_type_lottery":
                        lot_rid = extend.get("businessId")
                        lot_type = "官方抽奖"

            premsg = self.BAPI.pre_msg_processing(dynamic_content)
            dynIdStr = extend.get("dynIdStr")
            if dynIdStr:
                dynamic_calculated_ts = int(
                    (int(dynIdStr) + 6437415932101782528) / 4294939971.297
                )
                pub_time = self.BAPI.timeshift(dynamic_calculated_ts)
            else:
                pub_time = ""

            lot_dyn_data = lotDynData()
            lot_dyn_data.dyn_url = dyn_url
            lot_dyn_data.lot_rid = str(lot_rid)
            lot_dyn_data.dynamic_content = repr(dynamic_content)
            lot_dyn_data.lot_type = str(lot_type)
            lot_dyn_data.premsg = premsg
            lot_dyn_data.forward_count = str(forward_count)
            lot_dyn_data.comment_count = str(comment_count)
            lot_dyn_data.like_count = str(like_count)
            lot_dyn_data.Manual_judge = bool(
                self.manual_reply_judge.call("manual_reply_judge", dynamic_content)
            )
            lot_dyn_data.pub_time = str(pub_time)
            lot_dyn_data.official_verify_type = str(official_verify_type)
            lot_dyn_data.author_name = author_name
            lot_dyn_data.author_space = author_space
            return lot_dyn_data
        except Exception as e:
            myfastapi_logger.exception(f"解析抽奖信息出错：{e}\n原始数据：\n{dyn_data}")
            return None
