"""抽奖信息提取结果模型（与 Service.GetOthersLotDyn 包解耦的独立模块）。

独立放置的原因：消息队列模型（PrizeExtractMQModel）与 prize_extractor 都需要引用
PrizeExtractResult，但若 MQ 模型从 Service.GetOthersLotDyn.parser.prize_extractor 导入，
会触发 Service.GetOthersLotDyn 包的 __init__ 重导入链，与 MQ publisher 形成循环导入。
因此把它放在无副作用依赖的 Models/MQ 下，供两处共用。
"""
from pydantic import BaseModel, Field


class PrizeExtractResult(BaseModel):
    """抽奖信息提取结果"""

    prize_names: list[str] = Field(default_factory=list, description="奖品名称列表")
    lottery_time: str | None = Field(
        default=None, description="开奖时间，格式YYYY-MM-DD，没有则为None")
    is_lot: bool = Field(default=False, description="是否是抽奖动态")
    need_repost: bool = Field(default=False, description="是否需要转发")
    required_topic_text: str = Field(
        default="", description="转发/评论所需要携带的话题文本，如 #抽奖#，无则为空字符串")
    is_grand_prize: bool = Field(
        default=False, description="是否大奖，奖品价值高/知名品牌/电子产品")
