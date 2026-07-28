"""入库消息队列模型（按目标数据库拆分队列）。

设计要点：
- 按目标数据库拆分队列：biliopusdb / dyndetail 各一条队列，可独立扩缩容、独立监控；
- 但「大模型提取 + 写库」的处理逻辑共享同一套（同一消费者类，按 params.target_db 分支）；
- 消息体统一为 PrizeExtractParams，其中的 target_db 决定最终写入哪个数据库；
- result（大模型返回数据）不放在消息体里，单独传递/返回，避免引用指针混乱。
"""

from pydantic import BaseModel, Field
from datetime import datetime
from enum import StrEnum

from Models.lottery_database.bili.LotteryDataModels import LotExtraInfoLotType


class PrizeExtractTargetEnum(StrEnum):
    """入库目标数据库 & 提取逻辑标识"""

    BILIOPUSDB = "普通抽奖动态"  # 普通抽奖动态 → biliopusdb
    DYNDETAIL = "官方充电抽奖"  # 官方/充电抽奖 → dyndetail


class PrizeExtractParams(BaseModel):
    """自定义参数类：决定写入哪个数据库，以及写入所需的全部数据。

    target_db 决定落库目标；其余字段按 target_db 选择性填充：
      * biliopusdb → ref_id / lot_type / dyn_content / dyn_publish_time / need_comment
      * dyndetail  → lottery_id / lottery_text
    """

    target_db: PrizeExtractTargetEnum

    # —— biliopusdb 用 ——
    ref_id: int | None = None  # 对应 dynId
    lot_type: LotExtraInfoLotType = LotExtraInfoLotType.common
    dyn_content: str = ""  # 用于大模型提取的原始动态文本
    dyn_publish_time: datetime | None = None
    # 是否需要评论（来自抽奖元信息，非大模型结果）；None 表示不更新该字段
    need_comment: int | None = None
    # 互动数据（用于 is_lot 辅助判断：评论/转发超阈值也算抽奖）
    comment_count: int | None = None
    forward_count: int | None = None

    # —— dyndetail 用 ——
    lottery_id: int | None = None
    lottery_text: str = ""  # 用于大模型提取的奖品文案


if __name__ == "__main__":
    params = PrizeExtractParams(
        target_db=PrizeExtractTargetEnum.DYNDETAIL,
        lottery_id=123,
        lottery_text="一等奖 iPhone",
    )
    print(params.model_dump())
