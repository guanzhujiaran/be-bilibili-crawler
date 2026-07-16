"""入库消息队列模型（按目标数据库拆分队列）。

设计要点：
- 按目标数据库拆分队列：biliopusdb / dyndetail 各一条队列，可独立扩缩容、独立监控；
- 但「大模型提取 + 写库」的处理逻辑共享同一套（同一消费者类，按 params.target_db 分支）；
- 消息体统一为 PrizeExtractReq，核心是一个 pydantic 自定义参数类 PrizeExtractParams，
  其中的 target_db 决定最终写入哪个数据库。
"""
from datetime import datetime
from enum import StrEnum
from typing import Optional

from Models.base.custom_pydantic import CustomBaseModel
# 从独立模块导入，避免触发 Service.GetOthersLotDyn 包的 __init__ 重导入链（与 MQ publisher 形成循环导入）
from Models.MQ.PrizeExtractResult import PrizeExtractResult


class PrizeExtractTargetEnum(StrEnum):
    """入库目标数据库 & 提取逻辑标识"""

    BILIOPUSDB = "biliopusdb"   # 普通抽奖动态 → biliopusdb
    DYNDETAIL = "dyndetail"     # 官方/充电抽奖 → dyndetail


class PrizeExtractParams(CustomBaseModel):
    """自定义参数类：决定写入哪个数据库，以及写入所需的全部数据。

    target_db 决定落库目标；其余字段按 target_db 选择性填充：
      * biliopusdb → ref_id / lot_type / dyn_content / dyn_publish_time / need_comment
      * dyndetail  → lottery_id / lottery_text
    """

    target_db: PrizeExtractTargetEnum

    # —— biliopusdb 用 ——
    ref_id: Optional[int] = None                 # 对应 dynId
    lot_type: str = "common"
    dyn_content: str = ""                        # 用于大模型提取的原始动态文本
    dyn_publish_time: Optional[datetime] = None
    # 是否需要评论（来自抽奖元信息，非大模型结果）；None 表示不更新该字段
    need_comment: Optional[int] = None

    # —— dyndetail 用 ——
    lottery_id: Optional[int] = None
    lottery_text: str = ""                       # 用于大模型提取的奖品文案


class PrizeExtractReq(CustomBaseModel):
    """入库消息体的统一外层。

    params：自定义参数（决定入哪个库）；
    result：大模型返回数据；未提取时为空默认值，由消费者填充后写库。
    """

    params: PrizeExtractParams
    result: PrizeExtractResult = PrizeExtractResult()


if __name__ == "__main__":
    req = PrizeExtractReq(
        params=PrizeExtractParams(
            target_db=PrizeExtractTargetEnum.DYNDETAIL,
            lottery_id=123,
            lottery_text="一等奖 iPhone",
        )
    )
    print(req.model_dump())
