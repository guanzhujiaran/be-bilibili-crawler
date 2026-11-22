"""入库消息队列模型（按目标数据库拆分队列）。

设计要点：
- 按目标数据库拆分队列：biliopusdb / dyndetail 各一条队列，可独立扩缩容、独立监控；
- 但「大模型提取 + 写库」的处理逻辑共享同一套（同一消费者类，按 params.target_db 分支）；
- 消息体统一为 PrizeExtractParams，其中的 target_db 决定最终写入哪个数据库；
- result（大模型返回数据）不放在消息体里，单独传递/返回，避免引用指针混乱。
"""

import hashlib

from bili_common.models import StrEnumAutoDoc
from pydantic import BaseModel, Field
from datetime import datetime

from Models.lottery_database.bili.LotteryDataModels import LotExtraInfoLotType


# 官方/充电抽奖大奖判定的「输入 + 规则」版本号。
# 语义：t_lot_extra_info.judge_version 与该值一致且 input_hash 一致时才跳过重判；
# 之后任何 prompt / 输入格式变更都应 +1，老记录会被 MQ 消费者与手动脚本自动重判。
# 0 表示历史未版本化数据。
OFFICIAL_JUDGE_VERSION = 1

#: business_type → 中文类型名（仅用于拼入判定输入，帮助模型理解活动类型）
OFFICIAL_BUSINESS_TYPE_LABELS = {
    1: "官方/转发抽奖",
    10: "预约抽奖",
    12: "充电抽奖",
}

#: 一/二/三等奖在输入文本中的标签
OFFICIAL_PRIZE_LEVEL_LABELS = ("一等奖", "二等奖", "三等奖")


def _as_int(value) -> int | None:
    """把接口 / ORM 里可能为 None / 字符串的数字统一成 int，失败返回 None。"""
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def build_official_lottery_text(
    *,
    business_type=None,
    participants=None,
    prizes: list[tuple[str | None, int | None]] | None = None,
) -> str:
    """把 lotdata 的奖品名、数量、参与人数、business_type 拼成判定输入。

    只喂三个奖品名时，模型拿不到「显卡 1 份 / 鼠标 2 份 / Q币 10 份」这类数量与
    活动规模信息，大奖判定会明显偏保守。这里统一成结构化文本（多行），
    MQ 发布、手动脚本、重判都复用同一份实现，保证输入指纹一致。

    Args:
        business_type: lotdata.business_type（1 官方 / 10 预约 / 12 充电）。
        participants: lotdata.participants（抓取时快照的参与人数）。
        prizes: [(奖品名, 数量)]，固定按一/二/三等奖顺序传入；名称空则跳过该档。

    Returns:
        多行文本；没有任何奖品名时返回空字符串（调用方应跳过发布）。
    """
    lines: list[str] = []
    bt = _as_int(business_type)
    if bt is not None:
        label = OFFICIAL_BUSINESS_TYPE_LABELS.get(bt, "未知类型")
        lines.append(f"抽奖类型：{label}（business_type={bt}）")
    participant_count = _as_int(participants)
    if participant_count is not None:
        lines.append(f"参与人数：{participant_count}")

    for level_label, prize in zip(OFFICIAL_PRIZE_LEVEL_LABELS, prizes or ()):
        name = (prize[0] or "").strip()
        if not name:
            continue
        count = _as_int(prize[1])
        if count and count > 0:
            lines.append(f"{level_label}：{name} ×{count}")
        else:
            lines.append(f"{level_label}：{name}")
    return "\n".join(lines)


def official_input_hash(lottery_text: str) -> str:
    """判定输入文本的 MD5（t_lot_extra_info.input_hash）。

    输入格式或奖品信息变化时指纹随之变化，老记录会被自动重判。
    """
    return hashlib.md5((lottery_text or "").encode("utf-8")).hexdigest()


class PrizeExtractTargetEnum(StrEnumAutoDoc):
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
