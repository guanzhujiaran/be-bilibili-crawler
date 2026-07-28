"""drop_is_lot_need_comment_need_repost_from_lot_extra_info

官方抽奖的抽奖方式（是否需转发/评论）已由 lotdata.business_type 固定，
无需再落库存储 is_lot / need_comment / need_repost，
仅保留 is_grand_prize 作为大模型判断结果入库。

Revision ID: a3b8c7d1e2f4
Revises: 5fcc7a2955d5
Create Date: 2026-07-28 21:45:00.000000

"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql

revision: str = 'a3b8c7d1e2f4'
down_revision: Union[str, None] = '5fcc7a2955d5'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 移除抽奖方式相关列，官方抽奖的 is_lot / need_comment / need_repost
    # 现已由 OfficialLotExtraInfoResp 根据 lottery_type 以 computed_field 实时计算。
    op.drop_column('t_lot_extra_info', 'is_lot')
    op.drop_column('t_lot_extra_info', 'need_comment')
    op.drop_column('t_lot_extra_info', 'need_repost')


def downgrade() -> None:
    op.add_column('t_lot_extra_info', sa.Column(
        'need_repost', mysql.TINYINT(display_width=1),
        server_default=sa.text('0'), nullable=False,
        comment='是否需要转发: 1-是, 0-否',
    ))
    op.add_column('t_lot_extra_info', sa.Column(
        'need_comment', mysql.TINYINT(display_width=1),
        server_default=sa.text('0'), nullable=False,
        comment='是否需要评论: 1-是, 0-否',
    ))
    op.add_column('t_lot_extra_info', sa.Column(
        'is_lot', mysql.TINYINT(display_width=1),
        server_default=sa.text('0'), nullable=False,
        comment='LLM判断是否为抽奖: 1-是, 0-否',
    ))
