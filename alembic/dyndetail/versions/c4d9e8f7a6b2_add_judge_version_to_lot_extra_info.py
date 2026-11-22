"""add_judge_version_input_hash_judge_reason_to_lot_extra_info

官方/充电抽奖大奖判定结果可重判：
- judge_version：判定规则/输入格式版本（0=历史未版本化）；
- input_hash：判定输入文本 MD5，输入变化时自动重判；
- judge_reason：LLM 判定理由，便于回溯。

Revision ID: c4d9e8f7a6b2
Revises: b7f1c2d3e4a5
Create Date: 2026-11-22 00:00:00.000000

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import mysql

revision: str = "c4d9e8f7a6b2"
down_revision: Union[str, None] = "b7f1c2d3e4a5"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "t_lot_extra_info",
        sa.Column(
            "judge_version",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
            comment="大奖判定规则/输入版本号，0=历史未版本化",
        ),
    )
    op.add_column(
        "t_lot_extra_info",
        sa.Column(
            "input_hash",
            sa.String(length=32),
            nullable=True,
            comment="判定输入文本 MD5，用于输入变化时自动重判",
        ),
    )
    op.add_column(
        "t_lot_extra_info",
        sa.Column(
            "judge_reason",
            mysql.TEXT(),
            nullable=True,
            comment="大奖判定理由（LLM 输出，便于回溯）",
        ),
    )


def downgrade() -> None:
    op.drop_column("t_lot_extra_info", "judge_reason")
    op.drop_column("t_lot_extra_info", "input_hash")
    op.drop_column("t_lot_extra_info", "judge_version")
