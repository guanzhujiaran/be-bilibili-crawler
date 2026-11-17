"""add t_lotdyninfo_raw (rawJsonStr vertical split)

把 `t_lotdyninfo.rawJsonStr`（单行约 8KB、占全表 95% 以上数据量）拆到独立分表：
主表只保留元数据与统计字段，原始 JSON 仅按 dynId 点查，避免全表聚合时被大字段
挤出 buffer pool。

历史数据迁移见 `scripts/database/split_lotdyninfo_raw/`，本迁移只建表。

Revision ID: a1b2c3d4e5f6
Revises: f1a2b3c4d5e6
Create Date: 2026-11-17 00:00:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql

revision: str = "a1b2c3d4e5f6"
down_revision: Union[str, None] = "f1a2b3c4d5e6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _table_exists(table: str) -> bool:
    """检查表是否已存在（幂等保护）"""
    conn = op.get_bind()
    insp = sa.inspect(conn)
    return table in insp.get_table_names()


def _index_exists(table: str, index: str) -> bool:
    conn = op.get_bind()
    insp = sa.inspect(conn)
    return index in [i["name"] for i in insp.get_indexes(table)]


def upgrade() -> None:
    if _table_exists("t_lotdyninfo_raw"):
        return
    op.create_table(
        "t_lotdyninfo_raw",
        sa.Column("dynId", sa.BigInteger(), autoincrement=False, nullable=False),
        sa.Column("rawJsonStr", mysql.JSON(), nullable=True),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=True,
        ),
        sa.PrimaryKeyConstraint("dynId"),
    )
    if not _index_exists("t_lotdyninfo_raw", "idx_created_at"):
        op.create_index(
            "idx_created_at", "t_lotdyninfo_raw", ["created_at"], unique=False
        )


def downgrade() -> None:
    if not _table_exists("t_lotdyninfo_raw"):
        return
    if _index_exists("t_lotdyninfo_raw", "idx_created_at"):
        op.drop_index("idx_created_at", table_name="t_lotdyninfo_raw")
    op.drop_table("t_lotdyninfo_raw")
