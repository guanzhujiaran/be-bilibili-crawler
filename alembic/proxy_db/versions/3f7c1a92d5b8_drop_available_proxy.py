"""drop available_proxy

取代理改为 MySQL 随机后，available_proxy 表不再使用（也不再写入），直接删表。

Revision ID: 3f7c1a92d5b8
Revises: eaa452a8765c
Create Date: 2026-11-10 02:30:00.000000

"""

from typing import Sequence, Union

from alembic import op

revision: str = "3f7c1a92d5b8"
down_revision: Union[str, None] = "eaa452a8765c"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.drop_table("available_proxy")


def downgrade() -> None:
    # 回滚会重建空表（数据无法恢复：该表只存“业务验证过的代理”计数，价值很低）
    import sqlalchemy as sa
    from sqlalchemy.dialects import mysql

    op.create_table(
        "available_proxy",
        sa.Column("proxy_tab_id", sa.Integer(), nullable=False),
        sa.Column("pk", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("ip", sa.String(length=1024), nullable=True),
        sa.Column("counter", sa.Integer(), nullable=True),
        sa.Column("max_counter_ts", mysql.TIMESTAMP(), nullable=True),
        sa.Column("resp_code", sa.Integer(), nullable=True),
        sa.Column("available", mysql.TINYINT(display_width=1), nullable=True),
        sa.Column("latest_352_ts", mysql.TIMESTAMP(), nullable=True),
        sa.Column("latest_used_ts", mysql.TIMESTAMP(), nullable=True),
        sa.ForeignKeyConstraint(
            ["proxy_tab_id"],
            ["proxy_tab.proxy_id"],
            name="FK_available_proxy_proxy_tab",
        ),
        sa.PrimaryKeyConstraint("pk"),
        sa.UniqueConstraint("proxy_tab_id", name="proxy_tab_id"),
    )
