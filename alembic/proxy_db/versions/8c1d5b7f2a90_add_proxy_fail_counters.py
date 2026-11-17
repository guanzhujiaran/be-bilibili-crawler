"""add proxy fail reason counters

按失败原因清理僵尸代理的第一块基础设施：把 network / response / unknown /
risk_control 四类失败在 MySQL 侧分桶累计，并记录「当前计数窗口起点」与
「最近成功时间」，供 ``clear_unusable_proxy`` 按原因 + 时间窗口判定。

- ``fail_window_start_ts``：窗口内滚动计数。下一次失败若距起点超过
  ``zombie_window_days``，先把四个计数清零再累加，保证计数始终是「近 N 天」。
- ``last_success_ts``：判定「窗口内 success_total == 0」用
  （为空或早于窗口起点才算窗口内没成功过）。
- 索引 ``ix_proxy_tab_fail_window``：清理任务先按窗口起点缩小范围，
  再在索引内过滤 network 次数。存量行全部为 NULL，不占索引空间。

Revision ID: 8c1d5b7f2a90
Revises: a7c4e19b6d20
Create Date: 2026-11-17 10:00:00.000000

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "8c1d5b7f2a90"
down_revision: Union[str, None] = "a7c4e19b6d20"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # MySQL 8 加带默认值的 NOT NULL 列走 INSTANT，不重建 158 万行的表
    op.add_column(
        "proxy_tab",
        sa.Column(
            "fail_network_times",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("'0'"),
        ),
    )
    op.add_column(
        "proxy_tab",
        sa.Column(
            "fail_response_times",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("'0'"),
        ),
    )
    op.add_column(
        "proxy_tab",
        sa.Column(
            "fail_unknown_times",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("'0'"),
        ),
    )
    op.add_column(
        "proxy_tab",
        sa.Column(
            "quarantine_times",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("'0'"),
        ),
    )
    op.add_column("proxy_tab", sa.Column("fail_window_start_ts", sa.Integer()))
    op.add_column("proxy_tab", sa.Column("last_fail_ts", sa.Integer()))
    op.add_column("proxy_tab", sa.Column("last_success_ts", sa.Integer()))
    op.create_index(
        "ix_proxy_tab_fail_window",
        "proxy_tab",
        ["fail_window_start_ts", "fail_network_times"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_proxy_tab_fail_window", table_name="proxy_tab")
    op.drop_column("proxy_tab", "last_success_ts")
    op.drop_column("proxy_tab", "last_fail_ts")
    op.drop_column("proxy_tab", "fail_window_start_ts")
    op.drop_column("proxy_tab", "quarantine_times")
    op.drop_column("proxy_tab", "fail_unknown_times")
    op.drop_column("proxy_tab", "fail_response_times")
    op.drop_column("proxy_tab", "fail_network_times")
