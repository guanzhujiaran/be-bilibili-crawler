"""proxy_tab counter / timestamp columns default to 0

背景：``add_to_proxy_tab_database`` 的 INSERT 会把 ORM 实例上未赋值的计数 / 时间列
显式写成 ``NULL``，MySQL 报 1048（``Column 'fail_network_times' cannot be null``）。
代码侧已在 ``normalize_proxy_tab_insert_values`` 统一归一为 0；这里再给 DDL 补上
``DEFAULT 0`` 作为双保险，避免任何环境出现「列 NOT NULL 且无默认值」的差异。

- 四个失败计数列：NOT NULL + DEFAULT 0（与 8c1d5b7f2a90 的模型定义一致，幂等）；
- ``success_times``：可空 + DEFAULT 0（首个迁移已给默认值，这里保持幂等）；
- ``fail_window_start_ts`` / ``last_fail_ts`` / ``last_success_ts``：可空 + DEFAULT 0。

不回溯 UPDATE 存量 NULL：这三列的 NULL 与 0 在窗口判定上等价
（窗口起点 0 必然被视为过期，last_success_ts=0 必然早于窗口起点），
对 170 万行做全表 UPDATE 得不偿失。

Revision ID: c7e2a4d91f30
Revises: 8c1d5b7f2a90
Create Date: 2026-11-18 10:30:00.000000

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "c7e2a4d91f30"
down_revision: Union[str, None] = "8c1d5b7f2a90"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

#: NOT NULL 的失败计数列（缺省值必须落在 DDL 与代码两侧）
_NOT_NULL_COUNTERS = (
    "fail_network_times",
    "fail_response_times",
    "fail_unknown_times",
    "quarantine_times",
)
#: 可空的计数 / 时间列
_NULLABLE_COUNTERS = (
    "success_times",
    "fail_window_start_ts",
    "last_fail_ts",
    "last_success_ts",
)


def upgrade() -> None:
    for column_name in _NOT_NULL_COUNTERS:
        op.alter_column(
            "proxy_tab",
            column_name,
            existing_type=sa.Integer(),
            existing_nullable=False,
            server_default=sa.text("'0'"),
        )
    for column_name in _NULLABLE_COUNTERS:
        op.alter_column(
            "proxy_tab",
            column_name,
            existing_type=sa.Integer(),
            existing_nullable=True,
            server_default=sa.text("'0'"),
        )


def downgrade() -> None:
    # 四个计数列与 success_times 在 down_revision 里本来就是 DEFAULT 0，不回退；
    # 只把本迁移新增默认值的三列恢复为无默认值。
    for column_name in ("fail_window_start_ts", "last_fail_ts", "last_success_ts"):
        op.alter_column(
            "proxy_tab",
            column_name,
            existing_type=sa.Integer(),
            existing_nullable=True,
            server_default=None,
        )
