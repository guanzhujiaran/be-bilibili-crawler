"""add proxy status indexes

`GetProxyStatus` 单次 3.3~5.2s 的两个根因都在缺索引（本地 158 万行 proxy_tab 实测）：

1. ``get_latest_add_ts``：``ORDER BY add_ts DESC LIMIT 1`` 没有可用索引，
   只能扫「覆盖索引」全部 156 万行再排序（2004ms）。补 ``add_ts`` 单列索引后
   变成索引倒序取第一条（<2ms）。
2. ``get_usable_proxy_num``：``status = 0 AND coalesce(success_times, 0) >= 0``
   里的 ``coalesce`` 是函数调用，无法用索引做范围定位（1991ms，扫 779,312 行）。
   补 ``(status, success_times)`` 复合索引后，把条件改写成
   ``success_times IS NULL OR success_times >= 0`` 即可走索引范围扫描。
   同一索引也服务于「从未使用」口径（``success_times IS NULL OR success_times = 0``）。

Revision ID: a7c4e19b6d20
Revises: 3f7c1a92d5b8
Create Date: 2026-11-10 11:30:00.000000

"""

from typing import Sequence, Union

from alembic import op

revision: str = "a7c4e19b6d20"
down_revision: Union[str, None] = "3f7c1a92d5b8"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 158 万行：MySQL 8 加二级索引走 INPLACE + ONLINE，不阻塞读写
    op.create_index(
        "ix_proxy_tab_add_ts",
        "proxy_tab",
        ["add_ts"],
        unique=False,
    )
    op.create_index(
        "ix_proxy_tab_status_success_times",
        "proxy_tab",
        ["status", "success_times"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_proxy_tab_status_success_times", table_name="proxy_tab")
    op.drop_index("ix_proxy_tab_add_ts", table_name="proxy_tab")
