"""add_crawl_failed_task

爬虫「至少一次」重试队列表：任务最终失败 / 进程重启 / 调度器杀旧开新时，
把「还没写入成功」的业务 id 持久化成 pending，下一轮 main() 优先补跑。
没有它时，任务队列只在内存里，代理池整体故障会直接造成永久数据缺口
（2026-11-16 dyn_detail 丢了 8 条 rid，而断点已经越过它们）。

Revision ID: b7f1c2d3e4a5
Revises: a3b8c7d1e2f4
Create Date: 2026-11-16 11:30:00.000000

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import mysql

revision: str = "b7f1c2d3e4a5"
down_revision: Union[str, None] = "a3b8c7d1e2f4"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "crawl_failed_task",
        sa.Column(
            "id",
            mysql.BIGINT(display_width=20),
            autoincrement=True,
            nullable=False,
            comment="自增主键",
        ),
        sa.Column(
            "task_type",
            mysql.VARCHAR(length=32),
            nullable=False,
            comment="任务类型（与爬虫的 retry_task_type 一致）",
        ),
        sa.Column(
            "business_id",
            mysql.BIGINT(display_width=20),
            nullable=False,
            comment="业务 id（rid / reserve_id / topic_id / business_id）",
        ),
        sa.Column(
            "status",
            mysql.ENUM("pending", "done", "dead"),
            server_default=sa.text("'pending'"),
            nullable=False,
            comment="pending=待重试 done=已写入 dead=人工确认（代码不会自动置 dead）",
        ),
        sa.Column(
            "attempts",
            mysql.INTEGER(display_width=11),
            server_default=sa.text("'0'"),
            nullable=False,
            comment="已重跑失败次数（退避用，不做上限）",
        ),
        sa.Column(
            "last_error",
            mysql.TEXT(),
            nullable=True,
            comment="最近一次失败原因",
        ),
        sa.Column(
            "next_run_at",
            mysql.BIGINT(display_width=20),
            server_default=sa.text("'0'"),
            nullable=False,
            comment="下次可重试时间（秒级时间戳）",
        ),
        sa.Column(
            "created_at",
            mysql.TIMESTAMP(),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=True,
            comment="创建时间",
        ),
        sa.Column(
            "updated_at",
            mysql.TIMESTAMP(),
            server_default=sa.text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"),
            nullable=True,
            comment="更新时间",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "task_type", "business_id", name="uq_crawl_failed_task_business"
        ),
        mysql_default_charset="utf8mb4",
        comment="爬虫至少一次重试队列",
    )
    op.create_index(
        "idx_crawl_failed_task_status_run",
        "crawl_failed_task",
        ["status", "next_run_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "idx_crawl_failed_task_status_run", table_name="crawl_failed_task"
    )
    op.drop_table("crawl_failed_task")
