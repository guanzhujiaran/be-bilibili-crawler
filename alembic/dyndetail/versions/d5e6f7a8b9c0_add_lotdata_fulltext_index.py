"""add_lotdata_fulltext_index

关键词搜索从 LIKE 升级为 MySQL FULLTEXT（ngram 解析器）：
- 支持中文子串/多关键词召回与 BM25 相关性排序；
- 与 Milvus 向量召回做 RRF 融合，兼顾关键词准确性与语义召回。

索引只建在 lotdata 的三档奖品文案列上，查询侧仍限定
``status=0 AND lottery_time > now`` 的进行中/未来抽奖。

Revision ID: d5e6f7a8b9c0
Revises: c4d9e8f7a6b2
Create Date: 2026-11-22 00:00:00.000000

"""

from typing import Sequence, Union

from alembic import op

revision: str = "d5e6f7a8b9c0"
down_revision: Union[str, None] = "c4d9e8f7a6b2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_index(
        "ft_lot_prize_cmt",
        "lotdata",
        ["first_prize_cmt", "second_prize_cmt", "third_prize_cmt"],
        unique=False,
        mysql_prefix="FULLTEXT",
        mysql_with_parser="ngram",
    )


def downgrade() -> None:
    op.drop_index("ft_lot_prize_cmt", table_name="lotdata")
