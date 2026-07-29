"""add index on t_lot_extra_info (lot_type, is_lot)

加速 GetOthersLotDynList 的 is_lot 计数：无时间筛选时直接 COUNT t_lot_extra_info。

Revision ID: f1a2b3c4d5e6
Revises: e7f8a9b0c1d2
Create Date: 2026-07-29 00:30:00.000000

"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa

revision: str = 'f1a2b3c4d5e6'
down_revision: Union[str, None] = 'e7f8a9b0c1d2'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _index_exists(table: str, index: str) -> bool:
    """检查索引是否已存在（幂等保护）"""
    conn = op.get_bind()
    insp = sa.inspect(conn)
    return index in [i["name"] for i in insp.get_indexes(table)]


def upgrade() -> None:
    if not _index_exists('t_lot_extra_info', 'idx_lot_type_is_lot'):
        op.create_index('idx_lot_type_is_lot', 't_lot_extra_info', ['lot_type', 'is_lot'], unique=False)


def downgrade() -> None:
    if _index_exists('t_lot_extra_info', 'idx_lot_type_is_lot'):
        op.drop_index('idx_lot_type_is_lot', table_name='t_lot_extra_info')
