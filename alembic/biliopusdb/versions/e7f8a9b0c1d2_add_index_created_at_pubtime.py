"""add index on created_at and pubTime

为第三方抽奖列表接口（GetOthersLotDynList）补齐索引：
- t_lotdyninfo.idx_created_at / idx_pub_time：消除 ORDER BY 全表 filesort
- t_lot_extra_info.idx_lot_type_is_lot：加速 is_lot 计数（直接 COUNT t_lot_extra_info）

Revision ID: e7f8a9b0c1d2
Revises: 1dfdc101f656
Create Date: 2026-07-29 00:00:00.000000

"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql

revision: str = 'e7f8a9b0c1d2'
down_revision: Union[str, None] = '1dfdc101f656'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _index_exists(table: str, index: str) -> bool:
    """检查索引是否已存在（幂等保护）"""
    conn = op.get_bind()
    insp = sa.inspect(conn)
    return index in [i["name"] for i in insp.get_indexes(table)]


def upgrade() -> None:
    if not _index_exists('t_lotdyninfo', 'idx_created_at'):
        op.create_index('idx_created_at', 't_lotdyninfo', ['created_at'], unique=False)
    if not _index_exists('t_lotdyninfo', 'idx_pub_time'):
        op.create_index('idx_pub_time', 't_lotdyninfo', ['pubTime'], unique=False)
    if not _index_exists('t_lot_extra_info', 'idx_lot_type_is_lot'):
        op.create_index('idx_lot_type_is_lot', 't_lot_extra_info', ['lot_type', 'is_lot'], unique=False)


def downgrade() -> None:
    if _index_exists('t_lot_extra_info', 'idx_lot_type_is_lot'):
        op.drop_index('idx_lot_type_is_lot', table_name='t_lot_extra_info')
    if _index_exists('t_lotdyninfo', 'idx_pub_time'):
        op.drop_index('idx_pub_time', table_name='t_lotdyninfo')
    if _index_exists('t_lotdyninfo', 'idx_created_at'):
        op.drop_index('idx_created_at', table_name='t_lotdyninfo')
