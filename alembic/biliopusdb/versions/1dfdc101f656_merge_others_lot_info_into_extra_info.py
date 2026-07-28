"""merge_others_lot_info_into_extra_info

Revision ID: 1dfdc101f656
Revises: 355cf857f7e5
Create Date: 2026-07-28 00:02:42.648652

"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql

revision: str = '1dfdc101f656'
down_revision: Union[str, None] = '355cf857f7e5'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _column_exists(table: str, column: str) -> bool:
    """检查列是否已存在（幂等保护）"""
    conn = op.get_bind()
    insp = sa.inspect(conn)
    cols = [c["name"] for c in insp.get_columns(table)]
    return column in cols


def _table_exists(table: str) -> bool:
    """检查表是否已存在（幂等保护）"""
    conn = op.get_bind()
    insp = sa.inspect(conn)
    return table in insp.get_table_names()


def upgrade() -> None:
    # 1. t_lot_extra_info 新增 prize_names / lottery_time 列（幂等：已存在则跳过）
    if not _column_exists('t_lot_extra_info', 'prize_names'):
        op.add_column('t_lot_extra_info', sa.Column('prize_names', sa.JSON(), nullable=True, comment='LLM 提取的奖品名称列表'))
    if not _column_exists('t_lot_extra_info', 'lottery_time'):
        op.add_column('t_lot_extra_info', sa.Column('lottery_time', sa.Text(), nullable=True, comment='LLM 提取的开奖时间字符串'))

    # 2. 把 t_others_lot_info 已有数据迁移到 t_lot_extra_info（幂等：旧表不存在则跳过数据迁移）
    if _table_exists('t_others_lot_info'):
        op.execute(
            """
            INSERT INTO t_lot_extra_info (ref_id, lot_type, prize_names, lottery_time, created_at,
                                          is_grand_prize, is_lot, need_comment, need_repost)
            SELECT dynId, 'common', prize_names, lottery_time, created_at,
                   0, 0, 0, 0
            FROM t_others_lot_info o
            ON DUPLICATE KEY UPDATE
                prize_names = IFNULL(t_lot_extra_info.prize_names, o.prize_names),
                lottery_time = IFNULL(t_lot_extra_info.lottery_time, o.lottery_time)
            """
        )
        # 3. 删除旧表
        op.drop_table('t_others_lot_info')


def downgrade() -> None:
    # 1. 重新创建 t_others_lot_info（幂等：已存在则跳过）
    if not _table_exists('t_others_lot_info'):
        op.create_table('t_others_lot_info',
        sa.Column('dynId', mysql.BIGINT(), server_default=sa.text('(0)'), autoincrement=False, nullable=False),
        sa.Column('prize_names', mysql.JSON(), nullable=True, comment='LLM 提取的奖品名称列表'),
        sa.Column('lottery_time', mysql.TEXT(), nullable=True, comment='LLM 提取的开奖时间字符串'),
        sa.Column('created_at', mysql.TIMESTAMP(), server_default=sa.text('CURRENT_TIMESTAMP'), nullable=True),
        sa.PrimaryKeyConstraint('dynId'),
        mysql_collate='utf8mb4_0900_ai_ci',
        mysql_default_charset='utf8mb4',
        mysql_engine='InnoDB'
        )

        # 2. 把 t_lot_extra_info 中 lot_type='common' 的奖品信息迁回旧表
        op.execute(
            """
            INSERT INTO t_others_lot_info (dynId, prize_names, lottery_time, created_at)
            SELECT ref_id, prize_names, lottery_time, created_at
            FROM t_lot_extra_info
            WHERE lot_type = 'common'
            ON DUPLICATE KEY UPDATE
                prize_names = t_others_lot_info.prize_names,
                lottery_time = t_others_lot_info.lottery_time
            """
        )

    # 3. 删除新增列（幂等：不存在则跳过）
    if _column_exists('t_lot_extra_info', 'lottery_time'):
        op.drop_column('t_lot_extra_info', 'lottery_time')
    if _column_exists('t_lot_extra_info', 'prize_names'):
        op.drop_column('t_lot_extra_info', 'prize_names')
