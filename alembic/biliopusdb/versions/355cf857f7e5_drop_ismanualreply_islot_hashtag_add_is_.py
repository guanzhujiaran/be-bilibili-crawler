"""drop_isManualReply_isLot_hashTag_add_is_lot_required_topic_text

Revision ID: 355cf857f7e5
Revises: 2af34e6c601b
Create Date: 2026-07-26 11:21:38.157061

"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql

revision: str = '355cf857f7e5'
down_revision: Union[str, None] = '2af34e6c601b'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1. t_lot_extra_info 新增 is_lot / required_topic_text 列
    op.add_column('t_lot_extra_info', sa.Column('is_lot', mysql.TINYINT(display_width=1), nullable=False, comment='LLM判断是否为抽奖: 1-是, 0-否'))
    op.add_column('t_lot_extra_info', sa.Column('required_topic_text', sa.Text(), nullable=True, comment='转发/评论所需携带的话题文本，如 #抽奖#'))
    op.alter_column('t_lot_extra_info', 'predicted_at',
               existing_type=mysql.TIMESTAMP(),
               comment='LLM判断时间',
               existing_comment='SVM判断时间',
               existing_nullable=True,
               existing_server_default=sa.text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'))

    # 2. 把 t_lotdyninfo 原有数据迁移到 t_lot_extra_info（只插入不覆盖已有记录）
    op.execute("""
        INSERT IGNORE INTO t_lot_extra_info (ref_id, lot_type, is_lot, need_comment, required_topic_text)
        SELECT dynId, 'common',
               COALESCE(isLot, 0),
               COALESCE(isManualReply, 0),
               hashTag
        FROM t_lotdyninfo
    """)

    # 3. 删除 t_lotdyninfo 旧列及索引
    op.drop_index(op.f('idx_is_lot_created_at'), table_name='t_lotdyninfo')
    op.drop_index(op.f('idx_is_lot_pub_time'), table_name='t_lotdyninfo')
    op.drop_column('t_lotdyninfo', 'isManualReply')
    op.drop_column('t_lotdyninfo', 'hashTag')
    op.drop_column('t_lotdyninfo', 'isLot')
    # ### end Alembic commands ###


def downgrade() -> None:
    # 还原 t_lotdyninfo 列（数据不还原，因为 t_lot_extra_info 可能已被 MQ 消费者更新）
    op.add_column('t_lotdyninfo', sa.Column('isLot', mysql.TINYINT(display_width=1), autoincrement=False, nullable=True))
    op.add_column('t_lotdyninfo', sa.Column('hashTag', mysql.TEXT(), nullable=True))
    op.add_column('t_lotdyninfo', sa.Column('isManualReply', mysql.TINYINT(display_width=1), autoincrement=False, nullable=True, comment='是否需要人工评论: 1-是, 0-否'))
    op.create_index(op.f('idx_is_lot_pub_time'), 't_lotdyninfo', ['isLot', 'pubTime'], unique=False)
    op.create_index(op.f('idx_is_lot_created_at'), 't_lotdyninfo', ['isLot', 'created_at'], unique=False)
    op.alter_column('t_lot_extra_info', 'predicted_at',
               existing_type=mysql.TIMESTAMP(),
               comment='SVM判断时间',
               existing_comment='LLM判断时间',
               existing_nullable=True,
               existing_server_default=sa.text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'))
    op.drop_column('t_lot_extra_info', 'required_topic_text')
    op.drop_column('t_lot_extra_info', 'is_lot')
    # ### end Alembic commands ###
