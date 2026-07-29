from typing import List, Optional

from sqlalchemy import BigInteger, Column, DateTime, ForeignKeyConstraint, Index, Integer, JSON, String, TIMESTAMP, Text, text
from sqlalchemy.dialects.mysql import TINYINT
from sqlalchemy.orm import Mapped, declarative_base, mapped_column, relationship
from sqlalchemy.orm.base import Mapped

Base = declarative_base()


class TLotmaininfo(Base):
    __tablename__ = 't_lotmaininfo'
    __table_args__ = (
        Index('lotRound_id', 'lotRound_id', unique=True),
    )

    id = mapped_column(Integer, primary_key=True)
    lotRound_id = mapped_column(Integer)
    allNum = mapped_column(Integer, comment='需要去检查的抽奖动态数量')
    lotNum = mapped_column(Integer, comment='检查完成之后的总共的抽奖数量')
    uselessNum = mapped_column(Integer)
    isRoundFinished = mapped_column(TINYINT(1))
    created_at = mapped_column(TIMESTAMP, server_default=text('(now())'))
    updated_at = mapped_column(TIMESTAMP, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'))

    t_lotdyninfo: Mapped[List['TLotdyninfo']] = relationship('TLotdyninfo', uselist=True, back_populates='dynLotRound')
    t_lotuserspaceresp: Mapped[List['TLotuserspaceresp']] = relationship('TLotuserspaceresp', uselist=True, back_populates='dynLotRound')


class TLotuserinfo(Base):
    __tablename__ = 't_lotuserinfo'

    uid = mapped_column(BigInteger, primary_key=True)
    uname = mapped_column(Text)
    updateNum = mapped_column(Integer)
    updatetime = mapped_column(DateTime)
    isUserSpaceFinished = mapped_column(Integer)
    offset = mapped_column(BigInteger, comment='保存每一次循环之后的offset，如果中途推出了，从这个offset接着获取')
    latestFinishedOffset = mapped_column(BigInteger, comment='最后一次获取结束时候的offset，作为判断是否获取重复的标准')
    isPubLotUser = mapped_column(TINYINT(1), comment='0：要获取的抽奖用户的空间数据（判断这个抽奖号是否活跃的重要标志）\r\n1：发布抽奖用户的空间数据（不重要）')
    created_at = mapped_column(TIMESTAMP, server_default=text('CURRENT_TIMESTAMP'))
    updated_at = mapped_column(TIMESTAMP, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'))

    t_lotuserspaceresp: Mapped[List['TLotuserspaceresp']] = relationship('TLotuserspaceresp', uselist=True, back_populates='t_lotuserinfo')


class TRiddynid(Base):
    __tablename__ = 't_riddynid'

    dynamic_id = mapped_column(BigInteger, primary_key=True)
    rid = mapped_column(BigInteger)
    dynamic_type = mapped_column(TINYINT)


class TLotdyninfo(Base):
    __tablename__ = 't_lotdyninfo'
    __table_args__ = (
        ForeignKeyConstraint(['dynLotRound_id'], ['t_lotmaininfo.lotRound_id'], name='t_lotdyninfo_ibfk_1'),
        Index('dynLotRound_id', 'dynLotRound_id'),
        Index('idx_created_at', 'created_at'),
        Index('idx_pub_time', 'pubTime'),
    )

    dynId = mapped_column(BigInteger, primary_key=True, server_default=text('(0)'))
    dynamicUrl = mapped_column(Text)
    authorName = mapped_column(Text)
    up_uid = mapped_column(BigInteger)
    pubTime = mapped_column(DateTime)
    dynContent = mapped_column(Text)
    commentCount = mapped_column(Integer)
    repostCount = mapped_column(Integer)
    likeCount = mapped_column(Integer)
    officialLotType = mapped_column(Text)
    officialLotId = mapped_column(Text)
    isOfficialAccount = mapped_column(TINYINT(1))
    dynLotRound_id = mapped_column(Integer)
    rawJsonStr = mapped_column(JSON)
    created_at = mapped_column(TIMESTAMP, server_default=text('CURRENT_TIMESTAMP'))
    updated_at = mapped_column(TIMESTAMP, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'))

    dynLotRound: Mapped[Optional['TLotmaininfo']] = relationship('TLotmaininfo', back_populates='t_lotdyninfo')


class TLotuserspaceresp(Base):
    __tablename__ = 't_lotuserspaceresp'
    __table_args__ = (
        ForeignKeyConstraint(['dynLotRound_id'], ['t_lotmaininfo.lotRound_id'], name='FK_t_lotuserspaceresp_t_lotmaininfo'),
        ForeignKeyConstraint(['spaceUid'], ['t_lotuserinfo.uid'], name='t_lotuserspaceresp_ibfk_1'),
        Index('FK_t_lotuserspaceresp_t_lotmaininfo', 'dynLotRound_id'),
        Index('spaceUid', 'spaceUid')
    )

    spaceOffset = mapped_column(BigInteger, primary_key=True, server_default=text('(0)'))
    spaceUid = mapped_column(BigInteger)
    spaceRespJson = mapped_column(JSON)
    dynLotRound_id = mapped_column(Integer)

    dynLotRound: Mapped[Optional['TLotmaininfo']] = relationship('TLotmaininfo', back_populates='t_lotuserspaceresp')
    t_lotuserinfo: Mapped[Optional['TLotuserinfo']] = relationship('TLotuserinfo', back_populates='t_lotuserspaceresp')


class TLotExtraInfo(Base):
    """抽奖附加信息表（biliopusdb） — 存储 LLM 提取的全部额外信息，独立于原有表。

    与 dyndetail 的 t_lot_extra_info（对应 GrpcModule/models.LotExtraInfo）不同：
    本表服务于普通/第三方抽奖，抽奖方式（is_lot / need_comment / need_repost）需由 LLM
    动态提取并落库；而 dyndetail 的官方抽奖抽奖方式已由 lotdata.business_type 固定，
    无需落库，因此 dyndetail 已移除这三列，仅依赖响应中的 computed_field property 实时计算。

    原先分散在 t_others_lot_info（奖品名/开奖时间）的信息，已统一合并到本表，
    通过 (ref_id, lot_type) 唯一约束关联原始记录（普通抽奖 ref_id=dynId, lot_type='common'）。
    """
    __tablename__ = 't_lot_extra_info'
    __table_args__ = (
        Index('idx_ref_id_type', 'ref_id', 'lot_type', unique=True),
        Index('idx_lot_type_is_lot', 'lot_type', 'is_lot'),
    )

    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    ref_id = mapped_column(BigInteger, nullable=False, comment='关联原始记录的ID (dynId/business_id/sid)')
    lot_type = mapped_column(String(32), nullable=False, comment='抽奖类型: common/reserve/official/charge')
    is_lot = mapped_column(TINYINT(1), nullable=False, default=0, comment='LLM判断是否为抽奖: 1-是, 0-否')
    is_grand_prize = mapped_column(TINYINT(1), nullable=False, default=0, comment='是否大奖: 1-是, 0-否')
    need_comment = mapped_column(TINYINT(1), nullable=False, default=0, comment='是否需要评论: 1-是, 0-否')
    need_repost = mapped_column(TINYINT(1), nullable=False, default=0, comment='是否需要转发: 1-是, 0-否')
    required_topic_text = mapped_column(Text, comment='转发/评论所需携带的话题文本，如 #抽奖#')
    prize_names = mapped_column(JSON, nullable=True, comment='LLM 提取的奖品名称列表')
    lottery_time = mapped_column(Text, nullable=True, comment='LLM 提取的开奖时间字符串')
    predicted_at = mapped_column(TIMESTAMP, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'), comment='LLM判断时间')
    created_at = mapped_column(TIMESTAMP, server_default=text('CURRENT_TIMESTAMP'))
