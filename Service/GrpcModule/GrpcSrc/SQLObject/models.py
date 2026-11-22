from typing import List, Optional

from sqlalchemy import (
    BigInteger,
    Column,
    Computed,
    ForeignKeyConstraint,
    Index,
    Integer,
    String,
    TIMESTAMP,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.mysql import ENUM, LONGTEXT, TEXT, TINYINT, VARCHAR
from sqlalchemy.orm import Mapped, declarative_base, mapped_column, relationship
from sqlalchemy.orm.base import Mapped

Base = declarative_base()


class BiliUserInfo(Base):
    __tablename__ = "bili_user_info"
    __table_args__ = (Index("name", "name", "uid"),)

    uid = mapped_column(BigInteger, primary_key=True)
    name = mapped_column(VARCHAR(50))
    face = mapped_column(TEXT)

    bili_atari_info: Mapped[List["BiliAtariInfo"]] = relationship(
        "BiliAtariInfo", uselist=True, back_populates="bili_user_info"
    )


class Lotdata(Base):
    __tablename__ = "lotdata"
    __table_args__ = (
        Index("UQ_business_id", "business_id", unique=True),
        Index(
            "idx_lottery_id",
            "lottery_id",
            "business_id",
            "lottery_time",
            "sender_uid",
            "business_type",
            "status",
        ),
        Index("lottery_time", "lottery_time"),
        Index("sender_uid", "sender_uid"),
        # 排序字段复合索引（业务类型在前，避免 filesort）
        Index("idx_bt_lottery_time", "business_type", "lottery_time"),
        Index("idx_bt_participants", "business_type", "participants"),
        Index("idx_bt_first_prize", "business_type", "first_prize"),
        Index("idx_bt_created_at", "business_type", "created_at"),
        Index("idx_bt_status_lottery_time", "business_type", "status", "lottery_time"),
        # 关键词搜索：中文 ngram 全文索引（迁移 d5e6f7a8b9c0 同步创建）
        Index(
            "ft_lot_prize_cmt",
            "first_prize_cmt",
            "second_prize_cmt",
            "third_prize_cmt",
            mysql_prefix="FULLTEXT",
            mysql_with_parser="ngram",
        ),
    )

    lottery_id = mapped_column(BigInteger, primary_key=True)
    business_id = mapped_column(BigInteger)
    status = mapped_column(BigInteger)
    lottery_time = mapped_column(BigInteger)
    lottery_at_num = mapped_column(BigInteger)
    lottery_feed_limit = mapped_column(BigInteger)
    first_prize = mapped_column(BigInteger)
    second_prize = mapped_column(BigInteger)
    third_prize = mapped_column(BigInteger)
    lottery_result = mapped_column(TEXT)
    first_prize_cmt = mapped_column(TEXT)
    second_prize_cmt = mapped_column(TEXT)
    third_prize_cmt = mapped_column(TEXT)
    first_prize_pic = mapped_column(TEXT)
    second_prize_pic = mapped_column(TEXT)
    third_prize_pic = mapped_column(TEXT)
    need_post = mapped_column(BigInteger)
    business_type = mapped_column(BigInteger)
    sender_uid = mapped_column(BigInteger)
    prize_type_first = mapped_column(TEXT)
    prize_type_second = mapped_column(TEXT)
    prize_type_third = mapped_column(TEXT)
    pay_status = mapped_column(BigInteger)
    ts = mapped_column(BigInteger)
    _gt_ = mapped_column(BigInteger)
    has_charge_right = mapped_column(TEXT)
    lottery_detail_url = mapped_column(TEXT)
    participants = mapped_column(BigInteger)
    participated = mapped_column(TEXT)
    vip_batch_sign = mapped_column(TEXT)
    exclusive_level = mapped_column(TEXT)
    followed = mapped_column(BigInteger)
    reposted = mapped_column(BigInteger)
    custom_extra_key = mapped_column(TEXT)
    created_at = mapped_column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"))
    updated_at = mapped_column(
        TIMESTAMP, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP")
    )

    article_pub_record: Mapped[List["ArticlePubRecord"]] = relationship(
        "ArticlePubRecord", uselist=True, back_populates="lot_data_business"
    )
    bili_atari_info: Mapped[List["BiliAtariInfo"]] = relationship(
        "BiliAtariInfo", uselist=True, back_populates="atari_lot"
    )
    bilidyndetail: Mapped[List["Bilidyndetail"]] = relationship(
        "Bilidyndetail", uselist=True, back_populates="lot"
    )
    extra_info: Mapped[Optional["LotExtraInfo"]] = relationship(
        "LotExtraInfo", uselist=False, back_populates="lotdata"
    )


class LotExtraInfo(Base):
    """抽奖附加信息表 — 存储大奖判断结果，独立于Lotdata。

    官方抽奖的抽奖方式（是否需转发/评论、是否话题抽奖等）已由 lotdata.business_type 固定，
    无需落库，因此 is_lot / need_comment / need_repost 不再作为存储列。
    仅 is_grand_prize 由大模型判断并落库；is_lot / need_comment / need_repost 由 OfficialLotExtraInfoResp
    在响应中根据 lottery_type 以 computed_field property 实时计算。
    """

    __tablename__ = "t_lot_extra_info"
    __table_args__ = (
        ForeignKeyConstraint(
            ["lottery_id"], ["lotdata.lottery_id"], name="FK_lot_extra_info_lotdata"
        ),
        Index("idx_lottery_id", "lottery_id", unique=True),
    )

    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    lottery_id = mapped_column(
        BigInteger, nullable=False, comment="关联lotdata.lottery_id"
    )
    is_grand_prize = mapped_column(
        TINYINT(1),
        nullable=False,
        server_default=text("0"),
        comment="是否大奖: 1-是, 0-否",
    )
    judge_version = mapped_column(
        Integer,
        nullable=False,
        server_default=text("0"),
        comment="大奖判定规则/输入版本号，0=历史未版本化",
    )
    input_hash = mapped_column(
        String(32),
        comment="判定输入文本 MD5，用于输入变化时自动重判",
    )
    judge_reason = mapped_column(
        TEXT,
        comment="大奖判定理由（LLM 输出，便于回溯）",
    )
    created_at = mapped_column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"))
    updated_at = mapped_column(
        TIMESTAMP, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP")
    )

    lotdata: Mapped["Lotdata"] = relationship("Lotdata", back_populates="extra_info")


class ArticlePubRecord(Base):
    __tablename__ = "article_pub_record"
    __table_args__ = (
        ForeignKeyConstraint(
            ["lot_data_business_id"],
            ["lotdata.business_id"],
            name="FK_article_pub_record_lotdata",
        ),
        Index("lot_data_business_id", "lot_data_business_id", unique=True),
        {"comment": "发布专栏记录"},
    )

    lot_data_business_id = mapped_column(BigInteger, nullable=False)
    pk = mapped_column(BigInteger, primary_key=True)
    round_id = mapped_column(Integer, comment="每一轮的号码")

    lot_data_business: Mapped["Lotdata"] = relationship(
        "Lotdata", back_populates="article_pub_record"
    )


class BiliAtariInfo(Base):
    __tablename__ = "bili_atari_info"
    __table_args__ = (
        ForeignKeyConstraint(
            ["atari_lot_id"], ["lotdata.lottery_id"], name="FK__lotdata_1"
        ),
        ForeignKeyConstraint(
            ["mid"], ["bili_user_info.uid"], name="FK_bili_atari_info_bili_user_info"
        ),
        Index("FK__lotdata_1", "atari_lot_id"),
        Index("atari_lot_rank", "atari_lot_rank"),
        Index("atari_lot_type", "atari_lot_type"),
        Index("atari_timestamp", "atari_timestamp"),
        Index("mid", "mid", "atari_lot_id", unique=True),
    )

    pk = mapped_column(BigInteger, primary_key=True)
    mid = mapped_column(BigInteger)
    hongbao_money = mapped_column(Integer)
    atari_lot_id = mapped_column(BigInteger)
    atari_lot_rank = mapped_column(
        TINYINT, comment="1：一等奖\r\n2：二等奖\r\n3：三等奖"
    )
    atari_lot_type = mapped_column(TINYINT, comment="中奖类型，对应B站business_id")
    atari_timestamp = mapped_column(TIMESTAMP)

    atari_lot: Mapped[Optional["Lotdata"]] = relationship(
        "Lotdata", back_populates="bili_atari_info"
    )
    bili_user_info: Mapped[Optional["BiliUserInfo"]] = relationship(
        "BiliUserInfo", back_populates="bili_atari_info"
    )


class Bilidyndetail(Base):
    __tablename__ = "bilidyndetail"
    __table_args__ = (
        ForeignKeyConstraint(
            ["lot_id"], ["lotdata.lottery_id"], name="biliDynDetail_FK_0_0"
        ),
        Index(
            "biliDynDetail_FK_0_0",
            "lot_id",
            "rid",
            "dynamic_id",
            "dynamic_created_time",
            "rid_int",
            "dynamic_id_int",
        ),
        Index("dynamic_id", "dynamic_id"),
        Index("dynamic_id_int", "dynamic_id_int"),
        Index("lot_id", "lot_id"),
        Index("rid", "rid"),
        Index("rid_int", "rid_int"),
    )

    rid = mapped_column(VARCHAR(255), primary_key=True, server_default=text("''"))
    dynamic_id = mapped_column(VARCHAR(50))
    dynData = mapped_column(LONGTEXT)
    lot_id = mapped_column(BigInteger)
    dynamic_created_time = mapped_column(VARCHAR(50))
    rid_int = mapped_column(
        BigInteger, Computed("(cast(`rid` as signed))", persisted=True)
    )
    dynamic_id_int = mapped_column(
        BigInteger, Computed("(cast(`dynamic_id` as signed))", persisted=True)
    )

    lot: Mapped[Optional["Lotdata"]] = relationship(
        "Lotdata", back_populates="bilidyndetail"
    )


class CrawlFailedTask(Base):
    """爬虫「至少一次」失败任务表（业务 id 级别，跨进程 / 跨轮次持久化）。

    为什么需要它：任务队列在内存里，进程重启、调度器「杀旧开新」、代理池故障、
    重入队次数到上限，任何一条都会让「还没写入成功」的业务 id 直接消失；而断点游标
    （``lotdata`` 水位 / ``bilidyndetail`` 最大 rid / Redis 里的 reserve 游标）只记
    「最后成功的 id」，失败 id 会被后续成功越过，永久缺口。

    口径：
    - ``status=pending``：该 id 还没成功写入，必须重试（**永不因为 attempts 超限置 dead**）；
    - ``status=done``：已成功写入（幂等 upsert 成功），可以安全推进游标；
    - ``status=dead``：只保留给人工确认，代码里不会自动写入；
    - ``task_type`` 与各爬虫声明的 ``retry_task_type`` 一致（见 ``Service/BaseCrawler/retry_queue.py``），
      游标推进时用它按爬虫过滤。

    放在 dyndetail 库：它与 ``bilidyndetail`` / ``lotdata`` 同库，游标判定与补数脚本
    都不需要跨库事务；表本身不带业务外键，任何爬虫都可以写自己的 task_type。
    """

    __tablename__ = "crawl_failed_task"
    __table_args__ = (
        UniqueConstraint("task_type", "business_id", name="uq_crawl_failed_task_business"),
        Index("idx_crawl_failed_task_status_run", "status", "next_run_at"),
    )

    id = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    task_type = mapped_column(VARCHAR(32), nullable=False)
    business_id = mapped_column(BigInteger, nullable=False)
    status = mapped_column(
        ENUM("pending", "done", "dead"),
        nullable=False,
        server_default=text("'pending'"),
    )
    attempts = mapped_column(Integer, nullable=False, server_default=text("'0'"))
    last_error = mapped_column(TEXT)
    next_run_at = mapped_column(BigInteger, nullable=False, server_default=text("'0'"))
    created_at = mapped_column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"))
    updated_at = mapped_column(
        TIMESTAMP, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP")
    )
