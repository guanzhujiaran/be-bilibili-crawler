from sqlalchemy import (
    Computed,
    Index,
    Integer,
    JSON,
    String,
    text,
)
from sqlalchemy.orm import declarative_base, mapped_column

Base = declarative_base()


class ProxyTab(Base):
    __tablename__ = "proxy_tab"
    __table_args__ = (
        Index("computed_proxy_str", "computed_proxy_str"),
        Index("proxy_id", "proxy_id", unique=True),
        Index("刷新代理索引", "status", "score", "success_times", "update_ts"),
        Index("获取可用代理索引", "status", "score", "update_ts"),
        # 代理池统计（GetProxyStatus）专用：
        # add_ts 单列索引 → get_latest_add_ts 的 ORDER BY add_ts DESC LIMIT 1；
        # (status, success_times) 复合索引 → 「可用 / 从未使用」两个计数的范围扫描
        Index("ix_proxy_tab_add_ts", "add_ts"),
        Index("ix_proxy_tab_status_success_times", "status", "success_times"),
        Index(
            "覆盖索引",
            "proxy_id",
            "status",
            "update_ts",
            "score",
            "add_ts",
            "success_times",
            "zhihu_status",
        ),
    )

    proxy_id = mapped_column(Integer, primary_key=True)
    proxy = mapped_column(JSON, nullable=False)
    status = mapped_column(Integer, nullable=False, server_default=text("'0'"))
    update_ts = mapped_column(Integer, nullable=False)
    score = mapped_column(Integer, nullable=False)
    add_ts = mapped_column(Integer)
    success_times = mapped_column(Integer, server_default=text("'0'"))
    zhihu_status = mapped_column(Integer, server_default=text("'0'"))
    computed_proxy_str = mapped_column(
        String(255),
        Computed(
            "(json_unquote(json_extract(`proxy`,concat(_utf8mb4'$.',json_unquote(json_extract(json_keys(`proxy`),_utf8mb4'$[0]'))))))",
            persisted=True,
        ),
    )

    def __str__(self):
        return f"ProxyTab(proxy_id={self.proxy_id}, proxy={self.proxy}, status={self.status}, update_ts={self.update_ts}, score={self.score}, add_ts={self.add_ts}, success_times={self.success_times}, zhihu_status={self.zhihu_status})"
