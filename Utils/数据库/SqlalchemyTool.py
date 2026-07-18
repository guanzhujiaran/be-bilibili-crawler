from typing import Any, Dict

from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncEngine
from CONFIG import CONFIG


def sqlalchemy_model_2_dict(instance) -> dict:
    return {c.name: getattr(instance, c.name) for c in instance.__table__.columns}


# 进程内按 (dburl, is_crawler) 缓存 Engine 与会话工厂，避免每次实例化 SqlHelperBase
# 都新建一个独立连接池（pool_size=100）。否则多个 helper 叠加会各自占满一池连接，
# 极易把 MySQL 连接数打满，进而触发 1040(Too many connections) / 2013(连接丢失) 等问题。
_engine_cache: Dict[tuple[str, bool], AsyncEngine] = {}
_session_cache: Dict[tuple[str, bool], async_sessionmaker] = {}


def sqlalchemy_session_factory(dburl: str, is_crawler: bool = False) -> tuple[async_sessionmaker, AsyncEngine]:
    """
    创建（或复用已缓存的）SQLAlchemy 异步会话工厂

    同一进程内，相同 (dburl, is_crawler) 只创建一个 Engine 并复用其连接池，
    所有 SqlHelperBase 子类共享该连接池。

    Args:
        dburl (str): 数据库连接URL
        is_crawler (bool): 是否为爬虫专用连接池（使用较小的连接数）

    Returns:
        tuple[async_sessionmaker, AsyncEngine]: 配置好的会话工厂和引擎
    """
    config = CONFIG.crawler_sql_alchemy_config if is_crawler else CONFIG.sql_alchemy_config
    key = (dburl, is_crawler)
    engine = _engine_cache.get(key)
    if engine is None:
        engine = create_async_engine(dburl, **config.engine_config)
        _engine_cache[key] = engine
    session = _session_cache.get(key)
    if session is None:
        # 显式关键字参数，避免 pyright 对「位置参数 + **dict 展开」的重载匹配报错
        session = async_sessionmaker(
            bind=engine,
            autoflush=config.session_config.get("autoflush", False),
            expire_on_commit=config.session_config.get("expire_on_commit", False),
        )
        _session_cache[key] = session
    return session, engine
