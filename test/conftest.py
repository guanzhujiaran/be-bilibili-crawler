# -*- coding: utf-8 -*-
"""
pytest 全局配置与共享 fixtures

运行方式:
    cd /home/minato_aqua/BilibiliExplosion/FastapiApp
    uv run --python 3.13 pytest test/ -v

Mock 策略:
    - 使用真实的 FastAPI app（所有路由均注册）
    - 在测试函数内部按需 mock 外部服务调用
    - fastapi_cdn_host.patch_docs 在测试环境跳过
"""

import os
import sys
from pathlib import Path
from unittest import mock

import pytest
from httpx import ASGITransport, AsyncClient

# 将 FastapiApp 目录添加到 Python 路径
_fastapi_app_path = Path(__file__).resolve().parent.parent
if str(_fastapi_app_path) not in sys.path:
    sys.path.insert(0, str(_fastapi_app_path))

# 将 GrpcProto 目录添加到 Python 路径（protobuf 生成的 bilibili 模块需要）
_grpc_proto_path = str(
    _fastapi_app_path / "Service" / "GrpcModule" / "Grpc" / "GrpcProto"
)
if _grpc_proto_path not in sys.path:
    sys.path.insert(0, _grpc_proto_path)


def pytest_configure(config):
    """pytest 启动时设置环境变量"""
    os.environ.setdefault("MYSQL_HOST", "localhost")
    os.environ.setdefault("MYSQL_PORT", "3306")
    os.environ.setdefault("MYSQL_USER", "test")
    os.environ.setdefault("MYSQL_PASSWORD", "test")
    os.environ.setdefault("REDIS_HOST", "localhost")
    os.environ.setdefault("REDIS_PORT", "6379")
    os.environ.setdefault("REDIS_PWD", "")
    os.environ.setdefault("RABBITMQ_HOST", "localhost")
    os.environ.setdefault("RABBITMQ_PORT", "5672")
    os.environ.setdefault("RABBITMQ_USER", "guest")
    os.environ.setdefault("RABBITMQ_PASSWORD", "guest")
    # 推送渠道统一使用 JSON 形式的 MESSAGE_CONFIG（与 docker-compose 共用）
    os.environ.setdefault(
        "MESSAGE_CONFIG",
        '{"pushme_key":"test_token","push_plus_token":"test_token"}',
    )
    os.environ.setdefault("UNIDBG_HOST", "localhost")
    os.environ.setdefault("UNIDBG_PORT", "8080")
    os.environ.setdefault("V2RAY_HOST", "localhost")
    os.environ.setdefault("V2RAY_PORT", "1080")
    os.environ.setdefault("LLAMA_HOST", "localhost")
    os.environ.setdefault("LLAMA_PORT", "8080")
    os.environ.setdefault("PROXY_SERVER", "")
    os.environ.setdefault("MILVUS_HOST", "localhost")
    os.environ.setdefault("MILVUS_PORT", "19530")


# ============================================================================
# FastAPI App fixture
# ============================================================================


@pytest.fixture(scope="session")
def app():
    """
    创建 FastAPI app，跳过 lifespan 和 CDN host patch。
    所有路由均正常注册，外部服务依赖由具体测试 mock。
    """
    from fastapi import FastAPI

    app = FastAPI(title="Test App")
    app.debug = True

    # 注册所有路由（跳过需要 broker 连接的 faststream MQ 路由）
    from controller.v1.lotttery_database.bili import LotteryData
    from controller.v1.lotttery_database.bili.lottery_statistic import LotteryStatistic
    from controller.v1.ip_info import get_ip_info as ip_info_module
    from controller.v1.background_service import BackgroundServiceController
    from controller.common import CommonRouter
    from controller.v1.mq.rpc_info_controller import router as RpcInfoRouter
    from controller.v1.samsClub import samsClubController
    from controller.v1.captcha import captchaController
    from controller.v1.lotttery_database.bili.zhuanlan import zhuanlanController

    app.include_router(LotteryData.router)
    app.include_router(LotteryStatistic.router)
    app.include_router(ip_info_module.router)
    app.include_router(BackgroundServiceController.router)
    app.include_router(CommonRouter.router)
    app.include_router(RpcInfoRouter)
    app.include_router(captchaController.router)
    app.include_router(samsClubController.router)
    app.include_router(zhuanlanController.router)

    yield app


@pytest.fixture(scope="session")
def anyio_backend():
    return "asyncio"


# ============================================================================
# 通用辅助 fixtures
# ============================================================================


@pytest.fixture
def base_pagination():
    """基本分页参数"""
    return {"page_num": 1, "page_size": 10}


@pytest.fixture(autouse=True)
def _stub_crawl_failed_task_dao(monkeypatch):
    """重试队列（``crawl_failed_task``）在单测里默认是内存 no-op。

    爬虫基类在 ``retry_task_type`` 非空时会往 ``dyndetail.crawl_failed_task`` 记账，
    单测不该为它起库；占位配置下真实连接还要等超时，会让用例变慢。
    需要验证记账行为的用例自行 monkeypatch 覆盖（见 ``test_backfill_missing.py``）。
    """
    from Service.BaseCrawler.retry_queue import RetryTaskStats, crawl_failed_task_dao

    async def _noop(*_args: object, **_kwargs: object) -> None:
        return None

    async def _noop_false(*_args: object, **_kwargs: object) -> bool:
        return False

    async def _noop_empty_list(*_args: object, **_kwargs: object) -> list:
        return []

    async def _noop_none(*_args: object, **_kwargs: object) -> None:
        return None

    async def _noop_stats(*_args: object, **_kwargs: object) -> RetryTaskStats:
        return RetryTaskStats()

    monkeypatch.setattr(crawl_failed_task_dao, "mark_pending", _noop, raising=False)
    monkeypatch.setattr(
        crawl_failed_task_dao, "record_retry_failure", _noop, raising=False
    )
    monkeypatch.setattr(
        crawl_failed_task_dao, "mark_done", _noop_false, raising=False
    )
    monkeypatch.setattr(
        crawl_failed_task_dao, "fetch_pending", _noop_empty_list, raising=False
    )
    monkeypatch.setattr(
        crawl_failed_task_dao, "min_pending_business_id", _noop_none, raising=False
    )
    monkeypatch.setattr(crawl_failed_task_dao, "get_stats", _noop_stats, raising=False)
    yield
