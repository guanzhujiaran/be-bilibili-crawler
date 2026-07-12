# -*- coding: utf-8 -*-
"""
后台服务接口测试

测试端点 (前缀: /api/v1/background_service):
    GET  /GetSingleScrapyStatus?scrapy_name=dyn   查询单个爬虫状态（按参数）
    GET  /GetAllLotScrapyStatus       /GetProxyStatus
    GET  /GlobalSchedule/GetJobs
    GET  /GlobalScheduler/Status       /BackgroundService/AllStat
    POST /BackgroundService/Start      /BackgroundService/Stop
    POST /BackgroundService/Restart
"""

import pytest
from unittest import mock
from httpx import ASGITransport, AsyncClient

PREFIX = "/api/v1/background_service"


@pytest.fixture
async def client(app):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


# ============================================================================
# Scrapy Status GET 端点
# ============================================================================


class TestScrapyStatus:

    @pytest.mark.asyncio
    async def test_single_scrapy_status_valid(self, client):
        """遍历所有合法爬虫类型，确保单爬虫状态接口均可用"""
        valid_types = [
            "dyn",
            "topic",
            "reserve",
            "other_space",
            "other_dyn",
            "refresh_bili_official",
            "refresh_bili_reserve",
        ]
        for scrapy_name in valid_types:
            response = await client.get(
                f"{PREFIX}/GetSingleScrapyStatus",
                params={"scrapy_name": scrapy_name},
            )
            assert response.status_code == 200, f"{scrapy_name} 应返回 200"

    @pytest.mark.asyncio
    async def test_single_scrapy_status_invalid(self, client):
        """传入非法爬虫类型应被枚举校验拦截（422）"""
        response = await client.get(
            f"{PREFIX}/GetSingleScrapyStatus",
            params={"scrapy_name": "not_exist"},
        )
        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_single_scrapy_status_missing(self, client):
        """缺少 scrapy_name 参数时应返回 422"""
        response = await client.get(f"{PREFIX}/GetSingleScrapyStatus")
        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_all_scrapy_status(self, client):
        response = await client.get(f"{PREFIX}/GetAllLotScrapyStatus")
        assert response.status_code == 200
        data = response.json()["data"]
        assert "dyn_scrapy_status" in data
        assert "topic_scrapy_status" in data
        assert "reserve_scrapy_status" in data

    @pytest.mark.asyncio
    async def test_proxy_status(self, client):
        with mock.patch(
            "controller.v1.background_service.BackgroundServiceController.SQLHelper"
        ) as mock_sql:
            mock_sql.get_proxy_database_redis = mock.AsyncMock(return_value=None)
            response = await client.get(f"{PREFIX}/GetProxyStatus")
        assert response.status_code == 200


# ============================================================================
# Scheduler 端点
# ============================================================================


class TestScheduler:

    @pytest.mark.asyncio
    async def test_global_jobs(self, client):
        response = await client.get(f"{PREFIX}/GlobalSchedule/GetJobs")
        assert response.status_code == 200

    @pytest.mark.asyncio
    async def test_global_scheduler_status(self, client):
        with mock.patch(
            "controller.v1.background_service.BackgroundServiceController.GLOBAL_SCHEDULER"
        ) as mock_sch:
            mock_sch.running = True
            mock_sch.timezone = "Asia/Shanghai"
            mock_sch.get_jobs.return_value = []
            mock_sch._executors = {}
            response = await client.get(f"{PREFIX}/GlobalScheduler/Status")
        assert response.status_code == 200


# ============================================================================
# BackgroundService/AllStat
# ============================================================================


class TestAllStat:

    @pytest.mark.asyncio
    async def test_all_stat(self, client):
        with mock.patch(
            "controller.v1.background_service.BackgroundServiceController.background_service"
        ) as mock_bg:
            response = await client.get(f"{PREFIX}/BackgroundService/AllStat")
        assert response.status_code == 200


# ============================================================================
# Service Control (Start/Stop/Restart)
# ============================================================================


class TestServiceControl:

    @pytest.mark.asyncio
    async def test_start_invalid_enum(self, client):
        response = await client.post(f"{PREFIX}/BackgroundService/Start",
                                     json={"background_service_name": "NONEXISTENT"})
        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_start_valid_enum(self, client):
        """验证服务控制端点存在（非 404）"""
        response = await client.post(f"{PREFIX}/BackgroundService/Start",
                                     json={"background_service_name": "GET_DYN"})
        assert response.status_code != 404

    @pytest.mark.asyncio
    async def test_stop_valid_enum(self, client):
        response = await client.post(f"{PREFIX}/BackgroundService/Stop",
                                     json={"background_service_name": "GET_DYN"})
        assert response.status_code != 404

    @pytest.mark.asyncio
    async def test_restart_valid_enum(self, client):
        response = await client.post(f"{PREFIX}/BackgroundService/Restart",
                                     json={"background_service_name": "GET_DYN"})
        assert response.status_code != 404

    @pytest.mark.asyncio
    async def test_missing_field(self, client):
        response = await client.post(f"{PREFIX}/BackgroundService/Start", json={})
        assert response.status_code == 422
