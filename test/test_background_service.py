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

import asyncio

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
        response = await client.post(
            f"{PREFIX}/BackgroundService/Start",
            json={"background_service_name": "NONEXISTENT"},
        )
        assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_start_forces_one_round(self, client):
        """start = 排到 cron 下次触发 + 立即强制跑一轮（绕过间隔节流）。"""
        from Service.BackgroundService.CrawlerScheduler import background_service
        from Service.BaseCrawler.launcher.scheduler_launcher import BaseScheduler
        from Utils.通用.Common import GLOBAL_SCHEDULER

        scheduler = background_service.GET_DYN
        if GLOBAL_SCHEDULER.get_job(scheduler.job_id):
            GLOBAL_SCHEDULER.remove_job(scheduler.job_id)
        try:
            with mock.patch.object(
                BaseScheduler, "run_now", new=mock.AsyncMock()
            ) as run_now:
                response = await client.post(
                    f"{PREFIX}/BackgroundService/Start",
                    params={"background_service_name": "GET_DYN"},
                )
                # 强制触发是异步任务，让出一次事件循环再断言
                await asyncio.sleep(0)
            assert response.status_code == 200
            body = response.json()
            assert body["code"] == 0
            assert "已强制触发一轮执行" in body["msg"]
            assert "下次定时触发" in body["msg"]
            run_now.assert_awaited_once()
            job = GLOBAL_SCHEDULER.get_job(scheduler.job_id)
            assert job is not None
            assert job.next_run_time is not None
        finally:
            if GLOBAL_SCHEDULER.get_job(scheduler.job_id):
                GLOBAL_SCHEDULER.remove_job(scheduler.job_id)

    @pytest.mark.asyncio
    async def test_stop_valid_enum(self, client):
        response = await client.post(
            f"{PREFIX}/BackgroundService/Stop",
            json={"background_service_name": "GET_DYN"},
        )
        assert response.status_code != 404

    @pytest.mark.asyncio
    async def test_restart_forces_one_round(self, client):
        """restart = 杀掉旧任务并立即强制重跑，文案要明确说明已触发。"""
        from Service.BackgroundService.CrawlerScheduler import background_service
        from Service.BaseCrawler.launcher.scheduler_launcher import BaseScheduler
        from Utils.通用.Common import GLOBAL_SCHEDULER

        scheduler = background_service.GET_DYN
        try:
            with mock.patch.object(
                BaseScheduler, "run_now", new=mock.AsyncMock()
            ) as run_now:
                response = await client.post(
                    f"{PREFIX}/BackgroundService/Restart",
                    params={"background_service_name": "GET_DYN"},
                )
                # 强制触发是异步任务，让出一次事件循环再断言
                await asyncio.sleep(0)
            assert response.status_code == 200
            body = response.json()
            assert body["code"] == 0
            assert "已强制触发一轮执行" in body["msg"]
            assert "下次定时触发" in body["msg"]
            run_now.assert_awaited_once()
            assert GLOBAL_SCHEDULER.get_job(scheduler.job_id) is not None
        finally:
            if GLOBAL_SCHEDULER.get_job(scheduler.job_id):
                GLOBAL_SCHEDULER.remove_job(scheduler.job_id)

    @pytest.mark.asyncio
    async def test_missing_field(self, client):
        response = await client.post(f"{PREFIX}/BackgroundService/Start", json={})
        assert response.status_code == 422
