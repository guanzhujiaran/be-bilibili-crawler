# -*- coding: utf-8 -*-
"""重试必须「让出 worker 并发额度」的集成测试（不连 MySQL / Redis）。

历史 bug：``request_wrapper`` 的已知错误重试 sleep 发生在 ``async with self.sem`` 内部，
worker 槽位会被坏代理长期占住（表现为「无限重试、有效数据为 0」）。

这里直接驱动 ``UnlimitedCrawler.worker``：并发额度 = 1、两个 worker、两个任务。
- 若 sleep 仍占着额度，两个任务的执行区间**不可能重叠**（串行）；
- 现在重试前先 release、睡完再 acquire，因此第二个任务会在第一个任务 sleep 期间进入。
"""

import asyncio
from typing import AsyncGenerator

from Models.base.custom_pydantic import CustomBaseModelHashable
from Service.BaseCrawler.CrawlerType import UnlimitedCrawler
from Service.BaseCrawler.config import CrawlerConfig
from Service.BaseCrawler.model.base import WorkerModel, WorkerStatus
from Service.GrpcModule.Grpc.Bapi.BapiUtils import request_wrapper
from Service.GrpcModule.Models.CustomRequestErrorModel import (
    ProxyRetryBudgetExhausted,
    RequestProxyNetworkError,
)
from Utils.通用.Common import ConcurrencyLimit
from Utils.代理.mdoel.ProxyTier import ProxyTier
from Utils.代理.mdoel.RequestConf import RequestConf, RequestRetryPolicy


class _Params(CustomBaseModelHashable):
    rid: int

    def __hash__(self):
        return hash(self.rid)


class _Config(CrawlerConfig):
    crawler_name: str = "yield-test"
    concurrency_key: str = "reserve"
    proxy_tier: ProxyTier = ProxyTier.SLOW
    requeue_on_fetch_fail: bool = False


_FAIL_CONF = RequestConf(
    tier=ProxyTier.SLOW,
    retry_policy=RequestRetryPolicy(
        tier=ProxyTier.SLOW,
        max_rounds=2,
        attempts_per_round=1,
        backoff_base=0.3,
        backoff_max=0.3,
        backoff_jitter=0.0,
        log_interval=0,
    ),
)


@request_wrapper
async def _always_fail(request_conf: RequestConf = _FAIL_CONF):
    raise RequestProxyNetworkError("blackhole", -1, proxy_key="http://127.0.0.1:1")


class _YieldCrawler(UnlimitedCrawler[_Params]):
    Config = _Config

    def __init__(self):
        self.spans: list[tuple[int, float, float]] = []
        super().__init__()

    async def is_stop(self) -> bool:
        return False

    async def key_params_gen(
        self, params: _Params | None = None
    ) -> AsyncGenerator[_Params, None]:
        yield _Params(rid=1)

    async def handle_fetch(self, params: _Params) -> WorkerStatus:
        loop = asyncio.get_running_loop()
        start = loop.time()
        try:
            await _always_fail(request_conf=_FAIL_CONF)
        except ProxyRetryBudgetExhausted:
            pass
        finally:
            self.spans.append((params.rid, start, loop.time()))
        return WorkerStatus.fail

    async def main(self, *args, **kwargs):
        return None


def test_retry_sleep_releases_concurrency_slot():
    async def _run():
        crawler = _YieldCrawler()
        # 额度故意设为 1（模拟 slow 车道被背压收缩），但起 2 个 worker：
        # 只有「重试期间让出额度」才可能出现执行区间重叠
        gate = ConcurrencyLimit(1)
        queue: asyncio.Queue[WorkerModel | None] = asyncio.Queue()
        await queue.put(WorkerModel(params=_Params(rid=1), seqId=0))
        await queue.put(WorkerModel(params=_Params(rid=2), seqId=1))
        workers = [asyncio.create_task(crawler.worker(queue, gate)) for _ in range(2)]
        await asyncio.wait_for(queue.join(), timeout=20)
        for worker in workers:
            worker.cancel()
        await asyncio.gather(*workers, return_exceptions=True)
        return crawler

    crawler = asyncio.run(_run())

    assert len(crawler.spans) == 2
    ordered = sorted(crawler.spans, key=lambda item: item[1])
    first_end = ordered[0][2]
    second_start = ordered[1][1]
    overlap = first_end - second_start
    assert overlap > 0, (
        f"重试 sleep 期间没有让出并发额度（执行区间无重叠）：{crawler.spans}"
    )
