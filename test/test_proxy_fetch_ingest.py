# -*- coding: utf-8 -*-
"""代理抓取「入库阶段」的单测（不连 MySQL / Redis）。

覆盖：
1. `lookup_proxy_keys` 批量预筛：已存在 / 未判死两个集合的口径；
2. `_add_to_proxy_list` 用预筛结果时不再逐条查库：
   新代理 → 入库 + 写 fresh；已存在且存活 → 只写 fresh；已判死 → 什么都不做；
3. `_iter_raw_list_source` / `_pump_source`：流式分批与队列收尾。
"""

import asyncio

from Utils.代理.数据库操作.async_proxy_op_alchemy_mysql_ver import (
    ProxyKeyLookup,
    SQLHelper,
)
from Utils.代理.redisProxyRequest import GetProxyFromNet as mod

_PROXY_NEW = {"http": "http://1.1.1.1:80", "https": "http://1.1.1.1:80"}
_PROXY_ALIVE = {"http": "http://2.2.2.2:80", "https": "http://2.2.2.2:80"}
_PROXY_DEAD = {"http": "http://3.3.3.3:80", "https": "http://3.3.3.3:80"}


class _FakeResult:
    def __init__(self, rows: list):
        self._rows = rows

    def all(self):
        return self._rows


class _FakeSession:
    def __init__(self, rows: list, captured: list):
        self.rows = rows
        self.captured = captured

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc_info):
        return False

    async def execute(self, stmt):
        self.captured.append(stmt)
        return _FakeResult(self.rows)


async def test_lookup_proxy_keys(monkeypatch):
    """status=0 且非僵尸才算 alive；NULL success_times 属于「从未使用」，也算 alive。"""
    rows = [
        ("http://a:80", 0, 3),  # 成功过 → alive
        ("http://b:80", 0, None),  # 从未使用 → alive
        ("http://c:80", 0, -1),  # 僵尸 → 只 existing
        ("http://d:80", -412, 5),  # 状态被判死 → 只 existing
    ]
    captured: list = []
    monkeypatch.setattr(SQLHelper, "async_session", lambda: _FakeSession(rows, captured))

    lookup = await SQLHelper.lookup_proxy_keys(
        ["http://a:80", "http://b:80", "http://c:80", "http://d:80", "", None]
    )

    assert lookup.existing == {"http://a:80", "http://b:80", "http://c:80", "http://d:80"}
    assert lookup.alive == {"http://a:80", "http://b:80"}
    # 空值被过滤，不会拼进 IN (...)
    assert len(captured) == 1


async def test_lookup_proxy_keys_empty(monkeypatch):
    captured: list = []
    monkeypatch.setattr(SQLHelper, "async_session", lambda: _FakeSession([], captured))

    lookup = await SQLHelper.lookup_proxy_keys([])

    assert lookup.existing == set()
    assert lookup.alive == set()
    assert captured == []


async def test_add_to_proxy_list_uses_lookup(monkeypatch):
    """有预筛结果时不许再逐条查库；入库只写 MySQL，不再维护 Redis 池。"""
    from Utils.代理.数据库操作.async_proxy_op_alchemy_mysql_ver import to_proxy_key

    worker = mod.get_proxy_methods
    worker._round_stats = mod.ProxyFetchRoundStats()
    worker.proxy_key_lookup = ProxyKeyLookup(
        existing={to_proxy_key(_PROXY_ALIVE), to_proxy_key(_PROXY_DEAD)},
        alive={to_proxy_key(_PROXY_ALIVE)},
    )
    inserted: list[str] = []

    async def _fake_insert(proxy_tab) -> bool:
        inserted.append(proxy_tab.proxy["http"])
        return True

    async def _forbidden_get(proxy_key: str):
        raise AssertionError("有预筛结果时不应再逐条查库")

    monkeypatch.setattr(SQLHelper, "add_to_proxy_tab_database", _fake_insert)
    monkeypatch.setattr(SQLHelper, "get_proxy_by_key", _forbidden_get)

    for proxy_dict in (_PROXY_NEW, _PROXY_ALIVE, _PROXY_DEAD):
        await worker._add_to_proxy_list(
            {
                "proxy": proxy_dict,
                "status": 0,
                "update_ts": 0,
                "score": 50,
            }
        )

    # 新代理入库；已存在（无论是否判死）都不再写任何池，只计数
    assert inserted == ["http://1.1.1.1:80"]
    assert worker._round_stats.inserted == 1
    assert worker._round_stats.existed == 2
    assert worker._round_stats.dead_skipped == 1


async def test_add_to_proxy_list_falls_back_to_single_query(monkeypatch):
    """脱离抓取轮次（无预筛结果）时仍能按单条查询判断，且不会重复入库。"""

    class _Row:
        status = 0
        success_times = None

    async def _fake_get(proxy_key: str):
        return _Row()

    worker = mod.get_proxy_methods
    worker._round_stats = mod.ProxyFetchRoundStats()
    worker.proxy_key_lookup = None
    inserted: list[str] = []

    async def _fake_insert(proxy_tab) -> bool:
        inserted.append(proxy_tab.proxy["http"])
        return True

    monkeypatch.setattr(SQLHelper, "get_proxy_by_key", _fake_get)
    monkeypatch.setattr(SQLHelper, "add_to_proxy_tab_database", _fake_insert)

    await worker._add_to_proxy_list(
        {"proxy": _PROXY_ALIVE, "status": 0, "update_ts": 0, "score": 50}
    )

    assert inserted == []
    assert worker._round_stats.existed == 1


async def _append_and_return(target: list[str], value: str) -> int:
    target.append(value)
    return 1


def test_dedup_batch_across_sources():
    """跨源共享 seen：同一个代理串只入一批；空字典被跳过。"""
    seen: set[str] = {"http://1.1.1.1:80"}

    batch = mod.GetProxyMethods._dedup_batch(
        [
            {"http": "http://1.1.1.1:80", "https": "http://1.1.1.1:80"},
            {"http": "http://2.2.2.2:80", "https": "http://2.2.2.2:80"},
            None,
            {},
        ],
        seen,
    )

    assert batch == [{"http": "http://2.2.2.2:80", "https": "http://2.2.2.2:80"}]
    assert seen == {"http://1.1.1.1:80", "http://2.2.2.2:80"}


async def test_ingest_proxies_uses_batch_lookup(monkeypatch):
    """每批次先批量预筛一次，再逐条入库（lookup 从参数传入，不再走实例缓存）。"""
    worker = mod.get_proxy_methods
    worker._round_stats = mod.ProxyFetchRoundStats()
    worker.proxy_key_lookup = None
    looked_up: list[list[str]] = []
    ingested: list[str] = []

    async def _fake_lookup(keys: list[str]) -> ProxyKeyLookup:
        looked_up.append(keys)
        return ProxyKeyLookup(existing={"http://2.2.2.2:80"}, alive={"http://2.2.2.2:80"})

    async def _fake_add(
        self, proxy_dict: dict, lookup: ProxyKeyLookup | None = None
    ) -> None:
        assert lookup is not None, "入库必须带上本批次的预筛结果"
        # 入库前必须包装成 proxy/status/update_ts/score 结构（裸代理字典直接入库会 KeyError）
        assert proxy_dict["status"] == 0 and proxy_dict["score"] == 50
        ingested.append(proxy_dict["proxy"]["http"])

    monkeypatch.setattr(SQLHelper, "lookup_proxy_keys", _fake_lookup)
    monkeypatch.setattr(mod.GetProxyMethods, "_add_to_proxy_list", _fake_add)

    await worker._ingest_proxies(
        [
            {"http": "http://1.1.1.1:80", "https": "http://1.1.1.1:80"},
            {"http": "http://2.2.2.2:80", "https": "http://2.2.2.2:80"},
        ]
    )

    assert looked_up == [["http://1.1.1.1:80", "http://2.2.2.2:80"]]
    assert sorted(ingested) == ["http://1.1.1.1:80", "http://2.2.2.2:80"]


async def test_ingest_proxies_counts_failures(monkeypatch):
    """单条入库失败不能静默：要计入 ingest_failures 并打 WARNING。"""
    worker = mod.get_proxy_methods
    worker._round_stats = mod.ProxyFetchRoundStats()

    async def _fake_lookup(keys: list[str]) -> ProxyKeyLookup:
        return ProxyKeyLookup()

    async def _broken_add(self, proxy_dict, lookup=None) -> None:
        raise RuntimeError("insert failed")

    monkeypatch.setattr(SQLHelper, "lookup_proxy_keys", _fake_lookup)
    monkeypatch.setattr(mod.GetProxyMethods, "_add_to_proxy_list", _broken_add)

    await worker._ingest_proxies(
        [{"http": "http://1.1.1.1:80", "https": "http://1.1.1.1:80"}]
    )

    assert worker._round_stats.ingest_failures == 1


async def test_iter_raw_list_source_batches(monkeypatch):
    """流式源边读边解析，按 batch_size 分批 yield（不在内存里攒完整列表）。"""
    worker = mod.get_proxy_methods
    lines = [
        "1.1.1.1:80",
        "# comment",
        "31.43.63.70:4145:Ukraine",  # 带国家后缀
        "bad line",
        "2.2.2.2:80",
        "3.3.3.3:80",
    ]

    async def _fake_iter_lines(url, headers=None, proxies=None, timeout=300):
        for line in lines:
            yield line

    monkeypatch.setattr(mod.my_async_httpx, "iter_lines", _fake_iter_lines)

    batches = [
        batch
        async for batch in worker._iter_raw_list_source(
            "https://example.com/list.txt", protocol="http", batch_size=2
        )
    ]

    # 4 条有效代理（1.1.1.1 / 31.43.63.70 / 2.2.2.2 / 3.3.3.3），batch_size=2 → 两批
    assert [len(b) for b in batches] == [2, 2]
    assert batches[0][0] == {"http": "http://1.1.1.1:80", "https": "http://1.1.1.1:80"}
    assert batches[1][0] == {"http": "http://2.2.2.2:80", "https": "http://2.2.2.2:80"}


async def test_pump_source_puts_batches_and_sentinel(monkeypatch):
    """源协程把每批塞进队列，异常也要塞 None 收尾（否则主循环永远等下去）。"""
    worker = mod.get_proxy_methods
    queue: asyncio.Queue = asyncio.Queue()
    semaphore = asyncio.Semaphore(1)

    async def _good_source():
        yield [{"http": "http://1.1.1.1:80", "https": "http://1.1.1.1:80"}]
        yield []

    await worker._pump_source(_good_source, queue, semaphore)

    assert await queue.get() == [
        {"http": "http://1.1.1.1:80", "https": "http://1.1.1.1:80"}
    ]
    assert await queue.get() is None


async def test_pump_source_sentinel_on_failure():
    worker = mod.get_proxy_methods
    queue: asyncio.Queue = asyncio.Queue()

    async def _broken_source():
        raise RuntimeError("stream broke")
        yield  # pragma: no cover - 只为让它成为 async generator

    await worker._pump_source(_broken_source, queue, asyncio.Semaphore(1))

    assert await queue.get() is None


async def test_ingest_proxies_skips_empty_batch(monkeypatch):
    called = False

    async def _fake_lookup(keys):
        nonlocal called
        called = True
        return ProxyKeyLookup()

    monkeypatch.setattr(SQLHelper, "lookup_proxy_keys", _fake_lookup)

    await mod.get_proxy_methods._ingest_proxies([])

    assert called is False
