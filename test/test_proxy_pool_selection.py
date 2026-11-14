# -*- coding: utf-8 -*-
"""取代理策略的单测（不连 MySQL / Redis）。

覆盖：
1. `select_random_proxy`：随机主键起点 + 空结果回退 + 过滤条件（status=0 且非僵尸）；
2. `select_proxy("rand")` 走 MySQL 随机，不再碰 Redis；
3. 档位筛选：fast 只取「未冷却 + EWMA 达标 + 在途未满」的代理，slow 才会掺 cold 探测；
4. 僵尸口径 `success_times < 0`。
"""

from sqlalchemy.dialects import mysql as mysql_dialect

from Utils.代理.数据库操作.ProxyHealth import (
    ProxyHealthMap,
    ProxyHealthRecord,
    ProxyHealthState,
    ProxyInFlightMap,
    ProxyPrefixMap,
    ProxyPrefixRecord,
    ProxySelectionState,
    proxy_health_store,
    proxy_prefix_of,
)
from Utils.代理.数据库操作.async_proxy_op_alchemy_mysql_ver import (
    ZOMBIE_SUCCESS_TIMES_MAX,
    SQLHelper,
    is_zombie_success_times,
)
from Utils.代理.mdoel.ProxyTier import ProxyTier


class _FakeResult:
    def __init__(self, rows: list):
        self._rows = rows

    def scalars(self):
        return self

    def first(self):
        return self._rows[0] if self._rows else None

    def all(self):
        return self._rows


class _FakeTransaction:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc_info):
        return False


class _FakeSession:
    """按顺序返回预设结果，并记录执行过的语句。"""

    def __init__(self, results: list[list], statements: list | None = None):
        self._results = list(results)
        self.statements = statements if statements is not None else []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc_info):
        return False

    def begin(self):
        return _FakeTransaction()

    async def execute(self, stmt):
        self.statements.append(stmt)
        rows = self._results.pop(0) if self._results else []
        return _FakeResult(rows)


class _FakeProxyRow:
    """最小可用的代理行（只带筛选会用到的字段）。"""

    def __init__(
        self,
        key: str = "http://1.1.1.1:80",
        success_times: int = 5,
        score: int = 100,
    ):
        self.computed_proxy_str = key
        self.proxy = {"http": key, "https": key}
        self.status = 0
        self.success_times = success_times
        self.score = score


def _stub_health(
    monkeypatch,
    mapping=None,
    in_flight=None,
    verified=None,
    prefix_records=None,
    prefix_in_flight=None,
):
    """把健康缓存替换成内存桩，单测不依赖 Redis。

    :param verified: 「已验证分档取样」返回的代理串（默认空 = 走随机采样兜底）
    :param prefix_records: 前缀级明细（field=前缀串），用于验证「整段冷却」语义
    :param prefix_in_flight: 前缀在途并发数（field=前缀串）
    """
    health_map = ProxyHealthMap(records=mapping or {})
    flight_map = ProxyInFlightMap(counts=in_flight or {})
    prefix_map = ProxyPrefixMap(records=prefix_records or {})
    prefix_flight = ProxyInFlightMap(counts=prefix_in_flight or {})
    verified_keys = list(verified or [])

    async def _read_selection_state(keys, prefixes, excluded_prefixes=frozenset()):
        return ProxySelectionState(
            address=health_map,
            address_in_flight=flight_map,
            prefixes=prefix_map,
            prefix_in_flight=prefix_flight,
            excluded_prefixes=frozenset(excluded_prefixes),
        )

    async def _sample_keys(states, count):
        return verified_keys[:count]

    async def _forget(key):
        return None

    # 实例属性会遮蔽同名方法；桩函数不带 self
    monkeypatch.setattr(
        proxy_health_store, "read_selection_state", _read_selection_state, raising=False
    )
    monkeypatch.setattr(
        proxy_health_store, "sample_state_keys", _sample_keys, raising=False
    )
    monkeypatch.setattr(proxy_health_store, "forget_state", _forget, raising=False)


def _stub_db_rows(monkeypatch, rows_by_key: dict | None = None):
    """替换「按代理串批量取整行」，单测不连 MySQL。"""
    mapping = dict(rows_by_key or {})

    async def _get_by_keys(self, keys):
        return {k: mapping[k] for k in keys if k in mapping}

    monkeypatch.setattr(type(SQLHelper), "get_proxies_by_keys", _get_by_keys)


# ---------------------------------------------------------------------------
# MySQL 随机取代理
# ---------------------------------------------------------------------------


async def test_select_random_proxy_uses_random_id_start(monkeypatch):
    """第一次随机起点就命中：只查一次库。"""
    row = _FakeProxyRow()
    session = _FakeSession([[row]])
    _stub_health(monkeypatch)
    monkeypatch.setattr(SQLHelper, "_get_proxy_id_range", _fake_id_range(2, 2000000))
    monkeypatch.setattr(SQLHelper, "async_session", lambda: session)

    proxy = await SQLHelper.select_random_proxy()

    assert proxy is row
    assert len(session.statements) == 1


async def test_select_random_proxy_falls_back_to_head(monkeypatch):
    """随机起点落在尾部空洞（取空）时，后面几次尝试要能取到，不能直接返回 None。"""
    row = _FakeProxyRow()
    session = _FakeSession([[], [row]])
    _stub_health(monkeypatch)
    monkeypatch.setattr(SQLHelper, "_get_proxy_id_range", _fake_id_range(2, 2000000))
    monkeypatch.setattr(SQLHelper, "async_session", lambda: session)

    proxy = await SQLHelper.select_random_proxy()

    assert proxy is row
    assert len(session.statements) == 2


async def test_select_random_proxy_empty_pool(monkeypatch):
    _stub_health(monkeypatch)
    monkeypatch.setattr(SQLHelper, "_get_proxy_id_range", _fake_id_range(0, 0))
    monkeypatch.setattr(SQLHelper, "async_session", lambda: _FakeSession([[]]))

    assert await SQLHelper.select_random_proxy() is None


async def test_select_random_proxy_filters_dead(monkeypatch):
    """筛选条件必须排除 status!=0 与僵尸（success_times<0），不能全表随机。"""
    session = _FakeSession([[None]])
    _stub_health(monkeypatch)
    monkeypatch.setattr(SQLHelper, "_get_proxy_id_range", _fake_id_range(2, 100))
    monkeypatch.setattr(SQLHelper, "async_session", lambda: session)

    await SQLHelper.select_random_proxy(tier=ProxyTier.SLOW)

    compiled = str(
        session.statements[0].compile(
            dialect=mysql_dialect.dialect(), compile_kwargs={"literal_binds": True}
        )
    )
    assert "status" in compiled
    assert "success_times" in compiled
    # 必须是「随机起点 + 主键升序取一批」，不能是 ORDER BY RAND()
    assert "ORDER BY proxy_tab.proxy_id" in compiled
    assert "RAND" not in compiled.upper()
    # 候选批要有上限（否则随机起点后可能扫很多行）
    assert "LIMIT" in compiled.upper()


async def test_select_proxy_rand_delegates_to_mysql(monkeypatch):
    """`select_proxy("rand")` 必须走 MySQL 随机，并把档位 / 排除名单透传下去。"""
    captured = {}

    async def _fake_random(self, tier=ProxyTier.FAST, exclude=None):
        captured["tier"] = tier
        captured["exclude"] = exclude
        return _FakeProxyRow()

    monkeypatch.setattr(type(SQLHelper), "select_random_proxy", _fake_random)

    proxy = await SQLHelper.select_proxy(
        "rand", tier=ProxyTier.SLOW, exclude=frozenset({"http://x"})
    )

    assert isinstance(proxy, _FakeProxyRow)
    assert captured["tier"] is ProxyTier.SLOW
    assert captured["exclude"] == frozenset({"http://x"})


def _fake_id_range(min_id: int, max_id: int):
    async def _range() -> tuple[int, int]:
        return min_id, max_id

    return _range


# ---------------------------------------------------------------------------
# 档位筛选
# ---------------------------------------------------------------------------


async def test_fast_tier_rejects_cold_proxy(monkeypatch):
    """fast 档不接受没有任何健康记录的代理（未验证 = cold）。"""
    from CONFIG import CONFIG

    row = _FakeProxyRow(key="http://cold:1", success_times=0, score=10)
    attempts = CONFIG.proxy_health.pick_attempts
    session = _FakeSession([[row] for _ in range(attempts)])
    _stub_health(monkeypatch)
    monkeypatch.setattr(SQLHelper, "_get_proxy_id_range", _fake_id_range(2, 100))
    monkeypatch.setattr(SQLHelper, "async_session", lambda: session)

    assert await SQLHelper.select_random_proxy(tier=ProxyTier.FAST) is None
    # 每个 pick_attempts 都试过（候选非空时不会走「从头兜底」）
    assert len(session.statements) == attempts


async def test_fast_tier_accepts_hot_proxy(monkeypatch):
    row = _FakeProxyRow(key="http://hot:1")
    session = _FakeSession([[row]])
    _stub_health(
        monkeypatch,
        mapping={
            "http://hot:1": ProxyHealthRecord(
                state=ProxyHealthState.HOT, ewma_success=0.95
            )
        },
    )
    monkeypatch.setattr(SQLHelper, "_get_proxy_id_range", _fake_id_range(2, 100))
    monkeypatch.setattr(SQLHelper, "async_session", lambda: session)

    assert await SQLHelper.select_random_proxy(tier=ProxyTier.FAST) is row


async def test_fast_tier_rejects_cooling_and_busy(monkeypatch):
    """冷却中 / 在途并发打满的代理不能进 fast 档。"""
    cooling = _FakeProxyRow(key="http://cooling:1")
    busy = _FakeProxyRow(key="http://busy:1")
    session = _FakeSession([[cooling, busy], [cooling, busy], [cooling, busy]])
    _stub_health(
        monkeypatch,
        mapping={
            "http://cooling:1": ProxyHealthRecord(
                state=ProxyHealthState.HOT,
                ewma_success=1.0,
                cooldown_until=10**12,
            ),
            "http://busy:1": ProxyHealthRecord(
                state=ProxyHealthState.HOT, ewma_success=1.0
            ),
        },
        in_flight={"http://busy:1": 2},
    )
    monkeypatch.setattr(SQLHelper, "_get_proxy_id_range", _fake_id_range(2, 100))
    monkeypatch.setattr(SQLHelper, "async_session", lambda: session)

    assert await SQLHelper.select_random_proxy(tier=ProxyTier.FAST) is None


async def test_slow_tier_accepts_cold_proxy(monkeypatch):
    """slow 档允许用 cold 代理做探测（这是把新代理养进 hot 池的唯一入口）。"""
    row = _FakeProxyRow(key="http://cold:2", success_times=0, score=10)
    session = _FakeSession([[row]])
    _stub_health(monkeypatch)
    monkeypatch.setattr(SQLHelper, "_get_proxy_id_range", _fake_id_range(2, 100))
    monkeypatch.setattr(SQLHelper, "async_session", lambda: session)

    assert await SQLHelper.select_random_proxy(tier=ProxyTier.SLOW) is row


async def test_slow_tier_rejects_dead(monkeypatch):
    row = _FakeProxyRow(key="http://dead:1")
    session = _FakeSession([[row], [row], [row]])
    _stub_health(
        monkeypatch,
        mapping={
            "http://dead:1": ProxyHealthRecord(state=ProxyHealthState.DEAD)
        },
    )
    monkeypatch.setattr(SQLHelper, "_get_proxy_id_range", _fake_id_range(2, 100))
    monkeypatch.setattr(SQLHelper, "async_session", lambda: session)

    assert await SQLHelper.select_random_proxy(tier=ProxyTier.SLOW) is None


async def test_fast_uses_verified_pool_without_sql_scan(monkeypatch):
    """fast 档优先从已验证分档取样：命中时不走「随机主键起点采样」，省掉两次 MySQL 往返。"""
    row = _FakeProxyRow(key="http://good:1")
    _stub_health(
        monkeypatch,
        mapping={
            "http://good:1": ProxyHealthRecord(
                state=ProxyHealthState.HOT, ewma_success=0.99
            )
        },
        verified=["http://good:1"],
    )
    _stub_db_rows(monkeypatch, {"http://good:1": row})
    session = _FakeSession([])
    monkeypatch.setattr(SQLHelper, "_get_proxy_id_range", _fake_id_range(2, 100))
    monkeypatch.setattr(SQLHelper, "async_session", lambda: session)

    proxy = await SQLHelper.select_random_proxy(tier=ProxyTier.FAST)

    assert proxy is row
    assert session.statements == []


async def test_slow_prefers_verified_pool(monkeypatch):
    """slow 档默认（非探测轮）也优先用已验证代理，而不是随机撞冷代理。"""
    import Utils.代理.数据库操作.async_proxy_op_alchemy_mysql_ver as sql_mod

    row = _FakeProxyRow(key="http://warm:1", success_times=9, score=80)
    _stub_health(
        monkeypatch,
        mapping={
            "http://warm:1": ProxyHealthRecord(
                state=ProxyHealthState.WARM, ewma_success=0.9
            )
        },
        verified=["http://warm:1"],
    )
    _stub_db_rows(monkeypatch, {"http://warm:1": row})
    session = _FakeSession([])
    monkeypatch.setattr(SQLHelper, "_get_proxy_id_range", _fake_id_range(2, 100))
    monkeypatch.setattr(SQLHelper, "async_session", lambda: session)
    # 固定抽签结果，避免 15% 的探测概率让用例随机失败
    monkeypatch.setattr(sql_mod.random, "random", lambda: 0.99)

    proxy = await SQLHelper.select_random_proxy(tier=ProxyTier.SLOW)

    assert proxy is row
    assert session.statements == []


async def test_verified_pool_stale_member_is_forgotten(monkeypatch):
    """分档集合里有、明细已过期 → 清掉该成员，并回退到随机采样。"""
    forgotten: list[str] = []

    async def _forget(key):
        forgotten.append(key)

    row = _FakeProxyRow(key="http://fallback:1")
    _stub_health(
        monkeypatch,
        mapping={
            "http://fallback:1": ProxyHealthRecord(
                state=ProxyHealthState.HOT, ewma_success=1.0
            )
        },
        verified=["http://stale:1"],
    )
    _stub_db_rows(monkeypatch, {"http://fallback:1": row})
    monkeypatch.setattr(proxy_health_store, "forget_state", _forget, raising=False)
    session = _FakeSession([[row]])
    monkeypatch.setattr(SQLHelper, "_get_proxy_id_range", _fake_id_range(2, 100))
    monkeypatch.setattr(SQLHelper, "async_session", lambda: session)

    proxy = await SQLHelper.select_random_proxy(tier=ProxyTier.FAST)

    assert forgotten == ["http://stale:1"]
    assert proxy is row  # 回退到采样后仍能拿到可用代理


async def test_verified_pool_row_gone_in_mysql_is_forgotten(monkeypatch):
    """Redis 说它好、MySQL 里却已不可用（被删 / 被置黑）→ 摘掉，不能返回死代理。"""
    forgotten: list[str] = []

    async def _forget(key):
        forgotten.append(key)

    _stub_health(
        monkeypatch,
        mapping={
            "http://gone:1": ProxyHealthRecord(
                state=ProxyHealthState.HOT, ewma_success=1.0
            )
        },
        verified=["http://gone:1"],
    )
    _stub_db_rows(monkeypatch, {})  # 查库查不到
    monkeypatch.setattr(proxy_health_store, "forget_state", _forget, raising=False)
    monkeypatch.setattr(SQLHelper, "_get_proxy_id_range", _fake_id_range(2, 100))
    monkeypatch.setattr(SQLHelper, "async_session", lambda: _FakeSession([]))

    assert await SQLHelper.select_random_proxy(tier=ProxyTier.FAST) is None
    assert forgotten == ["http://gone:1"]


async def test_exclude_skips_failed_proxy(monkeypatch):
    """上次失败的代理必须被排除（fast 档「换代理」靠它生效）。"""
    row = _FakeProxyRow(key="http://bad:1")
    session = _FakeSession([[row], [row], [row]])
    _stub_health(
        monkeypatch,
        mapping={
            "http://bad:1": ProxyHealthRecord(
                state=ProxyHealthState.HOT, ewma_success=1.0
            )
        },
    )
    monkeypatch.setattr(SQLHelper, "_get_proxy_id_range", _fake_id_range(2, 100))
    monkeypatch.setattr(SQLHelper, "async_session", lambda: session)

    assert (
        await SQLHelper.select_random_proxy(
            tier=ProxyTier.FAST, exclude=frozenset({"http://bad:1"})
        )
        is None
    )


# ---------------------------------------------------------------------------
# 前缀级（网段）判定
# ---------------------------------------------------------------------------

def _prefix_key(ip: str) -> str:
    return proxy_prefix_of(f"http://{ip}:8080")


async def test_same_prefix_cooling_blocks_all_addresses(monkeypatch):
    """同 /24 内一个代理 412 → 整段冷却：同段其它地址也不能再被选中。"""
    from CONFIG import CONFIG

    sibling_a = _FakeProxyRow(key="http://45.74.31.7:8080")
    sibling_b = _FakeProxyRow(key="http://45.74.31.99:8080")
    attempts = CONFIG.proxy_health.pick_attempts
    session = _FakeSession([[sibling_a, sibling_b] for _ in range(attempts)])
    _stub_health(
        monkeypatch,
        mapping={
            "http://45.74.31.7:8080": ProxyHealthRecord(
                state=ProxyHealthState.HOT, ewma_success=1.0
            ),
            "http://45.74.31.99:8080": ProxyHealthRecord(
                state=ProxyHealthState.HOT, ewma_success=1.0
            ),
        },
        prefix_records={
            _prefix_key("45.74.31.7"): ProxyPrefixRecord(
                state=ProxyHealthState.QUARANTINE,
                cooldown_until=10**12,
                risk_level=1,
            )
        },
    )
    monkeypatch.setattr(SQLHelper, "_get_proxy_id_range", _fake_id_range(2, 100))
    monkeypatch.setattr(SQLHelper, "async_session", lambda: session)

    assert await SQLHelper.select_random_proxy(tier=ProxyTier.FAST) is None


async def test_other_prefix_still_selectable_when_one_cools(monkeypatch):
    """整段冷却只影响该段：另一个 /24 的代理照常可选。"""
    cooled = _FakeProxyRow(key="http://45.74.31.7:8080")
    healthy = _FakeProxyRow(key="http://8.8.8.8:8080")
    session = _FakeSession([[cooled, healthy]])
    _stub_health(
        monkeypatch,
        mapping={
            "http://45.74.31.7:8080": ProxyHealthRecord(
                state=ProxyHealthState.HOT, ewma_success=1.0
            ),
            "http://8.8.8.8:8080": ProxyHealthRecord(
                state=ProxyHealthState.HOT, ewma_success=1.0
            ),
        },
        prefix_records={
            _prefix_key("45.74.31.7"): ProxyPrefixRecord(
                state=ProxyHealthState.QUARANTINE, cooldown_until=10**12
            )
        },
    )
    monkeypatch.setattr(SQLHelper, "_get_proxy_id_range", _fake_id_range(2, 100))
    monkeypatch.setattr(SQLHelper, "async_session", lambda: session)

    assert await SQLHelper.select_random_proxy(tier=ProxyTier.FAST) is healthy


async def test_failed_proxy_blocks_its_prefix(monkeypatch):
    """任务内失败过的代理 → 同 /24 的其它地址一并跳过（同段换地址没有意义）。"""
    sibling = _FakeProxyRow(key="http://45.74.31.99:8080")
    session = _FakeSession([[sibling], [sibling], [sibling]])
    _stub_health(
        monkeypatch,
        mapping={
            "http://45.74.31.99:8080": ProxyHealthRecord(
                state=ProxyHealthState.HOT, ewma_success=1.0
            )
        },
    )
    monkeypatch.setattr(SQLHelper, "_get_proxy_id_range", _fake_id_range(2, 100))
    monkeypatch.setattr(SQLHelper, "async_session", lambda: session)

    assert (
        await SQLHelper.select_random_proxy(
            tier=ProxyTier.FAST, exclude=frozenset({"http://45.74.31.7:8080"})
        )
        is None
    )


async def test_prefix_inflight_limit_blocks_segment(monkeypatch):
    """同段在途打满（>= prefix_inflight_limit）→ 不再往该段压并发。"""
    from CONFIG import CONFIG

    limit = CONFIG.proxy_health.prefix_inflight_limit
    row = _FakeProxyRow(key="http://45.74.31.7:8080")
    session = _FakeSession([[row], [row], [row]])
    _stub_health(
        monkeypatch,
        mapping={
            "http://45.74.31.7:8080": ProxyHealthRecord(
                state=ProxyHealthState.HOT, ewma_success=1.0
            )
        },
        prefix_in_flight={_prefix_key("45.74.31.7"): limit},
    )
    monkeypatch.setattr(SQLHelper, "_get_proxy_id_range", _fake_id_range(2, 100))
    monkeypatch.setattr(SQLHelper, "async_session", lambda: session)

    assert await SQLHelper.select_random_proxy(tier=ProxyTier.FAST) is None


async def test_verified_pool_respects_prefix_cooling(monkeypatch):
    """已验证分档取样也要过前缀闸门：hot 里同段整片冷却时不能返回它。"""
    row = _FakeProxyRow(key="http://45.74.31.7:8080")
    _stub_health(
        monkeypatch,
        mapping={
            "http://45.74.31.7:8080": ProxyHealthRecord(
                state=ProxyHealthState.HOT, ewma_success=1.0
            )
        },
        verified=["http://45.74.31.7:8080"],
        prefix_records={
            _prefix_key("45.74.31.7"): ProxyPrefixRecord(
                state=ProxyHealthState.QUARANTINE, cooldown_until=10**12
            )
        },
    )
    _stub_db_rows(monkeypatch, {"http://45.74.31.7:8080": row})
    session = _FakeSession([[row], [row]])
    monkeypatch.setattr(SQLHelper, "_get_proxy_id_range", _fake_id_range(2, 100))
    monkeypatch.setattr(SQLHelper, "async_session", lambda: session)

    assert await SQLHelper.select_random_proxy(tier=ProxyTier.FAST) is None


# ---------------------------------------------------------------------------
# 僵尸口径
# ---------------------------------------------------------------------------


def test_zombie_success_times():
    assert ZOMBIE_SUCCESS_TIMES_MAX == 0
    assert is_zombie_success_times(None) is False
    assert is_zombie_success_times(0) is False
    assert is_zombie_success_times(3) is False
    assert is_zombie_success_times(-1) is True
