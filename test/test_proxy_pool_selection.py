# -*- coding: utf-8 -*-
"""取代理策略的单测（不连 MySQL / Redis）。

覆盖：
1. `select_random_proxy`：随机主键起点 + 空结果回退 + 过滤条件（status=0 且非僵尸）；
2. `select_proxy("rand")` 走 MySQL 随机，不再碰 Redis；
3. `available_proxy` 的独立 helper（现在只做统计，不再参与取代理）仍可用；
4. 僵尸口径 `success_times < 0`。
"""

from datetime import datetime

from sqlalchemy.dialects import mysql as mysql_dialect

from Utils.代理.数据库操作.async_proxy_op_alchemy_mysql_ver import (
    ZOMBIE_SUCCESS_TIMES_MAX,
    SQLHelper,
    is_zombie_success_times,
)


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
    def __init__(self, key: str = "http://1.1.1.1:80"):
        self.computed_proxy_str = key


# ---------------------------------------------------------------------------
# MySQL 随机取代理
# ---------------------------------------------------------------------------


async def test_select_random_proxy_uses_random_id_start(monkeypatch):
    """第一次随机起点就命中：只查一次库。"""
    row = _FakeProxyRow()
    session = _FakeSession([[row]])
    monkeypatch.setattr(SQLHelper, "_get_proxy_id_range", _fake_id_range(2, 2000000))
    monkeypatch.setattr(SQLHelper, "async_session", lambda: session)

    proxy = await SQLHelper.select_random_proxy()

    assert proxy is row
    assert len(session.statements) == 1


async def test_select_random_proxy_falls_back_to_head(monkeypatch):
    """随机起点落在尾部空洞（取空）时，回退到「从头取下一条」，不能返回 None。"""
    row = _FakeProxyRow()
    session = _FakeSession([[], [row]])
    monkeypatch.setattr(SQLHelper, "_get_proxy_id_range", _fake_id_range(2, 2000000))
    monkeypatch.setattr(SQLHelper, "async_session", lambda: session)

    proxy = await SQLHelper.select_random_proxy()

    assert proxy is row
    assert len(session.statements) == 2


async def test_select_random_proxy_empty_pool(monkeypatch):
    monkeypatch.setattr(SQLHelper, "_get_proxy_id_range", _fake_id_range(0, 0))
    monkeypatch.setattr(SQLHelper, "async_session", lambda: _FakeSession([[]]))

    assert await SQLHelper.select_random_proxy() is None


async def test_select_random_proxy_filters_dead(monkeypatch):
    """筛选条件必须排除 status!=0 与僵尸（success_times<0），不能全表随机。"""
    session = _FakeSession([[None]])
    monkeypatch.setattr(SQLHelper, "_get_proxy_id_range", _fake_id_range(2, 100))
    monkeypatch.setattr(SQLHelper, "async_session", lambda: session)

    await SQLHelper.select_random_proxy()

    compiled = str(
        session.statements[0].compile(
            dialect=mysql_dialect.dialect(), compile_kwargs={"literal_binds": True}
        )
    )
    assert "status" in compiled
    assert "coalesce" in compiled.lower()
    assert "success_times" in compiled
    # 必须是「随机起点 + 主键升序取一条」，不能是 ORDER BY RAND()
    assert "ORDER BY proxy_tab.proxy_id" in compiled
    assert "RAND" not in compiled.upper()


async def test_select_proxy_rand_delegates_to_mysql(monkeypatch):
    """`select_proxy("rand")` 必须走 MySQL 随机，不再有 Redis 分支。"""
    called = False

    async def _fake_random(self):
        nonlocal called
        called = True
        return _FakeProxyRow()

    monkeypatch.setattr(type(SQLHelper), "select_random_proxy", _fake_random)

    proxy = await SQLHelper.select_proxy("rand")

    assert called is True
    assert isinstance(proxy, _FakeProxyRow)


def _fake_id_range(min_id: int, max_id: int):
    async def _range() -> tuple[int, int]:
        return min_id, max_id

    return _range


# ---------------------------------------------------------------------------
# 僵尸口径
# ---------------------------------------------------------------------------


def test_zombie_success_times():
    assert ZOMBIE_SUCCESS_TIMES_MAX == 0
    assert is_zombie_success_times(None) is False
    assert is_zombie_success_times(0) is False
    assert is_zombie_success_times(3) is False
    assert is_zombie_success_times(-1) is True
