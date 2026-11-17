# -*- coding: utf-8 -*-
"""按失败原因清理僵尸代理的单测（纯 SQL 语义，不连 MySQL）。

用 SQLite 建一张与 proxy_tab 同列的最小表，直接执行生产代码里的
``build_*`` 表达式，覆盖：

1. 按 kind 分桶累计（network / response / unknown / risk_control）；
2. 时间窗口滚动：超窗后所有计数一起清零，边界与阈值边界；
3. 删除候选：network / unknown 阈值、窗口过滤、「窗口内无成功」；
4. response / risk_control 永不进删除候选；
5. 成功时负数 success_times 复位为 0（慢档探活重新分类的关键）。
"""

from types import SimpleNamespace

import pytest
from sqlalchemy import Column, Integer, MetaData, Table, create_engine, select

from CONFIG import CONFIG
from Utils.代理.数据库操作 import ProxyEvent
from Utils.代理.数据库操作.ProxyHealth import ProxyFailureKind
from Utils.代理.数据库操作.async_proxy_op_alchemy_mysql_ver import (
    CLEANUP_DELETE_KINDS,
    FAIL_COUNTER_COLUMNS,
    build_cleanup_candidate_condition,
    build_fail_counter_values,
    build_success_values,
)
from Utils.代理.数据库操作.SqlAlcheyObj.ProxyModel import ProxyTab

NOW = 1_800_000_000
WINDOW_SECONDS = 7 * 86400


@pytest.fixture()
def sqlite_proxy_table():
    """最小 proxy_tab（只保留计数相关列），列名与模型一致。"""
    metadata = MetaData()
    table = Table(
        "proxy_tab",
        metadata,
        Column("proxy_id", Integer, primary_key=True),
        Column("success_times", Integer, nullable=True),
        *[
            Column(name, Integer, nullable=False, server_default="0")
            for name in FAIL_COUNTER_COLUMNS.values()
        ],
        Column("fail_window_start_ts", Integer),
        Column("last_fail_ts", Integer),
        Column("last_success_ts", Integer),
    )
    engine = create_engine("sqlite://")
    metadata.create_all(engine)
    columns = SimpleNamespace(**{column.name: column for column in table.columns})
    with engine.begin() as conn:
        yield conn, table, columns
    engine.dispose()


def _insert(conn, table, proxy_id: int = 1, **overrides) -> None:
    values = {name: 0 for name in FAIL_COUNTER_COLUMNS.values()}
    values.update(
        {
            "proxy_id": proxy_id,
            "success_times": -5,
            "fail_window_start_ts": None,
            "last_fail_ts": None,
            "last_success_ts": None,
        }
    )
    values.update(overrides)
    conn.execute(table.insert().values(**values))


def _apply_fail(
    conn,
    table,
    columns,
    kind: ProxyFailureKind,
    proxy_id: int = 1,
    now: int = NOW,
    window_seconds: int = WINDOW_SECONDS,
) -> None:
    values = build_fail_counter_values(columns, kind, now, window_seconds)
    conn.execute(
        table.update().where(table.c.proxy_id == proxy_id).values(**values)
    )


def _row(conn, table, proxy_id: int = 1) -> dict:
    return (
        conn.execute(select(table).where(table.c.proxy_id == proxy_id))
        .mappings()
        .one()
    )


def _candidate_ids(conn, table, columns, kind, now: int = NOW) -> list[int]:
    cond = build_cleanup_candidate_condition(columns, kind, now, CONFIG.proxy_health)
    if cond is None:
        return []
    return list(
        conn.execute(select(table.c.proxy_id).where(cond).order_by(table.c.proxy_id)).scalars()
    )


# ---------------------------------------------------------------------------
# 采集：按 kind 分桶累计 + 窗口滚动
# ---------------------------------------------------------------------------


def test_failure_counters_accumulate_by_kind(sqlite_proxy_table):
    conn, table, columns = sqlite_proxy_table
    _insert(conn, table)

    _apply_fail(conn, table, columns, ProxyFailureKind.NETWORK)
    _apply_fail(conn, table, columns, ProxyFailureKind.NETWORK)
    _apply_fail(conn, table, columns, ProxyFailureKind.RESPONSE)
    _apply_fail(conn, table, columns, ProxyFailureKind.UNKNOWN)
    _apply_fail(conn, table, columns, ProxyFailureKind.RISK_CONTROL)

    row = _row(conn, table)
    assert row["fail_network_times"] == 2
    assert row["fail_response_times"] == 1
    assert row["fail_unknown_times"] == 1
    # risk_control 只累加 quarantine_times，不进入任何可删除计数
    assert row["quarantine_times"] == 1
    assert row["fail_window_start_ts"] == NOW
    assert row["last_fail_ts"] == NOW


def test_fail_window_rollover_resets_all_kinds(sqlite_proxy_table):
    conn, table, columns = sqlite_proxy_table
    _insert(
        conn,
        table,
        proxy_id=1,
        fail_network_times=4,
        fail_response_times=3,
        fail_unknown_times=2,
        quarantine_times=9,
        fail_window_start_ts=NOW - WINDOW_SECONDS - 1,
    )

    _apply_fail(conn, table, columns, ProxyFailureKind.NETWORK, proxy_id=1)

    row = _row(conn, table)
    assert row["fail_network_times"] == 1, "超窗后应从 1 重新累计"
    assert row["fail_response_times"] == 0
    assert row["fail_unknown_times"] == 0
    assert row["quarantine_times"] == 0
    assert row["fail_window_start_ts"] == NOW


def test_fail_window_boundary(sqlite_proxy_table):
    """窗口起点恰好等于 now - window 视为超窗；晚 1 秒则仍在窗口内。"""
    conn, table, columns = sqlite_proxy_table
    _insert(
        conn, table, proxy_id=1,
        fail_network_times=4, fail_window_start_ts=NOW - WINDOW_SECONDS,
    )
    _insert(
        conn, table, proxy_id=2,
        fail_network_times=4, fail_window_start_ts=NOW - WINDOW_SECONDS + 1,
    )

    _apply_fail(conn, table, columns, ProxyFailureKind.NETWORK, proxy_id=1)
    _apply_fail(conn, table, columns, ProxyFailureKind.NETWORK, proxy_id=2)

    assert _row(conn, table, 1)["fail_network_times"] == 1
    assert _row(conn, table, 2)["fail_network_times"] == 5


def test_success_resets_negative_success_times(sqlite_proxy_table):
    conn, table, columns = sqlite_proxy_table
    cases = {1: -3, 2: -1, 3: 0, 4: 7, 5: None}
    for proxy_id, success_times in cases.items():
        _insert(conn, table, proxy_id=proxy_id, success_times=success_times)
        conn.execute(
            table.update()
            .where(table.c.proxy_id == proxy_id)
            .values(**build_success_values(columns, NOW))
        )

    assert _row(conn, table, 1)["success_times"] == 0
    assert _row(conn, table, 2)["success_times"] == 0
    assert _row(conn, table, 3)["success_times"] == 1
    assert _row(conn, table, 4)["success_times"] == 8
    assert _row(conn, table, 5)["success_times"] == 1
    for proxy_id in cases:
        assert _row(conn, table, proxy_id)["last_success_ts"] == NOW


# ---------------------------------------------------------------------------
# 判定：阈值边界 / 窗口过滤 / 窗口内无成功 / 风险类永不入候选
# ---------------------------------------------------------------------------


def test_cleanup_threshold_boundary(sqlite_proxy_table):
    conn, table, columns = sqlite_proxy_table
    since = NOW - 86400
    _insert(conn, table, proxy_id=1, fail_network_times=4, fail_window_start_ts=since)
    _insert(conn, table, proxy_id=2, fail_network_times=5, fail_window_start_ts=since)
    _insert(conn, table, proxy_id=3, fail_unknown_times=9, fail_window_start_ts=since)
    _insert(conn, table, proxy_id=4, fail_unknown_times=10, fail_window_start_ts=since)

    assert _candidate_ids(conn, table, columns, ProxyFailureKind.NETWORK) == [2]
    assert _candidate_ids(conn, table, columns, ProxyFailureKind.UNKNOWN) == [4]


def test_cleanup_window_filter(sqlite_proxy_table):
    conn, table, columns = sqlite_proxy_table
    _insert(
        conn, table, proxy_id=1,
        fail_network_times=5, fail_window_start_ts=NOW - WINDOW_SECONDS,
    )
    _insert(
        conn, table, proxy_id=2,
        fail_network_times=5, fail_window_start_ts=NOW - WINDOW_SECONDS + 1,
    )
    _insert(
        conn, table, proxy_id=3,
        fail_network_times=5, fail_window_start_ts=NOW - 86400,
    )

    # 窗口起点恰好 7 天前不算「近 7 天内」，只有 2 / 3 是候选
    assert _candidate_ids(conn, table, columns, ProxyFailureKind.NETWORK) == [2, 3]


def test_cleanup_requires_no_success_in_window(sqlite_proxy_table):
    conn, table, columns = sqlite_proxy_table
    since = NOW - 86400
    _insert(conn, table, proxy_id=1, fail_network_times=5,
            fail_window_start_ts=since, last_success_ts=None)
    _insert(conn, table, proxy_id=2, fail_network_times=5,
            fail_window_start_ts=since, last_success_ts=NOW - 2 * 86400)
    _insert(conn, table, proxy_id=3, fail_network_times=5,
            fail_window_start_ts=since, last_success_ts=NOW - 3600)
    # 成功时刻恰好等于窗口起点：属于窗口内的成功，不能删
    _insert(conn, table, proxy_id=4, fail_network_times=5,
            fail_window_start_ts=since, last_success_ts=since)
    # 净成功仍为正（历史成功多于失败）：保守不删
    _insert(conn, table, proxy_id=5, fail_network_times=5,
            fail_window_start_ts=since, success_times=3)

    assert _candidate_ids(conn, table, columns, ProxyFailureKind.NETWORK) == [1, 2]


def test_risk_control_and_response_never_enter_candidates(sqlite_proxy_table):
    conn, table, columns = sqlite_proxy_table
    since = NOW - 86400
    _insert(
        conn, table, proxy_id=1,
        fail_response_times=999, quarantine_times=999,
        fail_window_start_ts=since,
    )

    # 口径：不可删除的原因直接没有候选条件
    for kind in (ProxyFailureKind.RESPONSE, ProxyFailureKind.RISK_CONTROL):
        assert (
            build_cleanup_candidate_condition(columns, kind, NOW, CONFIG.proxy_health)
            is None
        )
        assert _candidate_ids(conn, table, columns, kind) == []
    assert ProxyFailureKind.RISK_CONTROL not in CLEANUP_DELETE_KINDS
    assert ProxyFailureKind.RESPONSE not in CLEANUP_DELETE_KINDS
    # 风险类计数再高也不会被 network / unknown 条件命中
    assert _candidate_ids(conn, table, columns, ProxyFailureKind.NETWORK) == []


# ---------------------------------------------------------------------------
# 事件入口：网络失败不再复用 -412
# ---------------------------------------------------------------------------


async def test_network_failure_uses_independent_status(monkeypatch):
    proxy = ProxyTab(
        proxy_id=1,
        proxy={"http": "http://1.2.3.4:8080", "https": "http://1.2.3.4:8080"},
        status=0,
        update_ts=0,
        score=100,
    )
    captured: dict = {}

    async def _fake_update(proxy_tab, change_score_num=10, kind=None):
        captured["status"] = proxy_tab.status
        captured["kind"] = kind
        return True

    async def _noop(*_args, **_kwargs):
        return None

    monkeypatch.setattr(ProxyEvent.SQLHelper, "update_to_proxy_list", _fake_update)
    monkeypatch.setattr(
        ProxyEvent, "proxy_health_store", SimpleNamespace(record_failure=_noop)
    )

    await ProxyEvent.handle_proxy_request_fail(
        proxy_tab=proxy, kind=ProxyFailureKind.NETWORK
    )

    assert ProxyEvent.NETWORK_FAIL_STATUS == -1
    assert captured["status"] == -1
    assert captured["kind"] is ProxyFailureKind.NETWORK
