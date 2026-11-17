# -*- coding: utf-8 -*-
"""gRPC 通道 -352 处置的单测（内存桩，不连 Redis / MySQL）。

覆盖 2026-11-16 补 dyn_detail 时暴露的四条问题：

1. ``proxy_flag`` 被重复赋值：352 之后 30 分钟内专线被排除、请求全落到免费池；
2. gRPC 通道不区分代理协议：socks4 / 明文代理拿去发 HTTP/2 + TLS 必然失败；
3. 352 只换 IP 不换设备（``times_352 >= 3`` 才重建，阈值硬编码）；
4. 352 把代理在 MySQL 里永久置为不可用，几轮就能把池子打空。
"""

import time

import pytest

from CONFIG import CONFIG
from Service.GrpcModule.Grpc import grpc_api
from Service.GrpcModule.Grpc.grpc_api import BiliGrpc
from Service.GrpcModule.Models.CustomRequestErrorModel import Request352Error
from Service.GrpcModule.Models.GrpcApiBaseModel import MetaDataWrapper
from Utils.代理.数据库操作 import ProxyEvent
from Utils.代理.数据库操作.SqlAlcheyObj.ProxyModel import ProxyTab
from Utils.代理.mdoel.request_context import (
    ProxyAttemptState,
    bind_attempt_state,
    note_failed_proxy,
    reset_attempt_state,
)

#: 测试用的专线地址（不对应真实端口：单测不会真的发请求）
_LOCAL_EXIT = "http://127.0.0.1:3128"


def _make_grpc() -> BiliGrpc:
    """绕过 ``__init__``（它会拉版本号 / 读 UA 文件）的最小实例。"""
    grpc = BiliGrpc.__new__(BiliGrpc)
    grpc.debug_mode = False
    grpc.latest_352_ts = 0
    grpc.grpc_api_any_log = grpc_api.BiliGrpcApi_logger
    grpc.my_proxy_addr = _LOCAL_EXIT
    grpc.metadata_list = []
    grpc.queue_num = 0
    grpc._pool_grpc_fail_streak = 0
    grpc._pool_grpc_fail_ts = 0
    return grpc


def _pool_proxy(proxy: dict, proxy_id: int = 2) -> ProxyTab:
    return ProxyTab(
        **{
            "proxy_id": proxy_id,
            "proxy": proxy,
            "status": 0,
            "update_ts": 0,
            "score": 0,
            "add_ts": 0,
            "success_times": 0,
            "zhihu_status": 0,
            "computed_proxy_str": list(proxy.values())[0],
        }
    )


class _FakeHealthStore:
    """只实现本次改动用到的两个方法。"""

    def __init__(self, *, cooling: bool = False) -> None:
        self.cooling = cooling
        self.failures: list[tuple[str, object]] = []

    async def is_prefix_cooling(self, prefix: str, scope: str | None = None) -> bool:
        return self.cooling

    async def record_failure(
        self, key: str, kind: object, *, scope: str | None = None
    ) -> None:
        self.failures.append((key, kind))


@pytest.fixture
def local_exit(monkeypatch):
    """把专线地址指到一个固定值（``CONFIG.custom_proxy`` 是它的 property）。"""
    monkeypatch.setattr(CONFIG, "my_ipv6_addr", _LOCAL_EXIT)
    monkeypatch.setattr(
        grpc_api, "proxy_health_store", _FakeHealthStore(cooling=False)
    )
    return _LOCAL_EXIT


# ---------------------------------------------------------------------------
# 1. 352 之后 30 分钟内专线优先
# ---------------------------------------------------------------------------


async def test_after_352_local_exit_is_preferred(local_exit):
    grpc = _make_grpc()
    grpc.latest_352_ts = int(time.time()) - 60
    assert (
        await grpc._decide_use_proxy(
            force_proxy=False,
            validate_token="",
            ipv6_proxy_weights=1,
            real_proxy_weights=1000,
        )
        is False
    )


async def test_after_352_falls_back_to_pool_when_local_prefix_cooling(
    monkeypatch, local_exit
):
    monkeypatch.setattr(
        grpc_api, "proxy_health_store", _FakeHealthStore(cooling=True)
    )
    grpc = _make_grpc()
    grpc.latest_352_ts = int(time.time()) - 60
    assert (
        await grpc._decide_use_proxy(
            force_proxy=False,
            validate_token="",
            ipv6_proxy_weights=1,
            real_proxy_weights=1000,
        )
        is True
    )


async def test_after_352_falls_back_to_pool_when_local_failed_in_task(local_exit):
    grpc = _make_grpc()
    grpc.latest_352_ts = int(time.time()) - 60
    token = bind_attempt_state(ProxyAttemptState())
    try:
        note_failed_proxy(grpc._local_exit_proxy_key())
        assert (
            await grpc._decide_use_proxy(
                force_proxy=False,
                validate_token="",
                ipv6_proxy_weights=1,
                real_proxy_weights=1000,
            )
            is True
        )
    finally:
        reset_attempt_state(token)


async def test_pool_connect_failures_switch_to_local_exit(local_exit):
    """免费池对 HTTP/2 整片不可用时要快速回专线，而不是每条请求白等超时。"""
    grpc = _make_grpc()
    threshold = max(1, CONFIG.grpc_device.pool_fail_streak_to_local)

    async def _decide() -> bool:
        return await grpc._decide_use_proxy(
            force_proxy=False,
            validate_token="",
            ipv6_proxy_weights=1,
            real_proxy_weights=1000,
        )

    for _ in range(threshold):
        grpc._note_pool_grpc_fail()
    assert await _decide() is False
    # 池代理成功一次即回到原口径（这里只看计数被清零）
    grpc._note_pool_grpc_ok()
    assert grpc._pool_grpc_fail_streak == 0
    # 窗口过期同样回到原口径，不会永久钉在专线
    for _ in range(threshold):
        grpc._note_pool_grpc_fail()
    grpc._pool_grpc_fail_ts -= BiliGrpc._POOL_FAIL_DECAY_WINDOW + 1
    assert grpc._pool_grpc_pinned() is False
    assert grpc._pool_grpc_fail_streak == 0


async def test_force_flags_keep_priority(local_exit):
    grpc = _make_grpc()
    grpc.latest_352_ts = int(time.time()) - 60
    assert (
        await grpc._decide_use_proxy(
            force_proxy=True,
            validate_token="",
            ipv6_proxy_weights=1,
            real_proxy_weights=1000,
        )
        is True
    )
    grpc.debug_mode = True
    assert (
        await grpc._decide_use_proxy(
            force_proxy=False,
            validate_token="",
            ipv6_proxy_weights=1,
            real_proxy_weights=1000,
        )
        is False
    )


# ---------------------------------------------------------------------------
# 2. gRPC 只接受 http / https 代理
# ---------------------------------------------------------------------------


async def test_get_random_channel_skips_socks_proxy(monkeypatch, local_exit):
    socks = _pool_proxy(
        {"sock4": "socks4://103.221.228.50:5678", "socks4": "socks4://103.221.228.50:5678"}
    )
    http_proxy = _pool_proxy({"http": "http://1.2.3.4:8080", "https": "http://1.2.3.4:8080"})
    pool = [socks, http_proxy]
    calls: list[int] = []

    async def _fake_get_available_proxy(*args, **kwargs):
        calls.append(1)
        return pool.pop(0), False

    monkeypatch.setattr(grpc_api, "get_available_proxy", _fake_get_available_proxy)

    proxy_tab, channel = await _make_grpc()._get_random_channel()
    assert proxy_tab is http_proxy
    assert channel is None
    assert len(calls) == 2


async def test_get_random_channel_falls_back_to_local_exit(monkeypatch, local_exit):
    def _socks(i: int) -> ProxyTab:
        return _pool_proxy(
            {
                "sock5": f"socks5://10.0.0.{i}:1080",
                "socks5": f"socks5://10.0.0.{i}:1080",
            },
            proxy_id=10 + i,
        )

    rows = [_socks(1), _socks(2)]

    async def _fake_get_available_proxy(*args, **kwargs):
        return (rows.pop(0) if rows else None), False

    monkeypatch.setattr(grpc_api, "get_available_proxy", _fake_get_available_proxy)

    proxy_tab, _ = await _make_grpc()._get_random_channel()
    assert proxy_tab.proxy == CONFIG.custom_proxy
    assert proxy_tab.computed_proxy_str == _LOCAL_EXIT


# ---------------------------------------------------------------------------
# 3. times_352 阈值走配置（默认 1 就重建）
# ---------------------------------------------------------------------------


def test_times_352_threshold_is_configurable(monkeypatch):
    md = MetaDataWrapper(
        md=(),
        buvid="buvid-test",
        expire_ts=int(time.time()) + 3600,
        version_name="8.0.0",
        session_id="session",
        guestid=1,
    )
    md.times_352 = 1
    monkeypatch.setattr(CONFIG.grpc_device, "max_352_times", 1)
    assert md.is_need_delete is True
    monkeypatch.setattr(CONFIG.grpc_device, "max_352_times", 2)
    assert md.is_need_delete is False


async def test_recreate_metadata_drops_device(monkeypatch, local_exit):
    grpc = _make_grpc()
    md = MetaDataWrapper(
        md=(),
        buvid="buvid-test",
        expire_ts=int(time.time()) + 3600,
        version_name="8.0.0",
        session_id="session",
        guestid=1,
        device=object(),
    )
    grpc.metadata_list = [md]
    grpc.queue_num = 1
    new_md = MetaDataWrapper(
        md=(),
        buvid="buvid-new",
        expire_ts=int(time.time()) + 3600,
        version_name="8.0.0",
        session_id="session",
        guestid=1,
    )
    dropped: list[object] = []

    class _FakePool:
        async def drop(self, device):
            dropped.append(device)

    monkeypatch.setattr(grpc_api, "get_pool", lambda: _FakePool())

    async def _fake_productor(proxy):
        return new_md

    monkeypatch.setattr(grpc, "metadata_productor", _fake_productor)

    assert await grpc._recreate_metadata(md) is new_md
    assert md not in grpc.metadata_list
    assert grpc.queue_num == 0
    assert dropped == [md.device]


def test_request_352_error_marks_token_acquisition():
    assert Request352Error("captcha down").token_acquired is False
    assert Request352Error("still 352", -352, token_acquired=True).token_acquired is True


# ---------------------------------------------------------------------------
# 4. 352 不再把代理在 MySQL 里置为不可用
# ---------------------------------------------------------------------------


async def test_handle_proxy_352_keeps_db_status_by_default(monkeypatch):
    proxy = _pool_proxy({"http": "http://1.2.3.4:8080", "https": "http://1.2.3.4:8080"})
    health = _FakeHealthStore()
    monkeypatch.setattr(ProxyEvent, "proxy_health_store", health)
    updates: list[tuple[int, int]] = []

    async def _fake_update(proxy_tab, change_score_num=10):
        updates.append((proxy_tab.proxy_id, change_score_num))
        return True

    monkeypatch.setattr(ProxyEvent.SQLHelper, "update_to_proxy_list", _fake_update)

    await ProxyEvent.handle_proxy_352(proxy_tab=proxy, mark_db_invalid=False)
    assert proxy.status == 0
    assert updates == []
    assert proxy.proxy["http"] in health.failures[0][0]

    await ProxyEvent.handle_proxy_352(proxy_tab=proxy, mark_db_invalid=True)
    assert proxy.status == -352
    assert updates == [(proxy.proxy_id, 10)]
