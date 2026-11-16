"""风控传输层基线：TLS/UA 成对绑定、会话池、头序、节流。

覆盖 docs/be-bilibili-crawler-风控对抗加固计划书.md §4.5：

- ``UserAgentParser`` 组装顺序 == REA 抓包字典序；
- gRPC 真机抓包 22/22 的业务头序 == 字典序（作为 metadata 排序基线）；
- ``WEB_IMPERSONATE_TARGETS`` 全部为桌面 Chrome；
- 会话键 → 指纹 / UA 稳定且同主版本；
- AsyncSession 池的复用、LRU 淘汰、TTL、local_exit_only 豁免；
- 业务节流在 ``local_exit_only`` / ``skip_throttle`` / 关闭开关时返回 0。

明确**不测** H2 SETTINGS / WINDOW_UPDATE：curl_cffi 0.17.0 没有对应 API，
它们随 impersonate target 固定注入，无法作为变量断言（见计划书 §1.4）。
"""

import base64
import json
import re
import sys
from pathlib import Path

import pytest

_project_root = Path(__file__).resolve().parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from CONFIG import CONFIG
from Utils.GrpcUtils.UserAgentParser import UserAgentParser
from Utils.代理.BrowsingPacer import BrowsingPacer
from Utils.代理.SessionPool import WebSessionPool
from Utils.代理.SealedRequests import (
    WEB_IMPERSONATE_TARGETS,
    chrome_major_of,
    pick_web_fingerprint,
    resolve_web_profile,
)
from Utils.代理.http2_baseline import (
    BILI_WEB_HEADER_ORDER,
    header_names_in_order,
    is_alphabetical,
)
from Utils.代理.mdoel.RequestConf import RequestConf
from Utils.代理.session_profile import (
    profile_for_session_key,
    session_key_from_headers,
)

CHROME_WIN_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/153.0.0.0 Safari/537.36"
)
_GRPC_CAPTURE = (
    _project_root / "docs" / "grpc_reverse_evidence" / "capture" / "grpc_headers.jsonl"
)


# ---------------------------------------------------------------- header 顺序
def test_web_header_order_matches_rea_capture() -> None:
    headers = UserAgentParser(CHROME_WIN_UA).get_headers_dict()
    names = header_names_in_order(headers)
    assert is_alphabetical(headers)
    # 基线里的每个业务头都出现过（cookie / origin / referer 由调用方追加，
    # 这里用 extra_headers 覆盖一次完整形态）
    full = UserAgentParser(CHROME_WIN_UA).get_headers_dict(
        {
            "cookie": "buvid3=abc",
            "origin": "https://space.bilibili.com",
            "referer": "https://space.bilibili.com/1/dynamic",
        }
    )
    assert header_names_in_order(full) == BILI_WEB_HEADER_ORDER
    assert "user-agent" in names


def test_grpc_capture_header_order_is_alphabetical() -> None:
    """真机 22/22 的 gRPC 业务头顺序必须是字典序（metadata 排序基线）。"""
    lines = [line for line in _GRPC_CAPTURE.read_text().splitlines() if line.strip()]
    assert len(lines) == 22
    for line in lines:
        record = json.loads(line)
        names = []
        for key, _ in record["request_headers"]:
            if key.startswith("b64:"):
                key = base64.b64decode(key[4:]).decode()
            names.append(key)
        assert names == sorted(names), f"{record['path']} 头序不是字典序：{names}"


# ---------------------------------------------------------------- 指纹白名单
def test_all_web_targets_are_desktop_chrome() -> None:
    assert WEB_IMPERSONATE_TARGETS
    for target in WEB_IMPERSONATE_TARGETS:
        # 过滤后为空时回退裸 chrome；正常构建应全部是 chromeNNN
        assert target == "chrome" or re.fullmatch(r"chrome\d{3}", target), target


def test_profile_is_stable_and_ua_matches_target_major() -> None:
    first = profile_for_session_key("uuid-1234567890")
    second = profile_for_session_key("uuid-1234567890")
    assert first == second
    assert first.target in WEB_IMPERSONATE_TARGETS
    major = chrome_major_of(first.target)
    assert f"Chrome/{major}.0.0.0" in first.ua
    assert first.platform in first.ua


def test_different_session_keys_can_map_to_different_targets() -> None:
    targets = {
        profile_for_session_key(f"uuid-{i}").target
        for i in range(50)
    }
    # 50 个会话不应该全部撞在同一个指纹上（否则「按会话固定」没有意义）
    assert len(targets) >= 2


def test_pick_web_fingerprint_keeps_version_pairing() -> None:
    target, headers = pick_web_fingerprint({"user-agent": "old-ua"})
    major = chrome_major_of(target)
    assert headers is not None
    assert f"Chrome/{major}.0.0.0" in headers["user-agent"]


def test_cookie_header_session_key_parsing() -> None:
    assert session_key_from_headers({"cookie": "a=1; _uuid=ABC; buvid3=DEF"}) == "ABC"
    assert (
        session_key_from_headers([("cookie", "buvid3=DEF; x=1")]) == "DEF"
    )
    assert session_key_from_headers({}) is None


async def test_prepare_request_data_ua_matches_session_profile() -> None:
    """无 cookie 的业务请求也要用「会话键派生」的 UA，不能再用随机 UA。"""
    from Service.GrpcModule.Grpc.Bapi.RequestHandler import prepare_request_data

    _, headers = await prepare_request_data(
        RequestConf(is_use_cookie=False), {"cookie": "buvid3=SESSION-1"}
    )
    expected = profile_for_session_key("SESSION-1").ua
    assert headers["user-agent"] == expected


# ---------------------------------------------------------------- 会话池
class _FakeSession:
    def __init__(self, profile) -> None:
        self.profile = profile
        self.closed = False

    async def close(self) -> None:
        self.closed = True


def _pool(max_sessions: int = 2, ttl: float = 60.0) -> tuple[WebSessionPool, list]:
    created: list[_FakeSession] = []

    def factory(profile, _max_clients):
        session = _FakeSession(profile)
        created.append(session)
        return session

    pool = WebSessionPool(
        max_sessions=max_sessions,
        ttl_seconds=ttl,
        enabled=True,
        session_factory=factory,
    )
    return pool, created


async def test_session_pool_reuses_same_session() -> None:
    pool, created = _pool()
    profile = profile_for_session_key("uuid-reuse")
    async with pool.lease(profile) as first:
        pass
    async with pool.lease(profile) as second:
        pass
    assert first is second
    assert len(created) == 1
    assert pool.stats["reused"] == 1


async def test_session_pool_lru_evicts_idle_session() -> None:
    pool, created = _pool(max_sessions=2)
    profiles = [profile_for_session_key(f"uuid-lru-{i}") for i in range(3)]
    for profile in profiles:
        async with pool.lease(profile):
            pass
    assert len(pool._entries) == 2
    assert sum(session.closed for session in created) == 1


async def test_session_pool_ttl_closes_expired_session() -> None:
    pool, created = _pool(max_sessions=4, ttl=0.0)
    profile = profile_for_session_key("uuid-ttl")
    async with pool.lease(profile):
        pass
    async with pool.lease(profile):
        pass
    assert len(created) == 2
    assert created[0].closed is True


async def test_ephemeral_session_is_not_pooled() -> None:
    pool, created = _pool()
    profile = profile_for_session_key("uuid-ephemeral")
    async with pool.lease(profile, ephemeral=True) as session:
        pass
    assert session.closed is True
    assert pool.stats["sessions"] == 0
    assert len(created) == 1


async def test_pool_aclose_closes_all_sessions() -> None:
    pool, created = _pool(max_sessions=4)
    async with pool.lease(profile_for_session_key("uuid-close")):
        pass
    await pool.aclose()
    assert created[0].closed is True
    assert pool.stats["sessions"] == 0


# ---------------------------------------------------------------- 节流
def test_throttle_zero_for_local_exit_only_and_skip() -> None:
    pacer = BrowsingPacer()
    assert pacer.delay_for(RequestConf(local_exit_only=True)) == 0.0
    assert pacer.delay_for(RequestConf(skip_throttle=True)) == 0.0


def test_throttle_disabled_returns_zero(monkeypatch) -> None:
    monkeypatch.setattr(CONFIG.crawler_throttle, "business_throttle_enabled", False)
    assert BrowsingPacer().delay_for(RequestConf()) == 0.0


def test_business_throttle_within_configured_range(monkeypatch) -> None:
    cfg = CONFIG.crawler_throttle
    monkeypatch.setattr(cfg, "business_throttle_enabled", True)
    monkeypatch.setattr(cfg, "business_throttle_min_s", 0.1)
    monkeypatch.setattr(cfg, "business_throttle_max_s", 0.5)
    monkeypatch.setattr(cfg, "path_simulation_enabled", False)
    delay = BrowsingPacer().delay_for(RequestConf())
    assert 0.1 <= delay <= 0.5


def test_path_simulation_uses_longer_range(monkeypatch) -> None:
    cfg = CONFIG.crawler_throttle
    monkeypatch.setattr(cfg, "business_throttle_enabled", True)
    monkeypatch.setattr(cfg, "path_simulation_enabled", True)
    monkeypatch.setattr(cfg, "path_simulation_prob", 1.0)
    monkeypatch.setattr(cfg, "path_simulation_min_s", 0.8)
    monkeypatch.setattr(cfg, "path_simulation_max_s", 3.0)
    delay = BrowsingPacer().delay_for(RequestConf())
    assert 0.8 <= delay <= 3.0


# ---------------------------------------------------------------- 配置回滚
def test_pin_fingerprint_switch_falls_back_to_random(monkeypatch) -> None:
    monkeypatch.setattr(CONFIG.web_session, "pin_fingerprint", False)
    profiles = {resolve_web_profile("uuid-x") for _ in range(20)}
    assert len(profiles) >= 2
