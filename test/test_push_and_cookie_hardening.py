# -*- coding: utf-8 -*-
"""推送 / cookie / 日志脱敏的回归单测（不连网络、不连 Redis）。

覆盖 2026-11 线上问题对应的三条硬约束：

1. ``CookieWrapper`` 的 -352 计数按**出口前缀**去重：一个坏出口不能把整个 cookie 池废掉；
2. ``mask_secret`` 必须把 push_key / token / cookie 打码（含文本混排形态）；
3. ``parse_dynamic_for_push`` 字段缺失返回 None、正文截断 + 表格转义（不再 AttributeError）。
"""

from bili_common.core.log_mask import mask_secret, mask_text
from Models.AntiRisk.Bili.WebCookie import BiliWebCookie, CookieWrapper
from Service.GrpcModule.GrpcSrc.监控up动态.bili_dynamic_monitor import (
    parse_dynamic_for_push,
)
from Utils.加密.utils import GenWebCookieParams


def _cookie() -> CookieWrapper:
    ck = BiliWebCookie(
        gen_web_cookie_params=GenWebCookieParams(
            ua="test-ua",
            window_h=1080,
            window_w=1920,
            avail_w=1920,
            avail_h=1080,
        ),
        buvid3="buvid3",
        b_nut="1",
    )
    return CookieWrapper(ck=ck, ua="test-ua", expire_ts=2**31, max_352_prefixes=3)


def test_same_exit_prefix_only_counts_once():
    """同一出口前缀反复 -352 只记 1 次，cookie 仍然可用。"""
    cw = _cookie()
    for _ in range(20):
        cw.note_352("2001:db8:1::")
    assert cw.times_352 == 1
    assert cw.able is True


def test_multiple_exit_prefixes_disable_cookie():
    """多个不同出口都报 -352，才判定 cookie 本身不可用。"""
    cw = _cookie()
    for prefix in ("2001:db8:1::", "2001:db8:2::", "2001:db8:3::"):
        cw.note_352(prefix)
    assert cw.times_352 == 3
    assert cw.able is False


def test_mask_secret_hides_push_key_and_tokens():
    masked = mask_secret(
        {
            "push_key": "jPtlm4HGbPICyzJN9601pushplus.plus/send",
            "push_plus_token": "abc123",
            "title": "正常标题",
            "proxy_key": "http://1.2.3.4:8080",
        }
    )
    assert masked["push_key"] == "****"
    assert masked["push_plus_token"] == "****"
    # 非敏感字段保持原值，便于排查
    assert masked["title"] == "正常标题"
    assert masked["proxy_key"] == "http://1.2.3.4:8080"
    assert "jPtlm4HGbPICyzJN9601" not in str(masked)


def test_mask_text_handles_mixed_text():
    text = "PushMe 推送失败：url=https://push.i-i.me body={'push_key': 'SECRET-KEY'} Cookie: SESSDATA=xyz"
    masked = mask_text(text)
    assert "SECRET-KEY" not in masked
    assert "SESSDATA=xyz" not in masked
    assert "push_key" in masked  # 字段名保留，便于定位


def test_parse_dynamic_for_push_returns_none_without_id():
    assert parse_dynamic_for_push({}) is None
    assert parse_dynamic_for_push({"extend": {"origName": "up"}}) is None


def test_parse_dynamic_for_push_truncates_and_escapes():
    item = {
        "cardType": "word",
        "extend": {
            "dynIdStr": "1122334455",
            "origName": "up|名字",
            "uid": 123,
            "opusSummary": {
                "title": {"text": {"nodes": [{"rawText": "a|b" * 300}]}},
            },
        },
    }
    view = parse_dynamic_for_push(item, content_max_length=50)
    assert view is not None
    assert view.dynamic_id == "1122334455"
    assert view.author_space == "https://space.bilibili.com/123/dynamic"
    # 正文被截断，且表格分隔符已转义
    assert len(view.content) <= 51
    assert "|" not in view.content.replace("&#124;", "")
    # 昵称里的 | 也必须转义，否则整张 markdown 表会错列
    markdown = view.to_markdown()
    assert "up&#124;名字" in markdown


def test_parse_dynamic_for_push_uses_interface_pub_time():
    item = {
        "cardType": "word",
        "extend": {
            "dynIdStr": "1",
            "origName": "up",
            "uid": 1,
            "pubTs": 1700000000,
            "desc": "内容",
        },
    }
    view = parse_dynamic_for_push(item)
    assert view is not None
    # 接口字段优先：不再用动态 id 的魔数反推
    assert view.pub_time_text.startswith("2023-11-1")


# ---------------------------------------------------------------------------
# 首轮基线：新加的 uid 不能把历史动态一次性全推出去
# ---------------------------------------------------------------------------


def _dyn(dyn_id: str) -> dict:
    return {
        "cardType": "word",
        "extend": {"dynIdStr": dyn_id, "origName": "up", "uid": 1, "desc": f"内容{dyn_id}"},
    }


async def _run_monitor_rounds(monkeypatch, responses: list[list[dict]], rounds: int = 3):
    """跑若干轮 monitor_main（全模拟 grpc / 推送），返回被推送的动态 id 列表。"""
    import asyncio

    from Service.GrpcModule.GrpcSrc.监控up动态 import bili_dynamic_monitor as mod

    monitor = mod.BiliSpaceMonitor()
    monitor.sep_time = 0.01
    monitor.monitor_uid_list = [{"uid": 1, "latest_dynamic_id_list": []}]

    pushed: list[str] = []
    calls = {"n": 0}

    async def _fake_pull(_uid):
        idx = min(calls["n"], len(responses) - 1)
        calls["n"] += 1
        return responses[idx]

    async def _fake_push(view):
        pushed.append(view.dynamic_id)

    async def _noop_save():
        return None

    monkeypatch.setattr(monitor, "_pull_once", _fake_pull)
    monkeypatch.setattr(monitor, "push_dyn_notify", _fake_push)
    monkeypatch.setattr(monitor, "save_monitor_uid_list", _noop_save)

    task = asyncio.create_task(monitor.monitor_main(1))
    try:
        await asyncio.sleep(0.05 * rounds)
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
    return pushed, monitor


async def test_first_round_only_records_baseline(monkeypatch):
    """首轮只记录基线 id，不推送；后续出现的新动态才推送。"""
    history = [_dyn("100"), _dyn("101"), _dyn("102")]
    with_new = [_dyn("199")] + history
    pushed, monitor = await _run_monitor_rounds(
        monkeypatch, [history, with_new, with_new], rounds=3
    )
    # 历史 3 条一条都不推，只有第 2 轮新出现的 199 被推送一次
    assert pushed == ["199"]
    baseline = [info for info in monitor.monitor_uid_list if info["uid"] == 1][0]
    assert set(baseline["latest_dynamic_id_list"]) >= {"100", "101", "102", "199"}


async def test_pull_failure_does_not_kill_monitor(monkeypatch):
    """单次拉取失败只记 warning 并继续，协程不退出（历史上会直接打死该 uid）。"""
    import asyncio

    from Service.GrpcModule.GrpcSrc.监控up动态 import bili_dynamic_monitor as mod

    monitor = mod.BiliSpaceMonitor()
    monitor.sep_time = 0.01
    monitor.monitor_uid_list = [{"uid": 1, "latest_dynamic_id_list": []}]
    calls = {"n": 0}

    async def _flaky(_uid):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("模拟网络失败")
        return []

    monkeypatch.setattr(monitor, "_pull_once", _flaky)
    monkeypatch.setattr(monitor, "save_monitor_uid_list", lambda: asyncio.sleep(0))

    task = asyncio.create_task(monitor.monitor_main(1))
    await asyncio.sleep(0.06)
    alive = not task.done()
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    assert alive, "拉取失败后监控协程不应退出"
    assert calls["n"] >= 2
