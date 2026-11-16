"""Web cookie 池 Redis 持久化单测（不连真实 Redis）。

用内存假客户端覆盖 ``CookieRedisStore`` 用到的命令，验证：

- ``CookieWrapper`` 记录序列化 / 反序列化字段一致（含 -352 计数与 buvid_fp）；
- 换一个 store 实例（模拟进程重启）仍能读回同一 cookie；
- 过期 / 不可用 cookie 不会被随机命中，并从 LRU 集合剔除；
- 池容量上限生效；
- ``get_bili_cookie`` 优先命中 Redis，且不再走现场生成链路。
"""

import json
import sys
import time
from pathlib import Path

import pytest

_project_root = Path(__file__).resolve().parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from Models.AntiRisk.Bili.CookieRedisStore import CookieRedisStore
from Models.AntiRisk.Bili.WebCookie import BiliWebCookie, CookieWrapper
from Service.GrpcModule.Grpc.Bapi import BiliCookie
from Utils.加密.utils import GenWebCookieParams, uuidInfoc


class _FakePipeline:
    def __init__(self, backend: "FakeRedis") -> None:
        self._backend = backend
        self._ops: list[tuple] = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc_info) -> None:
        return None

    def delete(self, key: str):
        self._ops.append(("delete", key))
        return self

    def zrem(self, key: str, member: str):
        self._ops.append(("zrem", key, member))
        return self

    async def execute(self) -> list:
        for op in self._ops:
            if op[0] == "delete":
                self._backend.strings.pop(op[1], None)
            else:
                self._backend.zsets.setdefault(op[1], {}).pop(op[2], None)
        self._ops.clear()
        return []


class FakeRedis:
    """只实现 CookieRedisStore 用到的那几个命令。"""

    def __init__(self) -> None:
        self.strings: dict[str, str] = {}
        self.zsets: dict[str, dict[str, float]] = {}

    async def get(self, key: str):
        return self.strings.get(key)

    async def set(self, key: str, value: str, ex: int | None = None):
        self.strings[key] = value
        return True

    async def mget(self, keys):
        return [self.strings.get(key) for key in keys]

    async def zadd(self, key: str, mapping: dict):
        self.zsets.setdefault(key, {}).update(mapping)
        return len(mapping)

    async def zcard(self, key: str) -> int:
        return len(self.zsets.get(key, {}))

    async def zrandmember(self, key: str, count: int = 1):
        members = list(self.zsets.get(key, {}))
        return members[:count]

    async def zrange(self, key: str, start: int, end: int):
        members = sorted(self.zsets.get(key, {}).items(), key=lambda item: item[1])
        names = [name for name, _ in members]
        return names[start : end + 1] if end >= 0 else names[start:]

    def pipeline(self, transaction: bool = True):
        return _FakePipeline(self)


def _cookie(session_key: str | None = None, ttl: int = 3600) -> CookieWrapper:
    key = session_key or uuidInfoc.gen()
    ck = BiliWebCookie(
        gen_web_cookie_params=GenWebCookieParams(
            uuid=key,
            ua="Mozilla/5.0 test Chrome/152.0.0.0",
            window_h=1080,
            window_w=1920,
            avail_w=1920,
            avail_h=1080,
        ),
        buvid3=f"buvid3-{key}",
        buvid4=f"buvid4-{key}",
        bili_ticket="ticket-value",
        bili_ticket_expires=int(time.time()) + 3600,
        b_nut="1",
    )
    return CookieWrapper(
        ck=ck,
        ua=ck.gen_web_cookie_params.ua,
        expire_ts=int(time.time()) + ttl,
        max_352_prefixes=3,
    )


def test_cookie_record_roundtrip_keeps_business_fields() -> None:
    cookie = _cookie("uuid-roundtrip")
    cookie.note_352("2001:db8:1::")
    restored = CookieWrapper.from_record(cookie.to_record())

    assert restored.ck.buvid3 == cookie.ck.buvid3
    assert restored.ck.buvid4 == cookie.ck.buvid4
    assert restored.ck.bili_ticket == cookie.ck.bili_ticket
    assert restored.ck.gen_web_cookie_params.uuid == "uuid-roundtrip"
    assert restored.ck.buvid_fp == cookie.ck.buvid_fp
    assert restored.ua == cookie.ua
    assert restored.expire_ts == cookie.expire_ts
    assert restored.times_352 == 1
    assert restored.times_352_by_prefix == {"2001:db8:1::": 1}


async def test_store_survives_new_instance() -> None:
    backend = FakeRedis()
    cookie = _cookie("uuid-restart")
    writer = CookieRedisStore(prefix="test:ck:", max_entries=10, client=backend)
    assert await writer.save(cookie) is True

    # 模拟进程重启：新 store 实例 + 同一 Redis 后端
    reader = CookieRedisStore(prefix="test:ck:", max_entries=10, client=backend)
    restored = await reader.load("uuid-restart")
    assert restored is not None
    assert restored.ck.buvid3 == cookie.ck.buvid3
    assert restored.ck.gen_web_cookie_params.uuid == "uuid-restart"


async def test_random_usable_skips_expired_and_cleans_lru() -> None:
    backend = FakeRedis()
    store = CookieRedisStore(prefix="test:ck:", max_entries=10, client=backend)
    expired = _cookie("uuid-expired", ttl=-10)
    backend.strings[f"test:ck:uuid-expired"] = json.dumps(expired.to_record())
    backend.zsets["test:ck:lru"] = {"uuid-expired": int(time.time())}

    assert await store.random_usable() is None
    assert await store.size() == 0


async def test_store_trim_keeps_max_entries() -> None:
    backend = FakeRedis()
    store = CookieRedisStore(prefix="test:ck:", max_entries=2, client=backend)
    for index in range(3):
        await store.save(_cookie(f"uuid-trim-{index}"))
    assert await store.size() == 2


async def test_get_bili_cookie_prefers_redis(monkeypatch) -> None:
    backend = FakeRedis()
    store = CookieRedisStore(prefix="test:ck:", max_entries=10, client=backend)
    remote = _cookie("uuid-from-redis")
    await store.save(remote)

    monkeypatch.setattr(BiliCookie, "get_cookie_store", lambda: store)
    monkeypatch.setattr(BiliCookie, "_fake_cookie_list", [])

    async def _must_not_generate():
        raise AssertionError("Redis 命中时不应再走现场生成链路")

    monkeypatch.setattr(BiliCookie, "_next_bili_cookie", _must_not_generate)

    picked = await BiliCookie.get_bili_cookie()
    assert picked.ck.gen_web_cookie_params.uuid == "uuid-from-redis"
    assert picked.ck.buvid3 == remote.ck.buvid3


async def test_risk_counter_is_persisted() -> None:
    backend = FakeRedis()
    store = CookieRedisStore(prefix="test:ck:", max_entries=10, client=backend)
    cookie = _cookie("uuid-risk")
    await store.save(cookie)
    cookie.note_352("2001:db8:2::")
    await store.save(cookie)

    restored = await store.load("uuid-risk")
    assert restored is not None
    assert restored.times_352 == 1


async def test_store_degrades_when_redis_raises() -> None:
    class BrokenRedis(FakeRedis):
        async def get(self, key):
            raise ConnectionError("redis down")

        async def zrandmember(self, key, count=1):
            raise ConnectionError("redis down")

    store = CookieRedisStore(prefix="test:ck:", max_entries=10, client=BrokenRedis())
    assert await store.load("any") is None
    assert await store.random_usable() is None
