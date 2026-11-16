# -*- coding: utf-8 -*-
"""Web cookie 池的 Redis 持久化（crawler 跨进程 / 重启复用）。

范式与 ``Utils/GrpcUtils/metadata/device_pool.py`` 一致：

- ``<prefix><uuid>``  string，存 ``CookieWrapper.to_record()`` 的 JSON；
- ``<prefix>lru``     zset，member=uuid，score=最后访问时间（LRU 淘汰依据）；
- 写入用 ``SET ... EX``，TTL = cookie 剩余有效期，过期 key 自然消失。

与设备池的关键差别：**本模块永不阻塞业务**。Redis 超时 / 连接失败只记一条限流
warning，调用方按「未命中」处理并回退到进程内存池。设备池基类的 ``@retry``
会在异常时无限重试，这里不能沿用。
"""

from __future__ import annotations

import json
import time

import redis.asyncio as aioredis

from CONFIG import CONFIG
from log.base_log import bapi_log
from Models.AntiRisk.Bili.WebCookie import CookieWrapper

#: Redis 异常告警限流（秒）
_WARN_INTERVAL = 60.0


class CookieRedisStore:
    def __init__(
        self,
        *,
        prefix: str | None = None,
        max_entries: int | None = None,
        timeout: float | None = None,
        client: aioredis.Redis | None = None,
    ) -> None:
        cfg = CONFIG.bili_cookie
        self.prefix = prefix or cfg.redis_key_prefix
        self.max_entries = max(1, int(max_entries or cfg.redis_pool_size))
        self.timeout = float(timeout or cfg.redis_timeout)
        self._client = client
        self._pool: aioredis.ConnectionPool | None = None
        self._last_warn_ts = 0.0

    # ---- 连接 ----
    def _get_client(self) -> aioredis.Redis:
        if self._client is not None:
            return self._client
        conf = CONFIG.database.commStorageRedis
        if self._pool is None:
            self._pool = aioredis.ConnectionPool.from_url(
                f"redis://:{conf.pwd}@{conf.host}:{conf.port}/{conf.db}?decode_responses=True",
                socket_timeout=self.timeout,
                socket_connect_timeout=self.timeout,
                max_connections=16,
            )
        return aioredis.Redis(connection_pool=self._pool)

    def _warn(self, action: str, err: BaseException) -> None:
        now = time.monotonic()
        if now - self._last_warn_ts < _WARN_INTERVAL:
            bapi_log.debug(f"cookie Redis {action}失败（限流中）：{type(err).__name__}: {err}")
            return
        self._last_warn_ts = now
        bapi_log.warning(
            f"cookie Redis {action}失败，已降级为进程内存池：{type(err).__name__}: {err}"
        )

    def _key(self, uuid: str) -> str:
        return f"{self.prefix}{uuid}"

    @property
    def _lru_key(self) -> str:
        return f"{self.prefix}lru"

    @staticmethod
    def _uuid_of(cookie: CookieWrapper) -> str:
        return cookie.ck.gen_web_cookie_params.uuid

    # ---- 读 ----
    async def load(self, uuid: str) -> CookieWrapper | None:
        if not uuid:
            return None
        try:
            raw = await self._get_client().get(self._key(uuid))
        except Exception as e:  # noqa: BLE001 降级为未命中
            self._warn("读取", e)
            return None
        return self._decode(uuid, raw)

    def _decode(self, uuid: str, raw: str | None) -> CookieWrapper | None:
        if not raw:
            return None
        try:
            cookie = CookieWrapper.from_record(json.loads(raw))
        except (ValueError, TypeError, KeyError) as e:
            bapi_log.warning(f"cookie Redis 记录损坏，已剔除 {uuid}：{type(e).__name__} {e}")
            return None
        if cookie.expire_ts <= int(time.time()) or not cookie.able:
            return None
        return cookie

    async def random_usable(self) -> CookieWrapper | None:
        """在 LRU 集合里随机取若干候选，返回第一个仍可用的 cookie。"""
        try:
            client = self._get_client()
            candidates = await client.zrandmember(self._lru_key, 10)
            if not candidates:
                return None
            keys = [self._key(uuid) for uuid in candidates]
            values = await client.mget(keys)
            stale: list[str] = []
            picked: tuple[str, CookieWrapper] | None = None
            for uuid, raw in zip(candidates, values):
                cookie = self._decode(uuid, raw)
                if cookie is None:
                    stale.append(uuid)
                    continue
                if picked is None:
                    picked = (uuid, cookie)
            if stale:
                await self._drop_many(stale)
            if picked is None:
                return None
            uuid, cookie = picked
            await client.zadd(self._lru_key, {uuid: int(time.time())})
            return cookie
        except Exception as e:  # noqa: BLE001 降级为未命中
            self._warn("读取随机 cookie", e)
            return None

    async def size(self) -> int:
        try:
            return int(await self._get_client().zcard(self._lru_key) or 0)
        except Exception as e:  # noqa: BLE001
            self._warn("统计", e)
            return 0

    # ---- 写 ----
    async def save(self, cookie: CookieWrapper) -> bool:
        if not cookie.able:
            return False
        ttl = int(cookie.expire_ts - time.time())
        if ttl <= 0:
            return False
        uuid = self._uuid_of(cookie)
        try:
            client = self._get_client()
            await client.set(self._key(uuid), json.dumps(cookie.to_record()), ex=ttl)
            await client.zadd(self._lru_key, {uuid: int(time.time())})
            await self._trim(client)
            return True
        except Exception as e:  # noqa: BLE001
            self._warn("写入", e)
            return False

    async def drop(self, uuid: str) -> None:
        try:
            await self._drop_many([uuid])
        except Exception as e:  # noqa: BLE001
            self._warn("剔除", e)

    async def _drop_many(self, uuids: list[str]) -> None:
        if not uuids:
            return
        client = self._get_client()
        async with client.pipeline(transaction=False) as pipe:
            for uuid in uuids:
                pipe.delete(self._key(uuid))
                pipe.zrem(self._lru_key, uuid)
            await pipe.execute()

    async def _trim(self, client: aioredis.Redis) -> None:
        total = int(await client.zcard(self._lru_key) or 0)
        overflow = total - self.max_entries
        if overflow <= 0:
            return
        victims = await client.zrange(self._lru_key, 0, overflow - 1)
        if victims:
            await self._drop_many(list(victims))
