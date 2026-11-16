"""按会话复用的 ``curl_cffi.AsyncSession`` 池（LRU + TTL）。

为什么需要池：``AsyncSession`` 自带 curl 句柄池与连接复用，每次请求新建 session
等于每次重新握手 —— 既慢，又让「同一个 buvid 会话」在传输层看起来像不断新开的
客户端。真实浏览器会复用连接，所以业务请求应该复用同一个 session。

边界与豁免：

- **按会话键复用**：池键取 ``WebProfile.session_key``（cookie 的 ``_uuid``），
  同一会话固定使用同一个 impersonate target（见 ``session_profile``）；
- **LRU + TTL**：会话空闲超过 ``WEB_SESSION__POOL_TTL_SECONDS`` 秒关闭；
  池满时淘汰最久未使用且当前空闲的会话；
- **local_exit_only 豁免**：cookie / metadata 激活链路是一次性握手，
  复用连接没有收益，还有跨出口串味的风险，因此由调用方传 ``ephemeral=True``，
  请求结束立即关闭，不进入池。

Redis / 事件循环失败都不能影响业务：本模块内部吞掉关闭异常，只记 debug 日志。
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Any, Callable

from curl_cffi import CurlHttpVersion
from curl_cffi import requests

from CONFIG import CONFIG
from log.base_log import request_with_proxy_logger
from Utils.代理.session_profile import WebProfile

#: 是否读取 ``HTTP_PROXY`` / ``HTTPS_PROXY`` 等环境变量。显式关闭：容器 / 宿主机
#: 若设置了这些变量，curl_cffi 会把它们当成默认代理，绕过我们显式传入的出口。
TRUST_ENV = False


@dataclass
class _Entry:
    session: Any
    profile: WebProfile
    created_at: float
    last_used: float
    in_use: int = 0
    closed: bool = False


def _new_session(profile: WebProfile, max_clients: int) -> Any:
    return requests.AsyncSession(
        max_clients=max(1, max_clients),
        allow_redirects=True,
        timeout=30,
        verify=True,
        trust_env=TRUST_ENV,
        impersonate=profile.target,
        http_version=CurlHttpVersion.V2_0,
    )


class WebSessionPool:
    """会话池：``lease()`` 获取、退出时归还；``ephemeral`` 不落池。"""

    def __init__(
        self,
        *,
        max_sessions: int | None = None,
        ttl_seconds: float | None = None,
        max_clients: int | None = None,
        enabled: bool | None = None,
        session_factory: Callable[[WebProfile, int], Any] | None = None,
    ) -> None:
        self._max_sessions = max_sessions
        self._ttl_seconds = ttl_seconds
        self._max_clients = max_clients
        self._enabled = enabled
        self._factory = session_factory or _new_session
        self._entries: dict[str, _Entry] = {}
        self._lock = asyncio.Lock()
        self._closed = False
        self._created_total = 0
        self._reused_total = 0

    # ---- 配置（运行期读取，便于测试 / env 热改） ----
    @property
    def enabled(self) -> bool:
        return bool(CONFIG.web_session.pool_enabled if self._enabled is None else self._enabled)

    @property
    def max_sessions(self) -> int:
        value = CONFIG.web_session.pool_max_sessions if self._max_sessions is None else self._max_sessions
        return max(1, int(value))

    @property
    def ttl_seconds(self) -> float:
        value = CONFIG.web_session.pool_ttl_seconds if self._ttl_seconds is None else self._ttl_seconds
        return max(0.0, float(value))

    @property
    def max_clients(self) -> int:
        value = CONFIG.web_session.pool_max_clients if self._max_clients is None else self._max_clients
        return max(1, int(value))

    @property
    def stats(self) -> dict[str, int]:
        return {
            "sessions": len(self._entries),
            "created": self._created_total,
            "reused": self._reused_total,
        }

    async def _close(self, session: Any) -> None:
        try:
            await session.close()
        except Exception as e:  # noqa: BLE001 关闭失败不影响业务
            request_with_proxy_logger.debug(
                f"关闭 web 会话失败（忽略）：{type(e).__name__}: {e}"
            )

    async def _drop_expired_locked(self, now: float) -> list[Any]:
        """在锁内挑出过期且空闲的会话，返回待关闭列表（关闭在锁外做）。"""
        pending: list[Any] = []
        for key, entry in list(self._entries.items()):
            if entry.in_use == 0 and (now - entry.last_used) > self.ttl_seconds:
                entry.closed = True
                self._entries.pop(key, None)
                pending.append(entry.session)
        return pending

    def _trim_locked(self) -> list[Any]:
        """池满时按 LRU 淘汰空闲会话；返回待关闭列表。"""
        pending: list[Any] = []
        while len(self._entries) > self.max_sessions:
            candidates = [e for e in self._entries.values() if e.in_use == 0]
            if not candidates:
                break
            victim = min(candidates, key=lambda e: e.last_used)
            victim.closed = True
            self._entries.pop(victim.profile.session_key, None)
            pending.append(victim.session)
        return pending

    async def _acquire(self, profile: WebProfile, ephemeral: bool) -> tuple[Any, _Entry | None]:
        if ephemeral or not self.enabled or self._closed:
            self._created_total += 1
            return self._factory(profile, self.max_clients), None

        pending: list[Any] = []
        async with self._lock:
            now = time.monotonic()
            pending.extend(await self._drop_expired_locked(now))
            entry = self._entries.get(profile.session_key)
            if (
                entry is not None
                and not entry.closed
                and entry.profile.target == profile.target
            ):
                entry.in_use += 1
                entry.last_used = now
                self._reused_total += 1
                for session in pending:
                    await self._close(session)
                return entry.session, entry

            if entry is not None:
                # 同一会话键换了 target（配置变更 / 显式 impersonate）：旧连接必须废弃
                entry.closed = True
                self._entries.pop(profile.session_key, None)
                pending.append(entry.session)
            self._created_total += 1
            entry = _Entry(
                session=self._factory(profile, self.max_clients),
                profile=profile,
                created_at=now,
                last_used=now,
                in_use=1,
            )
            self._entries[profile.session_key] = entry
            pending.extend(self._trim_locked())
        for session in pending:
            await self._close(session)
        return entry.session, entry

    async def _release(self, entry: _Entry | None, session: Any) -> None:
        if entry is None:
            await self._close(session)
            return
        async with self._lock:
            entry.in_use = max(0, entry.in_use - 1)
            entry.last_used = time.monotonic()
            close_now = entry.closed
        if close_now:
            await self._close(session)

    def lease(self, profile: WebProfile, *, ephemeral: bool = False) -> "_Lease":
        """获取一个租约：``async with pool.lease(profile) as session: ...``。

        用 ``@asynccontextmanager`` 会引入额外生成器开销，这里直接实现
        ``__aenter__`` / ``__aexit__`` 的轻量对象，热路径只多两次 await。
        """
        return _Lease(self, profile, ephemeral)

    async def aclose(self) -> None:
        """进程退出：关闭全部会话。"""
        self._closed = True
        async with self._lock:
            entries = list(self._entries.values())
            self._entries.clear()
        for entry in entries:
            entry.closed = True
            await self._close(entry.session)


class _Lease:
    __slots__ = ("_pool", "_profile", "_ephemeral", "_session", "_entry")

    def __init__(self, pool: WebSessionPool, profile: WebProfile, ephemeral: bool) -> None:
        self._pool = pool
        self._profile = profile
        self._ephemeral = ephemeral
        self._session: Any = None
        self._entry: _Entry | None = None

    async def __aenter__(self) -> Any:
        self._session, self._entry = await self._pool._acquire(
            self._profile, self._ephemeral
        )
        return self._session

    async def __aexit__(self, *exc_info) -> None:
        if self._session is not None:
            await self._pool._release(self._entry, self._session)


#: 进程级单例：``MYASYNCHTTPX`` 与 lifespan shutdown 共用
web_session_pool = WebSessionPool()
