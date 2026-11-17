"""风控冷却维度：``(出口前缀) × (接口风控域 risk scope)``。

## 为什么需要 scope

B 站各接口的风控是**相互独立**的：web 与 gRPC 分开、topic 与 dynamic 分开。
历史实现只按「出口前缀」记账，一个接口被 -352 就把整条出口对所有接口一起冷却
（线上：gRPC DynDetail -352 → web get_web_topic 被本地闸门拦下，请求根本没发出去）。

本模块只负责把 url / func_name 归并成**稳定的 scope 串**：

- ``web`` 通道：``/x/topic/...`` → ``web:topic``；
- ``grpc`` 通道：``/bilibili.app.dynamic.v2.Dynamic/DynDetail`` → ``grpc:dynamic``；
- 未命中任何规则 → ``web:default`` / ``grpc:default``。

## 约束

- 归并粒度是「通道 + 一级业务域」，**禁止**用完整 url 当 key（会 key 爆炸）；
- 映射表全部来自 ``CONFIG.proxy_health.risk_scope_rules``，
  本模块**不包含任何站点域名 / 业务接口硬编码**；
- scope 只影响风控冷却维度：网络层冷却、在途并发、EWMA 仍按出口前缀 / 地址维度。
"""

from __future__ import annotations

import fnmatch
from typing import Final, Literal
from urllib.parse import urlsplit

from CONFIG import CONFIG

#: 全局桶：``scope=None`` / 灰度关闭时的回退维度（与历史数据完全兼容）
RISK_SCOPE_GLOBAL: Final[str] = "*"

#: 支持的通道（未知通道按 web 处理，避免脏值把 key 空间打散）
_CHANNELS: Final[tuple[str, ...]] = ("web", "grpc")
_NAMESPACED_PREFIXES: Final[tuple[str, ...]] = ("web", "grpc", "*")


def _normalized_path(url: str | None) -> str:
    """url → 只保留 path（去 scheme / host / query / fragment）。

    兼容完整 url、裸 path、以及 gRPC 的 ``func_name``（如
    ``/bilibili.app.dynamic.v2.Dynamic/DynDetail``）。
    """
    raw = str(url or "").strip()
    if not raw:
        return ""
    path = raw
    if "://" in raw:
        try:
            path = urlsplit(raw).path or ""
        except ValueError:
            # 非法 url（端口越界 / 方括号不匹配）：按原文兜底匹配
            path = raw
    path = path.split("?", 1)[0].split("#", 1)[0]
    if path and not path.startswith("/"):
        path = f"/{path}"
    return path


def _with_channel(channel: str, scope: str) -> str:
    """把配置里的业务域拼上通道；已显式带通道前缀时原样保留。"""
    scope = str(scope or "").strip()
    if not scope:
        return f"{channel}:default"
    if ":" in scope:
        prefix, rest = scope.split(":", 1)
        if prefix == "*":
            return f"{channel}:{rest or 'default'}"
        if prefix in _NAMESPACED_PREFIXES:
            return scope
    return f"{channel}:{scope}"


def risk_scope_of(url: str | None, *, channel: Literal["web", "grpc"]) -> str:
    """url / func_name → 风控域（``web:topic`` / ``grpc:dynamic`` / ``web:default``）。

    :param url: 完整 url、路径或 gRPC 方法名；``None`` 视为未命中规则；
    :param channel: ``web`` / ``grpc``；未知值按 ``web`` 处理。

    匹配口径：先对 path 做 ``fnmatch``，再对原始串兜底一次；
    规则顺序即优先级（配置里把更具体的规则放前面）。
    """
    ch = (channel or "web").lower()
    if ch not in _CHANNELS:
        ch = "web"
    raw = str(url or "").strip()
    path = _normalized_path(raw)
    rules = getattr(CONFIG.proxy_health, "risk_scope_rules", ()) or ()
    for pattern, scope in rules:
        if not pattern or not scope:
            continue
        pattern = str(pattern)
        if (path and fnmatch.fnmatchcase(path, pattern)) or (
            raw and fnmatch.fnmatchcase(raw, pattern)
        ):
            return _with_channel(ch, scope)
    return f"{ch}:default"


def scope_of_request(
    request_conf,
    url: str | None,
    *,
    channel: Literal["web", "grpc"] = "web",
) -> str:
    """本次请求的 scope：显式声明优先，否则按 url 自动推导。"""
    explicit = getattr(request_conf, "risk_scope", None) if request_conf is not None else None
    if explicit:
        return str(explicit)
    return risk_scope_of(url, channel=channel)


def scope_label(scope: str | None) -> str:
    """日志 / 异常文案里的 scope 展示（``None`` 表示全局桶）。"""
    return str(scope) if scope else RISK_SCOPE_GLOBAL


def is_global_scope(scope: str | None) -> bool:
    return not scope or str(scope) == RISK_SCOPE_GLOBAL


__all__ = [
    "RISK_SCOPE_GLOBAL",
    "is_global_scope",
    "risk_scope_of",
    "scope_label",
    "scope_of_request",
]
