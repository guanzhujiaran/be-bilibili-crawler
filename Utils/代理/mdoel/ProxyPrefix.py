"""代理出口的网段（CIDR 前缀）归并。

## 为什么必须按前缀归并

风控按网段聚合：IPv6 常见 `/64`、IPv4 常见 `/24`。自建 go-proxy 出口每次拨号在同一个 `/64`
内随机选地址，换个地址对服务端来说还是同一个来源（日志里依旧整段 `-352`）；public 池也一样，
实测 hot 936 条只来自 455 个 `/24`，单个 `/24` 最多 108 条 —— 整段被封时这些「好代理」同时报废。

所以健康度（连续失败 / EWMA / 冷却）与并发配额必须按前缀算，
地址级明细只保留给日志与排障。

## 口径（长度可配，见 ``CONFIG.proxy_health.prefix_v4_length`` / ``prefix_v6_length``）

| 输入 | 前缀 |
|---|---|
| IPv4 | `v4:45.74.31.0/24` |
| IPv6 | `v6:2001:db8:1:2::/64` |
| 域名 | `host:proxy.example.com`（无法判断托管段，按主机归并） |
| 无法解析 | `raw:<原串>`（等价于「每个代理自成一档」，不误伤别人） |

前缀串带命名空间前缀，Redis key 不会与地址级 key 撞车；本模块**不含任何站点 / 业务判断**。
"""

import ipaddress
from urllib.parse import urlsplit

from pydantic import Field

from Models.base.custom_pydantic import CustomBaseModel

#: 前缀串的命名空间（同时用于区分 v4 / v6 / 域名 / 兜底）
_PREFIX_NS_V4 = "v4"
_PREFIX_NS_V6 = "v6"
_PREFIX_NS_HOST = "host"
_PREFIX_NS_RAW = "raw"

#: 端口上限（``urlsplit`` 对超范围端口会抛 ValueError，这里自己兜住）
_MAX_PORT = 65535


class ProxyEndpoint(CustomBaseModel):
    """代理串解析出的出口端点。

    ``ip`` 为 ``None`` 表示 host 是域名（或非 IP 文本），此时按主机名归并。
    """

    host: str = Field(description="主机名或 IP 文本（IPv6 已去掉方括号）")
    port: int | None = Field(default=None, description="端口，仅用于日志，不参与归并")
    ip: ipaddress.IPv4Address | ipaddress.IPv6Address | None = Field(
        default=None, description="解析出的 IP；域名 / 非法文本为 None"
    )

    @property
    def is_ipv6(self) -> bool:
        return isinstance(self.ip, ipaddress.IPv6Address)

    @property
    def is_ipv4(self) -> bool:
        return isinstance(self.ip, ipaddress.IPv4Address)


def parse_proxy_endpoint(proxy_key: str) -> ProxyEndpoint | None:
    """代理串 → 出口端点；无法解析返回 ``None``。

    兼容 ``http://1.2.3.4:8080`` / ``socks5://[2001:db8::1]:1080`` / ``1.2.3.4:8080`` /
    带认证的 ``http://user:pass@host:port``。认证信息与端口都不参与前缀归并。
    """
    raw = (proxy_key or "").strip()
    if not raw:
        return None
    # urlsplit 需要 scheme 才能正确切出 hostname；裸 ip:port 补一个占位 scheme
    candidate = raw if "://" in raw else f"http://{raw}"
    try:
        parts = urlsplit(candidate)
        host = parts.hostname
        port = parts.port
    except ValueError:
        # 端口越界 / IPv6 方括号不匹配等异常串：直接按兜底口径处理
        return None
    if not host:
        return None
    if port is not None and not (0 < port <= _MAX_PORT):
        return None
    host = host.strip().strip("[]").lower()
    if not host:
        return None
    parsed_ip: ipaddress.IPv4Address | ipaddress.IPv6Address | None
    try:
        parsed_ip = ipaddress.ip_address(host)
    except ValueError:
        parsed_ip = None
    return ProxyEndpoint(host=host, port=port, ip=parsed_ip)


def prefix_of_endpoint(
    endpoint: ProxyEndpoint, v4_length: int, v6_length: int
) -> str:
    """端点 → 命名空间前缀串（纯计算，无 CONFIG 依赖，便于单测）。"""
    if isinstance(endpoint.ip, ipaddress.IPv6Address):
        length = min(max(int(v6_length), 1), 128)
        network = ipaddress.ip_network(f"{endpoint.ip}/{length}", strict=False)
        return f"{_PREFIX_NS_V6}:{network.with_prefixlen}"
    if isinstance(endpoint.ip, ipaddress.IPv4Address):
        length = min(max(int(v4_length), 1), 32)
        network = ipaddress.ip_network(f"{endpoint.ip}/{length}", strict=False)
        return f"{_PREFIX_NS_V4}:{network.with_prefixlen}"
    return f"{_PREFIX_NS_HOST}:{endpoint.host}"


def proxy_prefix(proxy_key: str, v4_length: int = 24, v6_length: int = 64) -> str:
    """代理串 → 前缀标识（长度由调用方从 CONFIG 传入）。

    无法解析时返回 ``raw:<原串>``：仍然是一个稳定的 key，只是归并粒度为「单个代理」。
    """
    raw = (proxy_key or "").strip()
    if not raw:
        return ""
    endpoint = parse_proxy_endpoint(raw)
    if endpoint is None:
        return f"{_PREFIX_NS_RAW}:{raw}"
    return prefix_of_endpoint(endpoint, v4_length, v6_length)
