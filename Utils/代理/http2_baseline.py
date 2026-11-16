"""B 站 Web 请求的 HTTP/2 + header 顺序回归基线。

证据与边界（详见 docs/be-bilibili-crawler-风控对抗加固计划书.md §1.3 / §1.4）：

- **Header 顺序**：REA 抓包中 17 个 ``api.bilibili.com`` XHR 的请求头顺序全部是
  字典序，因此 ``BILI_WEB_HEADER_ORDER`` 作为代码内组装顺序的基线；
- **HTTP/2 SETTINGS**：curl_cffi 0.17.0 没有独立 API，SETTINGS / WINDOW_UPDATE
  随 impersonate target 固定注入（与 gRPC 侧结论一致）。因此这里只记录
  Chromium 的典型 SETTINGS 作为**对照基线**，不尝试伪造，也不对它做单测。

真正的回归断言只覆盖「我们可控且可审计」的部分：请求头名字顺序、以及
``WEB_IMPERSONATE_TARGETS`` 必须全部是桌面 Chrome（与 UA / Client Hints 同源）。
"""

from __future__ import annotations

#: 浏览器 HTTP/2 伪头顺序（HPACK 编码顺序，curl_cffi 内部固定）
H2_PSEUDO_HEADER_ORDER: tuple[str, ...] = (
    ":method",
    ":authority",
    ":scheme",
    ":path",
)

#: REA 抓包观测到的业务头顺序（字典序）
BILI_WEB_HEADER_ORDER: tuple[str, ...] = (
    "accept",
    "accept-encoding",
    "accept-language",
    "cookie",
    "origin",
    "priority",
    "referer",
    "sec-ch-ua",
    "sec-ch-ua-mobile",
    "sec-ch-ua-platform",
    "sec-fetch-dest",
    "sec-fetch-mode",
    "sec-fetch-site",
    "user-agent",
)

#: Chromium 系 HTTP/2 的典型 SETTINGS（仅对照记录；curl_cffi 不可单独设置）
CHROME_H2_SETTINGS: dict[str, int] = {
    "HEADER_TABLE_SIZE": 65536,
    "ENABLE_PUSH": 0,
    "MAX_CONCURRENT_STREAMS": 1000,
    "INITIAL_WINDOW_SIZE": 6291456,
    "MAX_HEADER_LIST_SIZE": 262144,
}


def header_names_in_order(headers) -> tuple[str, ...]:
    """取出请求头名字序列（保持传入顺序，统一小写）。"""
    if isinstance(headers, dict):
        return tuple(str(key).lower() for key in headers)
    return tuple(str(key).lower() for key, _ in headers)


def is_alphabetical(headers) -> bool:
    names = header_names_in_order(headers)
    return list(names) == sorted(names)


def known_header_names(headers) -> tuple[str, ...]:
    """过滤出基线里列出的业务头（忽略 x-bili-* 等接口专属头）。"""
    order = set(BILI_WEB_HEADER_ORDER)
    return tuple(name for name in header_names_in_order(headers) if name in order)
