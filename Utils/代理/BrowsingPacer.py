"""业务请求间的随机节流 / 浏览路径模拟。

背景：爬虫原来的节奏只有「失败退避 + 代理健康冷却」，成功请求之间可以 0 延迟
连发，这在行为层与真人差异极大。本模块只做两件事：

1. **短节流**：每次业务请求前等 0.1~0.5s（可配），把整齐的并发脉冲打散；
2. **浏览路径模拟**：按低概率（默认 15%）插入一次 0.8~3s 的「看内容」停顿，
   模拟用户看完一条动态再点下一条，而不是机械地连续拉取。

豁免：``local_exit_only``（cookie / metadata 激活专线）与显式
``RequestConf.skip_throttle`` 返回 0 —— 前者是一次性握手，后者是探测 / 对时延
敏感的调用。节流发生在选代理**之前**，不占在途计数与专线 QPS 配额。
"""

from __future__ import annotations

import random

from CONFIG import CONFIG
from Utils.代理.mdoel.RequestConf import RequestConf


class BrowsingPacer:
    def __init__(self) -> None:
        self._request_count = 0

    @property
    def request_count(self) -> int:
        return self._request_count

    def delay_for(self, request_conf: RequestConf | None) -> float:
        """返回本次请求前应等待的秒数（0 = 不等待）。纯计算，不 sleep。"""
        if request_conf is not None and (
            request_conf.local_exit_only or request_conf.skip_throttle
        ):
            return 0.0
        cfg = CONFIG.crawler_throttle
        if not cfg.business_throttle_enabled:
            return 0.0
        self._request_count += 1
        if (
            cfg.path_simulation_enabled
            and cfg.path_simulation_prob > 0
            and random.random() < cfg.path_simulation_prob
        ):
            low, high = _ordered(cfg.path_simulation_min_s, cfg.path_simulation_max_s)
            return random.uniform(low, high)
        low, high = _ordered(cfg.business_throttle_min_s, cfg.business_throttle_max_s)
        return random.uniform(low, high)


def _ordered(low: float, high: float) -> tuple[float, float]:
    low = max(0.0, float(low))
    high = max(0.0, float(high))
    return (low, high) if low <= high else (high, low)


browsing_pacer = BrowsingPacer()
