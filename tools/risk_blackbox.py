"""风控策略黑箱对比探针。

按 docs/risk_control_blackbox_matrix.md 定义的组合矩阵，对同一只读 API 发请求，
统计各策略组合的 -352/-412 触发率，找出最不容易被风控的组合。

设计要点：
- 直接调 ``my_async_httpx``（与 tools/grpc_probe.py 同风格），不经过 request_with_proxy：
  这样指纹 / UA / 头序 / 会话 四个维度都能在脚本内精确控制，不受代理池随机性、QPS、
  风控冷却、重试等生产逻辑干扰。
- 出口通过 ``--proxy`` 固定（传入即所有请求共用），不传则直连 —— 对比必须在同一出口下做。
- **默认 dry-run**：只打印将执行的组合与请求形态，不发包。
  真正跑批必须显式 ``--execute --i-understand-risk``，避免误操作污染代理池 / 触发长时间冷却。

用法::

    uv run python tools/risk_blackbox.py --dry-run                 # 预览矩阵
    uv run python tools/risk_blackbox.py --dry-run --group A        # 只看 A 组
    uv run python tools/risk_blackbox.py --execute --i-understand-risk \\
        --group A --rounds 10 --proxy http://x.x.x.x:3128 \\
        --out /tmp/risk_blackbox_a.csv
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import random
import re
import statistics
import sys
import time
from collections import Counter
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from curl_cffi import CurlHttpVersion

from Utils.代理.SealedRequests import (
    WEB_IMPERSONATE_TARGETS,
    chrome_major_of,
    my_async_httpx,
)

# 探针：主域只读 GET，无登录，业务码明确。
PROBE_URL = "https://api.bilibili.com/x/activity/up/reserve/relation/info"
PROBE_PARAMS = {"ids": "1"}
CONTROL_URL = (
    "https://api.live.bilibili.com/xlive/web-interface/v1/index/getWebAreaList"
    "?source_id=2"
)

RISK_CODES = frozenset({-412, -352, 65539})
Verdict = Literal["OK", "RISK", "OTHER"]

CHROME_UA_TMPL = (
    "Mozilla/5.0 ({platform}) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/{major}.0.0.0 Safari/537.36"
)
PLATFORMS = ("Windows NT 10.0; Win64; x64", "Macintosh; Intel Mac OS X 10_15_7", "X11; Linux x86_64")


@dataclass(frozen=True)
class Strategy:
    fp: str
    ua: str
    sess: str
    ck: str
    hdr: str
    thr: str


@dataclass(frozen=True)
class Combo:
    group: str
    combo_id: str
    strategy: Strategy
    hypothesis: str


def _s(fp: str, ua: str, sess: str = "off", ck: str = "none", hdr: str = "insertion", thr: str = "off") -> Strategy:
    return Strategy(fp=fp, ua=ua, sess=sess, ck=ck, hdr=hdr, thr=thr)


COMBOS: tuple[Combo, ...] = (
    # A 组：单变量基线
    Combo("A", "baseline", _s("all-random", "fake-random"), "现状基线"),
    Combo("A", "fp-chrome", _s("chrome-only", "fake-random"), "只收窄指纹族"),
    Combo("A", "ua-match", _s("all-random", "match-fp"), "只对齐 UA"),
    Combo("A", "fp+ua", _s("chrome-only", "match-fp"), "指纹×UA 交叉校验是否关键"),
    Combo("A", "sess-on", _s("all-random", "fake-random", sess="on"), "只复用连接"),
    Combo("A", "ck-buvid", _s("all-random", "fake-random", ck="buvid"), "只带 cookie"),
    Combo("A", "hdr-alpha", _s("all-random", "fake-random", hdr="alphabetical"), "只改头序"),
    Combo("A", "thr-light", _s("all-random", "fake-random", thr="0.1-0.5s"), "轻节流"),
    Combo("A", "thr-heavy", _s("all-random", "fake-random", thr="1-3s"), "重节流"),
    Combo("A", "fp-android", _s("chrome-android", "fake-random"), "Android Chrome 指纹（UA 保持基线以隔离变量）"),
    Combo("A", "fp-firefox", _s("firefox", "fake-random"), "反面对照：纯 Firefox"),
    Combo("A", "fp-safari", _s("safari", "fake-random"), "反面对照：纯 Safari"),
    # B 组：两两交互
    Combo("B", "fp+ua+sess", _s("chrome-only", "match-fp", sess="on"), "一会话一指纹一连接"),
    Combo("B", "fp+ua+ck", _s("chrome-only", "match-fp", ck="buvid"), "指纹与 cookie 绑定"),
    Combo("B", "fp+ua+hdr", _s("chrome-only", "match-fp", hdr="alphabetical"), "传输+头序"),
    Combo("B", "sess+ck", _s("all-random", "fake-random", sess="on", ck="buvid"), "会话复用×cookie"),
    Combo("B", "sess+thr", _s("all-random", "fake-random", sess="on", thr="0.1-0.5s"), "会话复用×节流"),
    Combo("B", "hdr+thr", _s("all-random", "fake-random", hdr="alphabetical", thr="0.1-0.5s"), "头序×节流"),
    # C 组：全套候选
    Combo("C", "all-in", _s("chrome-only", "match-fp", "on", "buvid", "alphabetical", "0.1-0.5s"), "全部加固"),
    Combo("C", "all-in-no-thr", _s("chrome-only", "match-fp", "on", "buvid", "alphabetical", "off"), "去掉节流的边际贡献"),
    Combo("C", "all-in-heavy-thr", _s("chrome-only", "match-fp", "on", "buvid", "alphabetical", "1-3s"), "加强节流"),
    Combo("C", "all-in-fixed-ua", _s("chrome-only", "fixed-152", "on", "buvid", "alphabetical", "0.1-0.5s"), "固定 UA 版本"),
)


def pick_target(fp: str) -> str:
    if fp == "chrome-only":
        return random.choice(WEB_IMPERSONATE_TARGETS)
    if fp == "chrome-android":
        return "chrome131_android"
    if fp == "firefox":
        return "firefox154"
    if fp == "safari":
        return "safari270"
    from Utils.代理.SealedRequests import SUPPORTED_IMPERSONATE_TARGETS

    return random.choice(SUPPORTED_IMPERSONATE_TARGETS)


def build_ua(target: str, ua_mode: str) -> str:
    if ua_mode == "fixed-152":
        return CHROME_UA_TMPL.format(platform=random.choice(PLATFORMS), major=152)
    if ua_mode == "match-fp":
        major = chrome_major_of(target) if target.startswith("chrome") else 131
        return CHROME_UA_TMPL.format(platform=random.choice(PLATFORMS), major=major)
    # fake-random：模拟现状 CONFIG.rand_ua（非 Chrome 系也可能出现）
    return random.choice(
        (
            CHROME_UA_TMPL.format(platform=random.choice(PLATFORMS), major=random.randint(100, 152)),
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) Gecko/20100101 Firefox/128.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
        )
    )


def build_headers(st: Strategy, target: str, session_cookie: str | None) -> dict[str, str]:
    ua = build_ua(target, st.ua)
    major = chrome_major_of(target) if target.startswith("chrome") else 130
    base = {
        "accept": "*/*",
        "accept-encoding": "gzip, deflate, br, zstd",
        "accept-language": "zh-CN,zh;q=0.9",
        "origin": "https://www.bilibili.com",
        "referer": "https://www.bilibili.com/",
        "user-agent": ua,
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
        "priority": "u=1, i",
    }
    if target.startswith("chrome") and not target.endswith("_android"):
        base["sec-ch-ua"] = f'"Chromium";v="{major}", "Not_A Brand";v="8", "Google Chrome";v="{major}"'
        base["sec-ch-ua-mobile"] = "?0"
        base["sec-ch-ua-platform"] = '"Windows"'
    if st.ck == "buvid" and session_cookie:
        base["cookie"] = session_cookie
    if st.hdr == "alphabetical":
        return dict(sorted(base.items()))
    return base


def throttle_delay(st: Strategy) -> float:
    if st.thr == "0.1-0.5s":
        return random.uniform(0.1, 0.5)
    if st.thr == "1-3s":
        return random.uniform(1.0, 3.0)
    return 0.0


@dataclass
class Record:
    combo_id: str
    seq: int
    probe: str
    target: str
    ua: str
    code: int | str
    latency_ms: int
    verdict: Verdict
    timestamp: str
    proxy: str


def verdict_of(code: object) -> Verdict:
    if isinstance(code, int) and code in RISK_CODES:
        return "RISK"
    if code == 0:
        return "OK"
    if isinstance(code, str) and code in {"-412", "-352", "65539"}:
        return "RISK"
    return "OTHER"


async def probe_once(
    st: Strategy,
    session_cookie: str | None,
    proxy: str | None,
    control: bool = False,
) -> tuple[str, str, int | str, int, Verdict]:
    target = pick_target(st.fp)
    headers = build_headers(st, target, session_cookie)
    url = CONTROL_URL if control else PROBE_URL
    params = None if control else PROBE_PARAMS
    await asyncio.sleep(throttle_delay(st))
    t0 = time.monotonic()
    try:
        resp = await my_async_httpx.request(
            method="GET",
            url=url,
            headers=headers,
            params=params,
            proxies={"http": proxy, "https": proxy} if proxy else None,
            impersonate=target,
            timeout=15,
        )
        try:
            code = resp.json().get("code", "no-code")
        except Exception:
            code = f"http-{resp.status_code}"
    except Exception as exc:  # noqa: BLE001 黑箱探针需吞掉所有网络异常并归为 OTHER
        code = f"exc-{type(exc).__name__}"
    latency = int((time.monotonic() - t0) * 1000)
    return target, headers.get("user-agent", ""), code, latency, verdict_of(code)


def wilson(successes: int, total: int, z: float = 1.96) -> tuple[float, float]:
    if total == 0:
        return (0.0, 0.0)
    p = successes / total
    d = 1 + z * z / total
    c = (p + z * z / (2 * total)) / d
    h = z * ((p * (1 - p) / total + z * z / (4 * total * total)) ** 0.5) / d
    return (max(0.0, c - h), min(1.0, c + h))


async def run_group(
    combos: list[Combo], rounds: int, proxy: str | None, cookie: str | None
) -> list[Record]:
    records: list[Record] = []
    tasks = [c for c in combos for _ in range(rounds)]
    random.shuffle(tasks)
    for i, combo in enumerate(tasks, 1):
        target, ua, code, latency, verdict = await probe_once(combo.strategy, cookie, proxy)
        records.append(
            Record(
                combo_id=combo.combo_id,
                seq=i,
                probe="main",
                target=target,
                ua=ua,
                code=code,
                latency_ms=latency,
                verdict=verdict,
                timestamp=datetime.now(timezone.utc).isoformat(),
                proxy=proxy or "direct",
            )
        )
        print(
            f"[{i:>4}/{len(tasks)}] {combo.combo_id:<18} {target:<20} "
            f"code={code!s:<10} {verdict:<5} {latency}ms"
        )
    return records


def summarize(records: list[Record]) -> list[dict[str, object]]:
    by_combo: dict[str, list[Record]] = {}
    for r in records:
        by_combo.setdefault(r.combo_id, []).append(r)
    rows: list[dict[str, object]] = []
    for combo_id, rs in by_combo.items():
        counts = Counter(r.verdict for r in rs)
        ok, risk, other = counts["OK"], counts["RISK"], counts["OTHER"]
        denom = ok + risk
        lo, hi = wilson(risk, denom)
        rows.append(
            {
                "combo_id": combo_id,
                "n": len(rs),
                "ok": ok,
                "risk": risk,
                "other": other,
                "risk_rate": round(risk / denom, 4) if denom else None,
                "wilson_lo": round(lo, 4),
                "wilson_hi": round(hi, 4),
                "p50_ms": statistics.median(r.latency_ms for r in rs),
                "p95_ms": sorted(r.latency_ms for r in rs)[max(0, int(len(rs) * 0.95) - 1)],
                "targets": json.dumps(sorted({r.target for r in rs})),
            }
        )
    return sorted(rows, key=lambda x: (x["risk_rate"] is None, x["risk_rate"] or 0))


def write_csv(path: Path, records: list[Record]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(asdict(records[0]).keys()))
        w.writeheader()
        for r in records:
            w.writerow(asdict(r))


async def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true", help="只打印矩阵，不发包（默认行为）")
    ap.add_argument("--execute", action="store_true", help="真正发包")
    ap.add_argument("--i-understand-risk", action="store_true", help="确认已知风控风险")
    ap.add_argument("--group", choices=("A", "B", "C", "all"), default="all")
    ap.add_argument("--rounds", type=int, default=10)
    ap.add_argument("--proxy", default=None, help="固定出口，如 http://1.2.3.4:3128；强烈建议指定")
    ap.add_argument("--cookie", default=None, help="ck=buvid 组合使用的 cookie 串；不传则这些组退化为 none")
    ap.add_argument("--out", default="/tmp/risk_blackbox_result.csv")
    args = ap.parse_args()

    selected = [c for c in COMBOS if args.group == "all" or c.group == args.group]

    if args.dry_run or not (args.execute and args.i_understand_risk):
        print("=== DRY RUN（不发包）===\n")
        print(f"{'组':<3} {'组合ID':<20} {'fp':<15} {'ua':<13} {'sess':<5} {'ck':<7} {'hdr':<13} {'thr':<10} 假设")
        for c in selected:
            s = c.strategy
            print(
                f"{c.group:<3} {c.combo_id:<20} {s.fp:<15} {s.ua:<13} {s.sess:<5} "
                f"{s.ck:<7} {s.hdr:<13} {s.thr:<10} {c.hypothesis}"
            )
        print(f"\n共 {len(selected)} 个组合 × {args.rounds} 轮 = {len(selected) * args.rounds} 次主探针请求")
        print(f"出口：{args.proxy or '直连（不推荐，对比会被出口变更污染）'}")
        print("\n真正执行：--execute --i-understand-risk --proxy <固定出口>")
        return

    if not args.proxy:
        print("警告：未指定 --proxy，组合间出口可能变化，结果不可比。", file=sys.stderr)

    print(f"开始跑批：{len(selected)} 组合 × {args.rounds} 轮，出口 {args.proxy or 'direct'}")
    records = await run_group(selected, args.rounds, args.proxy, args.cookie)

    # 对照探针：每轮结束跑一次，确认出口本身没挂
    _, _, ctrl_code, _, ctrl_verdict = await probe_once(
        _s("chrome-only", "match-fp"), args.cookie, args.proxy, control=True
    )
    print(f"\n对照探针（live 域）：code={ctrl_code} verdict={ctrl_verdict}")

    write_csv(Path(args.out), records)
    print(f"\n明细已写入 {args.out}\n")
    print(f"{'组合':<20} {'N':>3} {'OK':>4} {'RISK':>5} {'OTHER':>6} {'risk_rate':>10} {'95%CI':>18} {'P50':>6} {'P95':>6}")
    for row in summarize(records):
        print(
            f"{row['combo_id']:<20} {row['n']:>3} {row['ok']:>4} {row['risk']:>5} "
            f"{row['other']:>6} {str(row['risk_rate']):>10} "
            f"[{row['wilson_lo']:.3f},{row['wilson_hi']:.3f}]".ljust(34)
            + f" {row['p50_ms']:>6} {row['p95_ms']:>6}"
        )
    print("\n判定标准见 docs/risk_control_blackbox_matrix.md §5（Wilson 区间不重叠 + OTHER<5% + 复测稳定）")


if __name__ == "__main__":
    asyncio.run(main())
