"""风控黑箱矩阵框架的单测：只验证纯逻辑，不发任何真实请求。

真正跑批见 tools/risk_blackbox.py，需人工 --execute --i-understand-risk。
"""

import sys
from pathlib import Path

import pytest

_project_root = Path(__file__).resolve().parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from tools.risk_blackbox import (
    COMBOS,
    Strategy,
    build_headers,
    pick_target,
    verdict_of,
    wilson,
)


def test_matrix_covers_all_three_groups() -> None:
    assert len(COMBOS) == 22
    groups = {c.group for c in COMBOS}
    assert groups == {"A", "B", "C"}
    # 组合 ID 唯一
    assert len({c.combo_id for c in COMBOS}) == len(COMBOS)


def test_single_variable_groups() -> None:
    """A 组除 ``fp+ua``（唯一指定的交互例外）外，必须严格只改一个维度。"""
    baseline = next(c for c in COMBOS if c.combo_id == "baseline").strategy
    exceptions = {"fp+ua"}
    for combo in COMBOS:
        if combo.group != "A" or combo.combo_id == "baseline":
            continue
        diff = [
            k
            for k in baseline.__dataclass_fields__
            if getattr(baseline, k) != getattr(combo.strategy, k)
        ]
        expected = 2 if combo.combo_id in exceptions else 1
        assert len(diff) == expected, f"{combo.combo_id} 改了 {len(diff)} 个维度: {diff}"


def test_verdict_mapping() -> None:
    assert verdict_of(0) == "OK"
    assert verdict_of(-412) == "RISK"
    assert verdict_of(-352) == "RISK"
    assert verdict_of(65539) == "RISK"
    assert verdict_of("-352") == "RISK"
    assert verdict_of(9999) == "OTHER"
    assert verdict_of("exc-TimeoutError") == "OTHER"


def test_wilson_interval_bounds() -> None:
    lo, hi = wilson(3, 10)
    assert 0.0 <= lo <= 0.3 <= hi <= 1.0
    assert wilson(0, 0) == (0.0, 0.0)
    assert wilson(10, 10) == pytest.approx((0.722, 1.0), abs=0.01)


def test_build_headers_alphabetical_mode() -> None:
    st = Strategy(fp="chrome-only", ua="match-fp", hdr="alphabetical", sess="off", ck="none", thr="off")
    headers = build_headers(st, "chrome152", None)
    keys = list(headers.keys())
    assert keys == sorted(keys)
    assert "cookie" not in headers


def test_build_headers_buvid_mode() -> None:
    st = Strategy(fp="chrome-only", ua="match-fp", hdr="insertion", sess="off", ck="buvid", thr="off")
    headers = build_headers(st, "chrome152", "buvid3=ABC; buvid4=DEF")
    assert headers["cookie"] == "buvid3=ABC; buvid4=DEF"


def test_ua_matches_target_major() -> None:
    st = Strategy(fp="chrome-only", ua="match-fp", sess="off", ck="none", hdr="insertion", thr="off")
    headers = build_headers(st, "chrome152", None)
    assert "Chrome/152.0.0.0" in headers["user-agent"]
    assert '"Google Chrome";v="152"' in headers["sec-ch-ua"]
    assert headers["sec-ch-ua-platform"] == '"Windows"'


def test_all_targets_are_resolvable() -> None:
    """矩阵里 fp 的每个取值都必须能选出一个非空 target。"""
    for fp in {c.strategy.fp for c in COMBOS}:
        for _ in range(5):
            assert pick_target(fp)
