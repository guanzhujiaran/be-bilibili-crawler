"""`Utils.通用.Tool.format_duration` 的单测：面向人的时长文案统一口径。

口径（2026-11-12）：
- 文案形如「X天X小时X分X秒」，省略为 0 的高位单位；
- 最小单位必出：不足 60 秒时输出秒（45 -> "45秒"，而不是 "0分45秒"）；
- 0 / None / 负数 / 非法值统一 "0秒"，不能抛异常（日志与推送里到处在用它）。

运行:
    uv run python -m pytest test/test_format_duration.py -v
"""

import sys
from pathlib import Path

import pytest

_project_root = Path(__file__).resolve().parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from Utils.通用.Tool import format_duration


@pytest.mark.parametrize(
    ("seconds", "expected"),
    [
        # 验收要求的样例
        (0, "0秒"),
        (45, "45秒"),
        (65, "1分5秒"),
        (3725, "1小时2分5秒"),
        (93784, "1天2小时3分4秒"),
        (None, "0秒"),
        (-1, "0秒"),
        # 边界与低位补零口径
        (1, "1秒"),
        (59, "59秒"),
        (60, "1分"),
        (3599, "59分59秒"),
        (3600, "1小时"),
        (86399, "23小时59分59秒"),
        (86400, "1天"),
        (93784.9, "1天2小时3分4秒"),  # float 截断到整秒，不四舍五入
        (43201, "12小时0分1秒"),
    ],
)
def test_format_duration(seconds: float | int | None, expected: str) -> None:
    assert format_duration(seconds) == expected


def test_format_duration_accepts_float_seconds() -> None:
    assert format_duration(3725.87) == "1小时2分5秒"


def test_format_duration_never_raises_on_bad_input() -> None:
    # 非法值不能把日志/推送链路带崩，统一降级成 "0秒"
    assert format_duration("不是数字") == "0秒"  # type: ignore[arg-type]
