"""采样参数预设

按任务类型分为 4 组，调用方通过 SamplingPreset.to_kwargs() 生成 bind 参数。
"""

from enum import StrEnum
from typing import Any

_PRESET_KWARGS: dict["SamplingPreset", dict[str, Any]] = {}


class SamplingPreset(StrEnum):
    """采样参数预设，按任务类型分为 4 组"""

    TEXT_NON_THINKING = "text_non_thinking"  # 文本任务，非思考模式
    VL_NON_THINKING = "vl_non_thinking"  # 视觉-语言任务，非思考模式
    TEXT_THINKING = "text_thinking"  # 文本任务，思考模式
    VL_THINKING = "vl_thinking"  # 视觉-语言 / 精确编码任务，思考模式

    def to_kwargs(self, **overrides: Any) -> dict[str, Any]:
        """根据预设生成 bind 参数，支持 overrides 覆盖"""
        kwargs = dict(_PRESET_KWARGS[self])
        kwargs.update(overrides)
        return kwargs


_PRESET_KWARGS.update(
    {
        SamplingPreset.TEXT_NON_THINKING: dict(
            temperature=1.0,
            top_p=1.0,
            top_k=20,
            num_predict=512,
        ),
        SamplingPreset.VL_NON_THINKING: dict(
            temperature=0.7,
            top_p=0.80,
            top_k=20,
            num_predict=512,
        ),
        SamplingPreset.TEXT_THINKING: dict(
            temperature=1.0,
            top_p=0.95,
            top_k=20,
            num_predict=512,
        ),
        SamplingPreset.VL_THINKING: dict(
            temperature=0.6,
            top_p=0.95,
            top_k=20,
            num_predict=512,
        ),
    }
)
