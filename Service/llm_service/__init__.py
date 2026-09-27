"""LangChain 模型服务（仅云端）

目录结构：
- presets.py      采样参数预设（SamplingPreset）
- tracked_llm.py  带调用统计的 ChatOpenAI 子类（TrackedChatOpenAI / LLMUsageStats）+ 槽位指纹
- pool.py         实例池：构建、缓存、轮询（get_all_free_llms / get_llm_slots / get_llm_stats）
- slot.py         LLM 槽位租约池：同一 (base_url, model, token) 跨进程最多 1 个在途请求

对外 API 与旧版 Service/llm_service.py 保持兼容：
    from Service.llm_service import get_all_free_llms, SamplingPreset
"""

from .presets import SamplingPreset
from .tracked_llm import LLMUsageStats, TrackedChatOpenAI
from .pool import (
    AllLLMsDisabledError,
    LLMSlot,
    create_llm_api,
    delete_llm_api,
    get_all_free_llms,
    get_llm_config,
    get_llm_configs,
    get_llm_slots,
    get_llm_stats,
    patch_llm_api,
    set_llm_apis,
    update_llm_api,
)

# 注意：slot.py 依赖 pool.py，必须在 pool 之后导入（避免循环导入）。
from .slot import LLMSlotLease, LLMSlotPool, llm_slot_pool

__all__ = [
    "SamplingPreset",
    "LLMUsageStats",
    "TrackedChatOpenAI",
    "AllLLMsDisabledError",
    "LLMSlot",
    "LLMSlotLease",
    "LLMSlotPool",
    "llm_slot_pool",
    "get_all_free_llms",
    "get_llm_slots",
    "get_llm_stats",
    "get_llm_configs",
    "get_llm_config",
    "set_llm_apis",
    "create_llm_api",
    "update_llm_api",
    "patch_llm_api",
    "delete_llm_api",
]
