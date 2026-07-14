"""LangChain 模型服务（仅云端）

通过环境变量 llm_apis 配置 OpenAI 兼容 API 列表（Pydantic list[LLMApiConfig]）：
    llm_apis='[{"base_url":"https://...","model_name":"gpt-3.5","token":"sk-xxx"}]'
    或：llm_apis__0__base_url=...  llm_apis__0__model_name=...  llm_apis__0__token=...

按列表顺序轮询选择主模型。所有调用均走云端，不再使用本地大模型。
调用方通过 get_all_free_llms() 获取全部云端 LLM 实例并逐个显式尝试，
全部失败时再决定回退到正则判断（由调用方控制）。

注意：已移除本地 LM Studio / 本地大模型的兜底。若未配置任何云端 API，
get_all_free_llms() 会抛出 RuntimeError，由调用方（如抽奖判断）捕获。

采样参数通过 get_all_free_llms() 的关键字参数传入：
    llms = get_all_free_llms(temperature=0.7, num_predict=256)
"""
from CONFIG import settings
from enum import StrEnum
from typing import Any

from pydantic import SecretStr

from langchain_openai import ChatOpenAI

from loguru import logger
# ============ 采样参数预设 ============

_PRESET_KWARGS: dict["SamplingPreset", dict[str, Any]] = {}


class SamplingPreset(StrEnum):
    """采样参数预设，按任务类型分为 4 组"""
    TEXT_NON_THINKING = "text_non_thinking"   # 文本任务，非思考模式
    VL_NON_THINKING = "vl_non_thinking"       # 视觉-语言任务，非思考模式
    TEXT_THINKING = "text_thinking"           # 文本任务，思考模式
    VL_THINKING = "vl_thinking"               # 视觉-语言 / 精确编码任务，思考模式

    def to_kwargs(self, **overrides: Any) -> dict[str, Any]:
        """根据预设生成 bind 参数，支持 overrides 覆盖"""
        kwargs = dict(_PRESET_KWARGS[self])
        kwargs.update(overrides)
        return kwargs


_PRESET_KWARGS.update({
    SamplingPreset.TEXT_NON_THINKING: dict(
        temperature=1.0, top_p=1.0, top_k=20, num_predict=512,
    ),
    SamplingPreset.VL_NON_THINKING: dict(
        temperature=0.7, top_p=0.80, top_k=20, num_predict=512,
    ),
    SamplingPreset.TEXT_THINKING: dict(
        temperature=1.0, top_p=0.95, top_k=20, num_predict=512,
    ),
    SamplingPreset.VL_THINKING: dict(
        temperature=0.6, top_p=0.95, top_k=20, num_predict=512,
    ),
})


# ============ 全局 LLM 实例（仅云端） ============
# 云端 API 列表（按顺序轮询使用）。
#
# 必须在每次调用时从当前 settings.llm_apis 现读，因为脚本运行期可能通过
# CLI 覆盖 settings.llm_apis（自定义 base_url / model_name / token）。
# 使用模块级缓存：仅当 llm_apis 配置发生变化时才重建实例，避免重复建连。
_free_llm_cache: list[ChatOpenAI] = []
_free_llm_cache_key: str = ""


def _build_free_llms() -> list[ChatOpenAI]:
    """从当前 settings.llm_apis 构建云端 LLM 实例列表"""
    llms: list[ChatOpenAI] = []
    for cfg in settings.llm_apis:
        if cfg.base_url and cfg.model_name:
            llms.append(ChatOpenAI(
                model=cfg.model_name,
                base_url=cfg.base_url,
                api_key=SecretStr(
                    cfg.token) if cfg.token else SecretStr("not-needed"),
            ))
    return llms


def _get_free_llms() -> list[ChatOpenAI]:
    """获取云端 LLM 实例列表（带缓存，配置变化自动失效）"""
    global _free_llm_cache, _free_llm_cache_key
    key = str([(c.base_url, c.model_name, c.token) for c in settings.llm_apis])
    if _free_llm_cache_key != key:
        _free_llm_cache = _build_free_llms()
        _free_llm_cache_key = key
    return _free_llm_cache


# 轮询索引，用于循环切换主模型（协程安全）
# 说明：本服务运行在 asyncio 单线程事件循环中，协程只在 await 处发生切换。
# 本函数内无任何 await，对 _robin_idx 的读写是原子的，无需加锁也不会阻塞主线程。
_robin_idx: int = 0


def _next_rotated_llms() -> tuple[list[ChatOpenAI], int]:
    """获取轮转后的 LLM 列表，当前轮到的排在第一位

    协程安全：函数体内无 await，事件循环不会中途切换协程，因此对全局
    _robin_idx 的递增操作是原子性的，不会与其他协程产生竞态。
    """
    global _robin_idx
    all_llms = _get_free_llms()
    if not all_llms:
        return all_llms, _robin_idx
    idx = _robin_idx
    _robin_idx = (_robin_idx + 1) % len(all_llms)
    rotated = all_llms[idx:] + all_llms[:idx]
    return rotated, idx


def _map_kwargs_for_openai(kwargs: dict[str, Any]) -> dict[str, Any]:
    """将 ChatOllama 的采样参数映射为 ChatOpenAI 兼容的参数"""
    mapped: dict[str, Any] = {}
    for k, v in kwargs.items():
        if k == "num_predict":
            mapped["max_tokens"] = v
        elif k == "top_k":
            continue  # ChatOpenAI 不支持 top_k
        else:
            mapped[k] = v
    return mapped


def get_all_free_llms(**kwargs: Any) -> list[ChatOpenAI]:
    """返回当前所有云端(免费) LLM 实例（已按轮询顺序旋转，并应用采样参数）。

    调用方应逐个尝试，只有当【所有】实例都调用失败时，才认为云端不可用
    （进而决定是否回退到正则判断等）。

    若未配置任何云端 API（llm_apis 为空），抛出 RuntimeError。

    用法：
        for llm in get_all_free_llms(num_predict=256):
            structured_llm = llm.with_structured_output(schema=MyModel)
            ...
    """
    rotated, _ = _next_rotated_llms()
    if not rotated:
        raise RuntimeError("未配置任何云端 LLM（llm_apis 为空），无法进行云端判断")
    openai_kwargs = _map_kwargs_for_openai(kwargs)
    if openai_kwargs:
        return [llm.bind(**openai_kwargs) for llm in rotated]
    return list(rotated)


if __name__ == "__main__":
    import httpx
    from pydantic import BaseModel, Field
    llm = ChatOpenAI(
        model="/mnt/workspace/models/Qwen3.6-35B-A3B-Uncensored-HauhauCS-Aggressive-Q8_K_P.gguf",
        base_url="https://2034000-proxy-1111.dsw-gateway-cn-hangzhou.data.aliyun.com/v1",
        api_key="not-needed",
    )

    class TestModel(BaseModel):
        greeting: str = Field(description="对用户的问候语")
        mood: str = Field(description="模型当前的心情或态度")
    structured_llm = llm.with_structured_output(schema=TestModel)
    response = structured_llm.invoke("你好", extra_body={"chat_template_kwargs": {
        "enable_thinking": False}
    })
    logger.info(response)
