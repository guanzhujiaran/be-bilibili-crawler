"""云端 LLM 实例池（构建、缓存、轮询）

通过环境变量 llm_apis 配置 OpenAI 兼容 API 列表（Pydantic list[LLMApiConfig]）：
    llm_apis='[{"base_url":"https://...","model_name":"gpt-3.5","token":"sk-xxx"}]'
    或：llm_apis__0__base_url=...  llm_apis__0__model_name=...  llm_apis__0__token=...

按列表顺序轮询选择主模型。所有调用均走云端，不再使用本地大模型。
调用方通过 get_all_free_llms() 获取全部云端 LLM 实例并逐个显式尝试，
全部失败时再决定回退到正则判断（由调用方控制）。

若未配置任何云端 API，get_all_free_llms() 会抛出 RuntimeError，由调用方捕获。
"""

from typing import Any

from pydantic import SecretStr

from CONFIG import settings

from .tracked_llm import TrackedChatOpenAI

# 使用模块级缓存：仅当 llm_apis 配置发生变化时才重建实例，避免重复建连、
# 同时保证统计信息（TrackedChatOpenAI.stats）跨调用累积。
_free_llm_cache: list[TrackedChatOpenAI] = []
_free_llm_cache_key: str = ""


def _build_free_llms() -> list[TrackedChatOpenAI]:
    """从当前 settings.llm_apis 构建云端 LLM 实例列表"""
    llms: list[TrackedChatOpenAI] = []
    for cfg in settings.llm_apis:
        if cfg.base_url and cfg.model_name:
            llms.append(
                TrackedChatOpenAI(
                    model=cfg.model_name,
                    base_url=cfg.base_url,
                    api_key=(
                        SecretStr(cfg.token) if cfg.token else SecretStr("not-needed")
                    ),
                )
            )
    return llms


def _get_free_llms() -> list[TrackedChatOpenAI]:
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


def _next_rotated_llms() -> tuple[list[TrackedChatOpenAI], int]:
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
    rotated: list[TrackedChatOpenAI] = all_llms[idx:] + all_llms[:idx]
    return rotated, idx


def _map_kwargs_for_openai(kwargs: dict[str, Any]) -> dict[str, Any]:
    """将 ChatOllama 风格的采样参数映射为 ChatOpenAI 兼容的参数"""
    mapped: dict[str, Any] = {}
    for k, v in kwargs.items():
        if k == "num_predict":
            mapped["max_tokens"] = v
        elif k == "top_k":
            continue  # ChatOpenAI 不支持 top_k
        else:
            mapped[k] = v
    return mapped


def get_all_free_llms(**kwargs: Any) -> list[Any]:
    """返回当前所有云端(免费) LLM 实例（已按轮询顺序旋转，并应用采样参数）。

    可用的实例（stats.available 为 True）排在前面；不可用实例不剔除，
    仅后置，给其恢复机会。调用方应逐个尝试，只有当【所有】实例都调用
    失败时，才认为云端不可用（进而决定是否回退到正则判断等）。

    若未配置任何云端 API（llm_apis 为空），抛出 RuntimeError。

    用法：
        for llm in get_all_free_llms(num_predict=256):
            structured_llm = llm.with_structured_output(schema=MyModel)
            ...
    """
    rotated, _ = _next_rotated_llms()
    if not rotated:
        raise RuntimeError("未配置任何云端 LLM（llm_apis 为空），无法进行云端判断")
    # 可用实例优先，不可用实例后置（保持各自相对顺序）
    ordered = [llm for llm in rotated if llm.available] + [
        llm for llm in rotated if not llm.available
    ]
    openai_kwargs = _map_kwargs_for_openai(kwargs)
    if openai_kwargs:
        return [llm.bind(**openai_kwargs) for llm in ordered]
    return list(ordered)


def get_llm_stats() -> list[dict[str, Any]]:
    """导出当前所有 LLM 实例的统计快照（便于日志 / 监控 / 接口直接返回）"""
    return [
        {
            "model": llm.model_name,
            "base_url": llm.openai_api_base,
            **llm.stats.model_dump(),
        }
        for llm in _get_free_llms()
    ]
