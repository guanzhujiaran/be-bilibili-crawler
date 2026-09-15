"""云端 LLM 实例池（构建、缓存、轮询）

通过环境变量 llm_apis 配置 OpenAI 兼容 API 列表（Pydantic list[LLMApiConfig]）：
    llm_apis='[{"base_url":"https://...","model_name":"gpt-3.5","token":"sk-xxx"}]'
    或：llm_apis__0__base_url=...  llm_apis__0__model_name=...  llm_apis__0__token=...

按列表顺序轮询选择主模型。所有调用均走云端，不再使用本地大模型。
调用方通过 get_all_free_llms() 获取全部云端 LLM 实例并逐个显式尝试，
全部失败时再决定回退到正则判断（由调用方控制）。

若未配置任何云端 API，get_all_free_llms() 会抛出 RuntimeError，由调用方捕获。

支持运行时热更新：set_llm_apis() 可在线替换 settings.llm_apis 并立即重建实例池
（get_llm_configs() 读取当前配置，token 已脱敏），对应内部接口 GET/POST /llm/config。
"""

from Service.llm_service import SamplingPreset
from Service.llm_service.tracked_llm import TrackedChatOpenAI

from typing import Any

from langchain_core.rate_limiters import InMemoryRateLimiter
from pydantic import SecretStr

from CONFIG import LLMApiConfig, LLMApiConfigPatch, settings

_free_llm_cache: list[TrackedChatOpenAI] = []
_free_llm_cache_key: str = ""


def _build_free_llms() -> list[TrackedChatOpenAI]:
    """从当前 settings.llm_apis 构建云端 LLM 实例列表"""
    llms: list[TrackedChatOpenAI] = []
    for cfg in settings.llm_apis:
        if cfg.base_url and cfg.model_name:
            # 每个云端实例独立限流：ainvoke/invoke 前会先获取令牌，
            # 超速时自动阻塞等待，避免触发上游 API 的 429 限制。
            rate_limiter = InMemoryRateLimiter(
                requests_per_second=cfg.requests_per_second,
                check_every_n_seconds=0.1,
                max_bucket_size=1,
            )
            llms.append(
                TrackedChatOpenAI(
                    model=cfg.model_name,
                    base_url=cfg.base_url,
                    api_key=(
                        SecretStr(cfg.token) if cfg.token else SecretStr("not-needed")
                    ),
                    rate_limiter=rate_limiter,
                )
            )
    return llms


def _config_key() -> str:
    """当前 llm_apis 的指纹：配置变化时用于自动失效实例缓存"""
    return str(
        [
            (c.base_url, c.model_name, c.token, c.requests_per_second)
            for c in settings.llm_apis
        ]
    )


def _get_free_llms() -> list[TrackedChatOpenAI]:
    """获取云端 LLM 实例列表（带缓存，配置变化自动失效）"""
    global _free_llm_cache, _free_llm_cache_key
    key = _config_key()
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


def get_all_free_llms() -> list[TrackedChatOpenAI]:
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


def _mask_token(token: str) -> str:
    """token 脱敏：仅保留首尾少量字符，避免接口直接回显明文密钥"""
    if not token:
        return ""
    if len(token) <= 8:
        return "****"
    return f"{token[:4]}****{token[-4:]}"


def get_llm_configs() -> list[dict[str, Any]]:
    """导出当前云端 LLM 配置（token 已脱敏），供内部配置接口读取"""
    return [
        {
            "base_url": cfg.base_url,
            "model_name": cfg.model_name,
            "token": _mask_token(cfg.token),
            "requests_per_second": cfg.requests_per_second,
        }
        for cfg in settings.llm_apis
    ]


def set_llm_apis(apis: list[LLMApiConfig]) -> list[dict[str, Any]]:
    """在线替换云端 LLM 配置并立即生效（无需重启服务）。

    - 仅修改运行时内存中的 settings.llm_apis，不写回 .env 文件，重启后回退为环境变量配置。
    - 立即重建实例池（不等待下一次调用），使新配置即时生效；
      若重建失败则回滚到原配置并抛出异常，避免留下「改一半」的坏状态。
    - 校验：每个配置项必须同时提供 base_url 与 model_name。

    返回：更新后的配置（token 已脱敏）。
    """
    invalid = [
        f"[{i}]"
        for i, cfg in enumerate(apis)
        if not cfg.base_url or not cfg.model_name
    ]
    if invalid:
        raise ValueError(
            f"llm_apis 配置非法：第 {', '.join(invalid)} 项缺少 base_url 或 model_name"
        )

    global _free_llm_cache, _free_llm_cache_key
    old_apis = settings.llm_apis
    old_cache, old_key = _free_llm_cache, _free_llm_cache_key
    settings.llm_apis = list(apis)
    try:
        _free_llm_cache = _build_free_llms()
        _free_llm_cache_key = _config_key()
    except Exception:
        # 回滚，保证「改一半失败」不会污染当前运行配置
        settings.llm_apis = old_apis
        _free_llm_cache, _free_llm_cache_key = old_cache, old_key
        raise
    return get_llm_configs()


def _validate_index(index: int) -> None:
    """校验索引合法；越界抛 IndexError（由控制器转 404）"""
    total = len(settings.llm_apis)
    if not 0 <= index < total:
        raise IndexError(f"索引越界：当前共 {total} 条配置，index={index}")


def get_llm_config(index: int) -> dict[str, Any]:
    """读取指定索引的单条云端 LLM 配置（token 已脱敏）"""
    _validate_index(index)
    return get_llm_configs()[index]


def create_llm_api(cfg: LLMApiConfig) -> list[dict[str, Any]]:
    """新增一条云端 LLM 配置（追加到列表末尾），返回更新后的完整配置列表"""
    return set_llm_apis([*settings.llm_apis, cfg])


def update_llm_api(index: int, cfg: LLMApiConfig) -> list[dict[str, Any]]:
    """整体更新指定索引的单条配置，返回更新后的完整配置列表"""
    _validate_index(index)
    apis = list(settings.llm_apis)
    apis[index] = cfg
    return set_llm_apis(apis)


def patch_llm_api(index: int, patch: LLMApiConfigPatch) -> list[dict[str, Any]]:
    """部分更新指定索引的单条配置（未传字段保持原值），返回更新后的完整配置列表"""
    _validate_index(index)
    apis = list(settings.llm_apis)
    merged = apis[index].model_dump()
    merged.update(
        {
            k: v
            for k, v in patch.model_dump(exclude_unset=True).items()
            if v is not None
        }
    )
    apis[index] = LLMApiConfig(**merged)
    return set_llm_apis(apis)


def delete_llm_api(index: int) -> list[dict[str, Any]]:
    """删除指定索引的单条配置，返回剩余配置列表（空列表表示已清空全部）"""
    _validate_index(index)
    apis = list(settings.llm_apis)
    del apis[index]
    return set_llm_apis(apis)


if __name__ == "__main__":

    async def _test():

        llms = get_all_free_llms()
        llm = llms[0]
        llm.bind(**SamplingPreset.TEXT_NON_THINKING.to_kwargs(num_predict=256))
        res = await llm.ainvoke("1+1=?")
        print(res)
    import asyncio
    asyncio.run(_test())