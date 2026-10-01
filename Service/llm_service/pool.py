"""云端 LLM 实例池（构建、缓存、轮询）

通过环境变量 llm_apis 配置 OpenAI 兼容 API 列表（Pydantic list[LLMApiConfig]）：
    llm_apis='[{"base_url":"https://...","model_name":"gpt-3.5","token":"sk-xxx"}]'
    或：llm_apis__0__base_url=...  llm_apis__0__model_name=...  llm_apis__0__token=...

按列表顺序轮询选择主模型。所有调用均走云端，不再使用本地大模型。
调用方通过 get_all_free_llms() 获取当前【可用】(healthy) 的实例并逐个显式尝试。

可用性由 ``Service/llm_service/health.py`` 的状态机描述：

- 不可恢复错误（鉴权失败 / 模型下线等）→ 该条配置**直接删除**，池里不再有它；
- 限流 / 今日额度 / 欠费 / 连续瞬时失败 → 进入冷却，到点自动恢复，本轮不参与尝试。

因此 get_all_free_llms() 可能抛出（均为 RuntimeError 子类，调用方按类型分流）：
- RuntimeError：未配置任何云端 API；
- AllLLMsDisabledError：全部已删除（不可恢复，需要人工重新配置）；
- AllLLMsCoolingError：全部在冷却中（可自动恢复，按 resume_at 等待）。

支持运行时热更新：set_llm_apis() 可在线替换 settings.llm_apis 并立即重建实例池
（get_llm_configs() 读取当前配置，token 已脱敏），对应内部接口 GET/POST /llm/config。
"""

import time
from typing import Any

from langchain_core.rate_limiters import InMemoryRateLimiter
from loguru import logger
from pydantic import BaseModel, Field, SecretStr

from Service.llm_service import SamplingPreset
from Service.llm_service.health import LLMState
from Service.llm_service.tracked_llm import (
    StateChangeHandler,
    TrackedChatOpenAI,
    slot_fingerprint,
)

from CONFIG import LLMApiConfig, LLMApiConfigPatch, settings

_free_llm_cache: list[TrackedChatOpenAI] = []
_free_llm_cache_key: str = ""


class AllLLMsDisabledError(RuntimeError):
    """所有云端 LLM 均已删除（模型下线 / 鉴权失败等不可恢复错误）。

    继承 RuntimeError，兼容调用方既有的 ``except RuntimeError`` 处理；
    调用方可据此单独发送告警：这不是「暂时失败、仍在重试」，
    而是不可恢复状态，只能等重新配置 llm_apis 后恢复（重启或热更新皆可）。
    """


class AllLLMsCoolingError(RuntimeError):
    """所有云端 LLM 都在冷却中（限流 / 今日额度 / 欠费 / 连续瞬时失败）。

    与 :class:`AllLLMsDisabledError` 的区别：这是**可自动恢复**的临时状态。
    ``resume_at`` 是最近一个槽位的自动恢复时间戳（unix 秒），
    调用方应据此等待，而不是当成不可恢复错误处理。
    """

    def __init__(self, message: str, *, resume_at: float | None = None) -> None:
        super().__init__(message)
        #: 最近一个槽位的自动恢复时间戳；None 表示没有会自动恢复的槽位
        self.resume_at = resume_at


class RemovedLLMRecord(BaseModel):
    """一条被删除的槽位记录（不可恢复错误的处置结果，仅供观测）"""

    fingerprint: str = Field(description="槽位指纹（sha256 前 16 位）")
    model_name: str = Field(description="模型名")
    base_url: str = Field(description="接口地址")
    reason: str | None = Field(default=None, description="删除原因（错误分类 + 服务端返回）")
    removed_at: float = Field(description="删除时间戳（unix 秒）")


# 被删除的槽位记录（不可恢复错误导致的删除）：配置已被摘除，这里只留观测痕迹
_removed_records: list[RemovedLLMRecord] = []


def _describe_resume(delay: float | None) -> str:
    """把「距自动恢复还有多少秒」压成可读文案"""
    if delay is None:
        return "不会自动恢复"
    return f"{delay:.0f}s 后自动恢复"


def _earliest_resume_at(llms: list[TrackedChatOpenAI]) -> float | None:
    """取一组实例里最早的自动恢复时间戳；都不会自动恢复时返回 None"""
    deadlines = [
        llm.stats.health.resume_at
        for llm in llms
        if llm.stats.health.resume_at is not None
    ]
    return min(deadlines) if deadlines else None


def _on_slot_state_change(
    llm: TrackedChatOpenAI, previous: LLMState, current: LLMState
) -> None:
    """状态迁移副作用：不可恢复（REMOVED）→ 直接删除该条配置。

    冷却类状态（COOLING / QUOTA_WAIT / SUSPENDED）不做删除，
    到点由状态机自动恢复；日志已在 ``TrackedChatOpenAI._log_state_change`` 打过。
    """
    if current is not LLMState.REMOVED:
        return
    _remove_slot_from_cache(
        llm.slot_fingerprint,
        model_name=llm.model_name,
        base_url=llm.openai_api_base or "",
        reason=llm.stats.health.reason,
    )


#: 无 token 的配置（本地 / 免鉴权上游）在实例里填的占位密钥。
_PLACEHOLDER_TOKEN = "not-needed"


def _token_for_config(cfg: LLMApiConfig) -> str:
    """配置里的 token 归一化为「实例实际持有的密钥串」（空 token 用占位符）"""
    return cfg.token or _PLACEHOLDER_TOKEN


def _fingerprint_of(cfg: LLMApiConfig) -> str:
    """按「建实例时的取值」计算槽位指纹，与 ``TrackedChatOpenAI.slot_fingerprint`` 对齐。

     必须用 :func:`_token_for_config` 而不是裸 ``cfg.token``：实例在 token 为空时
    会填占位密钥，若比对侧仍用空串，同一个槽位就会算出两个指纹 —— 后果是
    「不可恢复 → 删除配置」找不到它（配置删不掉）。
    """
    return slot_fingerprint(cfg.base_url, cfg.model_name, _token_for_config(cfg))


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
            llm = TrackedChatOpenAI(
                model=cfg.model_name,
                base_url=cfg.base_url,
                api_key=SecretStr(_token_for_config(cfg)),
                rate_limiter=rate_limiter,
            )
            # 状态迁移的副作用（删除配置等）由池处理：统计层不认识池，池也不管统计口径
            handler: StateChangeHandler = _on_slot_state_change
            llm.set_state_change_handler(handler)
            llms.append(llm)
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


def remove_llm_by_fingerprint(fingerprint: str) -> bool:
    """按槽位指纹删除「配置 + 缓存实例」，返回是否真的删掉了。

    与整池重建（``set_llm_apis``）不同：这里**保留其他槽位的实例对象**，
    因此其他槽位的统计与健康状态（冷却进度等）不会因为删掉一个坏 key 而被清零。

    仅影响运行时内存，不写回 .env：重启后以环境变量配置为准。
    """
    global _free_llm_cache, _free_llm_cache_key
    # 先确保缓存已按当前配置构建，否则可能只删了配置、留下悬空实例
    cached = _get_free_llms()
    if not any(llm.slot_fingerprint == fingerprint for llm in cached):
        return False
    settings.llm_apis = [
        cfg for cfg in settings.llm_apis if _fingerprint_of(cfg) != fingerprint
    ]
    _free_llm_cache = [
        llm for llm in _free_llm_cache if llm.slot_fingerprint != fingerprint
    ]
    _free_llm_cache_key = _config_key()
    return True


def get_removed_llm_records() -> list[RemovedLLMRecord]:
    """已被删除的槽位记录（不可恢复错误的处置痕迹，供日志 / 接口观测）"""
    return list(_removed_records)


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


def _remove_slot_from_cache(
    fingerprint: str, *, model_name: str, base_url: str, reason: str | None
) -> None:
    """把某个槽位从「配置 + 缓存实例」里摘掉，并记一条删除记录。

    只动运行时内存（与 :func:`set_llm_apis` 的语义一致，不写回 .env）：
    重启后以环境变量配置为准，运维可据此确认坏配置是否已从部署里清掉。
    """
    removed = remove_llm_by_fingerprint(fingerprint)
    _removed_records.append(
        RemovedLLMRecord(
            fingerprint=fingerprint,
            model_name=model_name,
            base_url=base_url,
            reason=reason,
            removed_at=time.time(),
        )
    )
    if not removed:
        logger.warning(
            "LLM 槽位标记为不可恢复，但配置中已找不到它（可能已被热更新替换）："
            "model={} base_url={}",
            model_name,
            base_url,
        )
        return
    logger.error(
        "LLM 槽位因不可恢复错误被删除配置：model={} base_url={} 原因={}"
        "（重启或重新配置 llm_apis 可恢复）",
        model_name,
        base_url,
        reason,
    )


def get_all_free_llms() -> list[TrackedChatOpenAI]:
    """返回当前【可用】(healthy) 的云端 LLM 实例（已按轮询顺序旋转）。

    - 不可恢复错误（鉴权失败 / 模型下线 / 模型不存在）的槽位在状态迁移时
      已被删除配置，这里不会再出现；
    - 冷却中的槽位（限流 / 今日额度 / 欠费 / 连续瞬时失败）本轮不参与尝试，
      到点会自动恢复为 healthy；调用方拿到 :class:`AllLLMsCoolingError`
      时可按其 ``resume_at`` 等到点再试。

    可用实例为空时抛错（均为 RuntimeError 子类，兼容既有 ``except RuntimeError``）：
    - 未配置任何云端 API → ``RuntimeError``；
    - 全部已删除 → :class:`AllLLMsDisabledError`；
    - 全部在冷却中 → :class:`AllLLMsCoolingError`。

    用法：
        for llm in get_all_free_llms():
            structured_llm = llm.with_structured_output(schema=MyModel)
            ...
    """
    rotated, _ = _next_rotated_llms()
    if not rotated:
        raise RuntimeError("未配置任何云端 LLM（llm_apis 为空），无法进行云端判断")
    usable = [llm for llm in rotated if llm.available]
    if usable:
        return usable

    cooling = [llm for llm in rotated if not llm.disabled]
    if not cooling:
        detail = "; ".join(
            f"{llm.model_name}（{llm.stats.health.reason}）" for llm in rotated
        )
        raise AllLLMsDisabledError(
            f"全部云端 LLM 均已因不可恢复错误被删除，无法进行云端判断：{detail}"
        )
    detail = "; ".join(
        f"{llm.model_name}（{llm.stats.state.value}，"
        f"{_describe_resume(llm.stats.health.resume_delay())}）"
        for llm in cooling
    )
    raise AllLLMsCoolingError(
        f"全部云端 LLM 均在冷却中，暂不可用：{detail}",
        resume_at=_earliest_resume_at(cooling),
    )


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