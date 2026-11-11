"""云端 LLM 实例池（构建、缓存、轮询）

通过环境变量 llm_apis 配置 OpenAI 兼容 API 列表（Pydantic list[LLMApiConfig]）：
    llm_apis='[{"base_url":"https://...","model_name":"gpt-3.5","token":"sk-xxx"}]'
    或：llm_apis__0__base_url=...  llm_apis__0__model_name=...  llm_apis__0__token=...

按列表顺序轮询选择主模型。所有调用均走云端，不再使用本地大模型。
调用方通过 get_all_free_llms() 获取当前【可用】(healthy) 的实例并逐个显式尝试。

可用性由 ``Service/llm_service/health.py`` 的状态机描述：

- 不可恢复错误（鉴权失败 / 模型下线 / 模型不存在等）→ **连续命中 2 次**才删除该条配置；
  第 1 次只做「停用 10 分钟 + 复验」（见 ``health.PERMANENT_CONFIRM_HITS``），
  避免网关抖动 / base_url 少写路径这类一次性 404 把好配置永久摘掉；
- 限流 / 今日额度 / 欠费 / 连续瞬时失败 → 进入冷却，到点自动恢复，本轮不参与尝试。

因此 get_all_free_llms() 可能抛出（均为 RuntimeError 子类，调用方按类型分流）：
- RuntimeError：未配置任何云端 API；
- AllLLMsDisabledError：全部已删除（不可恢复，需要人工重新配置）；
- AllLLMsCoolingError：全部在冷却中（可自动恢复，按 resume_at 等待）。

支持运行时热更新：set_llm_apis() 可在线替换 settings.llm_apis 并立即重建实例池
（get_llm_configs() 读取当前配置，token 已脱敏），对应内部接口 GET/POST /llm/config。

持久化（2026-11-08）：set_llm_apis() 在实例池构建成功后**落盘** ``runtime/llm_apis.json``，
因此「接口 / 管理页保存的配置」重启后仍然生效。事实来源为「runtime 文件 > .env」。
**但状态机的自动摘除（REMOVED）刻意不落盘**：那是运行时观测结论，
保留「上游误报 401 时重启即恢复」的防误删兜底（见 remove_llm_by_fingerprint）。

配置列表与运行状态分离（2026-11-11）：``settings.llm_apis`` 是「人工配置列表」
（``.env`` / 持久化文件 / 管理页保存的内容），**REMOVED 不再从列表里删条目**，
只把对应实例标记为终态。否则「内存 1 条、磁盘 9 条」的撕裂会让管理页按 index
回填密钥时全部错位，保存一次就把脱敏 token 当成真密钥写回文件。
GET /llm/config 始终返回完整人工列表，状态与原因由 GET /llm/stats 按
``base_url + model_name`` 合并展示。
"""

import time
from typing import Any

from langchain_core.rate_limiters import InMemoryRateLimiter
from loguru import logger
from pydantic import BaseModel, Field, SecretStr

from Service.llm_service import SamplingPreset, config_store
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
    reason: str | None = Field(
        default=None, description="删除原因（错误分类 + 服务端返回）"
    )
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
    """状态迁移副作用：不可恢复（REMOVED）→ 删除该条配置。

    冷却类状态（COOLING / QUOTA_WAIT / SUSPENDED）不做删除，
    到点由状态机自动恢复；日志已在 ``TrackedChatOpenAI._log_state_change`` 打过。

    注意 REMOVED 现在需要「连续 2 次不可恢复」才会出现（首次只停用复验），
    因此单次 404 / 401 不会走到这里删配置。
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
    """把槽位标记为 ``REMOVED`` 终态，返回是否在实例池里找到它。

    **不再从 ``settings.llm_apis`` / 实例缓存里删条目**：人工配置列表是管理页、
    ``probe.resolve_probe_config`` 的 index 口径与持久化文件的共同事实来源，
    一旦运行时把它缩短，索引就会整体前移，后续按 index 回填密钥必然串位。
    保留实例对象还有两个好处：其他槽位的统计 / 冷却进度不会被整池重建清零，
    GET /llm/stats 也能继续展示被摘除条目的 ``state=removed`` 与原因。

    实例在状态机迁移到 REMOVED 时 ``available=False`` / ``disabled=True``，
    ``get_all_free_llms()`` 不会再返回它；这里补一次 ``mark_removed`` 是为了
    兼容「外部直接调用本函数」（不经过状态机）的场景。

    仍然**只影响运行时内存**：不写 ``.env``，也不写 ``runtime/llm_apis.json``。
    运维要真正删除坏配置，应在管理页列表里删掉后保存一次。
    """
    cached = _get_free_llms()
    target = next(
        (llm for llm in cached if llm.slot_fingerprint == fingerprint), None
    )
    if target is None:
        return False
    target.stats.health.mark_removed(target.stats.health.reason)
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
    """把某个槽位标记为 ``REMOVED`` 终态并记一条观测记录。

    只动运行时内存：不写 ``.env``，也**不写** ``runtime/llm_apis.json``；
    配置条目仍留在 ``settings.llm_apis`` 里（管理页可见、index 不漂移）。
    重启或在管理页重新保存后该槽位会以 healthy 状态回来。
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
            "LLM 槽位标记为不可恢复，但实例池里已找不到它（可能已被热更新替换）："
            "model={} base_url={}",
            model_name,
            base_url,
        )
        return
    logger.error(
        "LLM 槽位连续命中不可恢复错误，已标记为 REMOVED：model={} base_url={} 原因={}"
        "（配置条目保留，管理页可见；重启或重新保存后恢复）",
        model_name,
        base_url,
        reason,
    )
    # 按 base_url 维度提示：如果这条 base_url 上的所有槽位都被摘掉了，
    # 真正错的很可能是 base_url（域名 / 网关 / 少了 /v1），而不是多个模型同时下线。
    if not any(
        llm.openai_api_base == base_url and llm.available
        for llm in _get_free_llms()
    ):
        logger.error(
            "LLM 的 base_url 上已没有任何可用槽位，请优先核对 base_url 是否正确"
            "（域名 / 反向代理 / 路径前缀，如 /v1）：base_url={}",
            base_url,
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


def normalize_base_url(base_url: str | None) -> str:
    """归一化接口地址：去空白 + 去尾斜杠。

    管理页把「同一地址 + 同一密钥下的多个模型」合并成一张卡片，分组键就是
    ``base_url + 脱敏 token``。``…/v1`` 与 ``…/v1/`` 若被当成两个源，同一张卡片
    会被拆成两个，所以配置接口与统计接口**都**按这里归一化后再返回。
    """
    return (base_url or "").strip().rstrip("/")


def get_llm_stats() -> list[dict[str, Any]]:
    """导出当前所有 LLM 实例的统计快照（便于日志 / 监控 / 接口直接返回）"""
    return [
        {
            "model": llm.model_name,
            "base_url": normalize_base_url(llm.openai_api_base),
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


def _slot_state(
    cfg: LLMApiConfig, llms: list[TrackedChatOpenAI]
) -> tuple[str, str | None]:
    """按槽位指纹找实例的健康状态；找不到（池未构建）时返回 unknown。"""
    fingerprint = _fingerprint_of(cfg)
    for llm in llms:
        if llm.slot_fingerprint == fingerprint:
            health = llm.stats.health
            return health.state.value, health.reason
    return "unknown", None


def get_llm_configs() -> list[dict[str, Any]]:
    """导出**完整**人工配置列表（token 已脱敏），供内部配置接口读取。

    条目不会因 REMOVED 被摘掉；额外的 ``state`` / ``reason`` / ``removed``
    只是把运行状态一并带出来，便于 ``curl /llm/config`` 直接看出哪条被摘除。
    管理页仍通过 ``GET /llm/stats`` 按 ``base_url + model_name`` 合并展示。
    """
    llms = _get_free_llms()
    configs: list[dict[str, Any]] = []
    for cfg in settings.llm_apis:
        state, reason = _slot_state(cfg, llms)
        configs.append(
            {
                # 与 get_llm_stats 同一口径，管理页据此把同源多模型合并成一张卡片
                "base_url": normalize_base_url(cfg.base_url),
                "model_name": cfg.model_name,
                "token": _mask_token(cfg.token),
                "requests_per_second": cfg.requests_per_second,
                "state": state,
                "reason": reason,
                "removed": state == LLMState.REMOVED.value,
            }
        )
    return configs


def _usable_token(cfg: LLMApiConfig) -> str:
    """取一条配置里「可以沿用的明文密钥」；脱敏值 / 空值一律视为没有。"""
    token = cfg.token
    if not token or config_store.is_masked_token(token):
        return ""
    return token


def _original_token(current: list[LLMApiConfig], index: int, base_url: str) -> str:
    """取该条配置可沿用的明文密钥：**同索引优先，其次按 base_url 找同源实例**。

    同索引必须 base_url 也对得上才用：管理页一张卡片可能一次勾选多个模型，
    保存时会展开成多条（行序整体后移），此时 index 位置已经是别的实例，
    无条件按 index 取会把 A 服务商的密钥发给 B 服务商。按 base_url 兜底后，
    「同一地址下增删模型」都能正确沿用密钥。

    找不到返回空串（新实例自带明文，或免鉴权上游）。
    ``base_url`` 只比较去尾斜杠后的值，避免 `…/v1` 与 `…/v1/` 被判成两个源。
    """
    normalized = base_url.rstrip("/")
    if index < len(current) and current[index].base_url.rstrip("/") == normalized:
        token = _usable_token(current[index])
        if token:
            return token
    for cfg in current:
        if cfg.base_url.rstrip("/") == normalized:
            token = _usable_token(cfg)
            if token:
                return token
    return ""


def _resolve_tokens(
    apis: list[LLMApiConfig], current: list[LLMApiConfig]
) -> list[LLMApiConfig]:
    """把「空 token / 脱敏 token」回填成**当前生效的明文密钥**。

    管理页永远拿不到明文，保存时只能把读到的脱敏串原样回传（或传空串）；
    这里按 :func:`_original_token` 取回真密钥，与 ``probe.resolve_api_token`` 同一口径。
    因为 REMOVED 不再删配置，``current`` 始终是完整人工列表，索引不会漂移。

    硬约束：任何含 ``****`` 的串都不得出现在结果里（新行给脱敏值直接报错）。
    ``save_persisted`` 还有一道同样的闸门，避免别的上游绕过本函数。
    """
    resolved: list[LLMApiConfig] = []
    for index, cfg in enumerate(apis):
        token = cfg.token
        if config_store.is_masked_token(token):
            # 脱敏值只承认「同索引 + 同地址」的回填：跨实例猜密钥是明确禁止的，
            # 接口禁止把 **** 写进配置，这里必须报错而不是「找个同源的顶上」。
            slot = current[index] if index < len(current) else None
            same_source = (
                slot is not None
                and slot.base_url.rstrip("/") == cfg.base_url.rstrip("/")
            )
            original = _usable_token(slot) if same_source and slot is not None else ""
            if not original:
                raise ValueError(
                    f"llm_apis 配置非法：第 {index} 项 token 是脱敏值且无可用原密钥，"
                    "请填写明文密钥"
                )
            token = original
        elif not token:
            # 管理页传空串 = 沿用原密钥（同索引优先，其次按地址找同源实例）；
            # 新行无原密钥时保留空串（免鉴权上游）
            token = _original_token(current, index, cfg.base_url)
        if config_store.is_masked_token(token):
            raise ValueError(
                f"llm_apis 配置非法：第 {index} 项 token 含脱敏标记，拒绝写入"
            )
        resolved.append(cfg.model_copy(update={"token": token}))
    return resolved


async def set_llm_apis(
    apis: list[LLMApiConfig], *, persist: bool = True
) -> list[dict[str, Any]]:
    """在线替换云端 LLM 配置并立即生效，同时**持久化**到本地文件（无需重启服务）。

    - 空 / 脱敏 token 按 index 回填当前明文密钥（:func:`_resolve_tokens`）；
    - 修改运行时内存中的 ``settings.llm_apis``，并重建实例池（不等待下一次调用）；
    - 实例池构建成功后，把整份列表写入 ``runtime/llm_apis.json``
      （见 :mod:`Service.llm_service.config_store`），因此重启后配置仍然生效；
      不写回 ``.env``（容器改宿主机文件不可靠且无审计）；
    - 任一步失败都回滚 ``settings.llm_apis`` 与实例池缓存到旧值并抛出异常，
      避免出现「内存已改、文件未改」的撕裂状态；
    - 校验：每个配置项必须同时提供 base_url 与 model_name。

    **async**：只有文件 IO 走 ``asyncio.to_thread``，内存态（settings / 实例池）
    始终在事件循环里改，避免多线程同时改全局缓存。

    ``persist=False`` 仅供「删除持久化文件后热切回 ``.env``」使用
    （:func:`restore_llm_apis_from_env`），否则切回动作会立刻把配置又写回去。

    返回：更新后的配置（token 已脱敏）。
    """
    invalid = [
        f"[{i}]" for i, cfg in enumerate(apis) if not cfg.base_url or not cfg.model_name
    ]
    if invalid:
        raise ValueError(
            f"llm_apis 配置非法：第 {', '.join(invalid)} 项缺少 base_url 或 model_name"
        )
    # 回填与硬校验必须在改内存之前完成，失败时 settings 保持原样
    resolved = _resolve_tokens(apis, list(settings.llm_apis))
    if any(config_store.is_masked_token(cfg.token) for cfg in resolved):
        raise ValueError("llm_apis 配置非法：拒绝把脱敏 token 写入运行配置")

    global _free_llm_cache, _free_llm_cache_key
    old_apis = settings.llm_apis
    old_cache, old_key = _free_llm_cache, _free_llm_cache_key
    settings.llm_apis = resolved
    try:
        _free_llm_cache = _build_free_llms()
        _free_llm_cache_key = _config_key()
        if persist:
            # 落盘放在建池成功之后：非法配置进不来，写失败则整体回滚
            await config_store.save_persisted_async(settings.llm_apis)
            config_store.mark_source("file")
    except Exception:
        # 回滚，保证「改一半失败」不会污染当前运行配置
        settings.llm_apis = old_apis
        _free_llm_cache, _free_llm_cache_key = old_cache, old_key
        raise
    return get_llm_configs()


async def restore_llm_apis_from_env() -> list[dict[str, Any]]:
    """删除持久化文件并热切回 ``.env`` 配置，返回切换后的脱敏列表。

    对应内部接口 ``DELETE /llm/config/override``：运维想「撤销所有人工保存」时用。
    文件删除后再切配置（``persist=False``），否则会立刻把 ``.env`` 的列表又写回文件。
    """
    await config_store.delete_persisted_async()
    configs = await set_llm_apis(config_store.env_apis(), persist=False)
    config_store.mark_source("env")
    logger.warning("云端 LLM 配置已切回环境变量（{} 条，持久化文件已删除）", len(configs))
    return configs


def _validate_index(index: int) -> None:
    """校验索引合法；越界抛 IndexError（由控制器转 404）"""
    total = len(settings.llm_apis)
    if not 0 <= index < total:
        raise IndexError(f"索引越界：当前共 {total} 条配置，index={index}")


def get_llm_config(index: int) -> dict[str, Any]:
    """读取指定索引的单条云端 LLM 配置（token 已脱敏）"""
    _validate_index(index)
    return get_llm_configs()[index]


async def create_llm_api(cfg: LLMApiConfig) -> list[dict[str, Any]]:
    """新增一条云端 LLM 配置（追加到列表末尾），返回更新后的完整配置列表"""
    return await set_llm_apis([*settings.llm_apis, cfg])


async def update_llm_api(index: int, cfg: LLMApiConfig) -> list[dict[str, Any]]:
    """整体更新指定索引的单条配置，返回更新后的完整配置列表"""
    _validate_index(index)
    apis = list(settings.llm_apis)
    apis[index] = cfg
    return await set_llm_apis(apis)


async def patch_llm_api(index: int, patch: LLMApiConfigPatch) -> list[dict[str, Any]]:
    """部分更新指定索引的单条配置（未传字段保持原值），返回更新后的完整配置列表"""
    _validate_index(index)
    apis = list(settings.llm_apis)
    merged = apis[index].model_dump()
    merged.update(
        {k: v for k, v in patch.model_dump(exclude_unset=True).items() if v is not None}
    )
    apis[index] = LLMApiConfig(**merged)
    return await set_llm_apis(apis)


async def delete_llm_api(index: int) -> list[dict[str, Any]]:
    """删除指定索引的单条配置，返回剩余配置列表（空列表表示已清空全部）"""
    _validate_index(index)
    apis = list(settings.llm_apis)
    del apis[index]
    return await set_llm_apis(apis)


if __name__ == "__main__":

    async def _test():

        llms = get_all_free_llms()
        llm = llms[0]
        llm.bind(**SamplingPreset.TEXT_NON_THINKING.to_kwargs(num_predict=256))
        res = await llm.ainvoke("1+1=?")
        print(res)

    import asyncio

    asyncio.run(_test())
