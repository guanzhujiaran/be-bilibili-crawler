"""云端 LLM 实例列表的本地持久化（``runtime/llm_apis.json``，不入库）。

为什么需要它：配置原本只来自根 ``.env`` 的 ``llm_apis``，而接口 / 管理页保存
（``set_llm_apis``）只改运行时内存 —— 进程一重启，运维的修正就被静默丢弃。

事实来源优先级：**runtime 文件 > ``.env``**

- 文件不存在 → 用 ``.env``（首次保存前行为与改造前完全一致，不会凭空创建文件）；
- 文件合法 → **整体覆盖** ``settings.llm_apis``（空列表也算合法：运维显式清空后重启仍为空）；
- 文件损坏 / 版本不符 / 校验失败 → 打 warning（不含 token）并回退 ``.env``，
  **绝不因为坏文件导致服务起不来**。

刻意**不**持久化的东西（见 ``docs/be-bilibili-crawler-LLM配置持久化计划书.md`` 第 3.4 节）：

状态机判定 ``REMOVED``（401 / 404 / 410 / 模型下线等不可恢复错误）后的自动摘除
**只改运行时内存**，既不写 ``.env`` 也不写本文件。理由是保留「上游误报 401 时重启即恢复」
的防误删兜底 —— 那份「删除」是运行时观测结论，不是运维意图。
运维要真正删掉坏配置，应在列表里删掉后保存一次（人工保存会落盘）。

介质选型：只用本地 JSON 文件。读写形态是「整体覆盖写 + 启动读一次」，
没有查询 / 分页 / 多字段更新，条目个位数，写频率是「运维偶尔点一次保存」，
引入 MySQL / Redis 只会带来迁移、连接池、密钥入库等成本，收益为零。
写入用 ``mkstemp`` + ``os.replace`` 原子整体覆盖，读者永远看到旧的或新的完整文件。
"""

import asyncio
import json
import os
import tempfile
import time
from pathlib import Path

from loguru import logger
from pydantic import BaseModel, Field

from CONFIG import LLMApiConfig, settings

#: 持久化文件名（落在 ``RUNTIME_DIR`` 下）
_FILE_NAME = "llm_apis.json"

#: 文件结构版本；读到不认识的值一律当「文件不可用」处理（回退 ``.env``），
#: 保证降级部署不会拿新格式乱跑。
_FILE_VERSION = 1

#: token 脱敏标记。任何含它的串都**不是**密钥，禁止写入 ``settings.llm_apis``
#: 或持久化文件（写进去会导致后续所有请求 401 → 状态机 REMOVED）。
MASK_FLAG = "****"


def is_masked_token(token: str) -> bool:
    """判断是否脱敏串（``_mask_token`` 生成的串一定含 ``****``）。"""
    return MASK_FLAG in token


def recover_masked_tokens(
    items: list[LLMApiConfig],
) -> tuple[list[LLMApiConfig], int, int]:
    """把持久化文件里被写坏的脱敏 token 恢复成明文密钥。

    恢复口径（从严到宽，都是同一份 ``.env`` 明文快照）：
    1. ``(base_url, model_name)`` 完全命中；
    2. 同一 ``base_url`` 命中（模型名被运维改过，但密钥通常按厂商共用一个）；
    3. 同一索引且 base_url 未变。
    都找不到时把 token 置空（保留条目、保留列表长度，绝不把脱敏串带回运行配置，
    也绝不把脱敏串再写回文件）。

    返回 ``(清洗后的列表, 已恢复条数, 置空条数)``；日志只打模型名与数量，
    不打 token。
    """
    plain_env = [cfg for cfg in _env_apis if not is_masked_token(cfg.token)]
    by_key = {(cfg.base_url, cfg.model_name): cfg.token for cfg in plain_env}
    by_url = {}
    for cfg in plain_env:
        # 同一厂商通常共用一个 key；保留快照里第一个，避免被后面的覆盖
        by_url.setdefault(cfg.base_url, cfg.token)
    cleaned: list[LLMApiConfig] = []
    recovered = 0
    cleared = 0
    for index, cfg in enumerate(items):
        if not is_masked_token(cfg.token):
            cleaned.append(cfg)
            continue
        original = by_key.get((cfg.base_url, cfg.model_name)) or by_url.get(
            cfg.base_url
        )
        if original is None and index < len(_env_apis):
            candidate = _env_apis[index]
            if candidate.base_url == cfg.base_url and not is_masked_token(
                candidate.token
            ):
                original = candidate.token
        if original is None:
            cleared += 1
            logger.warning(
                "LLM 配置文件第 {} 条（model={}）的 token 是脱敏值且 .env 无对应密钥，"
                "已置空，请在管理页重新填写",
                index,
                cfg.model_name,
            )
            cleaned.append(cfg.model_copy(update={"token": ""}))
        else:
            recovered += 1
            cleaned.append(cfg.model_copy(update={"token": original}))
    return cleaned, recovered, cleared


class LLMApiConfigFile(BaseModel):
    """``llm_apis.json`` 的文件结构（字段与 :class:`LLMApiConfig` 完全一致，便于校验）"""

    version: int = Field(default=_FILE_VERSION, description="文件结构版本，当前固定 1")
    updated_at: int = Field(description="最后保存时间戳（unix 秒）")
    items: list[LLMApiConfig] = Field(
        default_factory=list, description="云端 LLM 实例列表（token 为明文）"
    )


#: ``.env`` 提供的配置快照：**本模块 import 时就抓**（此时 settings 里只可能是 .env 的值），
#: 供 ``DELETE /llm/config/override`` 热切回使用。
#: 刻意不在 ``apply_to_settings()`` 里抓：那样「先 set_llm_apis 再切回」会抓成文件里的列表。
_env_apis: list[LLMApiConfig] = list(settings.llm_apis)

#: 当前生效来源：``file`` / ``env``（由 :func:`apply_to_settings` 维护）
_source: str = "env"


def _runtime_dir() -> Path:
    """运行期状态目录：优先 ``RUNTIME_DIR``，否则用项目根下的 ``runtime/``"""
    raw = (settings.RUNTIME_DIR or "").strip()
    if raw:
        return Path(raw)
    return Path(__file__).resolve().parents[2] / "runtime"


def config_file_path() -> Path:
    """持久化文件的绝对路径"""
    return _runtime_dir() / _FILE_NAME


def env_apis() -> list[LLMApiConfig]:
    """``.env`` 提供的配置快照（回退入口热切回用，不受运行时修改影响）"""
    return list(_env_apis)


def current_source() -> str:
    """当前生效来源：``file`` 或 ``env``"""
    return _source


def load_persisted(*, repair: bool = True) -> LLMApiConfigFile | None:
    """读取并校验持久化文件；缺失 / 损坏 / 版本不符时返回 None（并打 warning）。

    warning 里**只**出现路径与错误，绝不回显 token。

    自愈（``repair=True``）：历史版本把管理端回传的脱敏 token 当真密钥落了盘，
    这类文件读进来会立刻让所有实例 401。这里把脱敏 token 从 ``.env`` 快照
    按「同名配置 / 同索引」恢复，并把清洗后的列表原子写回文件，保证
    「文件里永远没有 ``****``」；写回失败也不影响本次启动（内存已清洗）。
    """
    path = config_file_path()
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as f:
            payload = json.load(f)
    except Exception as e:  # noqa: BLE001 —— 坏文件只降级，不能让服务起不来
        logger.warning("读取 LLM 配置文件失败，回退环境变量（{}）：{!r}", path, e)
        return None
    try:
        parsed = LLMApiConfigFile.model_validate(payload)
    except Exception as e:  # noqa: BLE001 —— 同上
        logger.warning("LLM 配置文件结构不合法，回退环境变量（{}）：{!r}", path, e)
        return None
    if parsed.version != _FILE_VERSION:
        logger.warning(
            "LLM 配置文件版本不受支持（期望 {}，实际 {}），回退环境变量（{}）",
            _FILE_VERSION,
            parsed.version,
            path,
        )
        return None

    cleaned, recovered, cleared = recover_masked_tokens(parsed.items)
    if not (recovered or cleared):
        return parsed
    logger.warning(
        "LLM 配置文件含脱敏 token，已自愈：恢复 {} 条、置空 {} 条（共 {} 条，路径 {}）",
        recovered,
        cleared,
        len(cleaned),
        path,
    )
    parsed = parsed.model_copy(update={"items": cleaned})
    if repair:
        try:
            # 原子写 + 0600 由 save_persisted 保证；失败只降级为「内存已清洗」
            save_persisted(cleaned)
        except Exception as e:  # noqa: BLE001 —— 自愈失败不能阻塞启动
            logger.warning("LLM 配置文件自愈写回失败（内存已清洗）：{!r}", e)
    return parsed


def save_persisted(apis: list[LLMApiConfig]) -> Path:
    """把整份列表原子覆盖写入文件（含明文 token，权限 0600），返回文件路径。

    先写同目录临时文件再 ``os.replace``：读者永远看到旧的或新的完整 JSON，
    不会读到半截内容。失败时清理临时文件并抛出（由调用方回滚内存）。

    **硬校验**：任何含 ``****`` 的 token 一律拒绝落盘 —— 这是最后一道闸门，
    即使上游（接口 / 管理页 / 脚本）漏了校验也不会把脱敏串写成「密钥」。
    """
    masked = [i for i, cfg in enumerate(apis) if is_masked_token(cfg.token)]
    if masked:
        raise ValueError(
            f"拒绝落盘：第 {', '.join(str(i) for i in masked)} 条配置的 token 是脱敏值"
        )
    path = config_file_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = LLMApiConfigFile(
        version=_FILE_VERSION,
        updated_at=int(time.time()),
        items=apis,
    ).model_dump(mode="json")
    fd, tmp_path = tempfile.mkstemp(prefix=f".{_FILE_NAME}.", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        # 文件里是明文密钥：比 push_switch 的 0644 更严，只允许属主读写
        os.chmod(tmp_path, 0o600)
        os.replace(tmp_path, path)
    except Exception:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        raise
    # 只打条数与路径，绝不打 items（里面有明文 token）
    logger.info("云端 LLM 配置已保存到 {}：{} 条", path, len(apis))
    return path


def delete_persisted() -> bool:
    """删除持久化文件（不存在返回 False），用于热切回 ``.env``"""
    path = config_file_path()
    try:
        path.unlink()
    except FileNotFoundError:
        return False
    logger.info("云端 LLM 配置持久化文件已删除：{}", path)
    return True


def apply_to_settings(*, repair: bool = True) -> str:
    """启动阶段应用持久化配置，返回生效来源（``file`` / ``env``）。

    - ``.env`` 快照在本模块 import 时已抓好（见 ``_env_apis``），这里只做应用；
    - 文件合法时**整体覆盖** ``settings.llm_apis``（空列表也算合法保存状态）；
    - 文件缺失 / 不可用时不改动 ``settings.llm_apis``，继续用 ``.env``；
    - 只改配置、不重建实例池：池在首次 ``get_all_free_llms()`` 时按配置指纹惰性构建，
      启动阶段不做无谓的预热与噪音日志。
    """
    global _source
    parsed = load_persisted(repair=repair)
    if parsed is None:
        _source = "env"
        logger.info("LLM 配置来源: env（{} 条）", len(settings.llm_apis))
        return _source
    settings.llm_apis = list(parsed.items)
    _source = "file"
    # 用 WARNING 级：默认 LOG_LEVEL=WARNING，这条要能出现在日志里 ——
    # 「本地文件覆盖了 .env」是运维必须知道的事实，不该只在调试级别可见。
    logger.warning(
        "LLM 配置来源: file（{} 条，保存于 {}，覆盖 .env）",
        len(settings.llm_apis),
        parsed.updated_at,
    )
    return _source


def mark_source(source: str) -> None:
    """标记当前生效来源（回退入口切换后调用）"""
    global _source
    _source = source


# region 非阻塞文件 IO
# 上面四个函数是同步实现（启动阶段、脚本、单测直接调用），全部走本地小文件读写；
# 但 async 路由 / MQ 消费在事件循环里跑，磁盘抖动时一次 open/write/replace 就足以
# 让整条事件循环卡住（表现为 /health 偶发超时）。这里用 asyncio.to_thread 把
# **真正的同步 IO** 挪到线程池，内存态的 settings / 实例池仍只在事件循环里修改。
async def load_persisted_async(*, repair: bool = True) -> LLMApiConfigFile | None:
    """``load_persisted`` 的非阻塞版本（供 async 调用点 await）。"""
    return await asyncio.to_thread(load_persisted, repair=repair)


async def save_persisted_async(apis: list[LLMApiConfig]) -> Path:
    """``save_persisted`` 的非阻塞版本（供 async 调用点 await）。"""
    return await asyncio.to_thread(save_persisted, apis)


async def delete_persisted_async() -> bool:
    """``delete_persisted`` 的非阻塞版本（供 async 调用点 await）。"""
    return await asyncio.to_thread(delete_persisted)


async def apply_to_settings_async(*, repair: bool = True) -> str:
    """``apply_to_settings`` 的非阻塞版本（lifespan 里 await，不阻塞启动）。

    启动阶段还没有并发请求，整段（含自愈写回）放进线程池是安全的。
    只读查询角色应传 ``repair=False``：它不该写共享配置文件，自愈由 crawler 负责。
    """
    return await asyncio.to_thread(apply_to_settings, repair=repair)


# endregion


__all__ = [
    "LLMApiConfigFile",
    "MASK_FLAG",
    "apply_to_settings",
    "apply_to_settings_async",
    "config_file_path",
    "current_source",
    "delete_persisted",
    "delete_persisted_async",
    "env_apis",
    "is_masked_token",
    "load_persisted",
    "load_persisted_async",
    "mark_source",
    "recover_masked_tokens",
    "save_persisted",
    "save_persisted_async",
]
