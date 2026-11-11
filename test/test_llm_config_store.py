"""云端 LLM 配置持久化（runtime/llm_apis.json）的单测

纯本地、不联网、不连库：只覆盖
1. 写入 → 读回一致（顺序 / 字段 / 时间戳为秒级 int）；
2. token 明文只在文件里（接口返回脱敏、日志不回显）；
3. 文件缺失 / 损坏 / 版本不符 → 回退 .env，不抛异常；
4. 原子写（无残留临时文件、权限 0600）；
5. 落盘失败时 set_llm_apis 回滚内存与实例池缓存；
6. 自动摘除（REMOVED）不落盘；
7. 删除持久化文件后回到 .env。

运行:
    uv run python -m pytest test/test_llm_config_store.py -v
"""

import asyncio
import json
import os
import sys
import threading
from pathlib import Path

import pytest
from loguru import logger

_project_root = Path(__file__).resolve().parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from CONFIG import LLMApiConfig, settings
from Service.llm_service import config_store, pool
from Service.llm_service.tracked_llm import slot_fingerprint

_TOKEN = "nvapi-SECRET-token-should-never-be-logged"


def _cfg(model: str, *, rps: float = 0.5, token: str = _TOKEN) -> LLMApiConfig:
    return LLMApiConfig(
        base_url="https://integrate.api.nvidia.com/v1",
        model_name=model,
        token=token,
        requests_per_second=rps,
    )


@pytest.fixture()
def store_env(tmp_path, monkeypatch):
    """把持久化目录指向临时目录，并清掉全局状态（配置 / 实例池缓存 / env 快照）"""
    monkeypatch.setattr(settings, "RUNTIME_DIR", str(tmp_path))
    original_apis = list(settings.llm_apis)
    env_apis = [_cfg("env-model-a"), _cfg("env-model-b")]
    settings.llm_apis = list(env_apis)
    # .env 快照在 config_store import 时就抓好了，测试里显式替换成可控值
    monkeypatch.setattr(config_store, "_env_apis", list(env_apis))
    monkeypatch.setattr(config_store, "_source", "env")
    monkeypatch.setattr(pool, "_free_llm_cache", [])
    monkeypatch.setattr(pool, "_free_llm_cache_key", "")
    yield tmp_path
    settings.llm_apis = original_apis
    pool._free_llm_cache = []
    pool._free_llm_cache_key = ""


# ================================================================
# 1. 写入 → 读回一致
# ================================================================


def test_save_then_load_roundtrip(store_env):
    apis = [_cfg("model-b", rps=1.5), _cfg("model-a", rps=0.25)]
    path = config_store.save_persisted(apis)

    assert path == store_env / "llm_apis.json"
    parsed = config_store.load_persisted()
    assert parsed is not None
    assert parsed.version == 1
    assert isinstance(parsed.updated_at, int)  # 秒级 int，不写 ISO 字符串
    assert [item.model_name for item in parsed.items] == ["model-b", "model-a"]
    assert [item.requests_per_second for item in parsed.items] == [1.5, 0.25]
    # 文件里是明文 token（供实例构建使用），字段与 LLMApiConfig 完全一致
    raw = json.loads(path.read_text(encoding="utf-8"))
    assert raw["items"][0]["token"] == _TOKEN


def test_save_overwrites_whole_list(store_env):
    config_store.save_persisted([_cfg("old-a"), _cfg("old-b")])
    config_store.save_persisted([_cfg("new-only")])
    parsed = config_store.load_persisted()
    assert parsed is not None
    assert [item.model_name for item in parsed.items] == ["new-only"]


# ================================================================
# 2. token 不外泄
# ================================================================


@pytest.mark.asyncio
async def test_token_masked_in_api_view_and_logs(store_env):
    messages: list[str] = []
    sink_id = logger.add(lambda m: messages.append(str(m)), level="DEBUG")
    try:
        await pool.set_llm_apis([_cfg("model-a")])
    finally:
        logger.remove(sink_id)

    configs = pool.get_llm_configs()
    assert len(configs) == 1
    assert _TOKEN not in configs[0]["token"]
    assert "****" in configs[0]["token"]
    # 保存 / 加载日志里都不能出现明文 token
    joined = "\n".join(messages)
    assert _TOKEN not in joined


# ================================================================
# 3. 缺失 / 损坏 / 版本不符 → 回退 .env
# ================================================================


def test_missing_file_falls_back_to_env(store_env):
    before = [c.model_name for c in settings.llm_apis]
    assert config_store.apply_to_settings() == "env"
    assert [c.model_name for c in settings.llm_apis] == before
    assert not (store_env / "llm_apis.json").exists()  # 不存在时不创建


@pytest.mark.parametrize(
    "content",
    [
        "{ this is not json",
        json.dumps({"version": 1, "updated_at": 1, "items": "not-a-list"}),
        json.dumps({"version": 99, "updated_at": 1, "items": []}),
    ],
)
def test_broken_file_falls_back_to_env(store_env, content):
    (store_env / "llm_apis.json").write_text(content, encoding="utf-8")
    before = [c.model_name for c in settings.llm_apis]
    # 不抛异常，且配置保持 .env 原样
    assert config_store.apply_to_settings() == "env"
    assert [c.model_name for c in settings.llm_apis] == before


def test_valid_file_overrides_env(store_env):
    config_store.save_persisted([_cfg("file-model")])
    assert config_store.apply_to_settings() == "file"
    assert [c.model_name for c in settings.llm_apis] == ["file-model"]
    assert config_store.current_source() == "file"


def test_empty_file_is_valid_cleared_state(store_env):
    config_store.save_persisted([])
    assert config_store.apply_to_settings() == "file"
    assert settings.llm_apis == []  # 运维显式清空后重启仍为空，不从 .env 复活


# ================================================================
# 4. 原子写
# ================================================================


def test_atomic_write_no_temp_left_and_mode_0600(store_env):
    config_store.save_persisted([_cfg("model-a")])
    config_store.save_persisted([_cfg("model-b")])
    assert [p.name for p in store_env.iterdir()] == ["llm_apis.json"]
    mode = os.stat(store_env / "llm_apis.json").st_mode & 0o777
    assert mode == 0o600  # 含明文 token，只允许属主读写


def test_delete_persisted(store_env):
    config_store.save_persisted([_cfg("model-a")])
    assert config_store.delete_persisted() is True
    assert not (store_env / "llm_apis.json").exists()
    assert config_store.delete_persisted() is False  # 已不存在


# ================================================================
# 5. 落盘失败 → 回滚
# ================================================================


@pytest.mark.asyncio
async def test_set_llm_apis_rolls_back_when_persist_fails(store_env, monkeypatch):
    old_names = [c.model_name for c in settings.llm_apis]
    old_cache, old_key = pool._free_llm_cache, pool._free_llm_cache_key

    def _boom(_apis):
        raise OSError("磁盘只读")

    monkeypatch.setattr(config_store, "save_persisted", _boom)
    with pytest.raises(OSError):
        await pool.set_llm_apis([_cfg("new-model")])

    # 内存与实例池缓存都回到旧值，不留「内存已改、文件未改」的撕裂状态
    assert [c.model_name for c in settings.llm_apis] == old_names
    assert pool._free_llm_cache == old_cache
    assert pool._free_llm_cache_key == old_key
    assert not (store_env / "llm_apis.json").exists()


@pytest.mark.asyncio
async def test_set_llm_apis_persists_on_success(store_env):
    await pool.set_llm_apis([_cfg("model-a"), _cfg("model-b", rps=2.0)])
    parsed = config_store.load_persisted()
    assert parsed is not None
    assert [item.model_name for item in parsed.items] == ["model-a", "model-b"]
    assert config_store.current_source() == "file"


@pytest.mark.asyncio
async def test_invalid_config_rejected_before_persist(store_env):
    with pytest.raises(ValueError):
        await pool.set_llm_apis([LLMApiConfig(base_url="", model_name="x")])
    assert not (store_env / "llm_apis.json").exists()


# ================================================================
# 6. 自动摘除不落盘、不删配置（管理页 GET 仍是完整列表）
# ================================================================


@pytest.mark.asyncio
async def test_auto_removal_keeps_full_config_list(store_env):
    cfg = _cfg("doomed-model")
    await pool.set_llm_apis([cfg, _cfg("healthy-model")])
    path = store_env / "llm_apis.json"
    before = path.read_text(encoding="utf-8")

    fingerprint = slot_fingerprint(cfg.base_url, cfg.model_name, cfg.token)
    assert pool.remove_llm_by_fingerprint(fingerprint) is True

    # 配置条目与文件都**原样保留**：index 不漂移，管理页按 index 回填密钥才不会串位
    assert [c.model_name for c in settings.llm_apis] == [
        "doomed-model",
        "healthy-model",
    ]
    assert path.read_text(encoding="utf-8") == before
    # GET /llm/config 返回完整列表，被摘除的条目带 removed 状态 + 原因
    configs = pool.get_llm_configs()
    assert len(configs) == 2
    assert configs[0]["state"] == "removed"
    assert configs[0]["removed"] is True
    assert configs[1]["state"] == "healthy"
    # GET /llm/stats 也保留该实例，管理页可展示状态/原因
    states = {row["model"]: row["state"] for row in pool.get_llm_stats()}
    assert states == {"doomed-model": "removed", "healthy-model": "healthy"}
    # 重启 / 重新保存后槽位回到 healthy —— 防误删兜底
    assert config_store.apply_to_settings() == "file"
    assert "doomed-model" in [c.model_name for c in settings.llm_apis]


# ================================================================
# 6.5 脱敏 token 绝不写回（本次故障根因的回归用例）
# ================================================================


@pytest.mark.asyncio
async def test_masked_token_save_keeps_original_key(store_env):
    """管理页原样回传脱敏 token：必须按 index 沿用原明文密钥，且文件里没有 **** 。"""
    original_a = "sk-real-key-a-0001"
    original_b = "sk-real-key-b-0002"
    await pool.set_llm_apis(
        [
            _cfg("model-a", token=original_a),
            _cfg("model-b", token=original_b),
        ]
    )
    # 只改限流、token 原样回传脱敏值（甚至空串）
    await pool.set_llm_apis(
        [
            _cfg("model-a", token="sk-r****0001", rps=1.0),
            _cfg("model-b", token="", rps=2.0),
        ]
    )

    raw = json.loads((store_env / "llm_apis.json").read_text(encoding="utf-8"))
    tokens = [item["token"] for item in raw["items"]]
    assert tokens == [original_a, original_b]
    assert not any(config_store.MASK_FLAG in t for t in tokens)
    assert [c.token for c in settings.llm_apis] == [original_a, original_b]
    assert [c.requests_per_second for c in settings.llm_apis] == [1.0, 2.0]


@pytest.mark.asyncio
async def test_empty_token_falls_back_to_same_source_key(store_env):
    """同一地址「一个密钥展开多个模型」：行序变化后空 token 仍能沿用同源密钥。

    管理页一张卡片可一次勾选多个模型，保存时展开成多条（后续行的 index 整体后移），
    此时按 index 取密钥会把别的实例的密钥串过来；实现按 base_url 兜底。
    """
    original_a = "sk-real-key-a-0001"
    original_b = "sk-real-key-b-0002"
    await pool.set_llm_apis(
        [
            _cfg("model-a", token=original_a),
            LLMApiConfig(
                base_url="https://other.example.com/v1",
                model_name="model-x",
                token=original_b,
            ),
        ]
    )
    # model-a 展开成两条（新增 model-a2，token 留空），后面的 model-x 顺移到 index 2
    await pool.set_llm_apis(
        [
            _cfg("model-a", token=""),
            _cfg("model-a2", token=""),
            LLMApiConfig(
                base_url="https://other.example.com/v1",
                model_name="model-x",
                token="",
            ),
        ]
    )

    assert [c.token for c in settings.llm_apis] == [original_a, original_a, original_b]
    raw = json.loads((store_env / "llm_apis.json").read_text(encoding="utf-8"))
    assert [item["token"] for item in raw["items"]] == [original_a, original_a, original_b]


@pytest.mark.asyncio
async def test_masked_token_for_new_row_is_rejected(store_env):
    await pool.set_llm_apis([_cfg("model-a")])
    with pytest.raises(ValueError, match="脱敏值"):
        await pool.set_llm_apis(
            [_cfg("model-a", token=""), _cfg("new-model", token="abcd****wxyz")]
        )
    # 失败不能污染内存 / 磁盘
    assert [c.model_name for c in settings.llm_apis] == ["model-a"]
    raw = json.loads((store_env / "llm_apis.json").read_text(encoding="utf-8"))
    assert len(raw["items"]) == 1


def test_save_persisted_rejects_masked_token(store_env):
    with pytest.raises(ValueError, match="拒绝落盘"):
        config_store.save_persisted([_cfg("model-a", token="abcd****wxyz")])
    assert not (store_env / "llm_apis.json").exists()


def test_poisoned_file_self_heals_from_env(store_env, monkeypatch):
    """历史脏文件里 8 条脱敏 token：从 .env 快照恢复并原子写回，文件里不再有 **** 。"""
    env_snapshot = [
        _cfg("env-model-a", token="plain-key-a-0001"),
        _cfg("env-model-b", token="plain-key-b-0002"),
    ]
    (store_env / "llm_apis.json").write_text(
        json.dumps(
            {
                "version": 1,
                "updated_at": 1,
                "items": [
                    {
                        "base_url": "https://integrate.api.nvidia.com/v1",
                        "model_name": "env-model-a",
                        "token": "plai****0001",
                        "requests_per_second": 0.5,
                    },
                    {
                        "base_url": "https://integrate.api.nvidia.com/v1",
                        "model_name": "env-model-b",
                        "token": "****",
                        "requests_per_second": 0.5,
                    },
                ],
            }
        ),
        encoding="utf-8",
    )
    # 模拟 import 时抓到的 .env 明文快照
    monkeypatch.setattr(config_store, "_env_apis", list(env_snapshot))

    parsed = config_store.load_persisted()

    assert parsed is not None
    assert [item.token for item in parsed.items] == [
        "plain-key-a-0001",
        "plain-key-b-0002",
    ]
    raw = json.loads((store_env / "llm_apis.json").read_text(encoding="utf-8"))
    assert sum(config_store.MASK_FLAG in x["token"] for x in raw["items"]) == 0


# ================================================================
# 6.6 文件 IO 不阻塞 asyncio 事件循环
# ================================================================


@pytest.mark.asyncio
async def test_async_save_does_not_block_event_loop(store_env, monkeypatch):
    """把「慢磁盘」注入到同步落盘函数，事件循环仍必须能继续调度其它协程。"""
    entered = threading.Event()
    release = threading.Event()

    def _slow_save(_apis):
        entered.set()
        release.wait(timeout=3)
        return config_store.config_file_path()

    monkeypatch.setattr(config_store, "save_persisted", _slow_save)
    ticks = 0
    stop = asyncio.Event()

    async def _ticker():
        nonlocal ticks
        while not stop.is_set():
            ticks += 1
            await asyncio.sleep(0.005)

    ticker = asyncio.create_task(_ticker())
    try:
        pending = asyncio.create_task(
            config_store.save_persisted_async([_cfg("model-a")])
        )
        assert await asyncio.to_thread(entered.wait, 1.0)
        await asyncio.sleep(0.05)
        assert ticks > 0, "同步文件 IO 阻塞了事件循环"
        release.set()
        assert await pending == store_env / "llm_apis.json"
    finally:
        stop.set()
        release.set()
        await ticker


@pytest.mark.asyncio
async def test_apply_to_settings_async_returns_source(store_env):
    config_store.save_persisted([_cfg("file-model")])
    assert await config_store.apply_to_settings_async() == "file"
    assert await config_store.delete_persisted_async() is True
    assert await config_store.load_persisted_async() is None


# ================================================================
# 7. 删除持久化文件 → 回到 .env
# ================================================================


@pytest.mark.asyncio
async def test_restore_from_env_after_override(store_env):
    env_names = [c.model_name for c in settings.llm_apis]
    await pool.set_llm_apis([_cfg("file-model")])
    assert [c.model_name for c in settings.llm_apis] == ["file-model"]

    configs = await pool.restore_llm_apis_from_env()

    assert [c["model_name"] for c in configs] == env_names
    assert [c.model_name for c in settings.llm_apis] == env_names
    assert config_store.current_source() == "env"
    assert not (store_env / "llm_apis.json").exists()
    assert _TOKEN not in json.dumps(configs)  # 返回的列表同样是脱敏的
