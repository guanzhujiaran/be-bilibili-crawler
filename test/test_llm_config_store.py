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

import json
import os
import sys
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


def test_token_masked_in_api_view_and_logs(store_env):
    messages: list[str] = []
    sink_id = logger.add(lambda m: messages.append(str(m)), level="DEBUG")
    try:
        pool.set_llm_apis([_cfg("model-a")])
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


def test_set_llm_apis_rolls_back_when_persist_fails(store_env, monkeypatch):
    old_names = [c.model_name for c in settings.llm_apis]
    old_cache, old_key = pool._free_llm_cache, pool._free_llm_cache_key

    def _boom(_apis):
        raise OSError("磁盘只读")

    monkeypatch.setattr(config_store, "save_persisted", _boom)
    with pytest.raises(OSError):
        pool.set_llm_apis([_cfg("new-model")])

    # 内存与实例池缓存都回到旧值，不留「内存已改、文件未改」的撕裂状态
    assert [c.model_name for c in settings.llm_apis] == old_names
    assert pool._free_llm_cache == old_cache
    assert pool._free_llm_cache_key == old_key
    assert not (store_env / "llm_apis.json").exists()


def test_set_llm_apis_persists_on_success(store_env):
    pool.set_llm_apis([_cfg("model-a"), _cfg("model-b", rps=2.0)])
    parsed = config_store.load_persisted()
    assert parsed is not None
    assert [item.model_name for item in parsed.items] == ["model-a", "model-b"]
    assert config_store.current_source() == "file"


def test_invalid_config_rejected_before_persist(store_env):
    with pytest.raises(ValueError):
        pool.set_llm_apis([LLMApiConfig(base_url="", model_name="x")])
    assert not (store_env / "llm_apis.json").exists()


# ================================================================
# 6. 自动摘除不落盘
# ================================================================


def test_auto_removal_does_not_touch_file(store_env):
    cfg = _cfg("doomed-model")
    pool.set_llm_apis([cfg, _cfg("healthy-model")])
    path = store_env / "llm_apis.json"
    before = path.read_text(encoding="utf-8")

    fingerprint = slot_fingerprint(cfg.base_url, cfg.model_name, cfg.token)
    assert pool.remove_llm_by_fingerprint(fingerprint) is True

    # 内存里删掉了（配置 + 缓存），但文件**原样不动**
    assert [c.model_name for c in settings.llm_apis] == ["healthy-model"]
    assert path.read_text(encoding="utf-8") == before
    # 重启（重新读文件）后该槽位回来 —— 防误删兜底
    assert config_store.apply_to_settings() == "file"
    assert "doomed-model" in [c.model_name for c in settings.llm_apis]


# ================================================================
# 7. 删除持久化文件 → 回到 .env
# ================================================================


def test_restore_from_env_after_override(store_env):
    env_names = [c.model_name for c in settings.llm_apis]
    pool.set_llm_apis([_cfg("file-model")])
    assert [c.model_name for c in settings.llm_apis] == ["file-model"]

    configs = pool.restore_llm_apis_from_env()

    assert [c["model_name"] for c in configs] == env_names
    assert [c.model_name for c in settings.llm_apis] == env_names
    assert config_store.current_source() == "env"
    assert not (store_env / "llm_apis.json").exists()
    assert _TOKEN not in json.dumps(configs)  # 返回的列表同样是脱敏的
