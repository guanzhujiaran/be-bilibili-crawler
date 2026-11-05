"""爬虫推送开关的运行时状态（本地 JSON 文件持久化，不入库）。

优先级：文件里显式配置过的爬虫以文件为准，其余回退到环境变量
``CRAWLER_PUSH_START`` / ``CRAWLER_PUSH_END``（逗号分隔爬虫名，``*`` / ``all`` 全开）。

之所以要文件而不是只用环境变量：环境变量改了要重启容器，而推送开关属于
「临时想开一下就开」的运营动作，通过接口改完立即生效、重启后仍然保留。
"""

import json
import os
import tempfile
import time
from pathlib import Path

from log.base_log import pushme_logger
from CONFIG import CrawlerPushConfig, settings
from Models.v1.background_service.crawler_push_model import (
    CrawlerPushEffectiveItem,
    CrawlerPushState,
    CrawlerPushSwitchItem,
)

_STATE_FILE_NAME = "crawler_push_switch.json"


def _state_dir() -> Path:
    """开关文件所在目录：优先 ``RUNTIME_DIR``，否则用项目根下的 ``runtime/``"""
    raw = (settings.RUNTIME_DIR or "").strip()
    if raw:
        return Path(raw)
    return Path(__file__).resolve().parents[2] / "runtime"


def state_file_path() -> Path:
    """开关文件的绝对路径"""
    return _state_dir() / _STATE_FILE_NAME


def load_state() -> CrawlerPushState:
    """读取本地持久化的开关；文件缺失或损坏时返回空状态（等价于「没配过」）"""
    path = state_file_path()
    if not path.exists():
        return CrawlerPushState()
    try:
        with path.open("r", encoding="utf-8") as f:
            return CrawlerPushState.model_validate(json.load(f))
    except Exception as e:
        pushme_logger.warning(f"读取爬虫推送开关文件失败（{path}）：{e!r}")
        return CrawlerPushState()


def save_state(state: CrawlerPushState) -> CrawlerPushState:
    """整体覆盖保存；先写临时文件再 ``os.replace``，避免读到半截 JSON"""
    path = state_file_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    state.updated_at = int(time.time())
    payload = state.model_dump(mode="json")
    fd, tmp_path = tempfile.mkstemp(
        prefix=f".{_STATE_FILE_NAME}.", dir=str(path.parent)
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        # mkstemp 建出来是 0600 且属主为容器内 root，宿主机上看不到内容；
        # 这里统一放宽到 0644，方便运维直接 cat 排查。
        os.chmod(tmp_path, 0o644)
        os.replace(tmp_path, path)
    except Exception:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        raise
    pushme_logger.info(f"爬虫推送开关已保存到 {path}: {payload}")
    return state


def _env_config() -> CrawlerPushConfig:
    return CrawlerPushConfig(
        start=settings.CRAWLER_PUSH_START, end=settings.CRAWLER_PUSH_END
    )


def resolve(crawler_name: str) -> CrawlerPushEffectiveItem:
    """合并文件配置与环境变量，得到该爬虫的最终开关"""
    state = load_state()
    for item in state.items:
        if item.crawler_name == crawler_name:
            return CrawlerPushEffectiveItem(
                crawler_name=crawler_name,
                start=item.start,
                end=item.end,
                source="file",
            )
    env = _env_config()
    return CrawlerPushEffectiveItem(
        crawler_name=crawler_name,
        start=env.is_start_enabled(crawler_name),
        end=env.is_end_enabled(crawler_name),
        source="env",
    )


def is_start_enabled(crawler_name: str) -> bool:
    """该爬虫开始执行时是否需要推送"""
    return resolve(crawler_name).start


def is_end_enabled(crawler_name: str) -> bool:
    """该爬虫结束时是否需要推送"""
    return resolve(crawler_name).end


def upsert_items(items: list[CrawlerPushSwitchItem]) -> CrawlerPushState:
    """按 crawler_name 覆盖写入（未出现在 items 里的爬虫保持原样）"""
    state = load_state()
    merged = {item.crawler_name: item for item in state.items}
    for item in items:
        merged[item.crawler_name] = item
    return save_state(CrawlerPushState(items=list(merged.values())))


def delete_item(crawler_name: str) -> CrawlerPushState:
    """删除某个爬虫的文件配置，使其回退到环境变量"""
    state = load_state()
    return save_state(
        CrawlerPushState(
            items=[x for x in state.items if x.crawler_name != crawler_name]
        )
    )
