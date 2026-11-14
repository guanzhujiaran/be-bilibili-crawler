# -*- coding: utf-8 -*-
"""B 站 up 主动态监控（发现新动态 → 推送）。

设计要点（2026-11 线上问题修复）：

1. **解析与推送解耦**：``parse_dynamic_for_push`` 是纯函数，字段缺失返回 ``None``，
   不再在推送路径里裸取 ``extend.xxx``（历史上因此抛 ``AttributeError`` 把整个监控协程打死）；
2. **发布时间不再用魔数反推**：优先用接口字段（``pubTs`` / ``pubTime`` 等），
   拿不到就留空，绝不因为一条动态没有时间就崩；
3. **首轮只做基线**：``first_round`` 初值为 ``True``，首轮把现有动态 id 记录进文件但不推送，
   避免「新加的 uid 一次把 30 条历史动态全推出去」；
4. **异常隔离**：单次拉取 / 单条解析失败只记 warning 并继续下一轮，
   不允许因为一次失败退出该 uid 的监控协程。
"""

import asyncio
import json
import os
import time
from dataclasses import dataclass
from typing import Any

import aiofiles

from CONFIG import CONFIG, settings
from log.base_log import space_monitor_logger as log
from Utils.推送.PushMe import async_pushme_try_catch_decorator, a_pushme
from Utils.推送.push_log import log_push_failure
from Service.GrpcModule.Grpc.grpc_api import bili_grpc
from Utils.通用.Common import asyncio_gather
from bili_common.core.log_mask import mask_text, response_brief

#: 推送正文里需要转义的字符（markdown 表格分隔符会打乱整张表）
_PIPE = "|"
_PIPE_ESCAPED = "&#124;"

#: 发布时间的候选字段名（接口字段优先，按顺序尝试）
_PUB_TIME_KEYS: tuple[str, ...] = (
    "pubTs",
    "pub_ts",
    "pubTime",
    "pub_time",
    "publishTime",
    "publish_time",
    "ctime",
)
#: 只有「文案型」时间（如「3 小时前」）时可用的字段
_PUB_TIME_TEXT_KEYS: tuple[str, ...] = ("handlePtimeText", "ptimeLabelText")

#: 卡片类型 → (moduleDynamic 内的卡片字段, 需要拼进正文的文本字段)
_CARD_TEXT_FIELDS: dict[str, tuple[str, tuple[str, ...]]] = {
    "forward": ("dynForward", ("content", "desc")),
    "av": ("dynArchive", ("title", "desc")),
    "pgc": ("dynPgc", ("title", "subTitle")),
    "article": ("dynArticle", ("title", "desc")),
    "live": ("dynCommonLive", ("title", "descFirst")),
    "draw": ("dynDraw", ("title", "description")),
}


@dataclass(frozen=True)
class PushView:
    """一条动态「推送所需的最小视图」（已截断 / 转义，可直接进 markdown）。"""

    dynamic_id: str
    card_type: str
    author_name: str
    author_uid: str
    author_space: str
    pub_time_text: str
    content: str

    @property
    def jump_url(self) -> str:
        return f"https://www.bilibili.com/opus/{self.dynamic_id}"

    @property
    def app_url(self) -> str:
        return f"bilibili://opus/detail/{self.dynamic_id}"

    def to_markdown(self) -> str:
        return (
            "|信息|内容|\n|---|---|\n"
            f"|跳转APP|[__点击跳转app__]({self.app_url})|\n"
            f"|动态类型|{_escape_cell(self.card_type)}|\n"
            f"|up昵称|{_escape_cell(self.author_name)}|\n"
            f"|空间主页|{self.author_space}|\n"
            f"|发布时间|{_escape_cell(self.pub_time_text)}|\n"
            f"|动态内容|{self.content}|"
        )


def _escape_cell(text: str) -> str:
    """把任意文本安全地放进 markdown 表格单元格。

    ``|`` 会截断单元格、换行会截断整张表，因此统一转义/替换；
    ``author_name`` 也必须走这里（昵称里带 ``|`` 的人不少）。
    """
    if not text:
        return ""
    return (
        str(text)
        .replace("\r", "")
        .replace(_PIPE, _PIPE_ESCAPED)
        .replace("\n", "<br>")
    )


def _truncate(text: str, limit: int) -> str:
    if limit <= 0 or len(text) <= limit:
        return text
    return text[:limit] + "…"


def _collect_text(node: Any) -> str:
    """递归收集 ``TextParagraph`` / ``Paragraph`` 结构里的正文文本。

    兼容三种形态：``{"rawText": "..."}``、``{"text": "..."}``、``{"text": {"nodes": [...]}}``。
    只取文本字段，不碰其它字段（避免把链接、颜色值拼进正文）。
    """
    if isinstance(node, str):
        return node
    if isinstance(node, list):
        return "".join(_collect_text(item) for item in node)
    if not isinstance(node, dict):
        return ""
    parts: list[str] = []
    for key, value in node.items():
        if key in ("rawText",):
            if isinstance(value, str):
                parts.append(value)
            continue
        if key == "text":
            parts.append(_collect_text(value))
            continue
        if key in ("nodes", "paragraph", "summaries", "title", "summary"):
            parts.append(_collect_text(value))
            continue
    return "".join(parts)


def _iter_modules(item: dict) -> list[dict]:
    """``modules`` 在不同接口里是 list（gRPC）或 dict（web API），统一成 list。"""
    modules = item.get("modules")
    if isinstance(modules, list):
        return [m for m in modules if isinstance(m, dict)]
    if isinstance(modules, dict):
        return [modules]
    return []


def _module_payload(module: dict, name: str) -> dict:
    """取模块里的具体内容（gRPC 用 ``moduleAuthor``，web 接口用 ``module_author``）。"""
    snake = {
        "moduleAuthor": "module_author",
        "moduleDynamic": "module_dynamic",
        "moduleStat": "module_stat",
    }.get(name, name)
    for key in (name, snake):
        value = module.get(key)
        if isinstance(value, dict):
            return value
    return {}


def _first_present(mapping: dict, keys: tuple[str, ...]) -> Any:
    for key in keys:
        if key in mapping and mapping.get(key) not in (None, ""):
            return mapping.get(key)
    return None


def _format_pub_time(value: Any) -> str:
    """把接口里的发布时间字段转成可读文本；无法识别返回空串（不抛异常）。"""
    if value in (None, ""):
        return ""
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return ""
        # 纯数字字符串按时间戳处理，其余（「3 小时前」「发布于 11-13」）原样用
        if stripped.isdigit():
            value = int(stripped)
        else:
            return stripped
    if isinstance(value, (int, float)):
        ts = float(value)
        # 毫秒时间戳：13 位；秒级：10 位
        if ts > 1e12:
            ts /= 1000.0
        if ts <= 0:
            return ""
        try:
            return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))
        except (OverflowError, OSError, ValueError):
            return ""
    return ""


def _extract_pub_time(item: dict, extend: dict, modules: list[dict]) -> str:
    """发布时间：接口字段优先（extend / moduleAuthor / 卡片），拿不到就留空。"""
    raw = _first_present(extend, _PUB_TIME_KEYS)
    if raw is None:
        for module in modules:
            author = _module_payload(module, "moduleAuthor")
            raw = _first_present(author, _PUB_TIME_KEYS)
            if raw is not None:
                break
    text = _format_pub_time(raw)
    if text:
        return text
    # 只有「相对时间文案」时退而用文案，避免整列空白（文案不是精确时间，但可读）
    for module in modules:
        author = _module_payload(module, "moduleAuthor")
        label = _first_present(author, _PUB_TIME_TEXT_KEYS)
        if isinstance(label, str) and label.strip():
            return label.strip()
        if isinstance(label, dict):
            node_text = _collect_text(label).strip()
            if node_text:
                return node_text
    return ""


def _extract_content(item: dict, extend: dict, modules: list[dict], card_type: str) -> str:
    """正文：opus 摘要 → 按 cardType 取卡片文本 → 转发的 origDesc / desc。"""
    opus = extend.get("opusSummary") or {}
    if isinstance(opus, dict) and opus:
        text = _collect_text(opus.get("title")) + _collect_text(opus.get("summary"))
        text += _collect_text(opus.get("summaries"))
        if text.strip():
            return text

    spec = _CARD_TEXT_FIELDS.get(card_type)
    if spec:
        card_field, text_fields = spec
        for module in modules:
            dynamic = _module_payload(module, "moduleDynamic")
            card = dynamic.get(card_field)
            if not isinstance(card, dict):
                continue
            parts: list[str] = []
            for field in text_fields:
                value = card.get(field)
                if isinstance(value, str):
                    parts.append(value)
                elif value is not None:
                    parts.append(_collect_text(value))
            if isinstance(card.get("items"), list):
                for sub in card["items"]:
                    if isinstance(sub, dict):
                        parts.append(_collect_text(sub.get("description")))
                        parts.append(_collect_text(sub.get("title")))
            text = "".join(parts).strip()
            if text:
                return text

    # 转发动态的原文 / 通用描述
    for key in ("origDesc", "orig_desc", "desc"):
        value = extend.get(key)
        if isinstance(value, list):
            text = "".join(
                part.get("text", "") if isinstance(part, dict) else str(part)
                for part in value
            ).strip()
            if text:
                return text
        elif isinstance(value, str) and value.strip():
            return value
    return ""


def parse_dynamic_for_push(
    item: dict, *, content_max_length: int | None = None
) -> PushView | None:
    """把一条 DynSpace 动态解析成推送视图；关键字段缺失返回 ``None``（绝不抛异常）。

    :param item: ``grpc_get_space_dyn_by_uid`` 返回列表里的一条动态
    :param content_max_length: 正文最大长度（默认取 ``CONFIG.space_monitor``）
    """
    if not isinstance(item, dict):
        return None
    try:
        extend = item.get("extend")
        if not isinstance(extend, dict):
            extend = {}
        modules = _iter_modules(item)

        dynamic_id = _first_present(extend, ("dynIdStr", "dyn_id_str", "dynId"))
        if dynamic_id in (None, ""):
            dynamic_id = _first_present(item, ("id_str", "idStr", "dynamicId"))
        if dynamic_id in (None, ""):
            return None
        dynamic_id = str(dynamic_id)

        card_type = str(
            _first_present(item, ("cardType", "type", "card_type")) or ""
        )
        if not card_type and modules:
            # gRPC 的 cardType 是枚举名（如 ``word`` / ``draw``），缺失时用模块类型兜底
            card_type = str(_first_present(modules[0], ("moduleType", "module_type")) or "")

        author_name = str(
            _first_present(extend, ("origName", "orig_name", "upName", "up_name")) or ""
        )
        author_uid = _first_present(extend, ("uid", "origUid", "orig_uid"))
        for module in modules:
            author = _module_payload(module, "moduleAuthor")
            inner = author.get("author") if isinstance(author, dict) else None
            if isinstance(inner, dict):
                author_name = author_name or str(inner.get("name") or "")
                author_uid = author_uid or inner.get("mid") or inner.get("uid")
            author_uid = author_uid or author.get("mid")
            if author_name and author_uid:
                break
        if isinstance(author_uid, (int, float)):
            author_uid = str(int(author_uid))
        author_uid = str(author_uid or "")

        cfg = CONFIG.space_monitor
        limit = cfg.push_content_max_length if content_max_length is None else content_max_length
        content = _extract_content(item, extend, modules, card_type)
        content = _truncate(_escape_cell(content), limit)

        return PushView(
            dynamic_id=dynamic_id,
            card_type=card_type,
            author_name=author_name,
            author_uid=author_uid,
            author_space=(
                f"https://space.bilibili.com/{author_uid}/dynamic" if author_uid else ""
            ),
            pub_time_text=_extract_pub_time(item, extend, modules),
            content=content,
        )
    except Exception as e:  # noqa: BLE001 —— 解析必须「尽力而为」，不能打断监控
        log.warning(
            f"解析动态失败，已跳过本条：err={type(e).__name__}: {e} "
            f"item={response_brief(item)}"
        )
        return None


def _monitor_state_dir() -> str:
    """监控状态目录：优先 ``RUNTIME_DIR``（容器里是挂载卷），否则源码目录下的 data/。"""
    runtime = (settings.RUNTIME_DIR or "").strip()
    if runtime:
        return os.path.join(runtime, "space_monitor")
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")


class BiliSpaceMonitor:
    def __init__(self):
        self.dir_path = _monitor_state_dir()
        os.makedirs(self.dir_path, exist_ok=True)
        self.uid_list = [370877395]  # 监控的up的uid
        self.monitor_uid_list = None
        state_path = self._state_path()
        if os.path.exists(state_path):
            with open(state_path, "r", encoding="utf-8") as f:
                content = f.read()
                if content.strip():
                    try:
                        self.monitor_uid_list = json.loads(content)
                    except json.JSONDecodeError as e:
                        # 坏文件不能让监控起不来：按「没有历史基线」处理（首轮只记基线不推送）
                        log.warning(f"监控 uid 列表文件损坏，忽略并重新建立基线：{e}")
                        self.monitor_uid_list = None
        if not self.monitor_uid_list:
            self.monitor_uid_list = [
                {"uid": uid, "latest_dynamic_id_list": []} for uid in self.uid_list
            ]
        recorded_uid_list = [x.get("uid") for x in self.monitor_uid_list]
        for uid in self.uid_list:
            if uid not in recorded_uid_list:
                self.monitor_uid_list.append({"uid": uid, "latest_dynamic_id_list": []})
        for uid in recorded_uid_list:
            if uid not in self.uid_list:
                self.monitor_uid_list = [
                    info for info in self.monitor_uid_list if info.get("uid") != uid
                ]
        self.grpc_api = bili_grpc
        # 间隔时间默认 3 分钟，一天总共 20 * 24 = 480 次，间隔比较适中
        self.sep_time = CONFIG.space_monitor.interval_seconds

    def _state_path(self) -> str:
        return os.path.join(self.dir_path, "monitor_uid_list.json")

    def timeshift(self, timestamp):
        local_time = time.localtime(timestamp)
        realtime = time.strftime("%Y-%m-%d %H:%M:%S", local_time)
        return realtime

    async def save_monitor_uid_list(self):
        path = self._state_path()
        tmp_path = f"{path}.tmp"
        async with aiofiles.open(tmp_path, "w", encoding="utf-8") as f:
            await f.write(json.dumps(self.monitor_uid_list, indent="\t", ensure_ascii=False))
        # 原子替换：写一半被 kill 时不会留下半截 JSON，下次启动还能读到基线
        os.replace(tmp_path, path)

    def _record_dynamic_id(self, uid: str, dynamic_id: str) -> None:
        cfg = CONFIG.space_monitor
        for info in self.monitor_uid_list:
            if info.get("uid") != uid:
                continue
            ids: list[str] = info.setdefault("latest_dynamic_id_list", [])
            if dynamic_id in ids:
                return
            ids.append(dynamic_id)
            while len(ids) > max(1, cfg.history_keep):
                ids.pop(0)
            return

    def _known_dynamic_ids(self, uid: str) -> list[str]:
        for info in self.monitor_uid_list:
            if info.get("uid") == uid:
                return list(info.get("latest_dynamic_id_list") or [])
        return []

    async def push_dyn_notify(self, view: PushView) -> None:
        """推送一条动态；失败按统一字段记日志（不抛异常，不影响后续监控）。"""
        log.debug(
            f"【Bilibili】你关注的up主 {view.author_name}有新的动态！\n{view.jump_url}"
        )
        title = f"【Bilibili】你关注的up主 {view.author_name}有新的动态！"
        msg_id = await a_pushme(title, view.to_markdown(), "markdown")
        if msg_id:
            log.debug(f"已发布动态推送消息 msg_id={msg_id} dynamic_id={view.dynamic_id}")
            return
        log_push_failure(
            log,
            channel="message-service",
            msg_id="-",
            title=title,
            err=f"发布推送消息失败（Mq） dynamic_id={view.dynamic_id}",
        )

    async def _pull_once(self, uid: str) -> list[dict] | None:
        """拉一次空间动态；失败返回 ``None``（由调用方决定是否跳过本轮）。"""
        try:
            resp = await self.grpc_api.grpc_get_space_dyn_by_uid(uid)
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001 —— 单次拉取失败不能让协程退出
            log.warning(
                f"拉取 up={uid} 空间动态失败，{self.sep_time}s 后继续："
                f"err={type(e).__name__}: {mask_text(str(e))[:300]}"
            )
            return None
        if not isinstance(resp, dict):
            log.warning(f"拉取 up={uid} 空间动态返回非预期结构，本轮跳过")
            return None
        resp_list = resp.get("list")
        if resp_list is None:
            code = resp.get("code")
            if code not in (None, 0) or "list" not in resp:
                # 业务错误（风控 / cookie 失效）/ 结构缺失：**本轮不算「拉到了空列表」**，
                # 否则首轮基线会被错误地建立成空集，下一条历史动态就会被当成新动态推送。
                log.warning(f"拉取 up={uid} 空间动态返回异常：{response_brief(resp)}")
                return None
            return []
        return [item for item in resp_list if isinstance(item, dict)]

    async def monitor_main(self, uid):
        # 首轮只记录基线：新加的 uid 不会把当前 30 条历史动态一次性推出去
        first_round = True
        while 1:
            try:
                resp_list = await self._pull_once(uid)
                if resp_list is not None:
                    known = set(self._known_dynamic_ids(uid))
                    changed = False
                    for item in resp_list:
                        view = parse_dynamic_for_push(item)
                        if view is None:
                            continue
                        if view.dynamic_id in known:
                            continue
                        if first_round:
                            self._record_dynamic_id(uid, view.dynamic_id)
                            known.add(view.dynamic_id)
                            changed = True
                            continue
                        await self.push_dyn_notify(view)
                        self._record_dynamic_id(uid, view.dynamic_id)
                        known.add(view.dynamic_id)
                        changed = True
                    if changed:
                        await self.save_monitor_uid_list()
                    if first_round:
                        log.info(
                            f"up={uid} 首轮基线已建立（记录 {len(known)} 条动态 id，不推送历史动态）"
                        )
                        first_round = False
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001 —— 兜底隔离，保持监控存活
                log.warning(
                    f"up={uid} 动态监控本轮失败，{self.sep_time}s 后继续："
                    f"err={type(e).__name__}: {e}"
                )
            await asyncio.sleep(self.sep_time)

    @async_pushme_try_catch_decorator
    async def main(self, show_log=True):
        log.info("启动B站动态监控程序！！！")
        if not show_log:
            pass
        task_list = []
        for i in self.monitor_uid_list:
            task = asyncio.create_task(self.monitor_main(i.get("uid")))
            task_list.append(task)
        await asyncio_gather(*task_list, log=log)


bili_space_monitor = BiliSpaceMonitor()

__all__ = ["bili_space_monitor", "BiliSpaceMonitor", "PushView", "parse_dynamic_for_push"]
if __name__ == "__main__":
    a = BiliSpaceMonitor()
    asyncio.run(a.main())
