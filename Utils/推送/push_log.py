"""推送失败日志的统一格式（不依赖推送模块本身，避免循环导入）。

固定字段：

    event=push_failed channel=<channel> msg_id=<id> title=<title> err=<类型:消息>

为什么不带堆栈：推送失败绝大多数是「渠道配置错 / 上游临时不可用」，重试仍在继续，
把完整堆栈按 ERROR 打进业务日志只会把真正的故障淹没。需要堆栈时打开
``PUSH_FAILURE_TRACEBACK=true``（只建议本地排查时打开）。
"""

from CONFIG import settings

#: title / err 的最大长度：日志里只需要能辨认是哪条消息，不需要整段正文
_TITLE_LIMIT = 80
_ERR_LIMIT = 300


def _shorten(text: str, limit: int) -> str:
    text = (text or "").replace("\n", " ")
    return text if len(text) <= limit else text[:limit] + "..."


def push_failure_log(
    *,
    channel: str,
    msg_id: str,
    title: str,
    err: BaseException | str,
) -> str:
    """拼出统一的推送失败日志行（纯字符串，任何 logger 都能直接用）。"""
    if isinstance(err, BaseException):
        err_text = f"{type(err).__name__}: {err}"
    else:
        err_text = str(err)
    return (
        f"event=push_failed channel={channel or 'unknown'} msg_id={msg_id or '-'} "
        f"title={_shorten(title, _TITLE_LIMIT)} err={_shorten(err_text, _ERR_LIMIT)}"
    )


def traceback_enabled() -> bool:
    """推送失败时是否输出堆栈（``PUSH_FAILURE_TRACEBACK=true``）。"""
    return bool(getattr(settings, "push_failure_traceback", False))


def log_push_failure(
    logger,
    *,
    channel: str,
    msg_id: str,
    title: str,
    err: BaseException | str,
) -> None:
    """按统一字段记录一次推送失败；堆栈只在开关打开且 err 是异常时输出。"""
    message = push_failure_log(channel=channel, msg_id=msg_id, title=title, err=err)
    if traceback_enabled() and isinstance(err, BaseException):
        logger.opt(exception=err).error(message)
        return
    logger.error(message)


__all__ = ["log_push_failure", "push_failure_log", "traceback_enabled"]
