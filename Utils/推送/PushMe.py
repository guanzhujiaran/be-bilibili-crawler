import socket
import time
import traceback
from collections import deque
from functools import wraps
from typing import Literal, Optional

from log.base_log import pushme_logger

from CONFIG import settings
from Service.MQ.message.message_pub import publish_message


def server_label() -> str:
    """返回本服务标识前缀，例如 ``[be-bilibili-crawler@10.0.0.5]``。

    所有推送标题都会带上它，便于在告警中区分「是哪台服务器的哪个服务」报错。
    ``SERVER_NAME`` / ``SERVER_ADDRESS`` 来自全局 ``settings``（可被环境变量覆盖），
    ``SERVER_ADDRESS`` 缺省时自动取本机 hostname。
    """
    name = settings.SERVER_NAME or "be-bilibili-crawler"
    addr = settings.SERVER_ADDRESS or socket.gethostname()
    return f"[{name}@{addr}]"

push_msg_d = deque(maxlen=50)
_last_push_time: float = 0
_PUSH_INTERVAL = 60  # 推送间隔（秒），至少间隔1分钟，避免刷屏/刷接口


def async_pushme_try_catch_decorator(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            await func(*args, **kwargs)
        except Exception as e:
            # 标题只含「服务@地址 + 笼统的 subject」，具体错误放进内容
            await a_push_error(
                subject="服务异常",
                content=(
                    f"服务/方法：{func.__class__.__name__}.{func.__name__}\n"
                    f"错误信息：{e}\n"
                    f"错误堆栈：\n{traceback.format_exc()}"
                ),
            )
            pushme_logger.exception(e)
            raise e

    return wrapper


async def a_pushme(
    title: str,
    content: str,
    push_type: Optional[
        Literal[
            "text", "data", "markdata", "html", "txt", "json",
            "markdown", "cloudMonitor", "jenkins", "route", "pay",
        ]
    ] = "text",
) -> None:
    """统一的（信息类）推送入口。

    行为已从「直接调用 PushMe/PushPlus 接口」改为「发布到 RabbitMQ，
    由 message-service 统一完成实际推送」，以便集中管理推送渠道与限流。
    标题会自动加上本服务标识前缀（服务名@地址）。

    报错类推送请使用 :func:`a_push_error`，其标题只写服务+地址与笼统主题，
    不暴露具体错误，具体错误统一放进内容。
    """
    label = server_label()
    final_title = f"{label} {title}" if title else label
    await _dispatch(final_title, content, push_type or "text")


async def a_push_error(
    content: str,
    *,
    subject: str = "运行异常",
    push_type: Optional[
        Literal[
            "text", "data", "markdata", "html", "txt", "json",
            "markdown", "cloudMonitor", "jenkins", "route", "pay",
        ]
    ] = "text",
) -> None:
    """统一的「报错」推送入口（通用函数）。

    与 :func:`a_pushme` 的区别：标题只含「服务@地址 + 笼统的 subject（如 运行异常）」，
    不写入任何具体错误信息；具体错误内容（异常信息、堆栈、上下文）全部放入 content。
    """
    title = f"{server_label()} {subject}"
    await _dispatch(title, content, push_type or "text")


async def _dispatch(title: str, content: str, push_type: str) -> None:
    """实际的限流 / 去重 / 发布逻辑（a_pushme 与 a_push_error 共用）。"""
    global _last_push_time
    # 内容去重，避免重复告警刷屏
    if content in push_msg_d:
        return
    now = time.time()
    # 频率限制，避免短时间大量推送打爆推送服务
    if now - _last_push_time < _PUSH_INTERVAL:
        return
    _last_push_time = now
    push_msg_d.append(content)

    # 携带共享的全局渠道配置（MESSAGE_CONFIG），由 message-service 统一推送
    await publish_message(title, content, push_type, config=settings.message_config)
