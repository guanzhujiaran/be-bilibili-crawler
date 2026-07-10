import time
import traceback
from collections import deque
from functools import wraps
from typing import Literal, Optional

from log.base_log import pushme_logger

from CONFIG import settings
from Service.MQ.message.message_pub import publish_message

push_msg_d = deque(maxlen=50)
_last_push_time: float = 0
_PUSH_INTERVAL = 60  # 推送间隔（秒），至少间隔1分钟，避免刷屏/刷接口


def async_pushme_try_catch_decorator(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            await func(*args, **kwargs)
        except Exception as e:
            await a_pushme(
                f"服务：【{func.__class__.__name__} {func.__name__}】报错！",
                f"错误堆栈：\n{traceback.format_exc()}",
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
    """统一的推送入口。

    行为已从「直接调用 PushMe/PushPlus 接口」改为「发布到 RabbitMQ，
    由 message-service 统一完成实际推送」，以便集中管理推送渠道与限流。
    """
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
    await publish_message(title, content, push_type or "text", config=settings.message_config)
