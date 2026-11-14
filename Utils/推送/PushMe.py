import traceback
from contextvars import ContextVar
from functools import wraps
from typing import Literal, Optional

from log.base_log import pushme_logger

from bili_common.core.push_settings import build_server_label

from CONFIG import settings
from Service.MQ.message.message_pub import publish_message
from Utils.推送.push_log import log_push_failure, push_failure_log


class PushSubject(str):
    """推送消息主题标识。

    标题统一采用 ``[主题]`` 前缀，便于在告警/通知中区分消息性质：

    =======  ======  ======  =====================
    主题       标识    描述     示例（title）
    -------  ------  ------  ---------------------
    info      [i]    信息     [i]收到一条信息
    success   [s]    成功     [s]任务执行成功
    warning   [w]    警告     [w]服务器cpu告警
    failure   [f]    失败     [f]网站签到失败
    =======  ======  ======  =====================
    """

    INFO = "[i]"
    SUCCESS = "[s]"
    WARNING = "[w]"
    FAILURE = "[f]"


#: 是否处于「正在发布上一条失败告警」的流程内。
#: 用于切断「推送失败 → 再推一条失败告警 → 又失败 …」的递归。
_in_failure_notify: ContextVar[bool] = ContextVar(
    "pushme_in_failure_notify", default=False
)


def server_label() -> str:
    """返回本服务标识前缀，例如 ``[be-bilibili-crawler@10.0.0.5]``。

    实现统一在 ``bili_common.core.push_settings.build_server_label``（与 RPA-Browser
    共用一份），本函数只做本服务的零参包装，历史调用点保持不变。
    ``SERVER_ADDRESS`` 缺省时自动取本机 hostname。
    """
    return build_server_label(settings)


def async_pushme_try_catch_decorator(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            await func(*args, **kwargs)
        except Exception as e:
            # 递归保护：如果本协程就是「发布失败告警」链路的一部分（例如推送模块自身
            # 抛错），不再发布第二条告警，只记日志，避免失败告警互相触发。
            if _in_failure_notify.get():
                log_push_failure(
                    pushme_logger,
                    channel="pushme_decorator",
                    msg_id="-",
                    title=f"服务异常（{getattr(func, '__qualname__', func.__name__)}）",
                    err=e,
                )
                raise e
            token = _in_failure_notify.set(True)
            try:
                # 标题只含「服务@地址 + 失败主题[f] + 笼统 subject」，具体错误放进内容
                await a_push_error(
                    subject="服务异常",
                    content=(
                        # func 是被装饰的方法，用 __qualname__ 才能拿到「类.方法」；
                        # 绑定方法的 __class__.__name__ 只会是 "method"，没有信息量。
                        f"服务/方法：{getattr(func, '__qualname__', func.__name__)}\n"
                        f"错误信息：{e}\n"
                        f"错误堆栈：\n{traceback.format_exc()}"
                    ),
                )
            finally:
                _in_failure_notify.reset(token)
            pushme_logger.exception(e)
            raise e

    return wrapper


async def a_pushme(
    title: str,
    content: str,
    push_type: Optional[
        Literal[
            "text",
            "data",
            "markdata",
            "html",
            "txt",
            "json",
            "markdown",
            "cloudMonitor",
            "jenkins",
            "route",
            "pay",
        ]
    ] = "text",
    subject: str = PushSubject.INFO,
) -> str | None:
    """统一的（信息类）推送入口。

    行为已从「直接调用 PushMe/PushPlus 接口」改为「发布到 RabbitMQ，
    由 message-service 统一完成实际推送」，**限流 / 去重 / 聚合也统一交给 message-service**
    （见 be-message-service 的 ``push_aggregator``）。
    标题会自动加上本服务标识前缀（服务名@地址）与消息主题标识（默认 ``[i]`` 信息）。

    报错类推送请使用 :func:`a_push_error`，其标题只写服务+地址与失败主题，
    不暴露具体错误，具体错误统一放进内容。

    :param subject: 消息主题标识，取自 :class:`PushSubject`，如 ``[i]``/``[s]``/``[w]``/``[f]``。
    :return: 成功**发布到 MQ** 时返回本次消息 id；发布失败返回 ``None``。
        这里没有 HTTP 响应可读：真正的推送由 message-service 异步完成，
        **不要**再按 ``resp.status_code`` 判断（历史 bug：返回 None 被读 status_code，
        AttributeError 被吞后误报「推送失败」，实际消息已经发出）。
    """
    label = server_label()
    final_title = f"{subject}{label} {title}" if title else f"{subject}{label}"
    return await _dispatch(final_title, content, push_type or "text")


async def a_push_error(
    content: str,
    *,
    subject: str = "运行异常",
    push_type: Optional[
        Literal[
            "text",
            "data",
            "markdata",
            "html",
            "txt",
            "json",
            "markdown",
            "cloudMonitor",
            "jenkins",
            "route",
            "pay",
        ]
    ] = "text",
    topic: str = PushSubject.FAILURE,
) -> str | None:
    """统一的「报错」推送入口（通用函数）。

    与 :func:`a_pushme` 的区别：标题含「服务@地址 + 失败主题[f] + 笼统的 subject（如 运行异常）」，
    不写入任何具体错误信息；具体错误内容（异常信息、堆栈、上下文）全部放入 content。

    注意：failure 主题（``[f]``）的推送会被 message-service 按「接收人」在冷却期内
    聚合成一条摘要，因此 **content 必须稳定可去重**（同因失败内容完全一致），
    否则摘要会退化成「一长串互不相同的条目」；逐条明细请写本地日志。

    :param topic: 消息主题标识，默认 ``[f]``（失败），可传 ``[w]`` 等其它主题。
    :return: 成功发布到 MQ 时返回消息 id，失败返回 ``None``（语义同 :func:`a_pushme`）。
    """
    title = f"{topic}{server_label()} {subject}"
    return await _dispatch(title, content, push_type or "text")


async def _dispatch(title: str, content: str, push_type: str) -> str | None:
    """发布推送请求到 message-service（限流 / 去重 / 聚合都在那边统一做）。"""
    # 携带共享的全局渠道配置（MESSAGE_CONFIG），由 message-service 统一推送
    return await publish_message(
        title, content, push_type, config=settings.message_config
    )
