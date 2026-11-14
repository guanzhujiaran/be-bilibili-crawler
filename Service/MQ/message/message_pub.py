"""推送消息生产者：将推送请求发布到 message-service 消费的 RabbitMQ 队列。

复用 FastapiApp 已有的 FastStream router（其 broker 由 FastAPI lifespan 启动），
避免额外维护一条 RabbitMQ 连接。
"""

import secrets
import time
from typing import Optional, Union

from faststream.rabbit import RabbitExchange, ExchangeType, RabbitQueue
from pydantic import BaseModel

from Service.MQ.base.MQClient.base import router
from Utils.推送.push_log import log_push_failure
from log.base_log import MQ_logger

message_exchange = RabbitExchange(
    "message_exchange",
    type=ExchangeType.TOPIC,
    durable=True,
    auto_delete=False,
)
message_queue = RabbitQueue(
    "message_queue",
    routing_key="message.#",
    durable=True,
)


def _new_msg_id() -> str:
    """本地消息 id：只用于把「发布日志」和「调用方日志」串起来（不含业务语义）。"""
    return f"{int(time.time())}-{secrets.token_hex(4)}"


async def publish_message(
    title: str,
    content: str,
    push_type: Optional[str] = "text",
    config: Optional[Union[dict, BaseModel]] = None,
) -> str | None:
    """发布一条推送请求到 message-service。

    FastapiApp 的全局告警不携带 config，由 message-service 使用其全局环境变量渠道发送；
    RPA-Browser 的 per-user 推送会携带 config。

    config 可为 dict 或 pydantic 模型，模型会先序列化为 dict 再发送，兼容性更好。

    :return: 发布成功返回本地消息 id；失败返回 ``None``。
        **本函数只负责「把消息投递给 MQ」**，真正的 HTTP 推送由 message-service 异步完成，
        因此返回值的语义是「是否已发布」而不是「是否已送达」。
    """
    # 兼容 pydantic 模型：统一转为 dict 后再投递，避免序列化问题
    if isinstance(config, BaseModel):
        config = config.model_dump()

    msg_id = _new_msg_id()
    payload = {
        "title": title,
        "content": content,
        "push_type": push_type,
        "config": config,
    }
    try:
        broker = router.broker
        await broker.publish(
            message=payload,
            exchange=message_exchange,
            routing_key="message.push",
            queue=message_queue,
            # persist=True（delivery_mode=2）：推送请求也落盘，RabbitMQ 重启后不丢
            persist=True,
        )
        MQ_logger.debug(f"已发布推送消息到 message 队列: msg_id={msg_id} title={title}")
        return msg_id
    except Exception as e:  # noqa: BLE001
        # 推送失败不应影响主流程，仅记录日志（统一字段，见 log_push_failure）
        log_push_failure(
            MQ_logger,
            channel="mq",
            msg_id=msg_id,
            title=title,
            err=e,
        )
        return None
