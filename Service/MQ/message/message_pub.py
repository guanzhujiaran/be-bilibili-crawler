"""推送消息生产者：将推送请求发布到 message-service 消费的 RabbitMQ 队列。

复用 FastapiApp 已有的 FastStream router（其 broker 由 FastAPI lifespan 启动），
避免额外维护一条 RabbitMQ 连接。
"""

from typing import Optional, Union

from faststream.rabbit import RabbitExchange, ExchangeType, RabbitQueue
from pydantic import BaseModel

from Service.MQ.base.MQClient.base import router
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


async def publish_message(
    title: str,
    content: str,
    push_type: Optional[str] = "text",
    config: Optional[Union[dict, BaseModel]] = None,
    requires_login: bool = False,
) -> None:
    """发布一条推送请求到 message-service。

    FastapiApp 的全局告警不携带 config，由 message-service 使用其全局环境变量渠道发送；
    RPA-Browser 的 per-user 推送会携带 config。

    config 可为 dict 或 pydantic 模型，模型会先序列化为 dict 再发送，兼容性更好。
    requires_login 用于声明该推送是否需要触发方处于登录态（由 message-service
    依据上游 pptr 注入的 x-bili-mid 判定）。
    """
    # 兼容 pydantic 模型：统一转为 dict 后再投递，避免序列化问题
    if isinstance(config, BaseModel):
        config = config.model_dump()

    payload = {
        "title": title,
        "content": content,
        "push_type": push_type,
        "config": config,
        "requires_login": requires_login,
    }
    try:
        broker = router.broker
        await broker.publish(
            message=payload,
            exchange=message_exchange,
            routing_key="message.push",
            queue=message_queue,
        )
        MQ_logger.debug(f"已发布推送消息到 message 队列: {title}")
    except Exception as e:  # noqa: BLE001
        # 推送失败不应影响主流程，仅记录日志
        MQ_logger.error(f"发布推送消息到 message-service 失败: {e}")
