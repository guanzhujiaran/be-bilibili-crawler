import logging

from faststream import AckPolicy
from faststream.rabbit import RabbitExchange, ExchangeType
from faststream.rabbit.fastapi import RabbitRouter
from CONFIG import CONFIG, settings

from Models.MQ.BaseMQModel import ExchangeName, MQPropBase, QueueName, RoutingKey


class BaseFastStreamMQ:
    def __init__(self, mq_props: MQPropBase):
        self.mq_props = mq_props

    async def consume(self, *args, **kwargs):
        raise NotImplementedError("子类必须实现此方法")

    @property
    def sub_params(self) -> dict:
        return {
            "queue": self.mq_props.rabbit_queue,
            "exchange": self.mq_props.exchange,
            "ack_policy": AckPolicy.MANUAL,
        }

    @property
    def pub_params(self) -> dict:
        # 与 publisher_producer 保持一致：显式指定 queue，消息直接投递到该队列；
        # 同时带上 exchange + routing_key，使消息携带路由键（真实 MQ 下以 queue
        # 为准投递，routing_key 仅作绑定/分类信息）。test 队列已绑定到 bili_data
        # 交换机、绑定模式为「testRouter.#」。
        return {
            "queue": self.mq_props.rabbit_queue,
            "exchange": self.mq_props.exchange,
            "routing_key": self.mq_props.routing_key_name,
        }


# FastStream 的 log_level 是「复用」的：它既是日志门槛，又是框架自身通知
# （Received / Processed / `xxx waiting for messages`）的记录级别——见 faststream 的
# LoggerState.log：`log_level or self.log_level`。所以若直接把门槛设成 WARNING，
# 这些本质是 debug 的通知会伪装成 WARNING 刷屏
# （如 `... WARNING - bili_data | PrizeExtractDynDetailQueue | xxx - Processed`）。
#
# 按官方文档建议「lower the level of logs that the broker publishes itself」，
# 这里把 broker 的 log_level 固定为 DEBUG，让这些通知回归真实的 debug 语义；
# 再由下面的过滤器按 settings.faststream_log_level 统一控制输出门槛
# （生产 WARNING → 只打印 warning 及以上），真实的 WARNING / ERROR / CRITICAL 不受影响。
_faststream_output_level = getattr(
    logging, settings.faststream_log_level.upper(), logging.WARNING
)


class _FastStreamMinLevelFilter(logging.Filter):
    """按配置门槛过滤 FastStream access 日志（生产下只放行 warning 及以上）。

    FastStream 的 access logger 自身 handler 不设级别，门槛统一由本过滤器承担；
    这样与 broker 的 log_level（固定 DEBUG，仅用于给通知标级别）解耦。
    """

    def __init__(self, min_level: int) -> None:
        super().__init__()
        self._min_level = min_level

    def filter(self, record: logging.LogRecord) -> bool:
        return record.levelno >= self._min_level


# FastStream 的 access logger 固定名为 faststream.access.rabbit；此处提前挂上过滤器，
# 后续框架惰性创建 handler 时不会清除 logger 上已注册的 filter。
_faststream_access_logger = logging.getLogger("faststream.access.rabbit")
if not any(
    isinstance(f, _FastStreamMinLevelFilter) for f in _faststream_access_logger.filters
):
    _faststream_access_logger.addFilter(
        _FastStreamMinLevelFilter(_faststream_output_level)
    )


router = RabbitRouter(
    url=CONFIG.RabbitMQConfig.broker_url,
    # 固定 DEBUG：让框架通知回到真实的 debug 语义；输出门槛由上面的过滤器控制
    log_level=logging.DEBUG,
)


def get_broker():
    return router.broker


exch = RabbitExchange(ExchangeName.bili_data, auto_delete=False, type=ExchangeType.TOPIC, durable=True)
official_reserve_charge_lot_mq_prop = MQPropBase(
    queue_name=QueueName.OfficialReserveChargeLotMQ,
    routing_key_name=RoutingKey.OfficialReserveChargeLotMQ,
    exchange=exch
)
upsert_official_reserve_charge_lot_mq_prop = MQPropBase(
    queue_name=QueueName.UpsertOfficialReserveChargeLotMQ,
    routing_key_name=RoutingKey.UpsertOfficialReserveChargeLotMQ,
    exchange=exch
)
upsert_lot_data_by_dynamic_id_prop = MQPropBase(
    queue_name=QueueName.UpsertLotDataByDynamicIdMQ,
    routing_key_name=RoutingKey.UpsertLotDataByDynamicIdMQ,
    exchange=exch
)
upsert_topic_lot_prop = MQPropBase(
    queue_name=QueueName.UpsertTopicLotMQ,
    routing_key_name=RoutingKey.UpsertTopicLotMQ,
    exchange=exch
)
upsert_milvus_bili_lot_data_prop = MQPropBase(
    queue_name=QueueName.UpsertMilvusBiliLotDataMQ,
    routing_key_name=RoutingKey.UpsertMilvusBiliLotDataMQ,
    exchange=exch
)
upsert_bili_atari_prop = MQPropBase(
    queue_name=QueueName.UpsertBiliAtariMQ,
    routing_key_name=RoutingKey.UpsertBiliAtariMQ,
    exchange=exch
)
bili_voucher_prop = MQPropBase(
    queue_name=QueueName.BiliVoucherMQ,
    routing_key_name=RoutingKey.BiliVoucherMQ,
    exchange=exch
)

prize_extract_biliopus_mq_prop = MQPropBase(
    queue_name=QueueName.PrizeExtractBiliOpusMQ,
    routing_key_name=RoutingKey.PrizeExtractBiliOpusMQ,
    exchange=exch
)
prize_extract_dyndetail_mq_prop = MQPropBase(
    queue_name=QueueName.PrizeExtractDynDetailMQ,
    routing_key_name=RoutingKey.PrizeExtractDynDetailMQ,
    exchange=exch
)

test_mq_prop = MQPropBase(
    queue_name=QueueName.TestMQ,
    routing_key_name=RoutingKey.TestMQ,
    exchange=exch
)
