from faststream import AckPolicy
from faststream.rabbit import RabbitExchange, ExchangeType
from faststream.rabbit.fastapi import RabbitRouter
from CONFIG import CONFIG

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


router = RabbitRouter(
    url=CONFIG.RabbitMQConfig.broker_url,
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
