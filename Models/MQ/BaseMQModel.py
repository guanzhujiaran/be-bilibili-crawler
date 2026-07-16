from dataclasses import dataclass
from enum import StrEnum

from faststream.rabbit import RabbitQueue, RabbitExchange


class QueueName(StrEnum):
    TestMQ = "test"
    OfficialReserveChargeLotMQ = "OfficialReserveChargeLotQueue"
    UpsertOfficialReserveChargeLotMQ = "UpsertOfficialReserveChargeLotQueue"
    UpsertLotDataByDynamicIdMQ = "UpsertLotDataByDynamicIdQueue"
    UpsertTopicLotMQ = "UpsertTopicLotMQ"
    UpsertMilvusBiliLotDataMQ = "UpsertMilvusBiliLotDataMQ"
    UpsertBiliAtariMQ = "UpsertBiliAtariMQ"
    BiliVoucherMQ = "bili_352_voucher"
    PrizeExtractBiliOpusMQ = "PrizeExtractBiliOpusQueue"
    PrizeExtractDynDetailMQ = "PrizeExtractDynDetailQueue"


class ExchangeName(StrEnum):
    bili_data = "bili_data"


# 定义一个名为RoutingKey的类，继承自str和Enum
class RoutingKey(StrEnum):
    TestMQ = "testRouter"
    OfficialReserveChargeLotMQ = "BiliData.OfficialReserveChargeLotMQ"
    UpsertOfficialReserveChargeLotMQ = "BiliData.UpsertOfficialReserveChargeLotMQ"
    UpsertLotDataByDynamicIdMQ = "BiliData.UpsertLotDataByDynamicIdMQ"
    UpsertTopicLotMQ = "BiliData.UpsertTopicLotMQ"
    UpsertMilvusBiliLotDataMQ = "Milvus.BiliLotDataMQ"
    UpsertBiliAtariMQ = "BiliData.UpsertBiliAtariMQ"
    BiliVoucherMQ = "BiliData.bili_352_voucher"
    PrizeExtractBiliOpusMQ = "BiliData.PrizeExtractBiliOpusMQ"
    PrizeExtractDynDetailMQ = "BiliData.PrizeExtractDynDetailMQ"


@dataclass
class MQPropBase:
    queue_name: QueueName
    routing_key_name: RoutingKey
    exchange: RabbitExchange
    _rabbit_queue: RabbitQueue | None = None
    _exchange_name: ExchangeName | None = None

    def __post_init__(self):
        self._rabbit_queue = RabbitQueue(
            name=self.queue_name, routing_key=f"{self.routing_key_name}.#"
        )
        self._exchange_name = self.exchange.name  # type: ignore

    @property
    def rabbit_queue(self) -> RabbitQueue:
        if not self._rabbit_queue:
            raise ValueError("rabbit_queue is not initialized")
        return self._rabbit_queue

    @property
    def exchange_name(self) -> ExchangeName | str:
        if not self._exchange_name:
            raise ValueError("exchange_name is not initialized")
        return self._exchange_name

    # 新增：为发布者提供动态拼接路由键的方法
    def get_publish_routing_key(self, suffix: str | None = None) -> str:
        """
        获取用于发布的完整 Routing Key。
        如果传入了 suffix，则拼接为 'base_key.suffix'；
        如果不传，则返回基础 key（适用于不需要后缀的简单消息）。
        """
        if suffix:
            return f"{self.routing_key_name}.{suffix}"
        return self.routing_key_name
