from pydantic import BaseModel
from Models.MQ.UpsertLotDataModel import (
    LotDataReq,
    LotDataDynamicReq,
    TopicLotData,
)
from Service.GrpcModule.Models.RabbitmqModel import VoucherInfo


class RabbitMQTestMsgModel(BaseModel):
    a: int
    b: str
    c: dict
    d: list[str]


MQ_PARAMS_JOINED_TYPE = (
    LotDataReq
    | LotDataDynamicReq
    | TopicLotData
    | VoucherInfo
    | RabbitMQTestMsgModel
    | dict
    | int
)

__all__ = [
    "MQ_PARAMS_JOINED_TYPE",
    "LotDataReq",
    "LotDataDynamicReq",
    "TopicLotData",
    "VoucherInfo",
    "RabbitMQTestMsgModel"
]
