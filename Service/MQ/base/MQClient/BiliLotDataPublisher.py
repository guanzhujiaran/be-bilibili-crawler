import asyncio
import random
from datetime import datetime
from faststream.rabbit import RabbitExchange, ExchangeType
from Models.MQ.MQRouterModels import RabbitMQTestMsgModel
from Models.MQ.BaseMQModel import MQPropBase, QueueName, ExchangeName
from Models.MQ.UpsertLotDataModel import LotDataReq, LotDataDynamicReq, TopicLotData
from Models.MQ.PrizeExtractMQModel import (
    PrizeExtractReq,
    PrizeExtractParams,
    PrizeExtractTargetEnum,
)
from Service.MQ.base.BasicAsyncClient import _mq_retry_wrapper
from Service.MQ.base.MQClient.base import (
    official_reserve_charge_lot_mq_prop,
    upsert_official_reserve_charge_lot_mq_prop,
    upsert_lot_data_by_dynamic_id_prop,
    upsert_topic_lot_prop,
    upsert_milvus_bili_lot_data_prop,
    get_broker,
    bili_voucher_prop,
    upsert_bili_atari_prop,
    test_mq_prop,
    prize_extract_biliopus_mq_prop,
    prize_extract_dyndetail_mq_prop,
)
from Service.GrpcModule.Models.RabbitmqModel import VoucherInfo
from Service.GrpcModule.GrpcSrc.SQLObject.models import Lotdata
from Utils.数据库.SqlalchemyTool import sqlalchemy_model_2_dict
from Service.MQ.utils.RabbitmqPubCacheRedis import redis_obj, CachedMessage
from log.base_log import MQ_logger
from hashlib import md5
import time


def serialize_cached_message(
    mq_props: MQPropBase, message, extra_routing_key: str = ""
) -> CachedMessage:
    obj = md5()
    obj.update(str(message).encode("utf-8"))
    message_id = obj.hexdigest()
    cached_message = CachedMessage(
        id=message_id,
        msg=message,
        queue_name=mq_props.queue_name,
        routing_key=mq_props.routing_key_name,
        extra_routing_key=extra_routing_key,
        exchange_name=mq_props.exchange_name,
        timestamp=time.time(),
    )
    return cached_message


def publisher_producer(mq_props: MQPropBase):
    async def publisher(message, extra_routing_key: str = ""):
        cached_message = serialize_cached_message(mq_props, message, extra_routing_key)
        await redis_obj.add_pending_message(cached_message)
        broker = get_broker()
        if not broker._connection:
            await broker.start()
        routing_key = mq_props.get_publish_routing_key(extra_routing_key)
        await broker.publish(
            message=message,
            queue=mq_props.rabbit_queue,
            exchange=mq_props.exchange,
            routing_key=routing_key,
        )
        # 发布成功后删除缓存
        await redis_obj.remove_pending_message(cached_message.id)
        MQ_logger.info(f"消息发布成功，缓存已清除，ID: {cached_message.id}")

    return publisher


class BiliLotDataPublisher:
    @staticmethod
    @_mq_retry_wrapper(max_retries=-1)
    async def pub_official_reserve_charge_lot(
        business_type: int | str,
        business_id: int | str,
        origin_dynamic_id: int | str,
        extra_routing_key: str = "",
        *args,
        **kwargs,
    ):
        for i, value in enumerate(args, start=1):
            kwargs[f"extra_field_{i}"] = value
        do_publish = publisher_producer(official_reserve_charge_lot_mq_prop)
        return await do_publish(
            message=LotDataReq(
                business_type=business_type,
                business_id=business_id,
                origin_dynamic_id=origin_dynamic_id,
                **kwargs,
            ),
            extra_routing_key=extra_routing_key,
        )

    @staticmethod
    @_mq_retry_wrapper(max_retries=-1)
    async def pub_upsert_official_reserve_charge_lot(
        da: dict, extra_routing_key: str = "", *args, **kwargs
    ):
        """
        需要的数据是类似
        ```json
             {
                "lottery_id": 311007,
                "sender_uid": 401742377,
                "business_type": 1,
                "business_id": 962043520082772000,
                "status": 2,
                "lottery_time": 1723442400
            }
        ```
        这种响应的data字段
        """
        for i, value in enumerate(args, start=1):
            kwargs[f"extra_field_{i}"] = value
        do_publish = publisher_producer(upsert_official_reserve_charge_lot_mq_prop)
        return await do_publish(message=da, extra_routing_key=extra_routing_key)

    @staticmethod
    @_mq_retry_wrapper(max_retries=-1)
    async def pub_upsert_lot_data_by_dynamic_id(
        dynamic_id: int | str, extra_routing_key: str = "", *args, **kwargs
    ):
        for i, value in enumerate(args, start=1):
            kwargs[f"extra_field_{i}"] = value
        da = LotDataDynamicReq(dynamic_id=dynamic_id, **kwargs)
        do_publish = publisher_producer(upsert_lot_data_by_dynamic_id_prop)
        return await do_publish(message=da, extra_routing_key=extra_routing_key)

    @staticmethod
    @_mq_retry_wrapper(max_retries=-1)
    async def pub_upsert_topic_lot(
        topic_id: int | str, extra_routing_key: str = "", *args, **kwargs
    ):
        for i, value in enumerate(args, start=1):
            kwargs[f"extra_field_{i}"] = value
        da = TopicLotData(topic_id=topic_id, **kwargs)
        do_publish = publisher_producer(mq_props=upsert_topic_lot_prop)
        return await do_publish(message=da, extra_routing_key=extra_routing_key)

    @staticmethod
    @_mq_retry_wrapper(max_retries=-1)
    async def pub_upsert_milvus_bili_lot_data(
        body: Lotdata, extra_routing_key: str = "", *args, **kwargs
    ):
        for i, value in enumerate(args, start=1):
            kwargs[f"extra_field_{i}"] = value
        do_publish = publisher_producer(mq_props=upsert_milvus_bili_lot_data_prop)
        return await do_publish(
            message=sqlalchemy_model_2_dict(body), extra_routing_key=extra_routing_key
        )

    @staticmethod
    @_mq_retry_wrapper(max_retries=-1)
    async def pub_bili_voucher(
        body: VoucherInfo, extra_routing_key: str = "", *args, **kwargs
    ):
        for i, value in enumerate(args, start=1):
            kwargs[f"extra_field_{i}"] = value
        do_publish = publisher_producer(mq_props=bili_voucher_prop)
        return await do_publish(message=body, extra_routing_key=extra_routing_key)

    @staticmethod
    @_mq_retry_wrapper(max_retries=-1)
    async def pub_upsert_bili_atari(
        lottery_id: int, extra_routing_key: str = "", *args, **kwargs
    ):
        for i, value in enumerate(args, start=1):
            kwargs[f"extra_field_{i}"] = value
        do_publish = publisher_producer(mq_props=upsert_bili_atari_prop)
        return await do_publish(message=lottery_id, extra_routing_key=extra_routing_key)

    # region 入库消息队列（按目标数据库拆分队列；大模型提取+写库逻辑共享）
    @staticmethod
    def _select_prize_extract_mq_props(target_db: PrizeExtractTargetEnum):
        """根据 target_db 选择对应的入库队列（不同数据库不同队列）。"""
        if target_db == PrizeExtractTargetEnum.DYNDETAIL:
            return prize_extract_dyndetail_mq_prop
        return prize_extract_biliopus_mq_prop

    @staticmethod
    @_mq_retry_wrapper(max_retries=-1)
    async def pub_prize_extract(
        req: PrizeExtractReq,
        mq_props=None,
        extra_routing_key: str = "",
        *args,
        **kwargs,
    ):
        """发布入库请求：由对应队列的消费者判断并（按需）调用大模型写库。

        消息体为统一的 PrizeExtractReq（params 自定义参数决定入哪个库）。
        mq_props 不传时按 params.target_db 自动选择队列。
        消费者在「并发已满」重新入队时，也会带上自己的 mq_props 指定回原队列。
        """
        if mq_props is None:
            mq_props = BiliLotDataPublisher._select_prize_extract_mq_props(
                req.params.target_db
            )
        for i, value in enumerate(args, start=1):
            kwargs[f"extra_field_{i}"] = value
        do_publish = publisher_producer(mq_props=mq_props)
        return await do_publish(message=req, extra_routing_key=extra_routing_key)

    @staticmethod
    @_mq_retry_wrapper(max_retries=-1)
    async def pub_prize_extract_from_lot_data(
        lot_data_dict: dict, extra_routing_key: str = "", **kwargs
    ):
        """从 lottery_notice 的 data 中抽取 lottery_id 与奖品文案，发布入库请求（dyndetail 队列）。

        无 lottery_id 或抽奖文案为空时直接跳过。
        """
        lottery_id = lot_data_dict.get("lottery_id")
        prize_cmts = [
            lot_data_dict.get("first_prize_cmt"),
            lot_data_dict.get("second_prize_cmt"),
            lot_data_dict.get("third_prize_cmt"),
        ]
        lottery_text = " ".join(filter(lambda a: a, prize_cmts)).strip()
        if lottery_id is None or not lottery_text:
            return None
        params = PrizeExtractParams(
            target_db=PrizeExtractTargetEnum.DYNDETAIL,
            lottery_id=int(lottery_id),
            lottery_text=lottery_text,
            **kwargs,
        )
        return await BiliLotDataPublisher.pub_prize_extract(
            PrizeExtractReq(params=params), extra_routing_key=extra_routing_key
        )

    @staticmethod
    @_mq_retry_wrapper(max_retries=-1)
    async def pub_prize_extract_from_dyn(
        dyn_id: int,
        dyn_content: str,
        dyn_publish_time: datetime | None = None,
        lot_type: str = "common",
        need_comment: int | None = None,
        extra_routing_key: str = "",
        **kwargs,
    ):
        """发布普通抽奖动态的入库请求（biliopusdb 队列）。

        等价替代 SqlHelper.addDynInfo 中「直接调用大模型提取并写库」的同步逻辑，
        改为投递到 biliopusdb 入库队列，由消费者判断后异步提取写库。
        """
        params = PrizeExtractParams(
            target_db=PrizeExtractTargetEnum.BILIOPUSDB,
            ref_id=int(dyn_id),
            lot_type=lot_type,
            dyn_content=dyn_content,
            dyn_publish_time=dyn_publish_time,
            need_comment=need_comment,
            **kwargs,
        )
        return await BiliLotDataPublisher.pub_prize_extract(
            PrizeExtractReq(params=params), extra_routing_key=extra_routing_key
        )

    @staticmethod
    @_mq_retry_wrapper(max_retries=-1)
    async def pub_test_msg(
        body: RabbitMQTestMsgModel,
        extra_routing_key: str = "",
        *args,
        **kwargs,
    ):
        for i, value in enumerate(args, start=1):
            kwargs[f"extra_field_{i}"] = value
        do_publish = publisher_producer(mq_props=test_mq_prop)
        return await do_publish(message=body, extra_routing_key=extra_routing_key)


    # endregion

    @staticmethod
    async def retry_pending_messages():
        """
        重新尝试发送缓存中的消息
        """
        MQ_logger.info("开始重试发送缓存中的消息")
        pending_messages = await redis_obj.get_pending_messages()
        MQ_logger.info(f"发现 {len(pending_messages)} 条待发送消息")

        success_count = 0
        failed_count = 0

        for cached_msg in pending_messages:
            try:
                # 重新发送消息
                exchange_name = None
                for x in ExchangeName:
                    if x.value == cached_msg.exchange_name:
                        exchange_name = x
                        break
                if exchange_name is None:
                    raise ValueError(
                        f"无法找到对应的交换机: {cached_msg.exchange_name}"
                    )

                exch = RabbitExchange(
                    exchange_name,
                    auto_delete=False,
                    type=ExchangeType.TOPIC,
                    durable=True,
                )
                queue_name = None
                for x in QueueName:
                    if x == cached_msg.queue_name:
                        queue_name = x
                        break
                if queue_name is None:
                    raise ValueError(f"无法找到对应的队列: {cached_msg.queue_name}")
                MQ_logger.critical(f"准备重发消息: {cached_msg}")
                broker = get_broker()
                if not broker._connection:
                    await broker.start()
                await broker.publish(
                    message=cached_msg.msg,
                    queue=queue_name,
                    exchange=exch,
                    routing_key=cached_msg.routing_key,
                )
                # 发布成功后删除缓存
                await redis_obj.remove_pending_message(cached_msg.id)
                success_count += 1

            except Exception as e:
                MQ_logger.error(f"重发消息失败，ID: {cached_msg.id}，错误: {e}")
                failed_count += 1

        MQ_logger.info(f"重试完成。成功: {success_count}，失败: {failed_count}")


if __name__ == "__main__":
    _test_msg = "Ciallo～(∠・ω< )⌒★"

    async def _test_publisher():
        do_pubish = publisher_producer(mq_props=test_mq_prop)
        return await do_pubish(message=_test_msg)

    async def _test_redis_add_pending_message():
        msg = RabbitMQTestMsgModel(
            a=random.randint(1, 99999),
            b=_test_msg,
            c=dict(zip([random.randint(1, 99999)], [_test_msg])),
            d=[_test_msg],
        )
        cached_message = serialize_cached_message(test_mq_prop, msg, "1234")
        await redis_obj.add_pending_message(cached_message)
        MQ_logger.info("添加缓存消息成功")

    async def _test_redis_add_pending_message_bulk():
        await asyncio.gather(*[_test_redis_add_pending_message() for _ in range(10)])

    async def _test_get_pending_messages():
        pending_messages = await redis_obj.get_pending_messages()
        print(pending_messages)

    async def _test_retry_pending_messages():
        await BiliLotDataPublisher.retry_pending_messages()

    asyncio.run(_test_retry_pending_messages())
