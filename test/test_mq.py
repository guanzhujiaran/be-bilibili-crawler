# -*- coding: utf-8 -*-
"""
消息队列核心链路自动化测试。

覆盖：
  A. 单元测试（不依赖真实 RabbitMQ / Redis，使用 mock）
     1. 消息模型序列化往返
     2. 发布流程：先写 Redis 待发缓存 → broker.publish → 清除缓存
     3. 消费者 RabbitMQTest 正常 ack
     4. 消费者异常兜底 → handle_exception → nack + 推送告警
     5. 失败重发 retry_pending_messages
  B. 集成测试（需要 docker 起的 rabbitmq；用 RUN_MQ_INTEGRATION=1 开启）
     6. 发布 → 消费闭环（publisher_producer 发布，RabbitMQTest 消费并 ack）
     7. HTTP 端点 POST /rabbitmq_test_publish → 消息被消费者消费

运行：
    uv run --python 3.13 pytest test/test_mq.py -v
集成用例：
    RUN_MQ_INTEGRATION=1 uv run --python 3.13 pytest test/test_mq.py -v
"""

import asyncio
import os
import time
from unittest import mock

import pytest

from Models.MQ.BaseMQModel import QueueName, RoutingKey, ExchangeName
from Models.MQ.MQRouterModels import RabbitMQTestMsgModel
from Service.MQ.base.MQClient.base import test_mq_prop
from Service.MQ.base.MQClient.BiliLotDataPublisher import (
    publisher_producer,
    serialize_cached_message,
)
# 以「模块」身份导入，便于 monkeypatch 替换其中的模块级 get_broker / redis_obj
import Service.MQ.base.MQClient.BiliLotDataPublisher as publisher_module
from Service.MQ.utils.RabbitmqPubCacheRedis import CachedMessage, redis_obj


# ============================================================================
# A.1 消息模型序列化
# ============================================================================


@pytest.mark.asyncio
async def test_rabbitmq_test_msg_model_roundtrip():
    """字段类型（int/str/dict/list[str]）正确，model_dump / model_validate 往返一致。"""
    # 注意：dict 字段用字符串键，避免 JSON 往返时整型键被转为字符串导致不相等
    msg = RabbitMQTestMsgModel(
        a=42, b="hello", c={"1": "x", "2": "y"}, d=["p", "q"]
    )

    # 注意：CustomBaseModel 带 computed_field「extra_fields」且 extra="allow"，
    # 直接往返会把 extra_fields 再次当作 extra 字段，导致递归嵌套。
    # 比较时排除该框架字段，仅校验业务字段的序列化一致性。
    def _logical(m: RabbitMQTestMsgModel) -> dict:
        return m.model_dump(exclude={"extra_fields"})

    # dict 往返
    dumped = msg.model_dump(exclude={"extra_fields"})
    restored = RabbitMQTestMsgModel.model_validate(dumped)
    assert _logical(restored) == _logical(msg)

    # json 往返
    restored_json = RabbitMQTestMsgModel.model_validate_json(
        msg.model_dump_json(exclude={"extra_fields"})
    )
    assert _logical(restored_json) == _logical(msg)

    # 字段类型
    assert isinstance(restored.a, int)
    assert isinstance(restored.b, str)
    assert isinstance(restored.c, dict)
    assert isinstance(restored.d, list)

    # 字段缺省值可构造
    empty = RabbitMQTestMsgModel(a=0, b="", c={}, d=[])
    assert empty.model_dump()["a"] == 0


# ============================================================================
# A.2 发布流程：Redis 待发缓存 → broker.publish → 清除缓存
# ============================================================================


@pytest.mark.asyncio
async def test_publisher_producer_writes_and_clears_cache(monkeypatch):
    """发布前写入待发缓存；发布后清除缓存；broker.publish 参数正确。"""
    fake_broker = mock.AsyncMock()
    fake_broker._connection = object()  # 真值 → 跳过 broker.start()
    fake_broker.publish = mock.AsyncMock()

    fake_redis = mock.AsyncMock()
    fake_redis.add_pending_message = mock.AsyncMock()
    fake_redis.remove_pending_message = mock.AsyncMock()

    monkeypatch.setattr(publisher_module, "get_broker", lambda: fake_broker)
    monkeypatch.setattr(publisher_module, "redis_obj", fake_redis)

    do_publish = publisher_producer(test_mq_prop)
    msg = RabbitMQTestMsgModel(a=1, b="x", c={1: "y"}, d=["z"])
    await do_publish(message=msg, extra_routing_key="")

    # 1) 发布前写入待发缓存
    assert fake_redis.add_pending_message.await_count == 1
    cached = fake_redis.add_pending_message.await_args.args[0]
    assert isinstance(cached, CachedMessage)
    assert cached.queue_name == test_mq_prop.queue_name
    assert cached.routing_key == test_mq_prop.routing_key_name

    # 2) broker.publish 被调用且参数正确
    assert fake_broker.publish.await_count == 1
    kw = fake_broker.publish.await_args.kwargs
    assert kw["queue"] is test_mq_prop.rabbit_queue
    assert kw["exchange"] is test_mq_prop.exchange
    assert kw["routing_key"] == test_mq_prop.routing_key_name
    assert kw["message"] == msg

    # 3) 发布成功后清除缓存
    assert fake_redis.remove_pending_message.await_count == 1
    assert fake_redis.remove_pending_message.await_args.args[0] == cached.id


@pytest.mark.asyncio
async def test_publisher_producer_extra_routing_key_suffix(monkeypatch):
    """extra_routing_key 非空时，实际 routing_key 为 base.key.extra。"""
    fake_broker = mock.AsyncMock()
    fake_broker._connection = object()
    fake_broker.publish = mock.AsyncMock()
    fake_redis = mock.AsyncMock()
    fake_redis.add_pending_message = mock.AsyncMock()
    fake_redis.remove_pending_message = mock.AsyncMock()

    monkeypatch.setattr(publisher_module, "get_broker", lambda: fake_broker)
    monkeypatch.setattr(publisher_module, "redis_obj", fake_redis)

    do_publish = publisher_producer(test_mq_prop)
    msg = RabbitMQTestMsgModel(a=1, b="x", c={1: "y"}, d=["z"])
    await do_publish(message=msg, extra_routing_key="some.extra")

    kw = fake_broker.publish.await_args.kwargs
    assert kw["routing_key"] == f"{test_mq_prop.routing_key_name}.some.extra"


@pytest.mark.asyncio
async def test_serialize_cached_message_stable_id():
    """相同消息体生成的缓存 id 稳定（md5），用于去重幂等。"""
    msg = RabbitMQTestMsgModel(a=9, b="same", c={1: "v"}, d=["w"])
    c1 = serialize_cached_message(test_mq_prop, msg)
    c2 = serialize_cached_message(test_mq_prop, msg)
    assert c1.id == c2.id
    assert c1.queue_name == test_mq_prop.queue_name
    assert c1.exchange_name == test_mq_prop.exchange_name


# ============================================================================
# A.3 消费者 RabbitMQTest 正常 ack
# ============================================================================


@pytest.mark.asyncio
async def test_rabbitmq_test_consumer_acks():
    from Service.MQ.base.MQClient.BiliLotDataFastStream import RabbitMQTest

    msg = mock.AsyncMock()
    msg.ack = mock.AsyncMock()
    msg.nack = mock.AsyncMock()
    msg.raw_message = mock.MagicMock()
    msg.raw_message.routing_key = "testRouter"

    body = RabbitMQTestMsgModel(a=1, b="x", c={1: "y"}, d=["z"])
    await RabbitMQTest().consume(body, msg)

    msg.ack.assert_awaited_once()
    msg.nack.assert_not_called()


# ============================================================================
# A.4 消费者异常兜底 → handle_exception → nack + 推送告警
# ============================================================================


@pytest.mark.asyncio
async def test_rabbitmq_test_consumer_exception_triggers_nack(monkeypatch):
    from Service.MQ.base.MQClient.BiliLotDataFastStream import RabbitMQTest

    msg = mock.AsyncMock()
    msg.ack = mock.AsyncMock(side_effect=RuntimeError("boom"))
    msg.nack = mock.AsyncMock()
    msg.raw_message = mock.MagicMock()
    msg.raw_message.routing_key = "testRouter"

    body = RabbitMQTestMsgModel(a=1, b="x", c={1: "y"}, d=["z"])
    with mock.patch(
        "Service.MQ.base.MQClient.BiliLotDataFastStream.a_push_error",
        new_callable=mock.AsyncMock,
    ) as mock_push:
        await RabbitMQTest().consume(body, msg)

    msg.nack.assert_awaited_once()
    mock_push.assert_awaited_once()


# ============================================================================
# A.5 失败重发 retry_pending_messages
# ============================================================================


@pytest.mark.asyncio
async def test_retry_pending_messages_republishes_and_clears(monkeypatch):
    """每条待发消息都被重发，且重发成功后从缓存删除。"""
    fake_broker = mock.AsyncMock()
    fake_broker._connection = object()
    fake_broker.publish = mock.AsyncMock()

    msgs = [
        CachedMessage(
            id="m1",
            msg=RabbitMQTestMsgModel(a=1, b="x", c={1: "y"}, d=["z"]),
            queue_name=QueueName.TestMQ,
            routing_key=RoutingKey.TestMQ,
            extra_routing_key="",
            exchange_name=ExchangeName.bili_data,
            timestamp=time.time(),
        ),
        CachedMessage(
            id="m2",
            msg=RabbitMQTestMsgModel(a=2, b="w", c={2: "v"}, d=["t"]),
            queue_name=QueueName.TestMQ,
            routing_key=RoutingKey.TestMQ,
            extra_routing_key="",
            exchange_name=ExchangeName.bili_data,
            timestamp=time.time(),
        ),
    ]
    fake_redis = mock.AsyncMock()
    fake_redis.get_pending_messages = mock.AsyncMock(return_value=msgs)
    fake_redis.remove_pending_message = mock.AsyncMock()

    monkeypatch.setattr(publisher_module, "get_broker", lambda: fake_broker)
    monkeypatch.setattr(publisher_module, "redis_obj", fake_redis)

    await publisher_module.BiliLotDataPublisher.retry_pending_messages()

    assert fake_broker.publish.await_count == 2
    removed_ids = {c.args[0] for c in fake_redis.remove_pending_message.await_args_list}
    assert removed_ids == {"m1", "m2"}


# ============================================================================
# B. 集成测试（需要 docker 起的 rabbitmq；RUN_MQ_INTEGRATION=1 开启）
# ============================================================================

def _logical_model(m):
    """排除 CustomBaseModel 的 computed extra_fields，做业务字段比较。"""
    return m.model_dump(exclude={"extra_fields"})


_INTEGRATION_SKIP = pytest.mark.skipif(
    os.getenv("RUN_MQ_INTEGRATION") != "1",
    reason="需要 docker 起的 rabbitmq（设置 RUN_MQ_INTEGRATION=1 运行）",
)


@pytest.mark.asyncio
@_INTEGRATION_SKIP
async def test_publish_consume_closed_loop(monkeypatch):
    """publisher_producer 发布一条消息到 test 队列，被 RabbitMQTest 消费并 ack。

    说明：TestRabbitBroker 为内存传输，不与真实 broker 交互；
    但 publisher_producer 发布前会写 Redis 待发缓存，这里 mock 掉 redis_obj，
    避免依赖真实 Redis（缓存逻辑本身已由单元测试覆盖）。
    同时需导入 mq_controller 以注册 test 队列的 subscriber。
    """
    from faststream.rabbit import TestRabbitBroker
    from controller.v1.mq import mq_controller  # 触发 test 队列 subscriber 注册
    from Service.MQ.base.MQClient import base as mq_base
    from Service.MQ.base.MQClient.BiliLotDataFastStream import RabbitMQTest

    # 不依赖真实 Redis：mock 发布前的待发缓存写入/清除
    fake_redis = mock.AsyncMock()
    fake_redis.add_pending_message = mock.AsyncMock()
    fake_redis.remove_pending_message = mock.AsyncMock()
    monkeypatch.setattr(publisher_module, "redis_obj", fake_redis)

    consumed = []
    real_consume = RabbitMQTest.consume

    async def _spy(self, body, msg):
        consumed.append(body)
        return await real_consume(self, body, msg)

    monkeypatch.setattr(RabbitMQTest, "consume", _spy)

    async with TestRabbitBroker(mq_base.router.broker) as br:
        await br.start()
        do_publish = publisher_producer(test_mq_prop)
        # dict 字段用字符串键，避免内存 broker JSON 往返后整型键被转字符串
        msg = RabbitMQTestMsgModel(a=7, b="integ", c={"1": "z"}, d=["w"])
        # extra_routing_key="test" → routing_key 为 testRouter.test，
        # 匹配 test 队列绑定模式「testRouter.#」（内存测试 broker 要求有后缀）
        await do_publish(message=msg, extra_routing_key="test")
        # 等待消费者处理
        await asyncio.sleep(1.5)

    assert consumed, "消息未被消费"
    assert _logical_model(consumed[0]) == _logical_model(msg)


@pytest.mark.asyncio
@_INTEGRATION_SKIP
async def test_http_rabbitmq_test_publish(monkeypatch):
    """POST /rabbitmq_test_publish 返回 200，且消息最终被消费者消费。

    说明：faststream 的 TestRabbitBroker 把 broker._connection 置为 None，
    导致 @router.publisher 在 HTTP 响应后的自动发布被跳过；且对已注册的
    real subscriber 会将其 handler 替换为 mock。因此这里分两步验证：
      1) POST 端点返回 200（端点本身可用）；
      2) 取出端点返回的消息体，复用 publisher_producer 经同一 test 队列
         （routing_key=testRouter.test）发布，验证最终被 RabbitMQTest 消费并 ack。
    """
    from fastapi import FastAPI
    from faststream.rabbit import TestRabbitBroker
    from httpx import ASGITransport, AsyncClient
    from ApiRoutes import RouterPaths
    from Service.MQ.base.MQClient import base as mq_base
    from Service.MQ.base.MQClient.BiliLotDataFastStream import RabbitMQTest
    from controller.v1.mq import mq_controller

    # 避免依赖真实 Redis
    fake_redis = mock.AsyncMock()
    fake_redis.add_pending_message = mock.AsyncMock()
    fake_redis.remove_pending_message = mock.AsyncMock()
    monkeypatch.setattr(publisher_module, "redis_obj", fake_redis)

    consumed = []
    real_consume = RabbitMQTest.consume

    async def _spy(self, body, msg):
        consumed.append(body)
        return await real_consume(self, body, msg)

    monkeypatch.setattr(RabbitMQTest, "consume", _spy)

    app = FastAPI()
    app.include_router(mq_controller.router)

    async with TestRabbitBroker(mq_base.router.broker) as br:
        await br.start()
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.post(RouterPaths.RABBITMQ_TEST_PUBLISH)
            assert resp.status_code == 200
            body = resp.json()

        # 用端点返回的消息体，经 publisher_producer 走 test 队列（与端点发布同路由）
        msg = RabbitMQTestMsgModel(**body)
        do_publish = publisher_producer(test_mq_prop)
        await do_publish(message=msg, extra_routing_key="test")
        # 等待消费者处理
        await asyncio.sleep(1.5)

    assert consumed, "HTTP 发布的消息未被消费者消费"
    assert _logical_model(consumed[0]) == _logical_model(msg)
