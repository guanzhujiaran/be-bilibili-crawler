"""
RabbitMQ RPC 服务端（基于 FastStream 框架）

**职责边界（2.70.0）**：

- ``FastapiApp.rpc.*`` 一律是**只读取数**（请求-响应），服务端只由 **query 角色**
  （`query_main.py`，可指向只读账号 / 只读副本）提供，handler 在
  `controller/v1/mq/rpc_read_lottery_data.py`；
- **写库不走 RPC**：走 `bili_data` 的普通 MQ 消息
  （`BiliLotDataPublisher` 发布 → crawler 角色的 `mq_controller` 订阅者落库）。
  所以 crawler / api 角色都不注册任何 RPC 服务端（避免与 query 争抢同一队列）。

本模块只做**注册期校验**：只读 RPC 离开 query 角色就跳过注册并打 WARNING，
保证「消息不会被两个角色同时消费」（competing consumers）。

使用 @rpc_subscriber 装饰器注册 RPC 函数，handler 直接接收强类型参数模型，
直接返回 CommonResponseModel，全程由 FastStream + Pydantic 做参数校验和序列化。

router 实例复用 `Service/MQ/base/MQClient/base.py` 中的 RabbitRouter：
- query 角色由 `query_main.py` 直接 `app.include_router(router)`（只有只读 RPC 订阅者，
  不 import mq_controller，因此不会把入库消费者带进来）；
- crawler 角色把它和 bili_data 的入库消费者放在一起（同一个 broker），但不注册 RPC。

消息处理流程：
1. FastStream 接收 RabbitMQ 消息，自动将 body JSON validate 为 params_model
2. handler 处理后返回 CommonResponseModel，FastStream 自动序列化并发送到 reply_to

超时控制：
- 默认超时时间 DEFAULT_RPC_TIMEOUT 秒，超时后服务器主动取消内部 Task（CancelledError）
  释放数据库连接、HTTP 客户端等资源，避免长尾请求堆积导致负载升高
- 使用 asyncio.wait_for 而非 asyncio.timeout：wait_for 在独立的内部 Task 上运行 handler，
  超时只取消内部 Task，不影响 FastStream subscriber task，避免后续 _publish 步骤被误取消
- 装饰器可通过 timeout 参数覆盖默认值（None 表示不限制）
- 超时返回 code=504 的错误响应

消息确认策略：
- 使用 AckPolicy.ACK_FIRST：消息在 handler 执行前就被 ack
- 避免 _publish 失败后 FastStream 尝试 reject → ChannelInvalidStateError → 消息未 ack →
  RabbitMQ 重新投递 → 无限重试循环
- RPC 模式下客户端有自己的超时机制，服务端不需要重试

连接保活：
- broker_url 中配置 heartbeat=180 秒，避免 handler 执行时间较长时
  RabbitMQ heartbeat 超时导致连接关闭、channel 失效

使用方式：
    from controller.v1.mq.rpc_server import rpc_subscriber
    from bili_common.models import RpcMethodName
    from bili_common.models import GetReserveLotteryRpcParams

    @rpc_subscriber(RpcMethodName.GET_RESERVE_LOTTERY, GetReserveLotteryRpcParams)
    async def handle_get_reserve_lottery(params: GetReserveLotteryRpcParams) -> CommonResponseModel:
        ...
"""

import asyncio
from faststream import AckPolicy
from pydantic import BaseModel
from Service.MQ.base.MQClient.base import router
from log.base_log import MQ_logger as logger
from bili_common.models import (
    RpcMethodName,
    ROUTING_KEY_PREFIX,
    all_rpc_method_names,
)
from bili_common.models.response import StandardResponse as CommonResponseModel
from CONFIG import settings

# RPC 默认超时时间（秒）：超时后服务器主动取消任务以释放资源
DEFAULT_RPC_TIMEOUT: float = 60.0

# 当前进程实际注册成功的 RPC 方法（按注册顺序），供启动日志自证注册结果
_REGISTERED_RPC_METHODS: list[str] = []


def registered_rpc_methods() -> list[str]:
    """当前进程已注册（真正建了队列消费者）的 RPC 方法名"""
    return list(_REGISTERED_RPC_METHODS)


def rpc_subscriber(
    method_name: RpcMethodName | str,
    params_model: type[BaseModel],
    timeout: float | None = DEFAULT_RPC_TIMEOUT,
):
    """RPC subscriber 装饰器（只读，只允许 query 角色注册）

    基于 @router.subscriber，FastStream 自动将消息体 JSON validate 为 params_model，
    handler 直接接收强类型参数，返回 CommonResponseModel 即可。

    超时机制：使用 asyncio.wait_for 在独立内部 Task 上运行 handler，超时取消该内部 Task，
    CancelledError 会中断 handler 内部所有 await 中的协程（DB 查询、HTTP 请求等），及时释放资源。
    不使用 asyncio.timeout 是因为它会取消当前 FastStream subscriber task，导致后续 _publish
    步骤被误取消，引发 ChannelInvalidStateError。

    Args:
        method_name: RPC 方法名（推荐使用 RpcMethodName 枚举，也可传 str）
                    routing_key 自动生成为 `FastapiApp.rpc.<method_name>`
        params_model: 该方法的请求参数 Pydantic 模型类，FastStream 自动从消息体 validate
        timeout: 超时时间（秒），默认 DEFAULT_RPC_TIMEOUT；
                 None 表示不限制（不推荐，可能堆积长尾请求）

    异常：
        方法名不在 bili_common 的 RPC 方法清单里 → 直接抛错（契约错误，启动即暴露）
        当前角色不是 query（只读角色）→ 打 WARNING 并**跳过注册**（不建订阅者，避免双消费者）
    """
    method_name_str = (
        method_name.value
        if isinstance(method_name, RpcMethodName)
        else str(method_name)
    )
    routing_key = f"{ROUTING_KEY_PREFIX}.{method_name_str}"

    known_methods = all_rpc_method_names()
    if method_name_str not in known_methods:
        raise RuntimeError(
            f"RPC 方法 '{method_name_str}' 不在 bili_common 的 RPC 方法清单里"
            f"（{', '.join(known_methods)}）：请先在 bili_common.rpc.base 登记"
        )

    if not settings.is_query_only:
        logger.warning(
            f"[RpcServer] 跳过 RPC 订阅者注册: {routing_key}："
            f"只读 RPC 只由 query 角色提供（当前 SERVICE_ROLE={settings.SERVICE_ROLE.value}）；"
            "写库请走 bili_data 的普通 MQ 消息，不要用 RPC"
        )

        def _skip(func):
            return func

        return _skip

    def decorator(func):
        _REGISTERED_RPC_METHODS.append(method_name_str)
        logger.info(f"[RpcServer] 注册只读 RPC 订阅者: {routing_key}")

        # ACK_FIRST：消息在 handler 执行前就被 ack，避免 _publish 失败后
        # FastStream 尝试 reject → ChannelInvalidStateError → 消息未 ack →
        # RabbitMQ 重新投递 → 无限重试循环。
        # RPC 模式下客户端有自己的超时机制，服务端不需要重试。
        @router.subscriber(routing_key, ack_policy=AckPolicy.ACK_FIRST)
        async def wrapper(params: params_model) -> CommonResponseModel:
            try:
                # 安全日志：避免直接 {params} 触发 __str__/__repr__，
                # CustomBaseModel 的 @computed_field 在某些场景下会触发
                # __getattr__ 访问 __private_attributes__ 失败
                try:
                    params_repr = params.model_dump_json(indent=2)
                except Exception:
                    params_repr = f"<{type(params).__name__} (序列化失败)>"
                logger.info(f"[RpcServer] 收到 RPC 请求: {routing_key}\n{params_repr}")
                # asyncio.wait_for 在独立的内部 Task 上运行 handler，
                # 超时只取消内部 Task（CancelledError 中断 handler 内所有 await，
                # 释放 DB 连接、HTTP 客户端等资源），不影响当前 FastStream subscriber task，
                # 避免后续 _publish 步骤被误取消导致 ChannelInvalidStateError
                result = await asyncio.wait_for(func(params), timeout=timeout)
                return result
            except TimeoutError:
                logger.exception(
                    f"[RpcServer] handler 执行超时: {routing_key} (timeout={timeout}s)"
                )
                return CommonResponseModel(
                    code=504, msg=f"RPC 请求超时（{timeout}s）: {routing_key}"
                )
            except KeyError as e:
                logger.exception(f"[RpcServer] handler 参数缺失: {routing_key} - {e}")
                return CommonResponseModel(code=400, msg=f"参数缺失: {e}")
            except ValueError as e:
                logger.exception(
                    f"[RpcServer] handler 参数格式错误: {routing_key} - {e}"
                )
                return CommonResponseModel(code=400, msg=f"参数格式错误: {e}")
            except Exception as e:
                logger.exception(f"[RpcServer] handler 执行异常: {routing_key} - {e}")
                return CommonResponseModel(code=500, msg=f"handler 执行异常: {e}")

        return wrapper

    return decorator


__all__ = [
    "DEFAULT_RPC_TIMEOUT",
    "rpc_subscriber",
    "registered_rpc_methods",
    "router",
]
