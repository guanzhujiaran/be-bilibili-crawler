# -*- coding: utf-8 -*-
"""按「方法名」在当前仓库的 proto 中解析请求/响应消息类。

数据来源为 :mod:`test.grpc_capture._proto_index`（由 GrpcProto 下全部
``*_pb2.py`` 静态索引而来），因此**不需要任何动态 import**。

约定：
- 抓包目录名即 gRPC 方法名，例如 ``DynDetail`` / ``GetColdStartDeferredData``；
- 同名方法分属多个服务时，在该方法目录下放 ``case.json`` 指定 ``service``；
- 也可在 ``case.json`` 用 ``request_type`` / ``response_type`` 直接指定消息的
  protobuf 全限定名，完全跳过方法解析。
"""

from google.protobuf.message import Message

from test.grpc_capture._proto_index import METHOD_INDEX, MESSAGE_INDEX, ProtoMethod


class UnknownMethodError(LookupError):
    """本地 proto 中找不到该 gRPC 方法。"""


class AmbiguousMethodError(LookupError):
    """同名方法属于多个服务，需要显式指定 service。"""


def resolve_method(method_name: str, service: str | None = None) -> ProtoMethod:
    """把方法名解析成 :class:`ProtoMethod`。

    :param method_name: gRPC 方法名，如 ``DynDetail``
    :param service: 可选，服务全限定名，用于同名方法消歧
    :raise UnknownMethodError: 本地 proto 未定义该方法
    :raise AmbiguousMethodError: 同名方法有多个候选且未指定 service
    """
    candidates = METHOD_INDEX.get(method_name, ())
    if service:
        matched = tuple(item for item in candidates if item.service == service)
        if not matched:
            raise UnknownMethodError(
                f"服务 {service!r} 中没有方法 {method_name!r}"
            )
        return matched[0]
    if not candidates:
        raise UnknownMethodError(
            f"本地 proto 未定义方法 {method_name!r}，"
            "请补充对应 .proto 并重新生成 pb2 与 test/grpc_capture/_proto_index.py"
        )
    if len(candidates) > 1:
        services = "、".join(sorted({item.service for item in candidates}))
        raise AmbiguousMethodError(
            f"方法 {method_name!r} 存在于多个服务（{services}），"
            "请在 case.json 中用 service 指定，或用 request_type/response_type 直接指定消息类型"
        )
    return candidates[0]


def resolve_message(qualified_name: str) -> type[Message]:
    """按 protobuf 全限定名取出消息类。

    :param qualified_name: 形如 ``bilibili.app.dynamic.v2.DynDetailReq``
    :raise UnknownMethodError: 本地 proto 未定义该消息
    """
    try:
        return MESSAGE_INDEX[qualified_name]
    except KeyError as exc:
        raise UnknownMethodError(
            f"本地 proto 未定义消息 {qualified_name!r}，"
            "请补充对应 .proto 并重新生成 pb2 与 test/grpc_capture/_proto_index.py"
        ) from exc
