# -*- coding: utf-8 -*-
"""B 站 gRPC 请求头（``x-bili-*-bin``）与本地 metadata proto 的一致性测试。

这里的取值来自一次真实抓包（mitm，BiliApp 9.13.0 / build 9130500 / Android 9），
用于保证 ``Service/GrpcModule/Grpc/GrpcProto/bilibili/metadata`` 下的 proto 与
线上客户端实际发送的字节一致：

- **字节往返一致**：用本地 proto 解析抓包字节后重新序列化，必须与原始字节完全相同；
- **无未知字段**：按 wire-format 扫描，本地 proto 不得漏定义任何编号，
  否则 protobuf 会静默丢弃这些字节（``upb`` 实现也不暴露 ``UnknownFields()``）。

抓包中的 ``x-bili-ticket`` 与 ``x-bili-metadata-recent-region`` 是服务端签发的
JWT（HS256 / ES256），客户端无法自行构造，故不在本测试范围内。
"""

import base64

import pytest

from bilibili.metadata.device.device_pb2 import Device
from bilibili.metadata.fawkes.fawkes_pb2 import FawkesReq
from bilibili.metadata.locale.locale_pb2 import Locale
from bilibili.metadata.metadata_pb2 import Metadata
from bilibili.metadata.network.network_pb2 import Network, NetworkType
from bilibili.metadata.restriction.restriction_pb2 import Restriction
from test.grpc_capture._loader import scan_unknown_fields

# 抓包原值（base64；去掉尾部 '=' 填充的原始形态）
CAPTURED_HEADERS: dict[str, str] = {
    "x-bili-metadata-bin": (
        "EgdhbmRyb2lkIISkrQQqBGJpbGkyJVhVNDk2NTA0Mzg1MDAxNzA1OUZFNUQ3QTA2MDZDMEU5NjMxNzA6B2FuZHJvaWQ"
    ),
    "x-bili-device-bin": (
        "CAEQhKStBBolWFU0OTY1MDQzODUwMDE3MDU5RkU1RDdBMDYwNkMwRTk2MzE3MCIHYW5kcm9pZCoHYW5kcm9pZDoE"
        "YmlsaUIFUmVkbWlKCjIzMTEzUktDNkNSAjEyWkBkYzBhNDAwYzJmNzg5N2E2YTAwNDlmYTY4M2Q1OTY3YTIwMjYx"
        "MDAxMTM1NzQ2MTMzYjEwODc0NmI4NGE4NTIwYkBkYzBhNDAwYzJmNzg5N2E2YTAwNDlmYTY4M2Q1OTY3YTIwMjYx"
        "MDAxMTM1NzQ2MTMzYjEwODc0NmI4NGE4NTIwagY5LjEzLjByQGRjMGE0MDBjMmY3ODk3YTZhMDA0OWZhNjgzZDU5"
        "NjdhMjAyNjEwMDExMzU3NDYxMzNiMTA4NzQ2Yjg0YTg1MjB40+z31QaCAQ4yNjg3ODU5MjIyODQ0MA"
    ),
    "x-bili-locale-bin": (
        "Cg4KAnpoEgRIYW5zGgJDThIOCgJ6aBIESGFucxoCQ04iDUFzaWEvU2hhbmdoYWkqBiswODowMA"
    ),
    "x-bili-network-bin": "CAEqBQ0QPng/",
    "x-bili-restriction-bin": "OBA",
    "x-bili-fawkes-req-bin": "CglhbmRyb2lkNjQSBHByb2QaCGE1YTJmYjJh",
}

HEADER_MESSAGE_CLASSES: dict[str, type] = {
    "x-bili-metadata-bin": Metadata,
    "x-bili-device-bin": Device,
    "x-bili-locale-bin": Locale,
    "x-bili-network-bin": Network,
    "x-bili-restriction-bin": Restriction,
    "x-bili-fawkes-req-bin": FawkesReq,
}


def _decode_b64(value: str) -> bytes:
    return base64.b64decode(value + "=" * (-len(value) % 4))


@pytest.fixture(params=sorted(CAPTURED_HEADERS))
def captured_header(request) -> tuple[str, bytes]:
    """抓包中的一条 ``x-bili-*-bin``：``(header 名, 原始字节)``。"""
    name = request.param
    return name, _decode_b64(CAPTURED_HEADERS[name])


def test_header_bytes_roundtrip(captured_header: tuple[str, bytes]):
    """本地 metadata proto 能字节级还原抓包头部。"""
    name, raw = captured_header
    message = HEADER_MESSAGE_CLASSES[name]()
    message.ParseFromString(raw)

    assert not scan_unknown_fields(raw, message.DESCRIPTOR), (
        f"{name} 存在本地 proto 未定义的字段，解析时会静默丢字段"
    )
    assert message.SerializeToString() == raw, f"{name} 重新序列化后与抓包字节不一致"


def test_metadata_bin_values(captured_header: tuple[str, bytes]):
    """``x-bili-metadata-bin`` 关键字段与抓包一致。"""
    name, raw = captured_header
    if name != "x-bili-metadata-bin":
        pytest.skip("仅校验 x-bili-metadata-bin")
    metadata = Metadata()
    metadata.ParseFromString(raw)

    assert metadata.mobi_app == "android"
    assert metadata.platform == "android"
    assert metadata.build == 9130500
    assert metadata.channel == "bili"
    assert metadata.buvid == "XU4965043850017059FE5D7A0606C0E963170"


def test_device_bin_values(captured_header: tuple[str, bytes]):
    """``x-bili-device-bin`` 关键字段与抓包一致（device/guest_id 必须存在）。"""
    name, raw = captured_header
    if name != "x-bili-device-bin":
        pytest.skip("仅校验 x-bili-device-bin")
    device = Device()
    device.ParseFromString(raw)

    assert device.app_id == 1
    assert device.build == 9130500
    assert device.mobi_app == "android"
    assert device.platform == "android"
    assert device.channel == "bili"
    assert device.brand == "Redmi"
    assert device.model == "23113RKC6C"
    assert device.version_name == "9.13.0"
    assert device.guest_id == "26878592228440"
    # 抓包中 fp 三件套一致、fts 为上报时间戳；device 字段本次未下发
    assert device.fp_local == device.fp_remote == device.fp
    assert int(device.fts) > 0
    assert device.device == ""


def test_locale_bin_values(captured_header: tuple[str, bytes]):
    """``x-bili-locale-bin`` 关键字段与抓包一致。"""
    name, raw = captured_header
    if name != "x-bili-locale-bin":
        pytest.skip("仅校验 x-bili-locale-bin")
    locale = Locale()
    locale.ParseFromString(raw)

    assert locale.c_locale.language == "zh"
    assert locale.c_locale.region == "CN"
    assert locale.s_locale.language == "zh"
    assert locale.timezone == "Asia/Shanghai"
    assert locale.utc_offset == "+08:00"


def test_network_bin_values(captured_header: tuple[str, bytes]):
    """``x-bili-network-bin`` 关键字段与抓包一致。"""
    name, raw = captured_header
    if name != "x-bili-network-bin":
        pytest.skip("仅校验 x-bili-network-bin")
    network = Network()
    network.ParseFromString(raw)

    assert network.type == NetworkType.WIFI
    assert network.quality.success_rate == pytest.approx(0.969697, abs=1e-6)


def test_restriction_and_fawkes_values(captured_header: tuple[str, bytes]):
    """``x-bili-restriction-bin`` 与 ``x-bili-fawkes-req-bin`` 关键字段与抓包一致。"""
    name, raw = captured_header
    if name == "x-bili-restriction-bin":
        restriction = Restriction()
        restriction.ParseFromString(raw)
        assert restriction.teenagers_age == 16
    elif name == "x-bili-fawkes-req-bin":
        fawkes = FawkesReq()
        fawkes.ParseFromString(raw)
        assert fawkes.appkey == "android64"
        assert fawkes.env == "prod"
        assert len(fawkes.session_id) == 8
    else:
        pytest.skip("仅校验 restriction / fawkes")
