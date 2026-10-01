# -*- coding: utf-8 -*-
"""抓包 gRPC 报文的加载与解析工具。

gRPC over HTTP/2 的 DATA 帧载荷结构：

    +----------+-------------------+------------------+
    | 压缩标志  |  载荷长度(4字节,大端) |    protobuf 消息   |
    +----------+-------------------+------------------+
        1 字节

- 压缩标志 ``0x00``：未压缩；``0x01``：使用 ``grpc-encoding``（通常 gzip）压缩。
- 也兼容抓包工具已剥离帧头、直接保存 protobuf 裸字节的情况。
"""

import gzip
from dataclasses import dataclass
from pathlib import Path

from google.protobuf.json_format import MessageToJson
from google.protobuf.message import Message

# gRPC 帧头长度：1 字节压缩标志 + 4 字节大端长度
GRPC_FRAME_PREFIX_LEN = 5
# gzip 魔数，用于压缩标志不可靠时兜底识别
_GZIP_MAGIC = b"\x1f\x8b"


@dataclass(frozen=True, slots=True)
class GrpcFrame:
    """单个 gRPC 帧。"""

    compressed: bool
    payload: bytes


def split_grpc_frames(raw: bytes) -> list[GrpcFrame]:
    """把原始报文切分为 gRPC 帧列表。

    若报文不含合法帧头，则整体视为一个未压缩的裸 payload 帧。
    """
    frames: list[GrpcFrame] = []
    offset = 0
    while offset + GRPC_FRAME_PREFIX_LEN <= len(raw):
        flag = raw[offset]
        if flag not in (0, 1):
            break
        declared_len = int.from_bytes(raw[offset + 1:offset + GRPC_FRAME_PREFIX_LEN], "big")
        payload_start = offset + GRPC_FRAME_PREFIX_LEN
        payload_end = payload_start + declared_len
        if payload_end > len(raw):
            break
        frames.append(GrpcFrame(compressed=bool(flag), payload=raw[payload_start:payload_end]))
        offset = payload_end
    if not frames:
        frames.append(GrpcFrame(compressed=False, payload=raw))
    return frames


def _decode_frame_payload(frame: GrpcFrame) -> bytes:
    """按压缩标志（并用 gzip 魔数兜底）解压帧载荷。"""
    if frame.compressed or frame.payload[:2] == _GZIP_MAGIC:
        return gzip.decompress(frame.payload)
    return frame.payload


def extract_proto_bytes(raw: bytes) -> bytes:
    """从原始报文中提取拼接后的 protobuf 字节。"""
    return b"".join(_decode_frame_payload(frame) for frame in split_grpc_frames(raw))


def parse_grpc_message(raw: bytes, message_cls: type[Message]) -> Message:
    """把抓包原始报文解析成 protobuf 消息对象。

    :param raw: 抓包得到的请求/响应原始字节
    :param message_cls: 目标 protobuf 消息类型
    :raise google.protobuf.message.DecodeError: 解析失败时抛出
    """
    message = message_cls()
    message.ParseFromString(extract_proto_bytes(raw))
    return message


def read_capture_file(path: Path) -> bytes | None:
    """读取抓包文件；文件不存在或为空时返回 ``None``（测试据此 skip）。"""
    if not path.is_file():
        return None
    data = path.read_bytes()
    return data or None


def list_capture_files(folder: Path) -> list[Path]:
    """列出抓包目录下的有效报文文件（忽略隐藏文件与空文件）。"""
    if not folder.is_dir():
        return []
    return sorted(
        path
        for path in folder.iterdir()
        if path.is_file() and not path.name.startswith(".") and path.stat().st_size > 0
    )


def message_to_json(message: Message) -> str:
    """把 protobuf 消息序列化为可读 JSON 字符串。"""
    return MessageToJson(message, indent=2, ensure_ascii=False)


def message_to_bytes(message: Message) -> bytes:
    """把 protobuf 消息序列化回字节，用于往返校验。"""
    return message.SerializeToString()


def messages_equivalent(left: Message, right: Message) -> bool:
    """比较两个消息语义是否一致。

    不能直接比较 ``MessageToJson`` 文本：``map`` 字段的序列化顺序在 Python 实现里
    随进程 hash 种子变化，同一份数据两次输出可能顺序不同（曾导致用例偶发失败）。
    这里用确定性序列化作比较（map 会按键排序），结果稳定。
    """
    return left.SerializeToString(deterministic=True) == right.SerializeToString(
        deterministic=True
    )


def _read_varint(buf: bytes, offset: int) -> tuple[int, int]:
    """读取一个 varint，返回 ``(值, 新偏移)``。"""
    value = 0
    shift = 0
    while True:
        byte = buf[offset]
        offset += 1
        value |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return value, offset
        shift += 7


def scan_unknown_fields(buf: bytes, descriptor) -> list[str]:
    """按 wire-format 扫描字节，列出本地 descriptor 未定义的字段路径。

    protobuf 解析时会静默丢弃不认识的字段（``upb`` 实现亦不暴露
    ``UnknownFields()``），因此仅靠「能解析成功」无法发现本地 proto 缺字段。
    这里按 tag 逐层比对 descriptor，缺失字段以 ``11.8`` 这样的路径形式返回。

    :param buf: 已剥离 gRPC 帧头并解压后的 protobuf 字节
    :param descriptor: 目标消息的 ``Descriptor``
    """
    unknown: list[str] = []
    offset = 0
    while offset < len(buf):
        try:
            key, offset = _read_varint(buf, offset)
        except IndexError:
            break
        field_number, wire_type = key >> 3, key & 0x07
        field = descriptor.fields_by_number.get(field_number)
        if wire_type == 0:
            _, offset = _read_varint(buf, offset)
            continue
        if wire_type == 1:
            offset += 8
            continue
        if wire_type == 5:
            offset += 4
            continue
        if wire_type != 2:
            break
        size, offset = _read_varint(buf, offset)
        chunk = buf[offset:offset + size]
        offset += size
        if field is None:
            unknown.append(str(field_number))
            continue
        if field.message_type is not None:
            nested = scan_unknown_fields(chunk, field.message_type)
            unknown.extend(f"{field_number}.{path}" for path in nested)
    return unknown


def scan_field_mismatches(buf: bytes, descriptor) -> list[str]:
    """按 wire-format 扫描字节，列出**本地字段类型与线上不符**的位置。

    ``scan_unknown_fields`` 只能发现"本地没定义这个编号"；而**编号对、类型错**
    （例如把线上是消息的字段定义成 ``string``、把 ``int32`` 定义成 ``bytes``）
    时 protobuf 同样会跳过这些字节，表现为静默丢数据。这里按 descriptor 的期望
    类型与 wire type 交叉比对，把这类"看不出来"的错误一并暴露：

    - 本地 ``string``：内容必须是合法 UTF-8；
    - 本地 ``int32/int64/bool/enum``：wire type 必须是 varint(0)；
    - 本地 ``message``：会递归下钻比对。

    :param buf: 已剥离 gRPC 帧头并解压后的 protobuf 字节
    :param descriptor: 目标消息的 ``Descriptor``
    """
    mismatches: list[str] = []

    def walk(chunk: bytes, desc, path: str) -> None:
        offset = 0
        while offset < len(chunk):
            try:
                key, offset = _read_varint(chunk, offset)
            except IndexError:
                return
            field_number, wire_type = key >> 3, key & 0x07
            field = desc.fields_by_number.get(field_number) if desc else None
            if wire_type == 0:
                _, offset = _read_varint(chunk, offset)
                continue
            if wire_type == 1:
                offset += 8
                continue
            if wire_type == 5:
                offset += 4
                continue
            if wire_type != 2:
                return
            size, offset = _read_varint(chunk, offset)
            piece = chunk[offset:offset + size]
            offset += size
            if field is None:
                continue
            here = f"{path}.{field_number}" if path else str(field_number)
            if field.message_type is not None:
                walk(piece, field.message_type, here)
            elif field.type == 9:  # TYPE_STRING
                try:
                    piece.decode()
                except UnicodeDecodeError:
                    mismatches.append(
                        f"{here}({desc.name}.{field.name}) 本地 string，线上非 UTF-8")
            elif field.type in (5, 8, 13) and wire_type != 0:  # int32/int64/bool
                mismatches.append(
                    f"{here}({desc.name}.{field.name}) 本地 {field.type}，线上 wire={wire_type}")

    walk(buf, descriptor, "")
    return mismatches
