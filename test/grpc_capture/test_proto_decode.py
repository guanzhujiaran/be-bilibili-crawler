# -*- coding: utf-8 -*-
"""基于本地抓包文件的 gRPC proto 解析测试。

用法：
    把本地抓包（Charles / mitmproxy / Wireshark 等）得到的 gRPC **请求**与**响应**
    原始报文，按「方法名」分目录放到 ``test/grpc_capture/`` 下，请求放进 ``request/``、
    响应放进 ``response/``，用仓库内 proto 生成的 protobuf 消息解析，验证本地 proto
    与线上报文一致。

目录约定（``<Method>`` 为 gRPC 方法名，如 ``DynDetail`` / ``GetColdStartDeferredData``）：

    test/grpc_capture/<Method>/
    ├── case.json        # 可选，仅用于消歧或直指消息类型
    ├── request/         # 放请求报文（可放多个）
    └── response/        # 放响应报文（可放多个）

报文格式自动识别：完整 gRPC DATA 帧（1 字节压缩标志 + 4 字节大端长度 + payload）
或纯 protobuf 裸字节；压缩时自动 gunzip。

方法名解析不到的目录（本地没有对应 proto）会自动 skip，并给出补齐提示。
"""

from pathlib import Path

import pytest
from google.protobuf.json_format import ParseError
from google.protobuf.message import DecodeError, Message
from pydantic import BaseModel, ConfigDict, ValidationError

from test.grpc_capture._loader import (
    extract_proto_bytes,
    list_capture_files,
    message_to_bytes,
    message_to_json,
    messages_equivalent,
    parse_grpc_message,
    scan_field_mismatches,
    scan_unknown_fields,
)
from test.grpc_capture._resolver import (
    AmbiguousMethodError,
    UnknownMethodError,
    resolve_message,
    resolve_method,
)

_CAPTURE_DIR = Path(__file__).resolve().parent / "captures"
_PARSED_DIR = _CAPTURE_DIR / "_parsed"
_CASE_FILE_NAME = "case.json"
_REQUEST_DIR_NAME = "request"
_RESPONSE_DIR_NAME = "response"


class CaptureCase(BaseModel):
    """``case.json`` 配置，全部可选。

    - ``service``：同名方法消歧用的服务全限定名；
    - ``request_type`` / ``response_type``：直接指定消息的 protobuf 全限定名，
      指定后不再按方法名解析。
    """

    model_config = ConfigDict(extra="forbid")

    service: str | None = None
    request_type: str | None = None
    response_type: str | None = None


def _iter_method_dirs() -> list[Path]:
    """枚举 ``test/grpc_capture`` 下的方法目录（忽略 ``_`` 开头的辅助目录）。"""
    if not _CAPTURE_DIR.is_dir():
        return []
    return sorted(
        path
        for path in _CAPTURE_DIR.iterdir()
        if path.is_dir() and not path.name.startswith("_") and path.name != "__pycache__"
    )


_METHOD_DIRS = _iter_method_dirs()
_METHOD_IDS = [path.name for path in _METHOD_DIRS]


def _load_case(method_dir: Path) -> CaptureCase:
    """读取方法目录下的 ``case.json``；不存在时返回全空配置。"""
    case_path = method_dir / _CASE_FILE_NAME
    if not case_path.is_file():
        return CaptureCase()
    return CaptureCase.model_validate_json(case_path.read_text(encoding="utf-8"))


def _resolve_request_cls(method_dir: Path, case: CaptureCase) -> type[Message]:
    if case.request_type:
        return resolve_message(case.request_type)
    return resolve_method(method_dir.name, case.service).request_cls


def _resolve_response_cls(method_dir: Path, case: CaptureCase) -> type[Message]:
    if case.response_type:
        return resolve_message(case.response_type)
    return resolve_method(method_dir.name, case.service).response_cls


def _collect_capture_items(sub_dir_name: str) -> list[tuple[str, Path]]:
    """收集所有待解析报文，返回 ``(方法目录名, 文件路径)`` 列表。"""
    items: list[tuple[str, Path]] = []
    for method_dir in _METHOD_DIRS:
        for capture_file in list_capture_files(method_dir / sub_dir_name):
            items.append((method_dir.name, capture_file))
    return items


_REQUEST_ITEMS = _collect_capture_items(_REQUEST_DIR_NAME)
_RESPONSE_ITEMS = _collect_capture_items(_RESPONSE_DIR_NAME)

# parametrize 参数：列表为空时给一个占位项，避免 "empty parameter set" 噪音
_REQUEST_PARAMS: list[tuple[str | None, Path | None]] = _REQUEST_ITEMS or [(None, None)]
_RESPONSE_PARAMS: list[tuple[str | None, Path | None]] = _RESPONSE_ITEMS or [(None, None)]
_REQUEST_IDS = [f"{name}/{path.name}" for name, path in _REQUEST_ITEMS] or ["no-request-capture"]
_RESPONSE_IDS = [f"{name}/{path.name}" for name, path in _RESPONSE_ITEMS] or ["no-response-capture"]


def _dump_parsed_json(method: str, kind: str, slug: str, json_text: str) -> None:
    """把解析结果落盘，方便肉眼核对字段（输出目录可随时删除）。"""
    target_dir = _PARSED_DIR / method
    target_dir.mkdir(parents=True, exist_ok=True)
    (target_dir / f"{kind}_{slug}.json").write_text(json_text, encoding="utf-8")


def _parse_and_roundtrip(raw: bytes, message_cls: type[Message]) -> Message:
    """解析报文并做往返序列化 + 字段覆盖校验，返回解析后的消息。"""
    message = parse_grpc_message(raw, message_cls)

    unknown = scan_unknown_fields(extract_proto_bytes(raw), message_cls.DESCRIPTOR)
    assert not unknown, (
        f"{message_cls.__name__} 未覆盖抓包中的字段 {sorted(set(unknown))}"
        "（本地 proto 缺字段，protobuf 会静默丢弃这些字节）"
    )

    mismatches = scan_field_mismatches(extract_proto_bytes(raw), message_cls.DESCRIPTOR)
    assert not mismatches, (
        f"{message_cls.__name__} 字段类型与线上不符 {mismatches[:5]}"
        "（编号对但类型错，protobuf 同样会静默丢字节）"
    )

    reserialized = message_to_bytes(message)
    assert reserialized, "序列化结果为空，解析出的消息可能不完整"
    again = message_cls()
    again.ParseFromString(reserialized)
    # 用确定性序列化比较：MessageToJson 文本会受 map 顺序影响而偶发不一致
    assert messages_equivalent(again, message), "往返序列化后消息不一致"
    return message


def _skip_unresolvable(method_dir: Path, case: CaptureCase) -> None:
    """方法解析不到时统一 skip（而非失败），并给出补齐提示。"""
    try:
        _resolve_request_cls(method_dir, case)
        _resolve_response_cls(method_dir, case)
    except (UnknownMethodError, AmbiguousMethodError) as exc:
        pytest.skip(f"{method_dir.name}：{exc}")


# ============================================================================
# 方法目录结构 / 拓扑检查
# ============================================================================


@pytest.mark.parametrize("method_dir", _METHOD_DIRS, ids=_METHOD_IDS)
def test_method_folder_layout(method_dir: Path):
    """方法目录结构正确，且请求/响应消息类型可解析。"""
    try:
        case = _load_case(method_dir)
    except ValidationError as exc:
        pytest.fail(f"{method_dir.name}/case.json 配置非法：{exc}")

    _skip_unresolvable(method_dir, case)

    missing_dirs = [
        name
        for name in (_REQUEST_DIR_NAME, _RESPONSE_DIR_NAME)
        if not (method_dir / name).is_dir()
    ]
    if missing_dirs:
        pytest.fail(
            f"{method_dir.name} 缺少子目录：{'、'.join(missing_dirs)}"
            "（应为 request/ 与 response/）"
        )

    if not list_capture_files(method_dir / _REQUEST_DIR_NAME) and not list_capture_files(
        method_dir / _RESPONSE_DIR_NAME
    ):
        pytest.skip(f"{method_dir.name} 尚未放入请求/响应报文")


# ============================================================================
# 请求报文解析
# ============================================================================


@pytest.mark.parametrize(
    ("method_name", "capture_file"),
    _REQUEST_PARAMS,
    ids=_REQUEST_IDS,
)
def test_request_proto_parses(method_name: str | None, capture_file: Path | None):
    """抓包得到的请求报文能用本地 proto 正确解析。"""
    if method_name is None or capture_file is None:
        pytest.skip("尚无请求报文")
    method_dir = _CAPTURE_DIR / method_name
    case = _load_case(method_dir)
    _skip_unresolvable(method_dir, case)

    message_cls = _resolve_request_cls(method_dir, case)
    raw = capture_file.read_bytes()
    try:
        message = _parse_and_roundtrip(raw, message_cls)
    except (DecodeError, ParseError) as exc:
        pytest.fail(
            f"{method_name}/{capture_file.name} 请求解析失败"
            f"（{message_cls.__name__}）：{exc}\n"
            f"原始字节 hex：{extract_proto_bytes(raw).hex()}"
        )

    json_text = message_to_json(message)
    assert json_text != "{}", f"{method_name}/{capture_file.name} 请求解析结果为空"
    _dump_parsed_json(method_name, "request", capture_file.stem, json_text)


# ============================================================================
# 响应报文解析
# ============================================================================


@pytest.mark.parametrize(
    ("method_name", "capture_file"),
    _RESPONSE_PARAMS,
    ids=_RESPONSE_IDS,
)
def test_response_proto_parses(method_name: str | None, capture_file: Path | None):
    """抓包得到的响应报文能用本地 proto 正确解析。"""
    if method_name is None or capture_file is None:
        pytest.skip("尚无响应报文")
    method_dir = _CAPTURE_DIR / method_name
    case = _load_case(method_dir)
    _skip_unresolvable(method_dir, case)

    message_cls = _resolve_response_cls(method_dir, case)
    raw = capture_file.read_bytes()
    try:
        message = _parse_and_roundtrip(raw, message_cls)
    except (DecodeError, ParseError) as exc:
        pytest.fail(
            f"{method_name}/{capture_file.name} 响应解析失败"
            f"（{message_cls.__name__}）：{exc}\n"
            f"原始字节 hex：{extract_proto_bytes(raw).hex()}"
        )

    json_text = message_to_json(message)
    assert json_text != "{}", f"{method_name}/{capture_file.name} 响应解析结果为空"
    _dump_parsed_json(method_name, "response", capture_file.stem, json_text)
