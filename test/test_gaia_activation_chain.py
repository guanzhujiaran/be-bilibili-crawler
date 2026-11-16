"""gaia Web 激活链路：证据边界内的 schema / 解析 / 跳过行为单测。

不联网、不执行 WASM。只锁定：

- ExGetAxe 响应解析；
- ExClimbCongLing 信封字段与实测 schema 一致；
- 非法信封被拒绝；
- 没有加密器时函数返回 None（不伪造请求体）；
- 证据文件里的长度 / 类型常量与代码常量一致（防止文档与实现漂移）。
"""

import base64
import json
import sys
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from Service.GrpcModule.Grpc.Bapi import BiliApiBase
from Service.GrpcModule.Grpc.Bapi.GaiaActivation import (
    ENCODED_AES_KEY_BYTES,
    ENCODE_TYPE,
    PAYLOAD_TYPE,
    GaiaCongLingEnvelope,
    build_congling_body,
    parse_axe_response,
    validate_envelope,
)

_SUMMARY = _project_root / "docs" / "gaia_evidence" / "activation_summary.json"


def test_parse_axe_response() -> None:
    parsed = parse_axe_response(
        {
            "code": 0,
            "data": {
                "version": "v1",
                "public_key": "-----BEGIN rsa public key-----\nMII...\n-----END rsa public key-----",
                "deadline": 2524608000,
            },
        }
    )
    assert parsed is not None
    assert parsed.version == "v1"
    assert parsed.deadline == 2524608000
    assert "BEGIN rsa public key" in parsed.public_key


def test_parse_axe_response_rejects_malformed() -> None:
    assert parse_axe_response(None) is None
    assert parse_axe_response({"data": {}}) is None
    assert parse_axe_response({"data": {"public_key": "not-a-key"}}) is None


def test_build_congling_body_matches_captured_schema() -> None:
    envelope = GaiaCongLingEnvelope(
        encoded_aes_key="a2V5", encrypt_payload="ZGF0YQ==", ts=1700000000123
    )
    body = build_congling_body(envelope)
    assert set(body) == {"header", "encrypt_payload"}
    assert set(body["header"]) == {
        "encode_type",
        "payload_type",
        "encoded_aes_key",
        "ts",
        "encoded_version",
    }
    assert body["header"]["encode_type"] == ENCODE_TYPE
    assert body["header"]["payload_type"] == PAYLOAD_TYPE
    assert body["header"]["encoded_version"] == "v1"
    assert body["header"]["ts"] == 1700000000123
    assert body["encrypt_payload"] == "ZGF0YQ=="


def test_validate_envelope_requires_rsa_4096_sized_key() -> None:
    key = base64.b64encode(b"k" * ENCODED_AES_KEY_BYTES).decode()
    payload = base64.b64encode(b"p" * 64).decode()
    assert validate_envelope(GaiaCongLingEnvelope(key, payload))

    short_key = base64.b64encode(b"k" * 256).decode()
    assert not validate_envelope(GaiaCongLingEnvelope(short_key, payload))
    assert not validate_envelope(
        GaiaCongLingEnvelope(key, payload, encode_type=1)
    )


async def test_congling_without_encryptor_returns_none(monkeypatch) -> None:
    async def _must_not_call(**kwargs):
        raise AssertionError("没有加密信封时不应发起 ExClimbCongLing 请求")

    monkeypatch.setattr(
        BiliApiBase.request_with_proxy_internal, "request_with_proxy", _must_not_call
    )
    assert await BiliApiBase.gaia_gateway_ExClimbCongling() is None


def test_evidence_constants_match_code() -> None:
    summary = json.loads(_SUMMARY.read_text())
    envelope = next(
        step for step in summary["steps"] if step["name"] == "ExClimbCongLing"
    )["envelope"]
    assert envelope["encoded_aes_key_raw_bytes"] == ENCODED_AES_KEY_BYTES
    assert envelope["encode_type"] == ENCODE_TYPE
    assert envelope["payload_type"] == PAYLOAD_TYPE
    assert envelope["header_keys"] == [
        "encode_type",
        "payload_type",
        "encoded_aes_key",
        "ts",
        "encoded_version",
    ]
    assert summary["wasm_network_requests"] == 0
    # 加密入口确实在 WASM 内：这是「不能手搓信封」的硬证据
    assert summary["inline_wasm"]["magic"] == "0061736d01000000"
    assert "encrypt_data" in summary["inline_wasm"]["exports"]
    assert summary["inline_wasm"]["bytes"] > 200_000
