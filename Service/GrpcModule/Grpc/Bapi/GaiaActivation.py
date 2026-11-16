# -*- coding: utf-8 -*-
"""gaia 激活链路（Web 侧）的**证据边界内**实现。

链路（浏览器实测，2026-11，见 docs/gaia_evidence/）：

1. ``POST /x/internal/gaia-gateway/ExClimbWuzhi``：上报环境指纹 payload，
   返回 ``{"code":0}``；
2. ``GET  /x/internal/gaia-gateway/ExGetAxe``：返回 ``version=v1`` +
   RSA-4096 公钥 + ``deadline``；
3. ``POST /x/internal/gaia-gateway/ExClimbCongLing``：请求体是
   ``{"header": {"encode_type": 2, "payload_type": 4,
   "encoded_aes_key": "<base64>", "ts": <ms>, "encoded_version": "v1"},
   "encrypt_payload": "<base64>"}``。

**边界**：第 3 步的 ``encoded_aes_key`` / ``encrypt_payload`` 由
``bili-sc-sdk.umd.js`` 内联的 WASM（``minntaki-wasm-sdk``）生成；该 WASM 以
base64 内嵌在 JS 里，不在网络上单独请求 ``.wasm``，仓库中也没有可执行的导出。
因此本模块只做两件**可证实**的事：

- 解析 ExGetAxe 响应；
- 在拿到「已加密结果」时，按实测 schema 组装并发送 ExClimbCongLing。

没有加密器时函数返回 ``None`` 并记录原因，**不伪造** ``encoded_aes_key`` /
``encrypt_payload`` —— 伪造只会让服务端把会话标记得更可疑。
"""

from __future__ import annotations

import base64
import time
from dataclasses import dataclass, field

from log.base_log import bapi_log

#: 实测：RSA-4096 OAEP 输出 = 512 字节，base64 后 684 字符
ENCODED_AES_KEY_BYTES = 512
#: 实测：encode_type=2（AES-GCM 系）、payload_type=4（环境信息）
ENCODE_TYPE = 2
PAYLOAD_TYPE = 4


@dataclass(frozen=True, slots=True)
class GaiaAxeKey:
    version: str
    public_key: str
    deadline: int


@dataclass(frozen=True, slots=True)
class GaiaCongLingEnvelope:
    """ExClimbCongLing 的加密结果（必须由 WASM 生成，不能手搓）。"""

    encoded_aes_key: str
    encrypt_payload: str
    version: str = "v1"
    encode_type: int = ENCODE_TYPE
    payload_type: int = PAYLOAD_TYPE
    ts: int = field(default_factory=lambda: int(time.time() * 1000))


def parse_axe_response(payload: dict | None) -> GaiaAxeKey | None:
    """解析 ExGetAxe 响应；字段缺失 / 结构不符时返回 None。"""
    data = (payload or {}).get("data")
    if not isinstance(data, dict):
        return None
    public_key = data.get("public_key")
    if not isinstance(public_key, str) or "BEGIN rsa public key" not in public_key:
        return None
    return GaiaAxeKey(
        version=str(data.get("version") or "v1"),
        public_key=public_key,
        deadline=int(data.get("deadline") or 0),
    )


def build_congling_body(envelope: GaiaCongLingEnvelope | None) -> dict | None:
    """按实测 schema 组装请求体；``None`` 表示当前没有可用加密器。"""
    if envelope is None:
        return None
    return {
        "header": {
            "encode_type": envelope.encode_type,
            "payload_type": envelope.payload_type,
            "encoded_aes_key": envelope.encoded_aes_key,
            "ts": envelope.ts,
            "encoded_version": envelope.version,
        },
        "encrypt_payload": envelope.encrypt_payload,
    }


def log_missing_encryptor() -> None:
    bapi_log.debug(
        "gaia ExClimbCongLing 跳过：未接入 minntaki-wasm-sdk 加密器"
        "（WASM 内嵌于 bili-sc-sdk.umd.js，仓库无可执行导出；"
        "见 docs/gaia_evidence/README.md）。不伪造 encoded_aes_key / encrypt_payload"
    )


def validate_envelope(envelope: GaiaCongLingEnvelope) -> bool:
    """结构自检：避免把明显不合法的加密结果发到线上。"""
    try:
        key_raw = base64.b64decode(envelope.encoded_aes_key, validate=True)
        payload_raw = base64.b64decode(envelope.encrypt_payload, validate=True)
    except Exception:  # noqa: BLE001
        return False
    return (
        len(key_raw) == ENCODED_AES_KEY_BYTES
        and len(payload_raw) > 16
        and envelope.encode_type == ENCODE_TYPE
        and envelope.payload_type == PAYLOAD_TYPE
    )
