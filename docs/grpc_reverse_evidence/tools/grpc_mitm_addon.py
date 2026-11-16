"""mitmproxy addon: dump bilibili gRPC request headers verbatim.

Run with:
    mitmdump -p 18081 -s grpc_mitm_addon.py -q

Every request whose authority matches GRPC_HOST_SUFFIX is appended to
``grpc_headers.jsonl`` (one JSON object per call), keeping header order and
duplicates plus the HTTP/2 response trailers.  Binary ``-bin`` header values
are reported both as raw text and base64 so they can be compared with the
APK's padded base64 form.

用于核对 APK 实际发出的 x-bili-* 头是否与静态还原一致。
"""

from __future__ import annotations

import base64
import json
import os
import time

from mitmproxy import http

OUT = os.environ.get(
    "GRPC_CAPTURE_OUT",
    "/home/minato/BilibiliExplosion/be-bilibili-crawler/BiliReverseEnginnering/work/capture/grpc_headers.jsonl",
)
HOST_SUFFIX = os.environ.get("GRPC_HOST_SUFFIX", "biliapi.net")


def _value(raw: object) -> str:
    if isinstance(raw, bytes):
        return "b64:" + base64.b64encode(raw).decode("ascii")
    return str(raw)


def _fields(message: object) -> list[list[str]]:
    """Raw (name, value) pairs in wire order, no normalization."""
    if message is None:
        return []
    fields = getattr(message, "headers", None)
    if fields is None:
        return []
    return [[_value(name), _value(value)] for name, value in fields.fields]


def _trailers(flow: http.HTTPFlow) -> list[list[str]]:
    response = flow.response
    if response is None:
        return []
    trailers = getattr(response, "trailers", None)
    if not trailers:
        return []
    return [[_value(name), _value(value)] for name, value in trailers.fields]


def response(flow: http.HTTPFlow) -> None:
    request = flow.request
    if HOST_SUFFIX not in request.pretty_host:
        return
    record = {
        "ts": round(time.time(), 3),
        "http_version": request.http_version,
        "method": request.method,
        "scheme": request.scheme,
        "authority": request.pretty_host,
        "path": request.path,
        "request_headers": _fields(request),
        "request_body_len": len(request.raw_content or b""),
        "status_code": flow.response.status_code if flow.response else None,
        "response_headers": _fields(flow.response),
        "response_trailers": _trailers(flow),
        "response_body_len": len(flow.response.raw_content or b"") if flow.response else 0,
    }
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, ensure_ascii=False) + "\n")
