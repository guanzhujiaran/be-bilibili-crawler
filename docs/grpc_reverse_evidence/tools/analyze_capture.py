#!/usr/bin/env python3
"""Decode the FIRERPA/mitmproxy gRPC capture into a human-readable comparison.

Reads ``capture/grpc_headers.jsonl`` (written by grpc_mitm_addon.py), prints
every observed HTTP/2 header, base64-decodes the ``x-bili-*-bin`` values and
dumps their protobuf field numbers, then writes a markdown report.
"""

from __future__ import annotations

import base64
import json
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CAPTURE = ROOT / "capture" / "grpc_headers.jsonl"
REPORT = ROOT / "grpc_capture_raw_dump.md"


def _text(value: str) -> str:
    if not value.startswith("b64:"):
        return value
    return base64.b64decode(value[4:]).decode("latin-1")


def _varint(data: bytes, pos: int) -> tuple[int, int]:
    value = 0
    shift = 0
    while pos < len(data):
        byte = data[pos]
        pos += 1
        value |= (byte & 0x7F) << shift
        if byte < 0x80:
            return value, pos
        shift += 7
    raise ValueError("truncated varint")


def _decode_wire(data: bytes) -> list[tuple[int, int, object]]:
    out: list[tuple[int, int, object]] = []
    pos = 0
    while pos < len(data):
        tag, pos = _varint(data, pos)
        number, wire = tag >> 3, tag & 7
        if wire == 0:
            value, pos = _varint(data, pos)
            out.append((number, wire, value))
        elif wire == 2:
            size, pos = _varint(data, pos)
            payload = data[pos : pos + size]
            pos += size
            printable = all(32 <= byte < 127 or byte >= 0xC0 for byte in payload)
            shown: object = payload.decode("utf-8", "replace") if printable else payload.hex()
            out.append((number, wire, shown))
        else:
            break
    return out


def load() -> list[dict]:
    return [json.loads(line) for line in CAPTURE.read_text(encoding="utf-8").splitlines()]


def headers(row: dict, key: str) -> list[tuple[str, str]]:
    return [(_text(name), _text(value)) for name, value in row[key]]


def main() -> int:
    rows = load()
    key_counts: Counter[str] = Counter()
    values: dict[str, Counter[str]] = defaultdict(Counter)
    for row in rows:
        for name, value in headers(row, "request_headers"):
            key_counts[name] += 1
            values[name][value] += 1

    lines = [
        "# 真机 gRPC 抓包核对（FIRERPA + mitmproxy）",
        "",
        f"抓包调用数：**{len(rows)}**，接口：",
        "",
    ]
    for path, count in Counter(row["path"] for row in rows).most_common():
        lines.append(f"- `{path}` × {count}")
    lines += [
        "",
        "## 请求头全集",
        "",
        "| header | 出现次数 | 真机实际值（截取） |",
        "| --- | --- | --- |",
    ]
    for name, count in key_counts.most_common():
        sample = values[name].most_common(1)[0][0]
        lines.append(f"| `{name}` | {count}/{len(rows)} | `{sample[:120]}` |")

    lines += ["", "## `-bin` 头的真实 protobuf 字段", ""]
    sample = rows[1]
    for name, value in headers(sample, "request_headers"):
        if not name.endswith("-bin"):
            continue
        raw = base64.b64decode(value + "=" * (-len(value) % 4))
        fields = _decode_wire(raw)
        lines += [f"### `{name}`", "", f"base64 `{value[:100]}`（{len(raw)} bytes）", "", "| # | wire | value |", "| --- | --- | --- |"]
        for number, wire, shown in fields:
            lines.append(f"| {number} | {wire} | `{shown}` |")
        lines.append("")

    lines += ["", "## 响应 trailer", ""]
    for name, value in headers(sample, "response_trailers"):
        lines.append(f"- `{name}`: `{value[:100]}`")

    REPORT.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"report -> {REPORT}")
    print(f"calls={len(rows)} distinct headers={len(key_counts)}")
    for name, count in key_counts.most_common():
        print(f"  {count:3d}  {name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
