#!/usr/bin/env python3
"""Build a bili_grpc/device.json from a real FIRERPA/mitmproxy capture.

Reads ``capture/real_headers_sample.json`` (or any jsonl record) and extracts
the identity fields from the binary headers, so the Python client can replay
calls with a real device's buvid / fp / ticket / region tokens.
"""

from __future__ import annotations

import base64
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SAMPLE = ROOT / "capture" / "real_headers_sample.json"
OUT = ROOT.parent / "BiliReverseEnginnering" / "bili_grpc" / "device.json"


def _varint(data: bytes, pos: int) -> tuple[int, int]:
    value, shift = 0, 0
    while pos < len(data):
        byte = data[pos]
        pos += 1
        value |= (byte & 0x7F) << shift
        if byte < 0x80:
            return value, pos
        shift += 7
    raise ValueError("truncated varint")


def fields(raw: bytes) -> dict[int, object]:
    out: dict[int, object] = {}
    pos = 0
    while pos < len(raw):
        tag, pos = _varint(raw, pos)
        number, wire = tag >> 3, tag & 7
        if wire == 0:
            value, pos = _varint(raw, pos)
            out[number] = value
        elif wire == 2:
            size, pos = _varint(raw, pos)
            payload = raw[pos : pos + size]
            pos += size
            try:
                out[number] = payload.decode("utf-8")
            except UnicodeDecodeError:
                out[number] = payload.hex()
        else:
            break
    return out


def decode_bin(value: str) -> dict[int, object]:
    return fields(base64.b64decode(value + "=" * (-len(value) % 4)))


def main() -> int:
    headers = json.loads(SAMPLE.read_text(encoding="utf-8"))["headers"]
    device = decode_bin(headers["x-bili-device-bin"])
    metadata = decode_bin(headers["x-bili-metadata-bin"])
    fawkes = decode_bin(headers["x-bili-fawkes-req-bin"])
    locale = decode_bin(headers["x-bili-locale-bin"])

    config = {
        "buvid": str(device.get(3, "")),
        "guest_id": str(device.get(16, "")),
        "fp": str(device.get(14, "")),
        "fp_local": str(device.get(11, "")),
        "fp_remote": str(device.get(12, "")),
        "channel": str(metadata.get(5, "oppo")),
        "brand": str(device.get(8, "")),
        "model": str(device.get(9, "")),
        "osver": str(device.get(10, "")),
        "version_name": str(device.get(13, "9.19.0")),
        "build": int(device.get(2, 9190400)),
        "fawkes_appkey": str(fawkes.get(1, "android64")),
        "fawkes_env": str(fawkes.get(2, "prod")),
        "session_id": str(fawkes.get(3, "")),
        "ticket": headers.get("x-bili-ticket", ""),
        "recent_region": headers.get("x-bili-metadata-recent-region", ""),
        "ip_region": headers.get("x-bili-metadata-ip-region", "CN"),
        "timezone": str(locale.get(4, "Asia/Shanghai")),
        "utc_offset": str(locale.get(5, "+08:00")),
        "ua_override": headers.get("user-agent", ""),
    }
    OUT.write_text(json.dumps(config, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"wrote {OUT}")
    print(f"buvid={config['buvid']} channel={config['channel']} fp={str(config['fp'])[:16]}...")
    print(f"ticket={'yes' if config['ticket'] else 'no'} recent_region={'yes' if config['recent_region'] else 'no'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
