"""真机 FIRERPA + mitmproxy 抓包头的回归校验。

用仓库自己的 ``bilibili.metadata.*_pb2`` 解码 evidence 里的二进制头，
并静态断言生产 ``make_metadata`` 已包含真机实测的关键修正项。
"""

from __future__ import annotations

import base64
import json
import re
from pathlib import Path

from bilibili.metadata.device.device_pb2 import Device
from bilibili.metadata.fawkes.fawkes_pb2 import FawkesReq
from bilibili.metadata.locale.locale_pb2 import Locale
from bilibili.metadata.metadata_pb2 import Metadata
from bilibili.metadata.network.network_pb2 import Network, NetworkType
from bilibili.metadata.restriction.restriction_pb2 import Restriction

ROOT = Path(__file__).resolve().parents[2]  # test/grpc_capture/ -> 项目根
EVIDENCE = ROOT / "docs" / "grpc_reverse_evidence"
SAMPLE = EVIDENCE / "capture" / "real_headers_sample.json"
MAKE_METADATA = ROOT / "Utils" / "GrpcUtils" / "metadata" / "makeMetaData.py"

# 真机 20 个头里，8 个由 gRPC C-core 自动产生，12 个是业务头。
AUTO_HEADERS = frozenset(
    {
        "accept",
        "accept-encoding",
        "content-type",
        "grpc-accept-encoding",
        "grpc-encoding",
        "grpc-timeout",
        "te",
        "user-agent",
    }
)
BUSINESS_HEADERS = frozenset(
    {
        "buvid",
        "x-bili-device-bin",
        "x-bili-fawkes-req-bin",
        "x-bili-locale-bin",
        "x-bili-metadata-bin",
        "x-bili-metadata-ip-region",
        "x-bili-metadata-recent-region",
        "x-bili-moss-engine-type",
        "x-bili-network-bin",
        "x-bili-restriction-bin",
        "x-bili-ticket",
        "x-bili-trace-id",
    }
)


def _sample() -> tuple[str, dict]:
    payload = json.loads(SAMPLE.read_text(encoding="utf-8"))
    return payload["path"], payload["headers"]


def _decode(name: str, headers: dict) -> bytes:
    value = headers[name]
    # mitmproxy 记录的 wire 值是 gRPC 规范的无 padding base64
    return base64.b64decode(value + "=" * (-len(value) % 4))


def test_evidence_files_exist() -> None:
    assert SAMPLE.exists(), SAMPLE
    assert MAKE_METADATA.exists(), MAKE_METADATA
    assert (EVIDENCE / "grpc_capture_verification.md").exists()


def test_real_capture_header_set() -> None:
    path, headers = _sample()
    assert path.startswith("/bilibili.")
    assert set(headers) == AUTO_HEADERS | BUSINESS_HEADERS
    assert headers["x-bili-moss-engine-type"] == "1"
    assert headers["grpc-encoding"] == "gzip"
    assert headers["grpc-timeout"] == "7990m"
    assert re.fullmatch(r"[0-9a-f]{32}:[0-9a-f]{16}:0:0", headers["x-bili-trace-id"])
    assert headers["user-agent"].startswith("grpc-c++/1.66.2 Dalvik/2.1.0")
    assert " os/android model/" in headers["user-agent"]
    assert " channel/" in headers["user-agent"]


def test_real_capture_binary_headers_decode_with_project_protos() -> None:
    _, headers = _sample()

    device = Device.FromString(_decode("x-bili-device-bin", headers))
    assert device.app_id == 1
    assert device.build == 9190400
    assert device.buvid == headers["buvid"]
    assert device.channel == "oppo"
    assert device.fp == device.fp_local == device.fp_remote
    assert device.version_name == "9.19.0"
    assert device.guest_id

    metadata = Metadata.FromString(_decode("x-bili-metadata-bin", headers))
    assert metadata.build == 9190400
    assert metadata.channel == "oppo"
    assert metadata.buvid == headers["buvid"]
    assert not metadata.access_key  # 游客态

    fawkes = FawkesReq.FromString(_decode("x-bili-fawkes-req-bin", headers))
    assert fawkes.appkey == "android64"
    assert fawkes.env == "prod"
    assert re.fullmatch(r"[0-9a-f]{8}", fawkes.session_id)

    locale = Locale.FromString(_decode("x-bili-locale-bin", headers))
    assert locale.timezone == "Asia/Shanghai"
    assert locale.utc_offset == "+08:00"
    assert locale.c_locale.language == "zh"

    network = Network.FromString(_decode("x-bili-network-bin", headers))
    assert network.type == NetworkType.WIFI

    restriction = Restriction.FromString(_decode("x-bili-restriction-bin", headers))
    assert restriction.teenagers_age == 16


def test_production_metadata_matches_evidence() -> None:
    """静态断言生产代码已吸收 evidence 的修正（避免再次回归）。"""
    source = MAKE_METADATA.read_text(encoding="utf-8")
    assert '("x-bili-trace-id"' in source
    assert "grpc-c++/1.66.2" in source
    assert "7990m" in source
    assert "success_rate=-1.0" in source
    assert 'version_name="9.19.0"' in source
    assert "build=9190400" in source
    assert "region_headers()" in source


def test_region_is_per_device() -> None:
    """recent-region 只能来自设备自己领取的 JWT，不跨设备共享全局样本。"""
    fallback = ROOT / "Utils" / "GrpcUtils" / "metadata" / "region_fallback.json"
    assert json.loads(fallback.read_text(encoding="utf-8")) == {
        "x-bili-metadata-ip-region": "CN"
    }

    source = (ROOT / "Utils" / "GrpcUtils" / "metadata" / "device_env.py").read_text(
        encoding="utf-8"
    )
    region_headers_body = source.split("def region_headers", 1)[1].split(
        "async def fetch_region", 1
    )[0]
    assert "self.recent_region_valid()" in region_headers_body
    assert "self.recent_region" in region_headers_body
    assert "DEFAULT_RECENT_REGION" not in source
    # 领取后写回设备自身，随设备池持久化
    assert "self.learn_region(" in source


def test_ticket_and_region_share_one_lifecycle() -> None:
    """ticket 与 region 走同一套领取/缓存/过期刷新，不各搞一套。"""
    source = MAKE_METADATA.read_text(encoding="utf-8")
    assert "async def ensure_credentials" in source
    # GetTicket 响应头顺带学 region，两个凭证一次请求一起刷新
    assert "device.learn_region(" in source
    assert "await ensure_credentials(" in source
    assert "bili_ticket_resp" not in source

    device_source = (ROOT / "Utils" / "GrpcUtils" / "metadata" / "device_env.py").read_text(
        encoding="utf-8"
    )
    assert "ticket: str | None = None" in device_source
    assert "def ticket_valid" in device_source
