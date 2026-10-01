# -*- coding: utf-8 -*-
"""接口探针：直连真实请求 + 本地 proto 完整性校验（按需运行，不参与 pytest 收集）。

和 `test/grpc_capture/` 的关系：

- 那里的用例是**静态快照**，只校验"当初抓到的那份字节"是否还能被本地 proto 解析；
- 本探针负责**采集与体检**：真实发起请求（含 metadata → buvid → ticket → region →
  -352 极验换 vtoken），把服务端响应喂给本地 pb2，检查

  1. 能否解析（不抛 DecodeError）；
  2. **未知字段**：本地位号没定义（漏字段）；
  3. **类型错配**：位号对但类型错（例如把线上的 message 写成 string）——
     protobuf 同样会静默丢字节，只用 1/2 看不出来；
  4. 往返序列化是否等价。

  三者任一不为空即说明本地 proto 与线上已经不一致。

常见用途：

    # 线上字段变了 / 改了 proto 后体检（默认跑全部 gRPC 用例）
    uv run python tools/grpc_probe.py

    # 只跑某几个接口，并把真实请求/响应存成测试素材
    uv run python tools/grpc_probe.py DynSpace DynDetail --save

    # 附带 HTTP Bapi 读接口烟测（写接口如 article/creative/* 一律不碰）
    uv run python tools/grpc_probe.py --http

    # 走 CONFIG 配置的代理而不是直连（默认直连）
    uv run python tools/grpc_probe.py --proxy

注意：需要外网；接口会触发风控（-352），脚本会自动走极验换 token 后重试。
本脚本会真实写库？不会。除 `--save` 写素材文件外无任何副作用。
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import gzip
import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PROTO_ROOT = ROOT / "Service" / "GrpcModule" / "Grpc" / "GrpcProto"
for _p in (str(ROOT), str(PROTO_ROOT)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from google.protobuf.message import Message  # noqa: E402

from bilibili.app.archive.middleware.v1.preload_pb2 import PlayerArgs  # noqa: E402
from bilibili.app.dynamic.v2 import dynamic_pb2, opus_pb2  # noqa: E402
from Service.GrpcModule.Grpc.Bapi.BapiUtils import appsign, gen_trace_id  # noqa: E402
from Utils.GrpcUtils.UserAgentParser import UserAgentParser  # noqa: E402
from Utils.GrpcUtils.metadata.device_env import DIRECT  # noqa: E402
from Utils.GrpcUtils.metadata.makeMetaData import make_metadata  # noqa: E402
from Utils.代理.SealedRequests import my_async_httpx  # noqa: E402
from test.grpc_capture._loader import (  # noqa: E402
    scan_field_mismatches,
    scan_unknown_fields,
)

BASE = "https://grpc.biliapi.net"
GAIA_REGISTER = "https://api.bilibili.com/x/gaia-vgate/v1/register"
GAIA_VALIDATE = "https://api.bilibili.com/x/gaia-vgate/v1/validate"
CAPTURE_DIR = ROOT / "test" / "grpc_capture" / "captures"

# 探针用的真实业务 id：取自已抓取的 OpusDetail 响应
DYN_ID = "1251948609230864438"
DYN_OID = 410098971   # 该动态的稿件 id（reply/main 用）
UID = 8047632         # 该动态的 up mid（DynSpace 用）

P = "PROBE"
RESULTS: list[tuple[str, str, int]] = []


def _dyn_detail_req() -> Message:
    return dynamic_pb2.DynDetailReq(
        dynamic_id=DYN_ID,
        player_args=PlayerArgs(qn=112, fnval=17360, voice_balance=1),
        share_id="dt.dt-detail.0.0.pv",
        share_mode=3,
        local_time=8,
        config=dynamic_pb2.Config(),
    )


def _dyn_space_req() -> Message:
    req = dynamic_pb2.DynSpaceReq(host_uid=UID, history_offset="", page=1, local_time=8)
    setattr(req, "from", "space")  # `from` 是 Python 关键字，只能 setattr
    return req


def _gaia_headers(ua: str, buvid: str, ticket: str) -> tuple:
    return (
        ("native_api_from", "h5"),
        ("cookie", f"Buvid={buvid}"),
        ("buvid", buvid),
        ("accept", "application/json, text/plain, */*"),
        ("referer", "https://www.bilibili.com/h5/risk-captcha"),
        ("env", "prod"),
        ("app-key", "android"),
        ("user-agent", ua),
        ("x-bili-trace-id", gen_trace_id()),
        ("x-bili-gaia-vtoken", ""),
        ("x-bili-ticket", ticket),
        ("content-type", "application/x-www-form-urlencoded; charset=utf-8"),
    )


# ---------------------------------------------------------------------------
# 用例注册表：tag -> () -> (gRPC path, 请求消息, 响应类)
# ---------------------------------------------------------------------------
def _opus_detail_req() -> Message:
    return opus_pb2.OpusDetailReq(
        oid=int(DYN_ID), share_id="dt.opus-detail.0.0", share_mode=3, local_time=8,
        player_args=PlayerArgs(qn=32, fnval=404, voice_balance=1))


GRPC_CASES: dict[str, tuple] = {
    "OpusDetail": ("bilibili.app.dynamic.v2.Opus/OpusDetail", _opus_detail_req,
                   opus_pb2.OpusDetailResp),
    "DynDetail": ("bilibili.app.dynamic.v2.Dynamic/DynDetail", _dyn_detail_req,
                  dynamic_pb2.DynDetailReply),
    "DynDetails": ("bilibili.app.dynamic.v2.Dynamic/DynDetails",
                   lambda: dynamic_pb2.DynDetailsReq(
                       dynamic_ids=json.dumps({"dyn_ids": [int(DYN_ID)]})),
                   dynamic_pb2.DynDetailsReply),
    "DynSpace": ("bilibili.app.dynamic.v2.Dynamic/DynSpace", _dyn_space_req,
                 dynamic_pb2.DynSpaceRsp),
}

# ---------------------------------------------------------------------------
# HTTP Bapi 读接口烟测（不碰任何写接口）
# ---------------------------------------------------------------------------
HTTP_CASES: list[tuple[str, str, str, dict]] = [
    # tag, url, method, params/额外头
    ("version", "https://app.bilibili.com/x/v2/version", "GET", {"appsign": {"mobi_app": "android"}}),
    ("abserver", "https://app.bilibili.com/x/resource/abtest/abserver", "GET", {"appsign_bili": True}),
    ("webAreas", "https://api.live.bilibili.com/xlive/web-interface/v1/index/getWebAreaList",
     "GET", {"params": {"source_id": 2}}),
    ("replyMain", "https://api.bilibili.com/x/v2/reply/main", "GET",
     {"appsign": {"oid": DYN_OID, "type": 1, "mode": 3}}),
    ("reserveInfo", "https://api.bilibili.com/x/activity/up/reserve/relation/info", "GET",
     {"appsign": {"sid": 1}}),
]


class Probe:
    """复用一份 metadata（含 vtoken），逐个接口发真实请求并校验响应。"""

    def __init__(self, use_proxy: bool = False) -> None:
        self.proxy = None if use_proxy else DIRECT
        self.md: tuple = ()
        self.basic = None
        self.vtoken = ""

    async def init(self) -> None:
        self.md, _, self.basic = await make_metadata("", proxy=self.proxy)
        device = self.basic.device
        print(f"{P} 设备 {device.brand} {device.device_model} | buvid {self.basic.buvid} "
              f"| ticket {'有' if self.basic.ticket else '无'} "
              f"| 出口 {'代理' if self.proxy is None else '直连'}")

    def enc(self, extra: tuple = ()) -> tuple:
        return tuple(
            (k, base64.b64encode(v).decode().strip("=") if isinstance(v, bytes) else str(v))
            for k, v in tuple(self.md) + tuple(extra)
        )

    async def get_vtoken(self, voucher: str) -> str:
        """-352 后走极验换 x-bili-gaia-vtoken（与 grpc_api 同款链路）。"""
        device = self.basic.device
        h5_ua = UserAgentParser.parse_h5_ua(device.ua_http, device.buvid,
                                            session_id=device.session_id)
        statistics = json.dumps({"appId": 1, "platform": 3, "version": device.version_name,
                                 "abtest": ""}, separators=(",", ":"))
        headers = _gaia_headers(h5_ua, device.buvid, self.basic.ticket)
        reg = await my_async_httpx.request(
            method="POST", url=GAIA_REGISTER,
            data=appsign({"disable_rcmd": 0, "mobi_app": "android", "platform": "android",
                          "statistics": statistics, "ts": int(time.time()),
                          "v_voucher": voucher}),
            headers=headers, proxies=self._proxies, verify=False, timeout=15)
        data = (reg.json() or {}).get("data") or {}
        if not data.get("geetest"):
            print(f"{P}   gaia register 未返回 geetest：{reg.text[:120]}")
            return ""
        import bili_ticket_gt_python

        validate = await asyncio.to_thread(
            bili_ticket_gt_python.ClickPy().simple_match_retry,
            data["geetest"]["gt"], data["geetest"]["challenge"])
        if not validate:
            print(f"{P}   极验求解失败")
            return ""
        vr = await my_async_httpx.request(
            method="POST", url=GAIA_VALIDATE,
            data=appsign({"challenge": data["geetest"]["challenge"], "disable_rcmd": 0,
                          "mobi_app": "android", "platform": "android",
                          "seccode": validate + "|jordan", "statistics": statistics,
                          "token": data["token"], "ts": int(time.time()),
                          "validate": validate}),
            headers=headers, proxies=self._proxies, verify=False, timeout=15)
        return data["token"] if (vr.json() or {}).get("code") == 0 else ""

    @property
    def _proxies(self):
        from Utils.GrpcUtils.metadata.device_env import resolve_proxies

        return resolve_proxies(self.proxy)

    async def grpc(self, tag: str, path: str, req: Message, resp_cls: type,
                   save: bool = False) -> bool:
        compressed = gzip.compress(req.SerializeToString(), 6)
        frame = b"\x01" + len(compressed).to_bytes(4, "big") + compressed
        started, note, resp = time.time(), "", None
        for attempt in (1, 2):
            resp = await my_async_httpx.request(
                method="POST", url=f"{BASE}/{path}", data=frame,
                headers=self.enc((("x-bili-gaia-vtoken", self.vtoken),) if self.vtoken else ()),
                proxies=self._proxies, verify=False, timeout=20)
            code = str(resp.headers.get("bili-status-code"))
            if code == "0" or code != "-352" or attempt == 2:
                break
            self.vtoken = await self.get_vtoken(resp.headers.get("x-bili-gaia-vvoucher", ""))
            note = "vtoken 重试"
            if not self.vtoken:
                break
        cost = time.time() - started
        code = str(resp.headers.get("bili-status-code"))
        if code != "0" or not resp.content:
            print(f"{P} {tag:12} ❌ 业务失败 status={code} "
                  f"msg={resp.headers.get('grpc-message')} ({cost:.1f}s)")
            RESULTS.append((tag, f"业务失败 {code}", len(resp.content)))
            return False

        body = resp.content[5:]
        if "gzip" in str(resp.headers.get("grpc-encoding", "")):
            body = gzip.decompress(body)
        message = resp_cls()
        try:
            message.ParseFromString(body)
        except Exception as exc:  # noqa: BLE001
            print(f"{P} {tag:12} ❌ 解析失败 {type(exc).__name__}: {exc}")
            RESULTS.append((tag, f"解析失败 {type(exc).__name__}", len(body)))
            return False

        unknown = sorted(set(scan_unknown_fields(body, resp_cls.DESCRIPTOR)))
        mismatch = scan_field_mismatches(body, resp_cls.DESCRIPTOR)
        again = resp_cls()
        again.ParseFromString(message.SerializeToString())
        roundtrip = again == message
        ok = not unknown and not mismatch and roundtrip
        print(f"{P} {tag:12} {'✅' if ok else '⚠️ '} status={code} {len(body)}B {cost:.1f}s "
              f"未知字段={unknown or '无'} 类型错配={mismatch[:3] or '无'} "
              f"往返={'一致' if roundtrip else '不一致'} {note}")
        RESULTS.append((tag, "OK" if ok else f"未知={unknown} 错配={mismatch[:3]}", len(body)))

        if save and tag != "OpusDetail":  # OpusDetail 的素材来自真实抓包，不覆盖
            req_dir, resp_dir = CAPTURE_DIR / tag / "request", CAPTURE_DIR / tag / "response"
            req_dir.mkdir(parents=True, exist_ok=True)
            resp_dir.mkdir(parents=True, exist_ok=True)
            (req_dir / "1.bin").write_bytes(frame)
            (resp_dir / "1.bin").write_bytes(resp.content)
            print(f"{P}   ↳ 素材已写入 {req_dir.parent.relative_to(ROOT)}/{{request,response}}/1.bin")
        return ok

    async def http(self, tag: str, url: str, spec: dict) -> bool:
        device = self.basic.device
        params = dict(spec.get("params") or {})
        headers = [("user-agent", device.ua_bapi), ("app-key", "android64"),
                   ("env", "prod"), ("buvid", device.buvid)]
        if spec.get("appsign"):
            params = appsign({**params, "build": device.build})
        if spec.get("appsign_bili"):  # abserver 用 BiliDroid UA + 指纹/设备头
            ua = (f"Mozilla/5.0 BiliDroid/{device.version_name} (bbcallen@gmail.com) os/android "
                  f"model/{device.device_model} mobi_app/android build/{device.build} "
                  f"channel/{device.channel} innerVer/{device.build} osVer/{device.osver} network/2")
            headers = [("buvid", device.buvid), ("fp_local", device.fp_local),
                       ("fp_remote", device.fp_remote), ("session_id", device.session_id),
                       ("guestid", str(device.guest_id)), ("user-agent", ua),
                       ("x-bili-trace-id", gen_trace_id()), ("x-bili-gaia-vtoken", ""),
                       ("x-bili-ticket", self.basic.ticket), ("accept-encoding", "gzip")]
            params = appsign({
                "brand": device.brand, "build": device.build, "buvid": device.buvid,
                "c_locale": "zh_CN", "channel": device.channel, "device": "phone",
                "disable_rcmd": 0, "mobi_app": "android", "model": device.device_model,
                "osver": device.osver, "platform": "android", "s_locale": "zh_CN",
                "statistics": json.dumps({"appId": 1, "platform": 3,
                                          "version": device.version_name, "abtest": ""},
                                         separators=(",", ":")),
                "ts": int(time.time())})
        try:
            started = time.time()
            resp = await my_async_httpx.request(
                method="GET", url=url, params=params, headers=tuple(headers),
                proxies=self._proxies, verify=False, timeout=20)
            cost = time.time() - started
            try:
                payload = resp.json()
                code, message = payload.get("code"), str(payload.get("message", ""))[:40]
            except Exception:  # noqa: BLE001
                code, message = resp.status_code, resp.text[:40]
            ok = str(code) == "0"
            print(f"{P} {tag:12} {'✅' if ok else '• '} HTTP {resp.status_code} "
                  f"code={code} {message} ({cost:.1f}s)")
            RESULTS.append((tag, f"HTTP {resp.status_code} code={code}", len(resp.content)))
            return ok
        except Exception as exc:  # noqa: BLE001
            print(f"{P} {tag:12} ❌ 异常 {type(exc).__name__}: {str(exc)[:100]}")
            RESULTS.append((tag, f"异常 {type(exc).__name__}", 0))
            return False


async def run(args: argparse.Namespace) -> int:
    if args.list:
        print("gRPC 用例：" + "、".join(GRPC_CASES))
        print("HTTP 用例：" + "、".join(tag for tag, _, _, _ in HTTP_CASES))
        return 0

    tags = args.cases or list(GRPC_CASES)
    unknown_tags = [t for t in tags if t not in GRPC_CASES]
    if unknown_tags:
        print(f"{P} 未知用例 {unknown_tags}，可用：{list(GRPC_CASES)}")
        return 2

    probe = Probe(use_proxy=args.proxy)
    await probe.init()
    for tag in tags:
        path, factory, resp_cls = GRPC_CASES[tag]
        await probe.grpc(tag, path, factory(), resp_cls, save=args.save)

    if args.http:
        for tag, url, _method, spec in HTTP_CASES:
            await probe.http(tag, url, spec)

    print(f"\n{P} ===== 汇总 =====")
    for tag, result, size in RESULTS:
        print(f"{P}   {tag:12} {result:32} {size}B")
    grpc_failed = [r for r in RESULTS if r[0] in GRPC_CASES and not r[1].startswith("OK")]
    other = [r for r in RESULTS if r[0] not in GRPC_CASES and not r[1].startswith("HTTP 200")]
    print(f"{P} gRPC 失败 {len(grpc_failed)} 项；HTTP 非 200 共 {len(other)} 项（HTTP 需真实业务参数，"
          f"非 200 多为参数占位所致）")
    return 1 if grpc_failed else 0


def main() -> None:
    parser = argparse.ArgumentParser(
        description="接口探针：直连真实请求 + 本地 proto 完整性校验",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("cases", nargs="*", help=f"要跑的 gRPC 用例，默认全部：{list(GRPC_CASES)}")
    parser.add_argument("--save", action="store_true", help="把真实请求/响应写入 test/grpc_capture/captures/")
    parser.add_argument("--http", action="store_true", help="附带 HTTP Bapi 读接口烟测")
    parser.add_argument("--proxy", action="store_true", help="走 CONFIG 配置的代理（默认直连）")
    parser.add_argument("--list", action="store_true", help="只列出可用用例")
    args = parser.parse_args()
    raise SystemExit(asyncio.run(run(args)))


if __name__ == "__main__":
    main()
