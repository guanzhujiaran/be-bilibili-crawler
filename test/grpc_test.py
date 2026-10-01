# -*- coding: utf-8 -*-
"""DynDetail 手工调试脚本（**手动运行**，pytest 不会收集到用例）。

原来的写法在**导入期**就发请求，且用了新版 httpx 已移除的 `proxies=` 参数，
导致 `pytest test/` 直接收集失败；另外它按 `grpc-encoding: gzip` 声明却发未压缩
的 body、也没解压响应。这里一并修正。

用法：
    uv run python test/grpc_test.py                     # 直连
    uv run python test/grpc_test.py --proxy             # 走 CONFIG 配置的代理
    uv run python test/grpc_test.py --dynamic-id 123456 # 换动态 id

若返回 `bili-status-code: -352`（风控要求 gaia 校验），本脚本只打印提示；
需要自动换 `x-bili-gaia-vtoken` 重试时请用 `tools/grpc_probe.py`。
"""

import argparse
import asyncio
import base64
import gzip
import json
import random
import sys
from pathlib import Path

# 单独运行时（不走 pytest 的 pythonpath）需要自己把仓库根与 proto 根加进 sys.path
_ROOT = Path(__file__).resolve().parent.parent
for _p in (str(_ROOT), str(_ROOT / "Service" / "GrpcModule" / "Grpc" / "GrpcProto")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from bilibili.app.archive.middleware.v1.preload_pb2 import PlayerArgs  # noqa: E402
from bilibili.app.dynamic.v2 import dynamic_pb2
from bilibili.app.dynamic.v2.dynamic_pb2 import AdParam, Config
from google.protobuf.json_format import MessageToDict

from Utils.GrpcUtils.metadata.device_env import DIRECT, resolve_proxies
from Utils.GrpcUtils.metadata.makeMetaData import make_metadata
from Utils.代理.SealedRequests import my_async_httpx

DEFAULT_HOST = "https://grpc.biliapi.net"
METHOD = "/bilibili.app.dynamic.v2.Dynamic/DynDetail"


def build_request(dynamic_id: str | None, rid: int) -> dynamic_pb2.DynDetailReq:
    """构造 DynDetailReq：优先用 dynamic_id，否则用 dyn_type + rid。"""
    data_dict = {
        "uid": random.randint(1, 9223372036854775807),
        "dyn_type": 2,
        "rid": rid,
        "ad_param": AdParam(ad_extra=""),
        "player_args": PlayerArgs(qn=32, fnval=272, voice_balance=1),
        "share_id": "dt.dt-detail.0.0.pv",
        "share_mode": 3,
        "local_time": 8,
        "config": Config(),
    }
    if dynamic_id:
        data_dict.pop("dyn_type")
        data_dict.pop("rid")
        data_dict["dynamic_id"] = str(dynamic_id)
    return dynamic_pb2.DynDetailReq(**data_dict)


def encode_headers(md) -> dict:
    """metadata 元组 -> 请求头字典（`-bin` 头做 base64 去填充）。"""
    headers = {"content-type": "application/grpc"}
    for key, value in md:
        if isinstance(value, bytes):
            headers[key] = base64.b64encode(value).decode("utf-8").strip("=")
        else:
            headers[key] = str(value)
    return headers


async def main() -> None:
    parser = argparse.ArgumentParser(description="DynDetail 手工调试")
    parser.add_argument("--host", default=DEFAULT_HOST, help=f"gRPC 网关，默认 {DEFAULT_HOST}")
    parser.add_argument("--dynamic-id", default=None, help="动态 id（不传则用 --rid 的旧接口写法）")
    parser.add_argument("--rid", type=int, default=326834723, help="dyn_type + rid 写法用的稿件 id")
    parser.add_argument("--proxy", action="store_true", help="走 CONFIG 配置的代理（默认直连）")
    args = parser.parse_args()

    md, ticket, basic = await make_metadata("", proxy=None if args.proxy else DIRECT)
    print(f"[设备] buvid={basic.buvid} ticket={'有' if basic.ticket else '无'} "
          f"出口={'代理' if args.proxy else '直连'}")

    req = build_request(args.dynamic_id, args.rid)
    # metadata 声明了 grpc-encoding: gzip，body 必须真的 gzip，否则服务端解不开
    compressed = gzip.compress(req.SerializeToString(), compresslevel=6)
    flags = b"\x01"  # gRPC 帧压缩标志：1 = 已压缩
    data = flags + len(compressed).to_bytes(4, "big") + compressed

    headers = encode_headers(md)
    print(f"[请求] {args.host}{METHOD}\n[请求头] {json.dumps({k: str(v)[:40] for k, v in headers.items()}, ensure_ascii=False)}")

    resp = await my_async_httpx.request(
        method="POST", url=f"{args.host}{METHOD}", data=data, headers=tuple(headers.items()),
        proxies=resolve_proxies(None if args.proxy else DIRECT), verify=False, timeout=20)
    print(f"[响应] HTTP {resp.status_code} bili-status-code={resp.headers.get('bili-status-code')} "
          f"{len(resp.content)} 字节")

    code = str(resp.headers.get("bili-status-code"))
    if code == "-352":
        print(f"[提示] 风控拦截（vvoucher={resp.headers.get('x-bili-gaia-vvoucher')}），"
              f"请用 tools/grpc_probe.py 自动换取 x-bili-gaia-vtoken 后重试")
        return
    if code != "0" or len(resp.content) <= 5:
        print(f"[响应头] {dict(resp.headers)}")
        return

    payload = resp.content[5:]
    if "gzip" in str(resp.headers.get("grpc-encoding", "")):
        payload = gzip.decompress(payload)
    reply = dynamic_pb2.DynDetailReply()
    reply.ParseFromString(payload)
    print(json.dumps(MessageToDict(reply, preserving_proto_field_name=True),
                     ensure_ascii=False, indent=2)[:2000])


def parse_newdict_from_dict(orig_dict: dict) -> dict:
    """小驼峰字典 -> 下划线字典（历史调试辅助函数，保留供手工使用）。"""
    def _parse_newlist_form_list(orig_list: list) -> list:
        return [parse_newdict_from_dict(i) for i in orig_list]

    new_dict = {}
    for k, v in orig_dict.items():
        new_k = k[0]
        new_v = v
        if type(v) is str:
            if v.isdigit():
                new_v = int(v)
        for alpha in k[1:]:
            if alpha.isupper():
                new_k += "_" + alpha.lower()
            else:
                new_k += alpha
        if type(v) is dict:
            new_dict[new_k] = parse_newdict_from_dict(new_v)
        elif type(v) is list:
            new_dict[new_k] = _parse_newlist_form_list(new_v)
        else:
            new_dict[new_k] = new_v
    return new_dict


if __name__ == "__main__":
    asyncio.run(main())
