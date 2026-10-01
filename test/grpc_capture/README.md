# gRPC 抓包解析测试资源

本目录是 gRPC 抓包校验的**自包含**测试包：抓包报文、加载/解析工具、测试用例都放在
这里，由 `test/grpc_capture/test_proto_decode.py` 用仓库内 proto 生成的 protobuf 消息
解析校验。

## 目录结构

```
test/grpc_capture/
├── _proto_index.py         # 【自动生成】GrpcProto 全部 *_pb2.py 的静态索引
├── _resolver.py            # 方法名 -> 请求/响应消息类
├── _loader.py              # 去 gRPC 帧头 / gzip 解压 / 解析 / 未知字段扫描
├── test_proto_decode.py    # 抓包报文解析用例
├── test_metadata_headers.py# x-bili-*-bin 请求头与本地 metadata proto 一致性用例
├── README.md
└── captures/               # 抓包报文（唯一的数据目录）
    └── <Method>/           # 以 gRPC 方法名命名，一个方法一个目录
        ├── case.json       # 可选：消歧 / 直接指定消息类型
        ├── request/        # 放请求报文（可放多个）
        └── response/       # 放响应报文（可放多个）
```

已预置：

| 方法目录 | 服务 | 请求消息 | 响应消息 | 素材 |
| --- | --- | --- | --- | --- |
| `DynDetail`  | `bilibili.app.dynamic.v2.Dynamic` | `DynDetailReq`  | `DynDetailReply`  | 请求+响应 |
| `DynSpace`   | `bilibili.app.dynamic.v2.Dynamic` | `DynSpaceReq`   | `DynSpaceRsp`     | 请求+响应 |
| `DynDetails` | `bilibili.app.dynamic.v2.Dynamic` | `DynDetailsReq` | `DynDetailsReply` | 请求+响应 |
| `OpusDetail` | `bilibili.app.dynamic.v2.Opus` | `OpusDetailReq` | `OpusDetailResp` | 请求+响应 |
| `GetColdStartDeferredData` | （本地 proto 暂缺）| — | — | — |

前四个方法都放了**真实请求与真实响应**（响应为服务端原样返回的 gzip 帧，直连实测抓取）：
请求报文校验 metadata/player_args 等字段，响应报文做**反向校验**——它会一路走完
`OpusItem → Module → Extend / Paragraph / Avatar` 整棵消息树，任何字段编号或类型与线上
不一致都会被"未知字段 / 类型错配"断言当场抓出。本地 proto 曾经漏掉的
`OpusDetailResp.state`、`LinkNode.show_text` 类型、`Extend.up_name/up_face`、
`ParagraphFormat`/`AvatarItem`/`Layer` 的编号错位等，都是这样发现的。

## 怎么放抓包文件

1. 抓包工具（Charles / mitmproxy / Fiddler / Wireshark）里找到对应的 HTTP/2 请求；
2. **请求** body 的原始字节 → 放进 `captures/<Method>/request/`（文件名随意，如 `1.bin`）；
3. **响应** body 的原始字节 → 放进 `captures/<Method>/response/`；
4. 重跑测试即可，**文件名无需与方法名一致，目录名才是方法名依据**。

请求与响应分别存放，不要求一一对应；同一目录可放任意多份报文，都会被逐个解析。

> 支持两种格式（自动识别，无需配置）：
> - **完整 gRPC DATA 帧**：`1 字节压缩标志(0x00/0x01) + 4 字节大端长度 + protobuf`；
> - **纯 protobuf 裸字节**（抓包工具已剥离帧头）。
>
> 压缩标志为 `0x01`（或载荷带 gzip 魔数）时自动 gunzip。

若抓包工具保存的是文本 hex，先转二进制：

```bash
xxd -r -p request.hex > request.bin
```

## 消息类型是怎么确定的

默认**根据目录名（= gRPC 方法名）在本仓库 proto 中自动查找**服务的
`rpc <Method>(Req) returns (Rsp)`，从而得到请求/响应消息类，无需任何配置。

特殊情况可在方法目录下放 `case.json`：

```json
{
  "service": "bilibili.app.dynamic.v2.Dynamic",
  "request_type": "bilibili.app.dynamic.v2.DynDetailReq",
  "response_type": "bilibili.app.dynamic.v2.DynDetailReply"
}
```

- `service`：同名方法分属多个服务时用于消歧；
- `request_type` / `response_type`：protobuf **全限定名**，直接指定消息类型，
  指定后不再按方法名解析。

若方法名在本地 proto 中找不到（如 `GetColdStartDeferredData`），相关用例会
**skip** 并提示补齐 proto，不会让测试失败。

## 新增一个方法

1. 在 `captures/` 下新建目录 `<Method>/`，并在其下建 `request/`、`response/`；
2. 把抓包得到的请求/响应原始报文丢进对应子目录；
3. 若该方法本地 proto 不存在，先补充 `.proto` 并重新生成 stub（见
   `Service/GrpcModule/Grpc/GrpcProto/a.bash`），再重新生成下面的索引。

## 重新生成索引

新增/修改 proto 并重新生成 `*_pb2.py` 后，需要刷新 `_proto_index.py`。
在项目根目录执行下面的脚本即可（头部 docstring 与正文解析代码保持模板不变，
只重算模块 import 清单与 `_MODULES`）：

```bash
cd /home/minato/BilibiliExplosion/be-bilibili-crawler
uv run python - <<'PY'
from pathlib import Path

root = Path('Service/GrpcModule/Grpc/GrpcProto')
exclude = {'bilibili/app/playerunite/pugvanymodel/proto/pugvanymodel_pb2.py'}
files = sorted(
    p for p in root.rglob('*_pb2.py')
    if not p.name.endswith('_pb2_grpc.py')
    and str(p.relative_to(root)).replace('\\', '/') not in exclude
)

target = Path('test/grpc_capture/_proto_index.py')
old = target.read_text(encoding='utf-8')

# 保留文件头（docstring + import 段之前的模板）
head = old.split('\nimport ', 1)[0]

# 保留 _MODULES 之后的正文（dataclass / 索引构建代码）
tail = old.split('_MODULES: tuple[ModuleType, ...] = (', 1)[1]
tail = tail.split(')', 1)[1]

imports = '\n'.join(
    f"import {'.'.join(p.relative_to(root).with_suffix('').parts)} as _pb2_{i}"
    for i, p in enumerate(files)
)
aliases = '\n'.join(f'    _pb2_{i},' for i in range(len(files)))

target.write_text(
    f'{head}\n{imports}\n\n'
    f'_MODULES: tuple[ModuleType, ...] = (\n{aliases}\n){tail}',
    encoding='utf-8',
)
print(f'regenerated {target} with {len(files)} modules')
PY
```

> `exclude` 里的模块与另一份 `pugvanymodel_pb2.py` 符号重复，同时导入会触发
> descriptor pool 冲突，因此永久排除。

## 运行

```bash
cd /home/minato/BilibiliExplosion/be-bilibili-crawler
uv run pytest test/grpc_capture/ -v -rs
```

- 未放报文的目录、以及本地 proto 缺失的方法，都会 **skip**，不会失败；
- 解析成功后除了往返序列化校验，还会断言**没有未知字段**（本地 proto 漏字段会直接失败）；
- 解析成功的 JSON 输出到 `captures/_parsed/<Method>/<request|response>_<文件名>.json`，
  方便肉眼核对字段（该目录可随时删除，已在 `.gitignore` 中忽略）。

## 请求头（x-bili-*-bin）一致性

`test_metadata_headers.py` 内置一次真实抓包得到的 6 个 protobuf 请求头原值，
校验 `bilibili/metadata` 下的 proto 能**字节级**还原它们，并断言关键字段取值
（buvid / build / channel / guest_id / 时区 / 网络质量 / teenagers_age 等）。

抓包中的 `x-bili-ticket`、`x-bili-metadata-recent-region` 是服务端签发的 JWT，
客户端无法自行构造，不在校验范围内。
