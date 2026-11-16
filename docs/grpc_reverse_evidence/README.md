# B 站 gRPC 真机 evidence（APK 9.19.0）

本目录是 `be-bilibili-crawler` 内的 gRPC 逆向/抓包证据归档，供生产工具链
（`Utils/GrpcUtils/metadata/`、`Service/GrpcModule/Grpc/`）与
`test/grpc_capture/` 回归测试引用。

> 完整 APK、36 个 dex 的反编译 Java 源码、libignet.so 与 dd.json 仍在
> `BiliReverseEnginnering/work/`，体积很大，不在本目录重复存放。

## 文件清单

| 路径 | 内容 |
| --- | --- |
| `grpc_call_reconstruction.md` | 静态逆向结论：Moss 拦截器链、调用序列、请求头全表、proto 字段号、trace-id/UA 算法 |
| `grpc_capture_verification.md` | FIRERPA + mitmproxy 真机抓包核对：22 次调用、20 个头、静态 vs 实测差异表、重现命令 |
| `grpc_capture_raw_dump.md` | 抓包头全集与 `-bin` 字段解码原始表 |
| `capture/grpc_headers.jsonl` | 22 次真实 gRPC 调用的原始 HTTP/2 头（每行一条 JSON） |
| `capture/real_headers_sample.json` | 一条完整真机头样本（用于导出设备身份） |
| `tools/grpc_mitm_addon.py` | mitmproxy addon：把 `biliapi.net` 头写入 jsonl |
| `tools/setup_mitm_proxy.py` | 通过 firerpa/lamda 装系统 CA、设置/恢复设备级代理 |
| `tools/analyze_capture.py` | 解析 jsonl 并生成 markdown 报告 |
| `tools/make_device_from_capture.py` | 从真机头导出 `bili_grpc/device.json`（buvid/fp/ticket/recent-region/UA） |

## 关键结论（与生产代码直接相关）

1. 传输层是 `libignet.so` 里的 **gRPC C-core 1.66.2 + BoringSSL**，不是 grpc-java；Python `grpcio` 同源，HTTP/2/TLS 指纹最接近。
2. 真机每个 gRPC 请求都带 **`x-bili-trace-id`**（`<32hex>:<后16hex>:0:0`），旧注释「抓包未发送」是错的。
3. gRPC 的 `user-agent` = `grpc-c++/1.66.2 ` + `ua_http`（`Dalvik/... <version> os/android model/... channel/... network/2`）；`ia` 的 `device.ua`（`ignet_http/... ct/1`）不是这一套。
4. `-bin` 头在 wire 上是 **无 padding** base64（C-core 规范化），Java 层的 `Kotlin Base64.Default` 带 padding 只是中间态。
5. 真机 `grpc-timeout: 7990m`、`grpc-encoding: gzip`，请求体确实压缩。
6. `x-bili-network-bin`(success_rate=-1.0)、`x-bili-metadata-ip-region`、`x-bili-metadata-recent-region`、`x-bili-ticket`、`x-bili-restriction-bin`(teenagersAge=16) 每个请求都有。
7. `fawkes appkey=android64`、`env=prod`、`sessionId` 为 8 位 hex；`channel=oppo`；`version=9.19.0` / `build=9190400`。

## 回归测试

`test/grpc_capture/test_firerpa_headers.py` 用项目自身的 `bilibili.metadata.*_pb2`
解析真机 `-bin` 头，并静态断言 `make_metadata` 已包含本文件列出的关键修正。

```bash
uv run pytest test/grpc_capture -q
```

## 生产工具同步状态（已完成）

| 文件 | 按 evidence 修正 |
| --- | --- |
| `Utils/GrpcUtils/metadata/makeMetaData.py` | gRPC UA 改用 `grpc-c++/1.66.2 + ua_http`；补 `x-bili-trace-id`；`grpc-timeout: 7990m`；network `success_rate=-1.0`；默认版本 9.19.0/9190400；ticket/region 统一为 `ensure_credentials()` |
| `Utils/GrpcUtils/metadata/device_env.py` | 默认版本/inner_ver 对齐真机；新增 `gen_trace_id()`、`DeviceEnv.ticket` 与 `ticket_valid()` |
| `Service/GrpcModule/Grpc/grpc_api.py` | channel `compression=Gzip`，与 metadata 的 `grpc-encoding: gzip` 一致 |
| `Service/GrpcModule/Grpc/GrpcProto/` | 工具链升级 `grpcio/grpcio-tools 1.85.1`、`protobuf 7.36.2` 后用 `a.bash` 重编译全部 pb2；`_proto_index.py` 刷新（138 模块） |

验证结果：

- `uv run pytest test/grpc_capture -q` → **30 passed, 25 skipped**
- 头集合：已领取 region 的设备 **20/20 与真机一致**；全新设备冷启动未领到时只带
  `x-bili-metadata-ip-region=CN`（19 个头），`recent-region` 不跨设备共享。
- `recent-region` 与 `x-bili-ticket` 一样，都是**调 B 站接口由服务端签发**的按设备凭证：
  ticket 走 `GetTicket` 接口（响应体），recent-region 走 `tab/v2` 接口（响应头）。
  客户端只负责按设备请求并回带，不本地签发（`gen_x_bili_ticket` 的 HMAC 只是
  `GetTicket` 的请求校验参数，不是最终 ticket）。
  `DeviceEnv.fetch_region()` 按设备领取，结果随设备持久化到 Redis 设备池，过期自动刷新。
  环境变量 `BILI_RECENT_REGION` 仅用于单设备调试覆盖。
- 运行时验证：两台设备的 JWT 独立（payload 里的 `buvid` 各自不同），
  `to_dict()` / `from_dict()` 持久化后仍属于原设备；同一设备连续两次 `make_metadata`
  只触发一次 `GetTicket`（缓存命中），两设备各触发一次。
- 用真机头 + 项目 channel（`compression=Gzip`）直连 `View`：RPC 成功返回 **69 114 bytes**（`bili-status-code=0`）；工具链升级到 `grpcio 1.85.1` 重编译 pb2 后复跑仍成功（**69 033 bytes**）。

## 重现抓包

见 `grpc_capture_verification.md` 的「抓包方法（可重现）」一节。
抓包收尾必须执行 `setup_mitm_proxy.py off`，否则设备会断网。
