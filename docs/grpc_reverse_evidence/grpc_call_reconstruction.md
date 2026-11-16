# B 站 APK gRPC 调用方式还原

目标 APK：`iBiliPlayer-bili.apk`（`tv.danmaku.bili` 9.19.0，versionCode `9190400`，md5 `818a5840a330bb80202e7243200cf859`，231 MB，36 个 dex）。
工具链：REA（`inspect_android_*` 提供的 JADX 引擎）+ 本地 `jadx-core` API 批量反编译 + `apktool/aapt`。

工程产物：

| 路径 | 说明 |
| --- | --- |
| `work/dex/` | 从 APK 解出的 36 个 `classes*.dex` |
| `work/out/java{3,10,11,22,23,25,27,31}/sources/` | jadx 反编译的 Java 源码（`work/tools/DexToJava.java`） |
| `work/out/smali/` | 反编译失败方法的 smali 兜底（`work/tools/DexSmali.java`） |
| `work/native/lib/arm64-v8a/libignet.so` | gRPC 原生传输层 |
| `work/assets/.../device_decision/prod/dd.json` | fawkes 设备决策规则（3140 条 track、21 个 prop） |
| `bili_grpc/` | 按还原结果实现的 Python 客户端 |
| `docs/grpc_call_reconstruction.md` | 本文档 |

---

## 1. 结论摘要

1. B 站 App 的 gRPC **不是 grpc-java**。业务 `*Moss` stub → `kntr.base.moss`（自研 Moss RPC）→ `com.bilibili.rpc.ignet.RpcEngine`（JNI）→ `libignet.so`，后者静态链接 **gRPC C-core（chttp2）+ ngtcp2 + BoringSSL**。
2. 因此 HTTP/2 帧格式与 TLS 握手都来自 gRPC C-core，**Python `grpcio` 使用同一套 C-core**，是能拿到的最接近的传输层。
3. 风控相关的东西几乎全在**请求头**里：`x-bili-metadata-bin` / `x-bili-device-bin` / `x-bili-fawkes-req-bin` / `x-bili-trace-id` / `x-bili-metadata-gaia-bin` / `x-bili-ticket` / `buvid` / `authorization` / `user-agent`。
4. 实测：同一路径、同一 channel，仅把 B 站头去掉 —— `PlayViewUnite` 直接被拒（`-400`），`View` 虽 200 但响应体从 68 981 字节缩到 19 820 字节；带上完整头后 `PlayViewUnite` 通过（`bili-status-code=0`，9 428 字节）。
5. 两个易错点：`-bin` 头在 Java 层是 `Kotlin Base64.Default`（带 padding），但 gRPC C-core 在写 HTTP/2 header 时会规范化为**无 padding**；grpcio 传 `bytes` 正好与之相同。`x-bili-trace-id` 有固定三段式格式。
6. **已用 FIRERPA + mitmproxy 在真机上抓到 22 次 gRPC 调用核对**，并根据实测修正了 UA、`channel`、fawkes sessionId、locale 时区字段、`restriction`、`network-bin`、ip-region / recent-region / ticket 等头。详见 [`grpc_capture_verification.md`](grpc_capture_verification.md)。修正后 `View` 响应体从 69 277 → 78 458 字节，`PlayViewUnite` 保持 `bili-status-code=0`。

---

## 2. 传输栈证据

`com/bilibili/rpc/ignet/RpcEngine.java`（classes23.dex）：

```java
private native long native_create_rpc(String host, int port, String proxy, String userAgent);
private native boolean native_async_unary_call(long ptr, String path, Map<String,String> headers,
                                              byte[] body, int length, RpcStreamObserver observer,
                                              Map<String,String> options);
```

`libignet.so` 字符串证据（`work/out/libignet.strings.txt`）：

```
grpc-c++/
grpc-c/%s (%s; %s)
grpc_chttp2_transport
BN9grpc_core8internal32ClientChannelServiceConfigParserE
ngtcp2_crypto_boringssl_configure_client_context failed
```

→ 原生 gRPC C-core + BoringSSL，且编了 ngtcp2（QUIC/HTTP3 可选）。APK 初始化时传 `{"ignet_http_rpc_enable": "0"}`（`kntr/base/moss/ignet/impl/grpc/ignet/call/a.java` 的 static 块），即默认走 TCP+HTTP/2。

---

## 3. 调用序列（每次一元调用）

`kntr/base/moss/ignet/impl/grpc/ignet/call/a.java#a`：

1. `RpcEngine.getInstance()`；
2. host 取 `kam0.c` 的 `grpc.override_host`，否则取服务定义 host（默认 `grpc.biliapi.net`），port 取服务定义（`443`）；
3. UA 从请求头里 **case-insensitive** 找 `user-agent`；
4. `handle = createRpc(host, port, systemHttpProxy, ua)`；
5. `headers = m5n0.a(request, Set.of("user-agent"))` —— 即所有 Moss 头（`user-agent` 由 native 侧单独设置，不进 headers map）；
6. `asyncUnaryCall(handle, "/{pkg}.{Service}/{Method}", headers, body, body.size, observer, {"timeout": "8000"})`。

路径格式见 `defpackage/bbx0#b`：`"/" + package + "." + service + "/" + method`，例如
`/bilibili.app.view.v1.View/View`、`/bilibili.app.playerunite.v1.Player/PlayViewUnite`。

超时来自 `uax0.a(8000L, "kmoss.grpc_timeout_ms")`，即默认 8000 ms。

---

## 4. 请求头全表

拦截器链在 `GrpcEngine` 构造函数里按 `getPriority()` 升序排列，逐个 `intercept` 往请求里塞头；最后 `defpackage/m5n0#a` 汇总，并**无条件**追加 `x-bili-moss-engine-type: 1`。

| 头 | 生产者（classes23.dex 为主） | 值 |
| --- | --- | --- |
| `buvid` | `defpackage.d550#provideBuvidMetadata` | buvid3 |
| `user-agent` | `defpackage.d550` + `defpackage.q7k2#a` | 见 §5.2；由 native 作为 HTTP/2 `user-agent` 发出 |
| `authorization` | `defpackage.io0#provideAccessKeyMetadata` | `identify_v1 <access_key>` |
| `x-bili-mid` | `defpackage.io0#provideMidMetadata` | mid（未登录不发） |
| `x-bili-metadata-bin` | `defpackage.c550` | `KMetadata` |
| `x-bili-device-bin` | `defpackage.zh60` | `KDevice` |
| `x-bili-restriction-bin` | `defpackage.d1z1` | `KRestriction`（全默认时 proto 为空，实际不发） |
| `x-bili-exps-bin` | `defpackage.b3o1` | `KExps`（无实验时为空） |
| `x-bili-locale-bin` | `defpackage.hnt0` + `defpackage.uci0` | `KLocale` |
| `x-bili-trace-id` | `defpackage.syw0#c` → `uop0#getXtraceId` | 见 §5.1 |
| `x-bili-aurora-eid` | `defpackage.syw0#e` | 见 §5.3 |
| `x-bili-aurora-zone` | `defpackage.syw0#b` | 按 path 前缀路由 zone，游客为空 |
| `x-bili-fawkes-req-bin` | `kntr.base.net.comm.imp.FawkesInterceptorKt` | `KFawkesReq` |
| `x-bili-ticket` | `kntr.base.net.comm.imp.d` | `dna0.a(host, path)` 票据（游客为空） |
| `x-bili-metadata-gaia-bin` | `kntr.base.net.comm.fingerprint2.g` | `KGaia{dfp, dfpP}` |
| `x-bili-moss-engine-type` | `defpackage.m5n0` | 恒为 `1` |
| `x-bili-redirect` / `x-bili-idc-redirect` | `kntr.base.net.comm.imp.c` | 仅重定向/兜底链路 |
| `using_cmp` | `defpackage.atm0` | 鸿蒙对比实验，普通 Android 不发 |
| `uat-env` | `defpackage.xsk2` | 仅非 prod 环境 |

**真机抓包补充（静态链路上没找到的 4 个头，实现里已补齐）**：

| 头 | 真机值 |
| --- | --- |
| `x-bili-network-bin` | proto `field 1 = 1`, `field 5 = 0d000080bf`，每个请求都有 |
| `x-bili-metadata-ip-region` | `CN` |
| `x-bili-metadata-recent-region` | ES256 JWT（含 `buvid` / `br` / `exp`），服务端下发后缓存 |
| `x-bili-ticket` | HS256 JWT（含 `exp` / `iat` / `buvid`），**游客态也有** |

此外真机还带 `accept: */*`、`accept-encoding: gzip, deflate, br`、
`grpc-accept-encoding: identity,deflate,gzip`、`grpc-encoding: gzip`、
`grpc-timeout: 7990m`、`te: trailers`（除 `grpc-encoding` 外都由 gRPC 库自动产生）。
完整 20 个头见 [`grpc_capture_verification.md`](grpc_capture_verification.md)。

响应侧会读：`x-bili-fawkes-resp-bin`（DD 版本升级）、`x-bili-retry-after`（`FlowControlException`）、`bili-status-code` / `bili-status-message`（业务错误）、`bili-status-code-disable`、`x-ticket-status`、`x-bili-aurora-path-route`、`x-bili-idc-redirect`、`grpc-status-mock`。

### 4.1 `KMetadata`（`x-bili-metadata-bin`）

| # | 字段 | 来源 |
| --- | --- | --- |
| 1 | `accessKey` | 账号 access_key |
| 2 | `mobiApp` | `android` |
| 3 | `device` | 空串（`c550` 传 `""`） |
| 4 | `build` | versionCode `9190400` |
| 5 | `channel` | `master` |
| 6 | `buvid` | buvid3 |
| 7 | `platform` | `android` |

### 4.2 `KDevice`（`x-bili-device-bin`）

`defpackage.zh60` 的实参顺序即字段顺序：

| # | 字段 | 来源 |
| --- | --- | --- |
| 1 | `appId` | `etp0.a().getAppId()`（主站 1） |
| 2 | `build` | versionCode |
| 3 | `buvid` | buvid3 |
| 4 | `mobiApp` | `android` |
| 5 | `platform` | `ogn2.a(uop0)` → `android` |
| 6 | `device` | 空串 |
| 7 | `channel` | `l2y0.a()`（`kntr.moss.metadata.use_kntr_device_metadata` 为真时用 app 元数据 channel） |
| 8 | `brand` | `Build.BRAND` |
| 9 | `model` | `Build.MODEL` |
| 10 | `osver` | `Build.VERSION.RELEASE` |
| 11 | `fpLocal` | `q7r0.b()` |
| 12 | `fpRemote` | `q7r0.a()` |
| 13 | `versionName` | `9.19.0` |
| 14 | `fp` | `q7r0.getFingerprint()`（gaia dfp） |
| 15 | `fts` | epoch 秒（`ijr0.a()`） |
| 16 | `guestId` | `q7r0.getGuestId()` |

### 4.3 `KFawkesReq`（`x-bili-fawkes-req-bin`）

| # | 字段 | 值 |
| --- | --- | --- |
| 1 | `appkey` | `IFoundation.getFawkesAppKey()`；`AndroidManifest` 元数据 `FAWKES_APP_KEY = "android64"`，缺省 `android64` |
| 2 | `env` | `IDeviceDecision.version().getEnv().getValue()`，prod 设备为 `prod` |
| 3 | `sessionId` | `fdd0.getSessionId()` |

### 4.4 `KGaia`（`x-bili-metadata-gaia-bin`）

| # | 字段 | 说明 |
| --- | --- | --- |
| 1 | `dfpP` | 受保护指纹 |
| 2 | `dfp` | 原始设备指纹 |

### 4.5 `KLocale`（`x-bili-locale-bin`）

`defpackage/uci0` 构造 `com.bapis.bilibili.metadata.locale.a`，其字段 1、2 分别是 current / system 的 `KLocaleIds`：

`KLocaleIds { 1 language, 2 script, 3 region }`。

### 4.6 其它 proto

- `KRestriction`：1 `teenagersMode`、2 `lessonsMode`、3 `mode`、4 `review`、5 `disableRcmd`、6 `basicMode`、7 `teenagersAge`、8 `overseasTeenagersMode`
- `KExps`：1 `exps`（repeated `KExpsItem{1 id, 2 bucket}`）

---

## 5. 生成算法

### 5.1 `x-bili-trace-id`

`defpackage/uop0#getXtraceId`（classes25.dex，接口默认实现）：

```java
StringBuilder sb = new StringBuilder();
for (int i = 0; i < 13; i++) sb.append(hex(random(0, 256), 2));   // 26 hex
sb.append(hex(epochSecond, 8).substring(0, 6));                    // +6 hex = 32
return join(sb, ":", sb.substring(16), ":0:0");
```

即 `[32 hex]:[后 16 hex]:0:0`，例：`e6ce588298c6817df058be06216afa9c:f058be06216afa9c:0:0`。
服务端响应里的 `bili-trace-id` 是后 16 hex 的推导值，可用它核对链路是否被正确接收。

### 5.2 `user-agent`

**真机抓包修正**：`defpackage/q7k2#a` 只负责设备后缀，gRPC 的完整 UA 由
`q7r0.getUserAgent()` 交给 `RpcEngine.createRpc(host, port, proxy, ua)`，实测为：

```
grpc-c++/1.66.2 Dalvik/2.1.0 (Linux; U; Android 11; ONEPLUS A6000 Build/RKQ1.201217.002) 9.19.0 os/android model/ONEPLUS A6000 mobi_app/android build/9190400 channel/oppo innerVer/9190400 osVer/11 network/2
```

- `grpc-c++/1.66.2` 是 C-core 自动加的产品 token（与 `libignet.so` 里的版本号一致）；
- `Mozilla/5.0 BiliDroid/... (bbcallen@gmail.com)`（`infra.producerUA`，`defpackage.ni30`）
  **只用于普通 HTTP（Ktor）请求，不用于 gRPC**；这是静态反编译时最容易搞混的一点；
- `channel/oppo` 是渠道值（真机为厂商渠道 `oppo`，非 `master`）；
- `network` 映射 `GConnectivity.getNetwork()`：`1 → 2`（wifi）、`2 → 1`（蜂窝）、其它 → `0`。

### 5.3 `x-bili-aurora-eid`

`defpackage/syw0#e`：mid 与密钥 `ad1va46a7lza` 循环 XOR，再标准 base64。未登录（mid `0`）返回空串，实际不发。

### 5.4 `-bin` 头的 base64

`defpackage/m5n0#a` 对每个 `byte[]` 头做 `Kotlin Base64.Default` 编码（**带 padding**）后交给 native；
但 gRPC C-core 在把 `-bin` metadata 写成 HTTP/2 header 时会重新规范化为**无 padding**。
真机抓到的 `x-bili-restriction-bin: OBA`（而不是 `OBA=`）证实了这一点。
因此 grpcio 传 `bytes`、由 C-core 编码无 padding base64，与 APK 线上行为**完全一致**。

---

## 6. 与标准 gRPC 客户端的差异（落地时踩到的坑）

| 项 | APK | grpcio |
| --- | --- | --- |
| `-bin` 编码 | Java 层带 padding，wire 上 C-core 规范化为**无 padding** | 传 `bytes`，C-core 发无 padding → **一致** |
| `user-agent` | `grpc-c++/1.66.2 Dalvik/... 9.19.0 os/android ...` | 固定 `grpc-python/<ver> <primary_ua>`，前缀去不掉 |
| `grpc-encoding` | `gzip` | 需显式 `compression=grpc.Compression.Gzip`（已开启） |
| `x-bili-ticket` / `recent-region` | 服务端下发的 JWT，游客也有 | 需从真机导出后配置 |
| `x-bili-moss-engine-type` | 恒定注入 | 需手工加 |
| 默认超时 | 8000 ms | 无默认，需显式传 |
| 重试 | Moss 按 `x-bili-retry-after` 做流控重试 | 只能配 gRPC retryPolicy |

实测结果：给 grpcio 传 `str` 形式的 `-bin` 值会直接抛
`TypeError: Binary metadata key="x-bili-metadata-bin" expected bytes, got <class 'str'>`，
因此实现里 `HeaderEntry.value` 用 `bytes`，由 grpcio 编码。服务端 C-core 解码器同时接受两种形式。

---

## 7. 实测记录

**最新结果（按真机抓包修正后）**：

```
View           : grpc=0  bili-status=0  78458 bytes   （修正前 69277）
PlayViewUnite  : grpc=0  bili-status=0   9362 bytes
A/B PlayViewUnite: bare(无 B 站头) = -400 / apk(完整头) = 0
```

真机抓包核对与逐项差异见 [`grpc_capture_verification.md`](grpc_capture_verification.md)，
原始头表见 [`grpc_capture_raw_dump.md`](grpc_capture_raw_dump.md)。
以下为**修正前**的第一版直连实测记录（保留以说明演进过程）。

环境：`bili_grpc/`（`uv` + `grpcio==1.85.1`，CPython 3.13.14），游客态（buvid 现场从
`https://api.bilibili.com/x/frontend/finger/spi` 获取，无 access_key）。

```
$ uv run python examples/check_view.py --aid 2
buvid: C4C4AF47-50CC-5F52-9107-17E05B149FDF93815infoc
path        : /bilibili.app.view.v1.View/View
grpc status : 0 (OK)
payload len : 69277
bili-status  : 0
trailer     : bili-trace-id=50210192ad6afa9c
trailer     : x-bili-trace-id=0320e370f189dcf450210192ad6afa9c
RESULT: ok, 69277 bytes of ViewReply
```

```
$ uv run python examples/check_player_unite.py --aid 2
aid=2 cid=62131 fnval=4048 buvid=013C6018-03F3-149B-050D-E19C6F73EB2B01125infoc
path        : /bilibili.app.playerunite.v1.Player/PlayViewUnite
grpc status : 0 (OK)
payload len : 9431
bili-status  : 0
RESULT: accepted by risk control, 9431 bytes of PlayViewUniteReply
```

A/B（同一 channel、同一 TLS 指纹，只改 metadata）：

```
$ uv run python examples/compare_headers.py --aid 2
=== PlayViewUnite ===
[bare ] grpc=-1 bili-status=-400 bytes=0
[apk  ] grpc=0 bili-status=0 bytes=9428
=== View ===
[bare ] grpc=0 bili-status=0 bytes=19820
[apk  ] grpc=0 bili-status=0 bytes=68981
```

结论：

- `PlayViewUnite` 在没有 B 站头时直接被拒（`-400`），带完整头时通过风控并返回播放数据；
- `View` 裸调用虽 200，但响应体只有完整头的 29%，说明服务端按头做了降级；
- 响应 trailer 里出现 `bili-trace-id` / `x-bili-trace-id`，证明我们构造的 trace 已进入风控链路。

---

## 8. 复现

```bash
cd be-bilibili-crawler/BiliReverseEnginnering/bili_grpc
uv sync --python 3.13
uv run python examples/check_view.py --aid 2 --show-headers
uv run python examples/check_player_unite.py --aid 2
uv run python examples/compare_headers.py --aid 2
```

需要登录态时，准备 `device.json`（`uv run python -c 'from bili_grpc import DeviceIdentity; open("device.json","w").write(DeviceIdentity().to_json())'` 生成模板）后填 `access_key`、`mid`、`buvid`、`fp`、`fp_local`、`fp_remote`，再 `--identity device.json` 或设 `BILI_DEVICE_IDENTITY`。

重放任意接口：

```python
from bili_grpc import BiliGrpcClient, DeviceIdentity, Varint, encode

with BiliGrpcClient(identity=DeviceIdentity.load()) as client:
    resp = client.unary("/bilibili.app.view.v1.View/View", encode((Varint(1, 2),)))
    print(resp.describe())
```

---

## 9. 未完成 / 待补

| 项 | 状态 | 说明 |
| --- | --- | --- |
| `x-bili-metadata-gaia-bin` 的 `dfp/dfpP` | 未还原 | 设备指纹 SDK 产物；真机 22 次调用均未出现，游客可省略 |
| `KDevice.fp/fpLocal/fpRemote` | 未还原 | 真机三处为同一个 64-hex 值，可从 `work/capture/real_headers_sample.json` 导出后配置 |
| `x-bili-ticket` / `x-bili-metadata-recent-region` | 部分 | 服务端签发的 JWT，游客也有；目前从真机样本导出后由 `device.json` 配置，未实现自动续期 |
| 真机 `buvid` 生成（`XUBB…` 32 hex） | 部分 | 当前用 spi 接口的 `b_3`；复刻真机时用 `device.json` 覆盖 |
| `grpc-c++/1.66.2` UA 前缀 | 无法复刻 | grpcio 固定发 `grpc-python/<ver>`，C-core 产品 token 改不了 |
| `x-bili-aurora-zone` 路由 | 部分 | 只在下发 `x-bili-aurora-path-route` 时才需要更新 |
| `KLocale` 的 6..10 字段 | 部分 | 真机只出现 1/2/4/5，已补齐这四个 |
| 真机抓包比对 | **已完成** | 见 §10 与 `grpc_capture_verification.md` |

---

## 10. 真机抓包验证（已完成）

不需要 adb / 无线调试：克隆 `firerpa/lamda`（10.12），用它的 Python 客户端在
`192.168.42.238:65000` 上**装系统 CA + 设置设备级 `GproxyType.HTTP_CONNECT` 代理**
（仅作用于 `tv.danmaku.bili`），本机起 `mitmdump`（11.0.2，端口 18081）接收流量。
设备级代理是 iptables/uid 级的，**能截获原生 libignet 的 TCP 连接**，普通「系统 HTTP 代理」
做不到这一点。

- 抓到 **22 次真实 gRPC 调用**（`Module/List` ×18、`GetColdStartDeferredData`、
  `DefaultWords`、`ModeStatus`、`SetUserPreference`），全部 `grpc-status=0` /
  `bili-status-code=0`。
- 逐项核对结果、原始头表与重现命令：[`grpc_capture_verification.md`](grpc_capture_verification.md)
- 抓包原始记录：`work/capture/grpc_headers.jsonl`，真机头样本：`work/capture/real_headers_sample.json`
- 工具：`work/tools/grpc_mitm_addon.py`、`setup_mitm_proxy.py`、`analyze_capture.py`

收尾已执行：`stop_gproxy()` 恢复设备网络（`gproxy running: False`）、mitmdump 已停止；
系统 CA 保留在设备上（便于下次抓包，不影响正常使用）。
