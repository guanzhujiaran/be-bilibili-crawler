# 真机 gRPC 抓包核对（FIRERPA + mitmproxy）

抓包调用数：**22**，接口：

- `/bilibili.app.resource.v1.Module/List` × 18
- `/bilibili.app.coldstart.v1.ColdStart/GetColdStartDeferredData` × 1
- `/bilibili.app.interface.v1.Search/DefaultWords` × 1
- `/bilibili.app.interface.v1.Teenagers/ModeStatus` × 1
- `/bilibili.app.distribution.v1.Distribution/SetUserPreference` × 1

## 请求头全集

| header | 出现次数 | 真机实际值（截取） |
| --- | --- | --- |
| `accept` | 22/22 | `*/*` |
| `accept-encoding` | 22/22 | `gzip, deflate, br` |
| `buvid` | 22/22 | `XUBB021B6B4B15C7BB7E6D1D1DE0F7DCDE084` |
| `content-type` | 22/22 | `application/grpc` |
| `grpc-accept-encoding` | 22/22 | `identity, deflate, gzip` |
| `grpc-encoding` | 22/22 | `gzip` |
| `grpc-timeout` | 22/22 | `8S` |
| `te` | 22/22 | `trailers` |
| `user-agent` | 22/22 | `grpc-c++/1.66.2 Dalvik/2.1.0 (Linux; U; Android 11; ONEPLUS A6000 Build/RKQ1.201217.002) 9.19.0 os/android model/ONEPLUS` |
| `x-bili-device-bin` | 22/22 | `CAEQgPiwBBolWFVCQjAyMUI2QjRCMTVDN0JCN0U2RDFEMURFMEY3RENERTA4NCIHYW5kcm9pZCoHYW5kcm9pZDoEb3Bwb0IHT25lUGx1c0oNT05FUExVUyBB` |
| `x-bili-fawkes-req-bin` | 22/22 | `CglhbmRyb2lkNjQSBHByb2QaCDgyYjI0YTU5` |
| `x-bili-locale-bin` | 22/22 | `Cg4KAnpoEgRIYW5zGgJDThIOCgJ6aBIESGFucxoCQ04iDUFzaWEvU2hhbmdoYWkqBiswODowMA` |
| `x-bili-metadata-bin` | 22/22 | `EgdhbmRyb2lkIID4sAQqBG9wcG8yJVhVQkIwMjFCNkI0QjE1QzdCQjdFNkQxRDFERTBGN0RDREUwODQ6B2FuZHJvaWQ` |
| `x-bili-metadata-ip-region` | 22/22 | `CN` |
| `x-bili-metadata-recent-region` | 22/22 | `eyJhbGciOiJFUzI1NiIsImtpZCI6IlhVaVhRSFo5IiwidHlwIjoiSldUIn0.eyJidXZpZCI6IlhVQkIwMjFCNkI0QjE1QzdCQjdFNkQxRDFERTBGN0RDREUw` |
| `x-bili-moss-engine-type` | 22/22 | `1` |
| `x-bili-network-bin` | 22/22 | `CAEqBQ1Ob3o/` |
| `x-bili-restriction-bin` | 22/22 | `OBA` |
| `x-bili-ticket` | 22/22 | `eyJhbGciOiJIUzI1NiIsImtpZCI6InMwMyIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3OTQ4MzQ5ODYsImlhdCI6MTc5NDgwNTg4NiwiYnV2aWQiOiJYVUJCMDI` |
| `x-bili-trace-id` | 22/22 | `f7eaa741818621ed5e5aea59f36afaa0:5e5aea59f36afaa0:0:0` |

## `-bin` 头的真实 protobuf 字段

### `x-bili-device-bin`

base64 `CAEQgPiwBBolWFVCQjAyMUI2QjRCMTVDN0JCN0U2RDFEMURFMEY3RENERTA4NCIHYW5kcm9pZCoHYW5kcm9pZDoEb3Bwb0IHT25l`（327 bytes）

| # | wire | value |
| --- | --- | --- |
| 1 | 0 | `1` |
| 2 | 0 | `9190400` |
| 3 | 2 | `XUBB021B6B4B15C7BB7E6D1D1DE0F7DCDE084` |
| 4 | 2 | `android` |
| 5 | 2 | `android` |
| 7 | 2 | `oppo` |
| 8 | 2 | `OnePlus` |
| 9 | 2 | `ONEPLUS A6000` |
| 10 | 2 | `11` |
| 11 | 2 | `5e28be6c8f5309e36a1a8f654dcc41522026111613162724dace1b404a71d415` |
| 12 | 2 | `5e28be6c8f5309e36a1a8f654dcc41522026111613162724dace1b404a71d415` |
| 13 | 2 | `9.19.0` |
| 14 | 2 | `5e28be6c8f5309e36a1a8f654dcc41522026111613162724dace1b404a71d415` |
| 15 | 0 | `1794806179` |
| 16 | 2 | `27081952046355` |

### `x-bili-fawkes-req-bin`

base64 `CglhbmRyb2lkNjQSBHByb2QaCDgyYjI0YTU5`（27 bytes）

| # | wire | value |
| --- | --- | --- |
| 1 | 2 | `android64` |
| 2 | 2 | `prod` |
| 3 | 2 | `82b24a59` |

### `x-bili-locale-bin`

base64 `Cg4KAnpoEgRIYW5zGgJDThIOCgJ6aBIESGFucxoCQ04iDUFzaWEvU2hhbmdoYWkqBiswODowMA`（55 bytes）

| # | wire | value |
| --- | --- | --- |
| 1 | 2 | `0a027a68120448616e731a02434e` |
| 2 | 2 | `0a027a68120448616e731a02434e` |
| 4 | 2 | `Asia/Shanghai` |
| 5 | 2 | `+08:00` |

### `x-bili-metadata-bin`

base64 `EgdhbmRyb2lkIID4sAQqBG9wcG8yJVhVQkIwMjFCNkI0QjE1QzdCQjdFNkQxRDFERTBGN0RDREUwODQ6B2FuZHJvaWQ`（68 bytes）

| # | wire | value |
| --- | --- | --- |
| 2 | 2 | `android` |
| 4 | 0 | `9190400` |
| 5 | 2 | `oppo` |
| 6 | 2 | `XUBB021B6B4B15C7BB7E6D1D1DE0F7DCDE084` |
| 7 | 2 | `android` |

### `x-bili-network-bin`

base64 `CAEqBQ0AAIC/`（9 bytes）

| # | wire | value |
| --- | --- | --- |
| 1 | 0 | `1` |
| 5 | 2 | `0d000080bf` |

### `x-bili-restriction-bin`

base64 `OBA`（2 bytes）

| # | wire | value |
| --- | --- | --- |
| 7 | 0 | `16` |


## 响应 trailer

- `bili-status-code`: `0`
- `bili-trace-id`: `1295a152716afaa0`
- `cpu_usage`: `88`
- `grpc-message`: ``
- `grpc-status`: `0`
- `x-bili-trace-id`: `32bc0a01bf17649d1295a152716afaa0`

## 抓包方法（可重现）

```bash
cd work/tools
git clone --depth 1 https://github.com/firerpa/lamda.git   # 已克隆
# 1. 本机起 mitmdump + 记录 addon
uv run --no-project --python 3.11 --with mitmproxy mitmdump -p 18081 \
    -s grpc_mitm_addon.py --set http2=true --set ssl_insecure=true -q &
# 2. 用 lamda 客户端装系统 CA + 给 tv.danmaku.bili 设置设备级代理
cd lamda
uv run --no-project --python 3.11 --with . --with packaging \
    python ../setup_mitm_proxy.py on --package tv.danmaku.bili
# 3. firerpa 重启 B 站 App 产生流量
# 4. 解析
python3 ../analyze_capture.py
# 5. 收尾（务必执行，否则手机断网）
uv run --no-project --python 3.11 --with . --with packaging \
    python ../setup_mitm_proxy.py off
pkill -f 'mitmdump -p 18081'
```

设备：`ONEPLUS A6000` / Android 11 / `tv.danmaku.bili` 9.19.0，firerpa 服务端
`192.168.42.238:65000`，mitmproxy 11.0.2 在 `192.168.1.201:18081`。
代理为设备级（lamda `GproxyType.HTTP_CONNECT`），因此**原生 libignet 的 TCP 连接也会被截获**
——普通「系统 HTTP 代理」抓不到 native gRPC，这是本方案的关键。

## 静态还原 vs 真机实测

| 项 | 静态还原 | 真机实测 | 处理 |
| --- | --- | --- | --- |
| `x-bili-trace-id` | `32hex:后16hex:0:0` | 完全一致 | 已验证 |
| `x-bili-metadata-bin` | 7 字段 | 1/3 空（游客），2/4/5/6/7 一致；`channel=oppo` | 已把默认 channel 改为 `oppo` |
| `x-bili-device-bin` | 16 字段 | 除 6(device) 空外全部存在；11/12/14 为同一 64-hex `fp`；15=epoch 秒；16=14 位数字 `guestId` | 字段号全部正确；fp/guestId 改为可配置 |
| `x-bili-fawkes-req-bin` | appkey/env/sessionId | `android64` / `prod` / **8 位 hex sessionId** | sessionId 改成自动生成 8 hex |
| `x-bili-locale-bin` | 仅字段 1/2 | 还有 **4=`Asia/Shanghai`、5=`+08:00`** | 已补齐 |
| `x-bili-restriction-bin` | 全默认（不发头） | `field 7 = 16`（teenagersAge） | 已补，头现在恒发 |
| `x-bili-network-bin` | 静态未发现 | **每个请求都有**：field1=1、field5=`0d000080bf` | 已新增 `Network` 消息 |
| `x-bili-metadata-ip-region` | 静态未发现 | 恒为 `CN` | 已补 |
| `x-bili-metadata-recent-region` | 静态未发现 | ES256 JWT（含 buvid/br/exp），服务端下发后缓存 | 可配置 |
| `x-bili-ticket` | 已知存在但未实现 | HS256 JWT（含 exp/iat/buvid），**游客也有** | 可配置 |
| `x-bili-moss-engine-type` | `1` | `1` | 已验证 |
| `buvid` | spi 接口的 `b_3`（`...infoc`） | `XUBB021B6B4B15C7BB7E6D1D1DE0F7DCDE084`（32 hex） | 改为可填真机值 |
| `user-agent` | `Mozilla/5.0 BiliDroid/...` | **`grpc-c++/1.66.2 Dalvik/2.1.0 (Linux; U; Android 11; ONEPLUS A6000 Build/RKQ1.201217.002) 9.19.0` + ` os/android model/...`** | 重大修正：Mozilla 串只用于 HTTP，gRPC 用 native UA |
| `grpc-encoding` | 未设置 | `gzip` | 已启用 `grpc.Compression.Gzip` |
| `grpc-timeout` | 8000 ms | `7990m` | grpcio 用 8000 ms deadline，等价 |
| `authorization` / `x-bili-mid` / `x-bili-aurora-eid` | 登录才有 | 游客态均不出现 | 已验证 |
| `x-bili-metadata-gaia-bin` | 可选 | 这 22 次调用均未出现 | 保持可选 |

### 修正后的复刻效果

```
View           : grpc=0  bili-status=0  78458 bytes（修正前 69277）
PlayViewUnite  : grpc=0  bili-status=0   9362 bytes
A/B（PlayViewUnite）bare = -400 / apk = 0
```

再用 `make_device_from_capture.py` 从真机头导出完整身份（buvid / fp / guest_id /
ticket / recent-region / UA）重放：

```
业务头集合比对：ours = 12 / 真机 = 12，缺 0，多 0
View           : grpc=0  bili-status=0  69104 bytes
PlayViewUnite  : grpc=0  bili-status=0   9246 bytes
```

真机响应 trailer 里出现 `bili-trace-id` 与 `x-bili-trace-id`，与请求头里的
trace 同源，说明我们构造的 trace 已被服务端风控链路接受。

### 仍无法从 Python 完全复刻的部分

1. `grpc-c++/1.66.2` 前缀：grpcio 的 C-core 固定发 `grpc-python/<ver> <primary_user_agent>`，
   即使把 `primary_user_agent` 设成完整真机 UA，线上看到的仍是双前缀。
2. HTTP/2 SETTINGS、WINDOW_UPDATE 与 BoringSSL ClientHello 的版本差异（grpcio 1.85 vs APK 1.66.2）。
3. `x-bili-ticket` / `recent-region` 是服务端下发的带签名 JWT，`x-bili-metadata-gaia-bin`
   与 `KDevice.fp` 是设备指纹 SDK 产物，均需从真实设备导出（见 `work/capture/real_headers_sample.json`）。
