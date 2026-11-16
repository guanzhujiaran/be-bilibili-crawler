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
