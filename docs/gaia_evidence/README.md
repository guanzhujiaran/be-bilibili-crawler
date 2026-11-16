# gaia Web 激活链路证据（2026-11-16）

本目录是一次**真实浏览器抓包 + SDK 静态审阅**的证据快照，用于界定
`ExClimbWuzhi / ExGetAxe / ExClimbCongLing` 三段的实现边界。

- 采集方式：Playwright(chromium-headless-shell) 打开
  `https://www.bilibili.com/` 与 `https://space.bilibili.com/1/dynamic`，
  通过 CDP `Network.requestWillBeSent / responseReceived` 记录；
- 脱敏：cookie 值截断，`x-bili-gaia-*` 头不落盘，环境 payload 只保留结构；
- 原始抓取（未入库）见 `/tmp/gaia_web_activation.json` 与
  `/tmp/gaia_web_requests.json`，本目录 `activation_summary.json` 为可提交摘要。

## 1. 结论（先说边界）

| 事实 | 证据 |
| --- | --- |
| 页面初始化未拉取任何 `.wasm` 网络请求 | 229 个请求里 `.wasm` 命中 0；页面 HTML 无 `WebAssembly` / `.wasm` |
| 加密能力来自 `bili-sc-sdk.umd.js` 内联 WASM | `minntaki-wasm-sdk`；SDK 内 `WebAssembly.instantiate` 调用 + base64 二进制段 |
| SDK 地址 | `https://s1.hdslb.com/bfs/seed/jinkela/short/minntaki-wasm-sdk/bili-sc-sdk.umd.js` |
| SDK SHA-256 / 大小 | `1358f3b2006add8176f7258fbad44b4188fe48987efc5a8f932ea69a78468f0d` / 377583 bytes |
| ExGetAxe 返回 RSA-4096 公钥 | `data.version=v1`、`data.deadline=2524608000`、`data.public_key` 为 `BEGIN rsa public key` |
| ExClimbCongLing 使用 RSA + 对称加密信封 | `encoded_aes_key` base64 解码后 **512 bytes**（RSA-4096 OAEP 输出）；`encrypt_payload` 解码后 5584 bytes，首字节 `dfe9e611...` |
| 仓库内无 WASM 导出 / 无逆向脚本 | 全仓 `*.wasm` 为 0；SDK 只暴露 `default()` 与 `encrypt_data()` |
| 内联 WASM 实体 | 从 SDK base64 解码：231536 bytes，magic `00 61 73 6d 01 00 00 00`，SHA-256 `f28f166985a262a8e2980348c0f25e95f37f1a76e96b6e9474f051bc4483f317` |
| WASM 导出 / 导入 | export：`memory`、`encrypt_data`（wasm-bindgen）、`__wbindgen_*`；import：`wbg.__wbg_getRandomValues_*`、`wbg.__wbg_crypto_*` 等 JS 侧随机数 / crypto 胶水 |

> 尝试用 REA 的 `inspect_wasm_artifact` 做 WAT 级审阅，但本机未配置 WABT
> （`REA_WABT_BIN_DIRECTORY`），工具按设计拒绝执行；上面的导出 / 导入表由
> WASM 二进制 section 手工解析得到，可作为「加密入口确实在 WASM 内」的独立证据。


**因此**：`encoded_aes_key` / `encrypt_payload` 无法在不执行内联 WASM 的前提下
生成。代码只实现「解析 ExGetAxe + 按 schema 发送已加密信封」，
没有加密器时明确跳过（`GaiaActivation.log_missing_encryptor`），不伪造字段。

## 2. 三段请求实测

### 2.1 ExClimbWuzhi

```
POST https://api.bilibili.com/x/internal/gaia-gateway/ExClimbWuzhi
content-type: application/json;charset=UTF-8
body: {"payload":"<URL-encoded JSON 字符串>"}
resp: {"code":0,"message":"OK","ttl":1,"data":{}}
```

payload 关键字段：`3064`(=1)、`5062`(时间戳)、`03bf`(location.href)、
`39c8`(`333.1007.fp.risk`)、`6e7c`(屏幕分辨率)、`3c43`(环境指纹对象，
含 UA / 语言 / 时区 / WebGL / canvas 等)、`54ef`(AB 配置 JSON 字符串)。

### 2.2 ExGetAxe

```
GET https://api.bilibili.com/x/internal/gaia-gateway/ExGetAxe?web_location=333.1007
resp: {"code":0,"data":{"version":"v1","public_key":"-----BEGIN rsa public key-----\n...","deadline":2524608000}}
```

发起方（CDP initiator）：`bili-sc-sdk.umd.js:0:369904`。

### 2.3 ExClimbCongLing

```
POST https://api.bilibili.com/x/internal/gaia-gateway/ExClimbCongLing
content-type: application/json
body:
{
  "header": {
    "encode_type": 2,
    "payload_type": 4,
    "encoded_aes_key": "<base64, 684 chars / 512 bytes>",
    "ts": <毫秒时间戳>,
    "encoded_version": "v1"
  },
  "encrypt_payload": "<base64, 7448 chars / 5584 bytes>"
}
resp: {"code":0,"message":"OK","ttl":1,"data":{}}
```

调用点（SDK 内）：

```js
this.wasmModule.encrypt_data(
  JSON.stringify(envInfo), JSON.stringify(publicKeyData), userId, securityInfoStr
) // -> { key, data }
```

随后 `JSON.stringify({header:{...}, encrypt_payload: data})`。

## 3. 与现有实现的对应

| 段 | 代码 | 状态 |
| --- | --- | --- |
| ExClimbWuzhi | `BiliApiBase.gaia_gateway_ExClimbWuzhi` | 已实现（payload 由 `GenWebCookieParams.payload_str` 生成） |
| ExGetAxe | `BiliApi.gaia_gateway_ExGetAxe` + `GaiaActivation.parse_axe_response` | 已实现解析 |
| ExClimbCongLing | `BiliApiBase.gaia_gateway_ExClimbCongling` + `GaiaActivation.build_congling_body` | 信封 schema 已实现；**加密器未实现**，无 envelope 时跳过 |

后续若要补齐，唯一可行路径是：在受控 JS 运行时（Node + 官方 SDK）中调用
`encrypt_data`，把 `{key,data}` 回传给 Python，再由本模块发送。不要在 Python 里
手搓 RSA/OAEP + AES-GCM，因为 `payload_type=4` 的明文结构同样在 WASM 内，
无法从外部证实。
