# 风控策略黑箱对比矩阵

> 目的：用同一组只读 API 探针，在**固定出口**的前提下逐一测试各指纹 / 头 / 会话 / 节流组合，
> 统计 `-352 / -412` 触发率，用数据决定生产默认配置，而不是靠猜。
>
> 配套脚本：`tools/risk_blackbox.py`（`--dry-run` 只编译策略、不发包；真正跑批需人工确认）。

## 0. 实测发现（2026-11-16，实验设计修正）

首次按本矩阵跑批（A 组 × 30 轮，固定出口）后用数据修正了探针设计，结论如下：

| 探针 | 现象 | 原因 |
| --- | --- | --- |
| `reserve/relation/info?ids=1` | 360 次全部 `code=0`，12 组合 `risk_rate` 全 0、无区分度 | 响应 `data:null`，接口太轻，B 站不对其启用指纹级风控 |
| `x/space/wbi/acc/info?mid=1`（裸调） | 三种策略各 3 次全部 `-352` | 缺 WBI 签名，压倒性因素掩盖了指纹差异 |
| 接口扫描（`view`/`popular`/`upstat`/`reply`/`space-dyn`） | 前三者全 0，`reply` 全 -352，`space-dyn` 非 JSON | 接口要么不设防，要么因参数/签名不全必然拦截，**没有中间地带** |

**结论：B 站风控对单次请求是"阈值式"的——请求构造合法就 0，不合法就 -352，
不会因为 TLS 指纹不同而在单次请求上产生差异。真正的风险来自持续高频下的累积风控。**

因此本矩阵的 v1 设计（单次请求 × 多策略）**无法测出策略差异**，需要 v2 改造：
1. 探针必须走**完整业务请求构造**（WBI 签名 + dm_img 参数 + 合法 cookie），
   即复用 `get_space_dynamic_req_with_proxy()` 而非裸调 `my_async_httpx`；
2. 策略开关必须注入到业务函数内部（impersonate / UA / 会话 / 头序），而不是脚本侧覆盖；
3. 观测指标改为**持续跑 N 分钟后首次出现 -352 的时间 / 总请求数**，而非单次触发率。
   v2 需要改生产代码注入开关，待用户确认后再实施。

本次跑批产物保留：`/tmp/risk_blackbox_a.csv`（360 行全 OK）、`/tmp/risk_blackbox_a.log`。

## 1. 测试原则（v1 原始设计，已被 §0 修正）

1. **单变量原则**：每轮只改一个维度，基线组全默认；有交互嫌疑的组合（指纹×UA×会话）单独设一组。
2. **固定出口**：`RequestConf(local_exit_only=True, ignore_risk_cooldown=True)` +
   `CONFIG.my_ipv6_addr` 固定；消除代理随机性。QPS 配额临时设 0（不限速）。
3. **关掉重试**：`RequestRetryPolicy(max_rounds=1, attempts_per_round=1)`，否则一次调用发多次请求，
   触发率统计会被放大（`BapiUtils.request_wrapper` 默认按 tier 重试）。
4. **主探针**：`reserve_relation_info(ids=固定 sid)`——GET、单参数、无登录、`api.bilibili.com` 主域、
   自带 `check_reserve_relation_info` 校验。
   **对照探针**：`xlive_web_interface_v1_index_getWebAreaList()`——零参数，用于区分"出口全挂"
   与"某组合被针对"。
5. **不碰 `get_lot_notice`**：内置 100s 死等 + 推送 + 重试，不适合做统计探针。
6. **风控判定统一纯函数**：`code in (-412, -352, 65539)` 或抛 `Request412Error/Request352Error` → RISK；
   HTTP 200 且 code=0 → OK；其余（网络错误 / 5xx / 9999）→ OTHER，单独计数不混入分子。

## 2. 策略维度与取值

| 维度 | 代号 | 取值 | 注入点 |
| --- | --- | --- | --- |
| 指纹族 | `fp` | `all-random`(现状) / `chrome-only`(按会话固定) / `chrome-android` / `firefox` / `safari` | `SealedRequests.WEB_IMPERSONATE_TARGETS` / `SUPPORTED_IMPERSONATE_TARGETS` |
| UA 对齐 | `ua` | `fake-random`(现状 CONFIG.rand_ua) / `match-fp`(同主版本 Chrome UA) / `fixed-152` | `RequestHandler` / `pick_web_fingerprint` |
| 会话复用 | `sess` | `off`(每请求新建) / `on`(LRU 池复用) | `MYASYNCHTTPX` session 池开关 |
| Cookie 绑定 | `ck` | `none`(无 cookie) / `buvid`(buvid3/4 + ticket) | `RequestConf.is_use_cookie` |
| Header 顺序 | `hdr` | `insertion`(现状) / `alphabetical`(REA 观测序) | `UserAgentParser.get_headers_dict` |
| 业务节流 | `thr` | `off` / `0.1-0.5s` / `1-3s` | `RedisRequestProxy.request_with_proxy` 节流开关 |

## 3. 测试组合清单（共 3 组 22 项）

### A 组：单变量基线（12 项，`fp+ua` 是唯一指定的交互例外）

| # | 组合 ID | fp | ua | sess | ck | hdr | thr | 验证假设 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| A1 | `baseline` | all-random | fake-random | off | none | insertion | off | 现状基线 |
| A2 | `fp-chrome` | chrome-only | fake-random | off | none | insertion | off | 只收窄指纹族是否有效 |
| A3 | `ua-match` | all-random | match-fp | off | none | insertion | off | 只对齐 UA 是否有效 |
| A4 | `fp+ua` | chrome-only | match-fp | off | none | insertion | off | 指纹×UA 交叉校验是否关键 |
| A5 | `sess-on` | all-random | fake-random | on | none | insertion | off | 只复用连接是否有效 |
| A6 | `ck-buvid` | all-random | fake-random | off | buvid | insertion | off | 只带 cookie 是否有效 |
| A7 | `hdr-alpha` | all-random | fake-random | off | none | alphabetical | off | 只改头序是否有效 |
| A8 | `thr-light` | all-random | fake-random | off | none | insertion | 0.1-0.5s | 轻节流是否有效 |
| A9 | `thr-heavy` | all-random | fake-random | off | none | insertion | 1-3s | 重节流是否有效 |
| A10 | `fp-android` | chrome-android | fake-random | off | none | insertion | off | Android Chrome 指纹是否更接近 App 流量（UA 保持基线以隔离变量） |
| A11 | `fp-firefox` | firefox | fake-random | off | none | insertion | off | 反面对照：纯 Firefox |
| A12 | `fp-safari` | safari | fake-random | off | none | insertion | off | 反面对照：纯 Safari |

### B 组：两两交互（6 项）

| # | 组合 ID | fp | ua | sess | ck | hdr | thr | 验证假设 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| B1 | `fp+ua+sess` | chrome-only | match-fp | on | none | insertion | off | 一会话一指纹一连接（核心候选） |
| B2 | `fp+ua+ck` | chrome-only | match-fp | off | buvid | insertion | off | 指纹与 cookie 绑定 |
| B3 | `fp+ua+hdr` | chrome-only | match-fp | off | none | alphabetical | off | 传输+头序 |
| B4 | `sess+ck` | all-random | fake-random | on | buvid | insertion | off | 会话复用×cookie |
| B5 | `sess+thr` | all-random | fake-random | on | none | insertion | 0.1-0.5s | 会话复用×节流 |
| B6 | `hdr+thr` | all-random | fake-random | off | none | alphabetical | 0.1-0.5s | 头序×节流 |

### C 组：全套候选（4 项）

| # | 组合 ID | fp | ua | sess | ck | hdr | thr | 说明 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C1 | `all-in` | chrome-only | match-fp | on | buvid | alphabetical | 0.1-0.5s | 全部加固 |
| C2 | `all-in-no-thr` | chrome-only | match-fp | on | buvid | alphabetical | off | 去掉节流，看其边际贡献 |
| C3 | `all-in-heavy-thr` | chrome-only | match-fp | on | buvid | alphabetical | 1-3s | 加强节流，看是否有额外收益 |
| C4 | `all-in-fixed-ua` | chrome-only | fixed-152 | on | buvid | alphabetical | 0.1-0.5s | 固定 UA 版本 vs 随指纹变化 |

## 4. 执行协议

- **每组每组合 N 次**（建议 N=30，A 组先 N=10 粗筛），组合间随机打乱顺序，避免时间趋势污染。
- 每轮前后各跑一次对照探针（live 域），确认出口本身健康。
- 单次调用超时 15s；`OTHER` 超过 20% 判本轮无效，重跑。
- 命中 412/352 后按 `ProxyHealth` 既有冷却规则处理，**不要** `ignore_risk_cooldown` 硬绕。
- 结果写 CSV：`combo_id, seq, probe, code, latency_ms, verdict, timestamp, exit_prefix`。
- 汇总口径：`risk_rate = RISK / (OK + RISK)`，`OTHER` 单列；按 risk_rate 升序排列，95% 置信区间用 Wilson score。

## 5. 好组合的判定标准

1. `risk_rate` 显著低于 baseline（Wilson 区间不重叠）；
2. `OTHER` 比例 < 5%（说明不是靠"请求发不出去"假装没风控）；
3. P95 延迟在可接受范围（节流过重的组合要权衡吞吐）；
4. 连续两轮复测结果稳定（不是偶然）。

## 6. 风险与边界

- 真实请求会触发 412/352 冷却（2h→6h→24h 阶梯），**跑批前必须人工确认**；
- 固定出口若恰好被 B 站标记，所有组合都会高风控 → 用对照探针识别并换出口重跑；
- 本矩阵测的是**当前时间点**的 B 站策略，结论有时效性，建议每月复测；
- 不测 H2 SETTINGS / JA4 伪头顺序：curl_cffi 无独立 API，无法作为变量注入（见主计划书 §4.5）。
