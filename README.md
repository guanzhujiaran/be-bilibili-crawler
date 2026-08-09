# be-bilibili-crawler

BilibiliExplosion 的**核心爬虫后端**。基于 FastAPI，负责 B 站抽奖动态、话题抽奖、预约抽奖、山姆会员店等数据的抓取、解析、入库与判定，并向 RPA-Browser、消息推送、前端网关等提供数据 API。内置 Alembic 多库迁移、SVM/LLM 大奖判定、数据回填等运维脚本。

## 功能

- B 站抽奖：普通动态抽奖、官方（opus）抽奖、话题抽奖、充电/预约抽奖的抓取与入库
- 山姆会员店商品数据抓取入库
- 第三方用户抽奖动态挖掘（`GetOthersLotDyn`）
- 抽奖大奖判定：SVM 模型 + LLM（Qwen 等）二阶段判定，结果写入子表
- `rawJsonStr` 全字段回填脚本
- 统一消息告警推送（对接 `be-message-service`）
- 调用 `unidbgSpringBoot` 计算签名、`llama.cpp` 做本地推理
- 6 个 MySQL 业务库的 Alembic 版本管理

## 技术栈

| 类别 | 技术 |
| --- | --- |
| Web 框架 | FastAPI（uvicorn / uvloop） |
| 数据库 | MySQL 8（aiomysql）+ Redis + Milvus（向量库） |
| ORM / 迁移 | SQLAlchemy 2.x + Alembic（6 库多 target） |
| 消息队列 | RabbitMQ（FastStream / aio-pika） |
| 爬虫 | Playwright / Patchright、curl_cffi、cloudscraper、grpc（极验） |
| LLM | LangChain + Ollama / OpenAI 兼容 API |
| 签名 | 通过 HTTP 调用 `unidbgSpringBoot` |
| 依赖管理 | uv（Python 3.13+） |

## 目录结构

```
be-bilibili-crawler/
├── main.py                       # 主服务入口（端口 23333）：抽奖数据 API + 全局告警
├── faststream_app.py             # MQ 消费服务入口（端口 23334）
├── CONFIG.py                     # Settings / 数据库 / Redis / RabbitMQ / 推送 配置
├── create_database.py            # 初始化 6 个业务库
├── dev_env_main.py
├── pyproject.toml / uv.lock
├── alembic/  alembic.ini         # 6 库迁移配置
├── controller/                   # 路由层（v1: 抽奖库/统计/ip信息/后台/samsClub/验证码/mq）
├── Service/                      # 业务服务层
│   ├── BaseCrawler/              # 爬虫基类与注册表
│   ├── lottery_database/         # 抽奖数据解析入库
│   ├── opus新版官方抽奖/         # 官方抽奖（opus）专用
│   ├── GetOthersLotDyn/          # 第三方用户抽奖动态挖掘
│   ├── samsclub/                 # 山姆会员店
│   ├── BiliLiveScrape/           # 直播抓取
│   ├── PlayWright/  CaptchaGen/  # 浏览器自动化 / 验证码
│   ├── GrpcModule/               # 极验 grpc 调用
│   ├── MQ/  Auth/  BackgroundService/  # 消息队列 / 鉴权 / 后台任务
│   ├── llm_service/ LangChainCompo/     # LLM 相关
│   ├── toutiao/  zhihu/  ipinfo/        # 其它数据源
├── dao/  models/  Models/        # 数据访问 / ORM 模型
├── Utils/                        # 工具（推送、FastAPI、argParse 等）
└── scripts/                      # 运维脚本（judge_grand_prize / 数据回填等）
```

## 安装与启动

### 本地（uv）

```bash
cd be-bilibili-crawler
uv sync
# 前端静态资源（部分页面用到）
npm install
# 启动主服务
uv run python main.py          # http://0.0.0.0:23333
# 启动 MQ 消费服务（另一个进程）
uv run python faststream_app.py
```

### Docker（推荐）

```bash
cd /home/minato/BilibiliExplosion
docker compose up -d be-bilibili-crawler
```

容器内端口 `23333`，由 `docker-compose.yml` 的 `FASTAPI_PORT` 映射到宿主机；依赖 `mysql` / `redis` / `rabbitmq` / `unidbg` / `milvus` / `be-message-service`。

## 配置

通过 `docker-compose.yml` 注入环境变量（见 `CONFIG.py` 的 `Settings`）：

| 变量 | 说明 |
| --- | --- |
| `MYSQL_HOST` / `MYSQL_PORT` / `MYSQL_USER` / `MYSQL_PASSWORD` | MySQL 连接（6 个业务库） |
| `REDIS_HOST` / `REDIS_PORT` / `REDIS_PWD` | Redis 连接 |
| `RABBITMQ_HOST` / `RABBITMQ_USER` / `RABBITMQ_PASSWORD` | RabbitMQ 连接 |
| `UNIDBG_HOST` / `UNIDBG_PORT` | unidbg 签名服务地址 |
| `MILVUS_HOST` / `MILVUS_PORT` | Milvus 向量库 |
| `LLAMA_HOST` / `LLAMA_PORT` | llama.cpp 推理服务 |
| `PROXY_SERVER` / `V2RAY_HOST` / `V2RAY_PORT` | IPv6 代理池 / V2Ray 出口 |
| `MESSAGE_CONFIG` | 统一推送渠道配置（JSON，与 message-service / rpa-browser 共用） |
| `MESSAGE_SERVICE_HOST` / `MESSAGE_SERVICE_PORT` | message-service 地址 |
| `llm_apis` | 外部 LLM API 列表（OpenAI 兼容） |
| `SERVER_NAME` / `SERVER_ADDRESS` | 服务标识（写入告警标题） |
| `SHOW_LOG` / `IS_DEV` | 日志开关 / 是否开发环境 |

6 个业务库：`biliopusdb`（普通抽奖动态）、`bilidb`（话题抽奖）、`bili_reserve`（预约抽奖）、`dyndetail`（动态详情）、`proxy_db`（代理）、`samsclub`（山姆会员店）。

## 与其它服务的关系

```
                         ┌──────────────┐
            HTTP/数据 API│              │
  前端网关 ─────────────▶│ be-bilibili- │◀── RabbitMQ RPC ── RPA-Browser
  puppeteer_Bili         │   crawler    │
                         │              │
                         └──┬───┬───┬──┘
                            │   │   │
                  sign ─────┘   │   └───── 推送 ──▶ be-message-service
                  unidbgSpringBoot  │
                          llama.cpp ┘  (LLM 判定)
                          milvus (向量)
```

## 附录：运维脚本

### 数据库版本管理（Alembic）

通过 `-x db=xxx` 指定目标库，共管理 6 个 MySQL 库：

```bash
cd be-bilibili-crawler
alembic -x db=biliopusdb current        # 查看当前版本
alembic -x db=biliopusdb upgrade head   # 执行迁移
alembic -x db=biliopusdb downgrade -1   # 回滚一个版本
alembic -x db=biliopusdb history        # 迁移历史
```

| `-x db=` | 数据库 | 主要表 |
| --- | --- | --- |
| `biliopusdb` | 普通抽奖动态库 | `t_lotdyninfo` / `t_lot_grand_prize_flag` 等 |
| `bilidb` | 话题抽奖库 | `t_topic` / `t_traffic_card` 等 |
| `bili_reserve` | 预约抽奖库 | `t_up_reserve_relation_info` 等 |
| `dyndetail` | 动态详情库 | `bilidyndetail` / `lotdata` 等 |
| `proxy_db` | 代理数据库 | `proxy_tab` / `available_proxy` |
| `samsclub` | 山姆会员店库 | `spu_info` / `spu_category` 等 |

### SVM / LLM 大奖判断

对所有已入库抽奖数据执行判定，结果写入 `t_lot_grand_prize_flag`：

```bash
cd be-bilibili-crawler
uv run python -m scripts.judge_grand_prize --dry-run                 # 预演
uv run python -m scripts.judge_grand_prize                           # 正式（默认每批 200 条）
uv run python -m scripts.judge_grand_prize --batch-size 500         # 自定义批次
# 使用本地 ollama 判断并写入数据库
uv run python -m scripts.judge_grand_prize \
  --llm-base-url http://localhost:11434/v1 --llm-token ollama \
  --llm-model "modelscope.cn/unsloth/Qwen3.5-4B-GGUF"
```

### rawJsonStr 数据回填

从 `t_lotdyninfo.rawJsonStr` 重新解析并全量更新所有字段：

```bash
cd be-bilibili-crawler
uv run python -m scripts.database.backfill_dyninfo_from_rawjson.backfill --count   # 统计
uv run python -m scripts.database.backfill_dyninfo_from_rawjson.backfill --dry-run # 预演
uv run python -m scripts.database.backfill_dyninfo_from_rawjson.backfill --limit 500
uv run python -m scripts.database.backfill_dyninfo_from_rawjson.backfill            # 全量
```

回填字段包括互动数据（`commentCount`/`repostCount`/`likeCount`）、基本信息（`authorName`/`pubTime`/`dynContent`）、抽奖类型（`officialLotType`/`officialLotId`）、`isLot`、`isManualReply` 及 `t_lot_extra_info`（`need_comment`/`need_repost`）。

## FAQ

1. Milvus 报错无法读写：`sudo chown -R 999:999 ./docker_vol/milvus/data`
2. WSL2 mirrored 连不上网：重启 winnat（`net stop winnat` → `net start winnat`）
