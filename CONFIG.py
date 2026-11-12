from bili_common.models import StrEnumAutoDoc
import os
from dataclasses import dataclass
from fake_useragent import UserAgent
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy import AsyncAdaptedQueuePool

# 「推送 / 服务标识」配置片段与推送渠道配置模型统一来自 bili-common（单一来源）
from bili_common.core.push_settings import PushNotifySettingsMixin
from bili_common.models.push import PushChannelConfig

from Service.BaseCrawler.config import CrawlerConfig, get_crawler_config

_current_dir = os.path.dirname(os.path.abspath(__file__))


class ConcurrencyConfig(BaseModel):
    """**所有并发数的统一入口**——每个爬虫一项，另有代理抓取与分阶段采集的项。

    各爬虫的 ``CrawlerConfig`` 不再写并发数字，只用 ``concurrency_key`` 指向这里的字段：
        class ReserveScrapyRobotConfig(CrawlerConfig):
            concurrency_key: str = "reserve"

    部署时改这里，或用环境变量覆盖（嵌套模型用双下划线，见 Settings 的
    ``env_nested_delimiter="__"``）：
      CONCURRENCY__RESERVE=2
      CONCURRENCY__SPACE_DYN=6
      CONCURRENCY__DYN_DETAIL=20

    调参注意：服务启动后所有调度器的**首次运行时间都是「现在」**，
    即首次部署 / 重启时全部爬虫会同时开跑；把某一项的并发调大前，
    先确认单核 CPU 与接口 P95 还有余量（dyn_detail 曾设 100 把单核打满）。
    """

    # ---- 各爬虫 worker 池并发（一个爬虫一项）----
    crawler_workers: int = 50  # 兜底：爬虫未声明 concurrency_key 时用这个
    # 动态详情：**默认 20（沿用旧版 DynDetailScrapyConfig.max_sem = 20，生产长期验证过的值）**。
    # 本次把并发集中到 ConcurrencyConfig 时一度写成 100，后果很明确：生产重启后所有调度器
    # 「首次运行时间=现在」同时开跑，dyn_detail 一个爬虫就把单核 CPU 打满，
    # HTTP 接口劣化到 5~25s（数分钟后首批任务消化完才恢复）。
    # 调优建议：
    #   - 20~40 是安全区间；超过 40 需要同时看容器 CPU（单核容器尤其危险）与接口 P95；
    #   - 该爬虫是「gRPC 拉取 + 大 JSON 解析 + MySQL upsert」的混合负载，
    #     并发上去先打满的是 CPU 而不是带宽，所以不要按「网络 IO 型爬虫」调大；
    #   - 不要和其它爬虫同时调大：所有调度器共享同一个进程与同一次「首次立即执行」。
    dyn_detail: int = 20
    rm_following: int = 20  # 取关对象
    reserve: int = 1  # 预约抽奖（串行：接口要求顺序执行）
    topic: int = 1  # 话题抽奖（串行）
    lottery_api: int = 20  # 抽奖 API（LotteryApiRobot 的 DYN/RESERVE 两个实例共用）
    refresh_lot_db: int = 1  # 刷新抽奖库（串行，避免与采集抢 DB）
    sams_club: int = 1  # 山姆分类
    sams_club_spu: int = 1  # 山姆 SPU 详情
    bili_live: int = 20  # 直播监控
    get_others_lot_dyn: int = 1  # 第三方抽奖动态（初始 1，运行时按阶段切到下面两项）

    # ---- 第三方抽奖采集：按阶段切换（GetOthersLotDynRobot._set_concurrency）----
    space_dyn: int = 20
    judge_dyn: int = 20

    # ---- 代理抓取：同时流式读取的源数量 / 入库并发 / 补充源内部并发 ----
    proxy_source_fetch: int = 32
    proxy_source_ingest: int = 10
    proxy_source_supplement: int = 12


class CrawlerRuntimeConfig(BaseModel):
    """**所有爬虫的运行行为**统一入口（原先在每个 ``CrawlerConfig`` 子类里重复抄一遍）。

    各爬虫的 Config 子类只需在「与这里不同」时显式覆盖，未声明的字段跟随本配置。
    env 覆盖（嵌套双下划线）：``CRAWLER_RUNTIME__MAX_RETRIES=3``
    """

    # 单个任务最大超时（秒），None = 不限制。
    # 默认不限制：多数爬虫的单任务时长不可预估（刷新抽奖库 / 直播监控 / 分页抓取），
    # 强行给 300s 会把原本跑得通的长任务打断。需要限制的爬虫在自己的 Config 里写值。
    worker_max_timeout: int | None = None
    # 抓取失败 / 超时是否重新入队（两者独立）
    requeue_on_fetch_fail: bool = False
    requeue_on_timeout: bool = True
    # 失败任务最大重试次数，-1 = 无限重试
    max_retries: int = -1
    # 是否打印超时 / 抓取异常日志（默认只记超时以外的异常，减少噪音）
    log_timeout_error: bool = False
    log_error: bool = True
    # 任务失败后的等待时间（秒）
    worker_error_delay: int = 300


class GetOthersLotDynConfig(BaseModel):
    """第三方抽奖动态获取配置 —— 作为 Settings 的嵌套子模型，
    部署时可整个对象填 JSON，也可逐字段用双下划线覆盖。
    例如：get_others_lot='{"dyn_time_limit":864000}'
    或：  get_others_lot__dyn_time_limit=864000

    注意：**并发数不在这里**，统一由 ``ConcurrencyConfig``（CONFIG.concurrency）控制。
    """

    spare_time: int = 86400 * 7  # 多久以前的动态不再获取(秒)，默认7天
    get_dyn_interval: int = 86400 * 2  # 两次完整采集的最小间隔(秒)，默认2天
    dyn_time_limit: int = 1728000  # 返回数据的时间范围(秒)，默认20天
    max_user_list_size: int = 40  # 用户列表最大长度（硬上限，超出会被裁剪）
    remove_check_days: int = 7  # 剔除用户时检查最近N天内抓到的抽奖数
    # 统计口径：用户自己空间里转发的原动态命中抽奖数，<= 此值即剔除（N天内抓不到才会命中 0）
    min_valid_lot_threshold: int = 1
    # 单轮最多补充的新用户数，不得超过 max_user_list_size，避免语义不一致
    supplement_max_per_round: int = 40
    hot_lot_dyn_count: int = 5  # 从评论区挖掘用户时选取的高互动动态数量
    hot_lot_dyn_days: int = 7  # 高互动动态的时间范围(天)
    # 用户列表为空且无法从评论区补充时使用的默认用户 uid 列表
    default_user_uids: list[int] = [
        319857159,
        14017844,
        1234306704,
        31497476,
        2147319744,
        410550169,
        646686238,
        71583520,
        279262754,
        275744172,
        332793152,
        1397970246,
        3493092200024392,
        386051299,
        381282283,
        20958956,
        1869690859,
        1183157743,
        4586734,
        1741486871,
        266223923,
        646327721,
        1803790683,
        8544035,
        1123570168,
        3494361237031878,
        223712517,
        480906586,
        1040677577,
        471565816,
        343104186,
        2204166,
        290089137,
        1855888816,
        691536906,
        6477408,
        1586295950,
        1369967146,
        40809204,
        1992326018,
        649407876,
        256316789,
        143412922,
        1278208248,
        499023056,
        565064296,
        693445761,
        7538278,
    ]


class CrawlerPushConfig(BaseModel):
    """爬虫「开始 / 结束」推送开关，**默认全关**。

    两个触发点分别配置，值为逗号分隔的 ``crawler_name``（与
    ``BackgroundServiceName`` 枚举值一致），支持 ``*`` / ``all`` 通配全部爬虫。

    环境变量示例::

        CRAWLER_PUSH_START=GET_OTHERS_LOT_DYN,GET_DYN
        CRAWLER_PUSH_END=*

    留空（默认）表示不推送。推送只做通知，失败不影响爬虫任务本身。
    """

    start: str = ""
    end: str = ""

    @staticmethod
    def _parse(raw: str) -> set[str]:
        return {item.strip() for item in (raw or "").split(",") if item.strip()}

    @classmethod
    def _match(cls, raw: str, crawler_name: str) -> bool:
        names = cls._parse(raw)
        if not names:
            return False
        if "*" in names or "all" in names:
            return True
        return crawler_name in names

    def is_start_enabled(self, crawler_name: str) -> bool:
        """该爬虫开始执行时是否需要推送"""
        return self._match(self.start, crawler_name)

    def is_end_enabled(self, crawler_name: str) -> bool:
        """该爬虫结束（正常 / 终止 / 失败）时是否需要推送"""
        return self._match(self.end, crawler_name)


class LLMApiConfig(BaseModel):
    """OpenAI 兼容 API 配置 —— 作为 Settings 的嵌套子模型列表元素，
    部署时可整个列表填 JSON，也可逐字段用双下划线覆盖。
    例如：llm_apis='[{"base_url":"https://...","model_name":"gpt-3.5","token":"sk-xxx"}]'
    或：  llm_apis__0__base_url=https://...  llm_apis__0__model_name=gpt-3.5
    """

    base_url: str = ""
    model_name: str = ""
    token: str = ""
    # 每秒最大请求数（InMemoryRateLimiter），默认 0.5 即每 2 秒最多 1 次请求
    requests_per_second: float = 0.5


class LLMApiConfigPatch(BaseModel):
    """LLMApiConfig 的部分更新模型：仅传入需要修改的字段，未传字段保持原值。

    用于内部接口 PATCH /llm/config/{index}：因读取接口返回的 token 已脱敏，
    局部更新时可只提交需要变更的字段（如仅换 token / 仅调速率），无需回传明文 token。
    """

    base_url: str | None = None
    model_name: str | None = None
    token: str | None = None
    requests_per_second: float | None = None


class ServiceRole(StrEnumAutoDoc):
    """同一份代码承担的服务角色（由环境变量 SERVICE_ROLE 指定）。

    - ``crawler``：爬虫 + 查询 + 控制面 + MQ 消费 + RPC 服务端，默认值（不设变量时
      完全等同改造前）。开发环境想「不自动爬」用 ``IS_DEV=1`` 关掉定时任务，**不要**换这个角色：
      生产也是它，MQ 消费者与 ``FastapiApp.rpc.*`` 服务端只在这个角色里存在；
    - ``query``：纯查询，只挂只读路由，且**不做** alembic 迁移 / 不启动调度器 /
      不做 MQ 投递重试 / 不执行任何写库动作，可安全指向只读账号或生产只读副本；
    - ``api``：历史遗留的「只提供接口、不爬取」角色。对外路由与 ``crawler`` 基本一致
      （含 ``*ScrapyStatus`` 等控制面），但**不做** alembic 迁移 / 不启动调度器 /
      **不消费 MQ（因此没有 RPC 服务端）** / 不初始化 Milvus。已不再被任何 compose 使用
      （曾有 dev 容器 ``be-bilibili-crawler-api``，会让 ``FastapiApp.rpc.*`` 一直超时，已废弃），
      保留仅为兼容旧启动方式。

    三个角色共用同一份源码与镜像层：``crawler`` / ``api`` 走同一个入口 ``main.py``
    （api 由环境变量在启动期分支），只有 ``query`` 用独立入口 ``query_main.py``；
    镜像 target 见 Dockerfile.mono 的 ``crawler`` / ``crawler-query``。
    """

    CRAWLER = "crawler"
    QUERY = "query"
    API = "api"


class Settings(PushNotifySettingsMixin, BaseSettings):
    # 服务角色，见 ServiceRole。查询容器在 compose 里设 SERVICE_ROLE=query
    SERVICE_ROLE: ServiceRole = ServiceRole.CRAWLER
    MYSQL_HOST: str
    MYSQL_PORT: str
    MYSQL_USER: str
    MYSQL_PASSWORD: str
    REDIS_HOST: str
    REDIS_PORT: int
    REDIS_PWD: str
    RABBITMQ_HOST: str
    RABBITMQ_PORT: str
    RABBITMQ_USER: str
    RABBITMQ_PASSWORD: str
    # 推送渠道配置（message_config）、服务标识（SERVER_NAME / SERVER_ADDRESS）、
    # 渠道默认端点（pushme_url / pushplus_url）等共用项由 PushNotifySettingsMixin 提供，
    # 此处只覆盖本服务有差异的默认值（服务名）。
    SERVER_NAME: str = "be-bilibili-crawler"
    UNIDBG_HOST: str
    UNIDBG_PORT: str
    V2RAY_HOST: str
    V2RAY_PORT: str
    LLAMA_HOST: str  # llama 开个网络服务
    LLAMA_PORT: str
    PROXY_SERVER: str
    MILVUS_HOST: str
    MILVUS_PORT: str
    # be-message-service 地址：**已不再用于启动探活 / 运行时调用**（2.70.1）。
    # crawler 与它只通过 RabbitMQ 弱耦合（推送走 message_exchange / message.push，
    # message-service 不在线时消息留在队列里等它消费），所以这两个键保留仅为兼容
    # 现有 .env / compose（删除会导致带这些键的环境变量校验失败），代码里请勿再使用。
    MESSAGE_SERVICE_HOST: str = "be-message-service"
    MESSAGE_SERVICE_PORT: str = "18739"
    SHOW_LOG: int = 0
    # SHOW_LOG=0 时 stdout sink 的日志级别（DEBUG/INFO/WARNING/ERROR/CRITICAL），
    # 语义与 be-message-service 的 LOG_LEVEL 一致，可用同名环境变量覆盖。
    # 默认 WARNING：只保留告警与错误，压住日志量；
    # 需要看轮次 / 阶段等业务节点时，临时设 LOG_LEVEL=INFO 即可
    # （这些节点已在代码中按语义定级，不再需要用 CRITICAL 假扮）。
    LOG_LEVEL: str = "WARNING"
    IS_DEV: int = 1  # 默认开发环境

    # ===== GitHub 系资源（raw.githubusercontent.com / cdn.jsdelivr.net）的镜像 =====
    # 通用设置：任何需要通过 GitHub 拉文件的模块都读这里，不要各自硬编码镜像域名。
    # 语义与 RPA-Browser 的 settings.github_proxy_urls 一致：
    # - None 表示直连原始 URL（可与镜像混用）；
    # - 其余元素作为前缀拼在原 URL 之前（如 "https://ghproxy.net/" + raw URL）；
    # - **顺序即尝试顺序**，列表会持续追加新镜像，失效的留在原位置即可。
    # 可用同名环境变量覆盖（JSON 数组），例如：
    #   GITHUB_PROXY_URLS='[null, "https://ghproxy.net/"]'
    github_proxy_urls: list[str | None] = Field(
        default_factory=lambda: [
            None,
            "https://gh-proxy.com/",
            "https://gh-proxy.org/",
            "https://ghproxy.net/",
            "https://gh.b52m.cn/",
            "https://github.xxlab.tech/",
            "https://ghproxy.053000.xyz/",
            "https://proxy.yaoyaoling.net/",
            "https://gh.aaa.team/",
            "https://g.blfrp.cn/",
            "https://github.chenc.dev/",
            "https://github.dpik.top/",
            "https://github.cnxiaobai.com/",
            "https://gh.padao.fun/",
            "https://ghproxy.sakuramoe.dev/",
            "https://30006000.xyz/",
            "https://gh.monlor.com/",
            "https://ghp.keleyaa.com/",
            "https://ghproxy.mirror.skybyte.me/",
            "https://fastgit.cc/",
            "https://xiaomo-station.top/",
            "https://github-proxy.lixxing.top/",
            "https://gh.shiina-rimo.cafe/",
            "https://gh.idayer.com/",
            "https://gh.996986.xyz/",
            "https://gitproxy.mrhjx.cn/",
            "https://getgit.love8yun.eu.org/",
            "https://ghm.078465.xyz/",
            "https://gh.ddlc.top/",
            "https://git.yylx.win/",
            "https://gh.198962.xyz/",
            "https://proxy.baguoyuyan.com/",
            "https://ghproxy.imciel.com/",
            "https://jiashu.1win.eu.org/",
            "https://git.820828.xyz/",
            "https://gh.1k.ink/",
            "https://github.ihnic.com/",
            "https://ghpxy.hwinzniej.top/",
            "https://github.mlmle.cn/",
            "https://gp.871201.xyz/",
            "https://github.zzrbk.xyz/",
            "https://ghproxy.cxkpro.top/",
            "https://gh.catmak.name/",
            "https://ghproxy.xzhouqd.com/",
            "https://hub.ddayh.com/",
            "https://kenyu.ggff.net/",
            "https://gh.halonice.com/",
            "https://gh.nxnow.top/",
            "https://github.boringhex.top/",
            "https://github.crdz.eu.org/",
            "https://github.lsdfxdk.nyc.mn/",
            "https://github.ednovas.xyz/",
            "https://tvv.tw/",
            "https://ggg.clwap.dpdns.org/",
            "https://github.788787.xyz/",
            "https://github.tianrld.top/",
            "https://gh.chjina.com/",
            "https://github.1ms.xx.kg/",
            "https://git.951959483.xyz/",
            "https://github.880824.xyz/",
            "https://gh.chalin.tk/",
            "https://gh.noki.icu/",
            "https://www.5555.cab/",
            "https://y.whereisdoge.work/",
            "https://gh.xxooo.cf/",
            "https://github-proxy.memory-echoes.cn/",
            "https://free.cn.eu.org/",
            "https://github.geekery.cn/",
            "https://ghps.cc/",
            "https://gitproxy.127731.xyz/",
            "https://gh.con.sh/",
            "https://down.npee.cn/",
            "https://git.669966.xyz/",
            "https://ghfile.geekertao.top/",
            "https://ghproxy.cn/",
            "https://git.40609891.xyz/",
            "https://ghproxy.monkeyray.net/",
            "https://gitproxy1.127731.xyz/",
            "https://hub.gitmirror.com/",
            "https://ghproxy.xiaopa.cc/",
            "https://ghproxy.cfd/",
            "https://github.tbedu.top/",
            "https://ghproxy.vansour.top/",
            "https://gh.wsmdn.dpdns.org/",
            "https://gh.bugdey.us.kg/",
            "https://github.bullb.net/",
            "https://github.ruojian.space/",
            "https://code-hub-hk.freexy.top/",
            "https://gitproxy.197545.xyz/",
            "https://ghproxy.mf-dust.dpdns.org/",
            "https://gh.jasonzeng.dev/",
            "https://j.1lin.dpdns.org/",
            "https://j.1win.ggff.net/",
            "https://git.zeas.cc/",
            "https://gh.echofree.xyz/",
            "https://github.kkproxy.dpdns.org/",
            "https://ghb.nilive.top/",
            "https://github.cn86.dev/",
            "https://github.oterea.top/",
            "https://ghproxy.fangkuai.fun/",
            "https://gh-proxy.net/",
            "https://gitproxy.click/",
            "https://ghproxy.cc/",
            "https://cf.ghproxy.cc/",
            "https://proxy.atoposs.com/",
            "https://github-proxy.com/",
            "https://github.zjzzy.cloudns.org/",
            "https://ghfast.top/",
            "https://gp.zkitefly.eu.org/",
            "https://gh.jdck.fun/",
            "https://git.tangbai.cc/",
            "https://ghproxy.1888866.xyz/",
            "https://github.limoruirui.com/",
            "https://gh.llkk.cc/",
            "https://gh.39.al/",
        ]
    )
    # 单个 GitHub 资源的镜像候选上限：列表有 100+ 项，逐个试会把一轮抓取拖死，
    # 按配置顺序最多尝试 N 个（含 None 直连），命中即停。
    github_proxy_max_attempts: int = 6
    # 单个镜像候选的请求超时上限（秒）。代理源里的「历史归档」分支单文件能到 5MB/50 万行，
    # 不再按行数截断后必须完整下载，12s 测下来只能下 1~1.5MB，故放宽到 45s。
    # 单源最坏 10s(V2Ray) + 6×45s = 280s，仍小于单源任务上限 300s。
    github_proxy_timeout: float = 45.0

    # ===== 并发统一配置（所有并发数都从这里取，见 ConcurrencyConfig）=====
    concurrency: ConcurrencyConfig = ConcurrencyConfig()
    # ===== 爬虫运行行为统一配置（超时 / 重试 / 退避 / 日志开关）=====
    crawler_runtime: CrawlerRuntimeConfig = CrawlerRuntimeConfig()

    # ===== 第三方抽奖动态获取 =====
    get_others_lot: GetOthersLotDynConfig = GetOthersLotDynConfig()

    # ===== 运行期可变状态目录（不入库，落本地文件）=====
    # 目前用于：
    # - 爬虫推送开关（crawler_push_switch.json）：接口改完立即生效、重启后保留；
    # - 云端 LLM 配置（llm_apis.json）：人工保存的列表重启后仍生效，
    #   事实来源「runtime 文件 > .env」，见 Service/llm_service/config_store.py。
    # 空值 = 代码根目录下的 runtime/；容器里由 compose 指向挂载出来的 /app/runtime。
    RUNTIME_DIR: str = ""

    # ===== 爬虫开始 / 结束推送开关（默认全关，见 CrawlerPushConfig）=====
    # 直接用顶层环境变量，避免嵌套模型需要 env_nested_delimiter：
    #   CRAWLER_PUSH_START=GET_OTHERS_LOT_DYN,GET_DYN
    #   CRAWLER_PUSH_END=*
    CRAWLER_PUSH_START: str = ""
    CRAWLER_PUSH_END: str = ""

    # ===== 动态详情增量爬取（见 Service/GrpcModule/GrpcSrc/getDynDetail.py）=====
    # 库里的断点 rid 小于该值时的「兜底起始 rid」，从这条往后继续补。
    # 注意：它只用于「断点过小」的兜底，不用于「空库 / 断点过旧」——那两种情况下
    # 从几十万 rid 前开始补是重活且没意义，会改用抽奖动态反查 rid 重新定位。
    # 重定位的回补窗口（天），默认 0 = 跟随 COLD_ARCHIVE_KEEP_DAYS_DYNDETAIL（180 天）：
    # - 库里本来只保留这么多天的热数据（更早的整月已被冷归档删库），
    #   所以「清空」等价于「最近 keep_days 天全丢」，回补整个窗口即可；
    # - 回补起点取「窗口内**最早**一条抽奖动态」反查的 rid，而不是最新动态的 rid，
    #   否则只会补最近几天，窗口内其余月份仍然缺失；
    # - 想只补近期（如 30 天）时单独调小，不会顺带改变归档删库窗口。
    DYN_DETAIL_BACKFILL_DAYS: int = 0
    DYN_DETAIL_START_RID: int = 339283794

    # 外部 LLM API 列表（按顺序优先使用，全部失败后回退到本地 lmstuidio）
    llm_apis: list[LLMApiConfig] = []
    # 管理端「测试」按钮的探测超时（秒）：裸 ChatOpenAI 真实问一句 1+1=?，不重试。
    # 偏大是因为部分推理模型首个 token 很慢（实测 deepseek-v4.1-flash 需 1~3 分钟）：
    # 45s 能覆盖多数 chat 模型，超时会如实返回「请求超时」而不误判为配置错误。
    llm_test_timeout: float = 45.0
    # 管理端「获取模型列表」拉上游 `GET {base_url}/models` 的超时（秒）。
    # 该接口只返回元数据、不跑推理，通常 1~3 秒内返回，20s 足够并避免页面长时间转圈。
    llm_models_timeout: float = 20.0

    sqlalchemy_logging: bool = False
    # 冷数据归档（导出 csv.gz + 可选删库）的统一落盘根目录。
    # 空值表示用代码根目录下的 scripts/database/dyn_backup（本地直跑时的默认）。
    # 容器部署时由 docker-compose.yml 指向挂载出来的宿主机目录，
    # 避免备份产物写进 bind mount 的源码树。
    # 目录结构：{DYN_BACKUP_DIR}/{表名}/{年}/{YYYY_MM}_{表名}.csv.gz
    # （最里层是标准 CSV，外层 gzip；与历史归档文件格式保持一致）
    DYN_BACKUP_DIR: str = ""
    # ===== 冷数据归档保留窗口（见 Service/DataArchive/cold_data_archiver.py）=====
    # 每个表只归档「整月都早于 now - keep_days」的数据，窗口内的热数据不导出、不删除。
    # 三张表是同一批动态原始 JSON 的三份存储，统一用同一套窗口与产物格式：
    # - dyndetail 的 bilidyndetail（dynData）：动态详情，抽奖信息导出与查询都靠它；
    # - biliopusdb 的 t_lotdyninfo（rawJsonStr）：第三方抽奖动态，抽奖列表按时间范围查；
    # - biliopusdb 的 t_lotuserspaceresp（spaceRespJson）：用户空间分页响应，只服务
    #   本轮断点续爬，轮次结束后即死数据（体积最大）。
    COLD_ARCHIVE_KEEP_DAYS_DYNDETAIL: int = 180
    COLD_ARCHIVE_KEEP_DAYS_LOTDYNINFO: int = 180
    COLD_ARCHIVE_KEEP_DAYS_LOTUSERSPACERESP: int = 180
    # 归档文件校验通过后是否真的删库（0=只归档不删，1=归档后删）。先在 0 观察几轮再开。
    COLD_ARCHIVE_DELETE_AFTER: int = 1
    # ===== 表空间维护（归档删库后的 InnoDB 空洞回收，见 Service/DataArchive/tablespace_maintenance.py）=====
    # DELETE 只把页标记可复用，.ibd 不会缩小；每月单独跑一次整表重建（ALTER TABLE ... FORCE）
    # 把空洞还给操作系统。两个阈值必须**同时**满足才重建，避免每天归档后都重写整表：
    # - 空洞占比：data_free / (data_length + data_free)；
    # - 空洞绝对值：小表占比高也不值得重建，避免频繁重写。
    TABLESPACE_MAINTENANCE_MIN_FREE_PCT: float = 20.0
    TABLESPACE_MAINTENANCE_MIN_FREE_GB: float = 1.0
    # ===== 抽奖信息导出（业务视图，见 Service/DataArchive/lot_dyn_info_export.py）=====
    # 和上面的「冷数据归档」是同一个每日任务里的两个独立步骤，职责不要混：
    # - 冷数据归档：导出**原始表行**（dynData / rawJsonStr / spaceRespJson 原文），
    #   归档包回读校验通过后按 COLD_ARCHIVE_KEEP_DAYS_* 删库，目的是回收磁盘；
    # - 抽奖信息导出：只读 bilidyndetail.dynData，解析成发布者 / 链接 / 抽奖类型
    #   等业务字段写成给人看、给词云用的 CSV，**全程不删任何数据**，也不参与
    #   「能不能删库」的判断（删库只看归档包自己的行数校验）。
    #   它是只读步骤，但会在归档之前执行：一旦它抛异常，当天就不会继续删库。
    # 本项只控制「每次跑往回重刷几个整月」（不影响文件切分粒度）：
    # - 只导**已过完的整月**（当月还在写入，导了也是半截），一个月一个文件：
    #   {DYN_BACKUP_DIR}/lot_dyn_info/{年}/{YYYY_MM}_lot_dyn_info.csv.gz
    # - 同月重跑是幂等覆盖，所以默认 2 个月用于漏跑 / 延迟入库的自愈；
    #   调大只增加整月 dynData 的解析耗时，不会多产出文件；调成 1 则漏跑超过
    #   一个月时需要用 --lot-dyn-months 手工回补。
    LOT_DYN_INFO_MONTHS: int = 2
    # FastStream（RabbitMQ 消费者 / 发布者）内部日志级别：DEBUG / INFO / WARNING / ERROR / CRITICAL，
    # 通过环境变量 FASTSTREAM_LOG_LEVEL 覆盖。仅影响 FastStream 框架自身的标准库日志，
    # 不影响本项目 loguru 业务日志（MQ_logger 等）。
    faststream_log_level: str = "WARNING"

    # ===== MQ 消费者「等待回退（backoff）」重试策略 =====
    # 见 Service/MQ/base/MQClient/consume_backoff.py 与 bili_common.core.backoff。
    # 背景：faststream 的 nack 默认 requeue=True，失败后消息被立即重投，
    # 无等待策略时会形成热循环（生产日志里曾把 curl 句柄 / 日志文件句柄打爆）。
    # 单条消息在进程内最多处理几次（含首次）；<=0 表示不限次数、由 max_time 兜底
    mq_consume_max_tries: int = 0
    # 单条消息最长处理时间（秒）；<=0 表示不限
    mq_consume_max_time: float = 600.0
    # 指数等待底数：第 n 次失败等待 = factor * base ** n
    mq_consume_backoff_base: float = 2.0
    mq_consume_backoff_factor: float = 3.0
    # 单次等待上限（秒），避免指数增长后等待过久
    mq_consume_backoff_max_wait: float = 300.0

    # ===== MQ 消费者「消息确认超时预算」=====
    # 见 Service/MQ/base/MQClient/consume_budget.py。
    # 背景：入库队列在「等槽位 / 等 LLM 冷却」期间刻意保持消息未确认，而 RabbitMQ 的
    # consumer_timeout（默认 30 分钟）会在超时后强制关闭 channel 并重投消息 ——
    # 表现为「莫名重投 + 重复消费」，且 channel 已关，ack 也失败。
    # 因此每条消息都带一个自「收到消息」起算的预算，耗尽时主动 nack(requeue=True) 交还队列。
    # 收到消息后允许占用未确认状态的最长时长（秒）；应与 broker 的 consumer_timeout 对齐
    mq_consume_ack_timeout: float = 1800.0
    # 留给「释放锁 + nack + 日志」的收尾余量（秒），实际预算 = ack_timeout - ack_reserve
    mq_consume_ack_reserve: float = 60.0

    # ===== 抽奖提取队列的并发消费 =====
    # 每条队列在本进程内的 subscriber 数：mq_controller 已注册 1 个，其余由
    # PrizeExtract.register_parallel_consumers 补齐，作为 competing consumers
    # 分散投递（global_qos=True 下共用 channel 级 prefetch 额度）。1 表示维持单消费者。
    mq_prize_extract_consumers: int = 3
    # 单进程内同时调用大模型提取的上限（在途 LLM 请求闸）；
    # 提高 prefetch / 消费者数后用它兜底，避免一次把大量请求打到上游。
    mq_prize_extract_concurrency: int = 32

    @property
    def is_query_only(self) -> bool:
        """当前进程是否为「纯查询」角色（不做 DDL / 调度 / 写库）。

        读写分离下它也决定 RPC 服务端的归属：query 角色只提供 READ 类 RPC
        （`FastapiApp.rpc.*` 的只读取数），见 controller/v1/mq/rpc_server.py。
        """
        return self.SERVICE_ROLE is ServiceRole.QUERY

    @property
    def is_crawler(self) -> bool:
        """当前进程是否为完整爬虫角色（可写库 / 消费 MQ / 提供 WRITE 类 RPC）。"""
        return self.SERVICE_ROLE is ServiceRole.CRAWLER

    @property
    def allow_active_fetch(self) -> bool:
        """读路径上是否允许「主动刷新 / 回写」（会真的访问 B 站等外部站点并写库）。

        只有 ``crawler`` 角色为 True；``query`` 与 ``api`` 都是 False ——
        api 角色虽然可以写库（``Add*`` 是显式写入），但**读接口不该顺带发起网络抓取**，
        否则本地只想要一个「有接口」的爬虫，却会因为翻列表而对外发请求。
        """
        return self.is_crawler

    @property
    def is_api_only(self) -> bool:
        """当前进程是否为「只提供接口、不爬取」角色（不做 DDL / 调度 / MQ / Milvus）。

        注意：本角色**允许写库**（与 ``query`` 不同），否则 Add* 系列接口在本地开发时没有意义；
        「不爬取」靠的是不启动调度器 / 不消费 MQ，而不是把写能力关掉。
        """
        return self.SERVICE_ROLE is ServiceRole.API

    model_config = SettingsConfigDict(
        env_file=(
            os.path.join(_current_dir, ".env.fastapi.prod"),
            os.path.join(_current_dir, ".env.fastapi.dev"),
        ),
        # 嵌套模型可用「双下划线」逐字段覆盖，例如：
        #   CONCURRENCY__SPACE_DYN=6
        #   GET_OTHERS_LOT__DYN_TIME_LIMIT=864000
        # 不加这个开关时，嵌套字段只能整体填 JSON，逐字段 env 会被忽略。
        env_nested_delimiter="__",
    )


settings = Settings()


class ModelName(StrEnumAutoDoc):
    """模型名称枚举，集中管理所有使用的 LLM / 嵌入模型名"""

    #: llama.cpp 的 models.ini section 名（embedding 预设），走 `CONFIG.llama_url`
    TEXT_EMBEDDING_MULTILINGUAL_E5_BASE = "text-embedding-multilingual-e5-base"
    #: llama.cpp 的 models.ini section 名（对话模型，unsloth/Qwen3.5-0.8B-GGUF Q4_K_M）；
    #: 上游换了 GGUF 仓库后 section 名从 `Qwen3.5-0.8B-SpectralQuant-Q4_K_M` 改为现值，
    #: 必须与 `docker_vol/llama/models.ini` 完全一致，否则 `/v1/models` 里找不到该模型
    QWEN3_5_0_8_Q4_K_M_GGUF = "Qwen3.5-0.8B-Q4_K_M"


class PlaywrightUserDir(StrEnumAutoDoc):
    """
    枚举类，用于表示不同的用户数据目录
    """

    zhihu = "zhihu"


# region 基本配置
class PushMeChannel(BaseModel):
    """PushMe 推送渠道配置（pydantic）"""

    url: str = "https://push.i-i.me"
    token: str = ""


class PushPlusChannel(BaseModel):
    """PushPlus 推送渠道配置（pydantic）"""

    url: str = "http://www.pushplus.plus/send"
    token: str = ""


class PushNotifyConfig(BaseModel):
    """统一推送渠道配置（pydantic），数据全部从全局 message_config 提取。"""

    pushme: PushMeChannel = PushMeChannel()
    pushplus: PushPlusChannel = PushPlusChannel()

    @classmethod
    def from_message_config(cls, cfg: PushChannelConfig) -> "PushNotifyConfig":
        # 渠道端点缺省值单一来源：settings.pushme_url / pushplus_url
        # （由 bili_common 的 PushNotifySettingsMixin 提供，可被同名环境变量覆盖）
        return cls(
            pushme=PushMeChannel(
                url=cfg.pushme_url or settings.pushme_url,
                token=cfg.pushme_key,
            ),
            pushplus=PushPlusChannel(
                url=cfg.push_plus_url or settings.pushplus_url,
                token=cfg.push_plus_token,
            ),
        )


class DataBaseConfig:
    @dataclass
    class _MYSQL:
        # 字段默认值仅在 dataclass 装饰器应用时求值一次，为避免在模块导入时
        # 就把 settings 固化进 URI（导致运行时 CLI 覆盖无法生效），这里把 URI
        # 的拼装延迟到 __post_init__，每次实例化都重新读取当前 settings。
        _base_url: str = ""
        _pwd: str = ""
        _user: str = ""
        proxy_db_URI: str = ""
        bili_db_URI: str = ""
        bili_reserve_URI: str = ""
        get_other_lot_URI: str = ""
        dyn_detail_URI: str = ""
        sams_club_URI: str = ""

        def __post_init__(self):
            self._base_url = f"{settings.MYSQL_HOST}:{settings.MYSQL_PORT}"
            self._pwd = settings.MYSQL_PASSWORD
            self._user = settings.MYSQL_USER
            self.proxy_db_URI = f"mysql+aiomysql://{self._user}:{self._pwd}@{self._base_url}/proxy_db?charset=utf8mb4&autocommit=true"
            self.bili_db_URI = (
                # 话题抽奖
                f"mysql+aiomysql://{self._user}:{self._pwd}@{self._base_url}/bilidb?charset=utf8mb4&autocommit=true"
            )
            self.bili_reserve_URI = f"mysql+aiomysql://{self._user}:{self._pwd}@{self._base_url}/bili_reserve?charset=utf8mb4&autocommit=true"
            self.get_other_lot_URI = f"mysql+aiomysql://{self._user}:{self._pwd}@{self._base_url}/biliopusdb?charset=utf8mb4&autocommit=true"
            self.dyn_detail_URI = f"mysql+aiomysql://{self._user}:{self._pwd}@{self._base_url}/dyndetail?charset=utf8mb4&autocommit=true"
            self.sams_club_URI = f"mysql+aiomysql://{self._user}:{self._pwd}@{self._base_url}/samsclub?charset=utf8mb4&autocommit=true"

    @dataclass
    class _REDISINFO:
        def __init__(self, db: int = 15):
            self.host: str = settings.REDIS_HOST
            self.port: int = settings.REDIS_PORT
            self.db: int = db
            self.pwd: str = settings.REDIS_PWD

        def toUrl(self):
            return f"redis://:{self.pwd}@{self.host}:{self.port}/{self.db}"

    MYSQL = _MYSQL()
    proxyRedis = _REDISINFO(15)
    proxySubRedis = _REDISINFO(6)
    lotDataRedisObj = _REDISINFO(2)
    ipInfoRedisObj = _REDISINFO(2)
    getOtherLotRedis = _REDISINFO(15)
    commStorageRedis = _REDISINFO(0)
    rabbitmqCacheRedis = _REDISINFO(0)


class SqlAlchemyConfig:
    """业务连接池配置 - 供 router/service 等业务使用。

    连接预算（务必与 MySQL 的 max_connections 对齐，否则会打满服务端连接数并报
    ``(1040, 'Too many connections')``）：
      本服务按 (dburl, is_crawler) 维度各建一个池，当前 6 个库 × 2 类池 = 12 个池，
      单池连接上限 = pool_size + max_overflow。
      业务池 6 × (10 + 5) = 90，爬虫池 6 × (20 + 10) = 180，
      合计上限 270，小于 MySQL 侧 max_connections=500
      （见 docker_vol/mysql_data/conf.d/custom_mysql.cnf），
      余量留给 alembic 迁移、其它服务与人工排查。
      原配置为单池 100 + 40，12 个池理论上限 1680，远超服务端上限，
      并发一上来必然触发 1040。
    核对方式：MySQL 侧执行 ``SHOW STATUS LIKE 'Threads_connected';``
      （稳态不应贴近 max_connections），必要时再按上面的公式等比调整。
    """

    engine_config = dict(
        echo=settings.sqlalchemy_logging,
        pool_size=10,
        max_overflow=5,
        # 池被占满时的最长等待时间（秒），超时抛 TimeoutError 交由重试包装处理，
        # 避免请求无限期挂起把上层并发槽位也拖住
        pool_timeout=30,
        pool_use_lifo=True,
        # 取出连接前先 ping 一下，避免拿到 MySQL 已关闭的陈旧连接（错误码 2013）
        pool_pre_ping=True,
        # 连接回收周期，应小于 MySQL 的 wait_timeout(600s)，防止空闲连接被服务端断开
        pool_recycle=300,
    )
    session_config = dict(
        expire_on_commit=False,
        autoflush=False,
    )


class CrawlerSqlAlchemyConfig:
    """
    爬虫专用连接池配置 - 与业务连接池完全隔离
    即使爬虫并发高占用大量连接，也不会影响业务请求

    单池上限 = pool_size + max_overflow。爬虫侧各爬虫 max_sem 多为 1~20
    （见 Service/BaseCrawler/config.py），单库 30 连接足够覆盖峰值，
    同时把「12 个池的总连接数」压在 MySQL max_connections 之内（见 SqlAlchemyConfig 说明）。
    """

    engine_config = dict(
        echo=settings.sqlalchemy_logging,
        pool_size=20,
        max_overflow=10,
        # 池被占满时的最长等待时间（秒），超时抛 TimeoutError 交由重试包装处理
        pool_timeout=30,
        pool_use_lifo=True,
        # 取出连接前先 ping 一下，避免拿到 MySQL 已关闭的陈旧连接（错误码 2013）
        pool_pre_ping=True,
        # 连接回收周期，应小于 MySQL 的 wait_timeout(600s)，防止空闲连接被服务端断开
        pool_recycle=300,
    )
    session_config = dict(
        expire_on_commit=False,
        autoflush=False,
    )


class RabbitMQConfig:
    class QueueName(StrEnumAutoDoc):
        ipv6_change = "ipv6_change"

    host = settings.RABBITMQ_HOST
    port = settings.RABBITMQ_PORT
    user = settings.RABBITMQ_USER
    pwd = settings.RABBITMQ_PASSWORD
    protocol = "amqp"
    queue_name_list = [x.value for x in QueueName]
    broker_url = f"{protocol}://{user}:{pwd}@{host}:{port}/?heartbeat=180"


# endregion


class _CONFIG:
    root_dir = os.path.dirname(os.path.abspath(__file__))  # 代码的根目录
    V2ray_proxy = f"http://{settings.V2RAY_HOST}:{settings.V2RAY_PORT}"
    llama_url = f"http://{settings.LLAMA_HOST}:{settings.LLAMA_PORT}/v1"
    pushnotify = PushNotifyConfig.from_message_config(
        settings.message_config
    )  # 推送设置
    database = DataBaseConfig()
    # 并发 / 爬虫运行行为的统一配置（CrawlerType、抓取模块都从这里读）
    concurrency = settings.concurrency
    crawler_runtime = settings.crawler_runtime
    my_ipv6_addr = settings.PROXY_SERVER
    unidbg_addr = f"http://{settings.UNIDBG_HOST}:{settings.UNIDBG_PORT}"
    RabbitMQConfig = RabbitMQConfig()
    sql_alchemy_config = SqlAlchemyConfig()
    crawler_sql_alchemy_config = CrawlerSqlAlchemyConfig()
    playwright_user_dir = PlaywrightUserDir
    _pc_ua = UserAgent(platforms=["desktop", "tablet"])
    _mobile_ua = UserAgent(platforms=["mobile"])

    # 爬虫配置注册表由 Service.BaseCrawler.config 模块自管理（不写入全局 Settings）。
    # 这里仅做转发，保持 CONFIG.get_crawler_config 作为统一入口。

    def get_crawler_config(self, config_cls: type[CrawlerConfig]) -> CrawlerConfig:
        """按爬虫的 ``Config`` 类返回集中管理的配置实例。

        配置实例来自 ``Service.BaseCrawler.config`` 的注册表，
        未知类（如测试用的临时子类）回退为默认实例。
        """
        return get_crawler_config(config_cls)

    @property
    def rand_ua(self):
        return self._pc_ua.random

    @property
    def rand_ua_mobile(self):
        return self._mobile_ua.random

    @property
    def custom_proxy(self):
        return {"http": self.my_ipv6_addr, "https": self.my_ipv6_addr}

    @property
    def custom_v2ray_proxy(self):
        return {"http": self.V2ray_proxy, "https": self.V2ray_proxy}


CONFIG = _CONFIG()

if __name__ == "__main__":
    print(settings)
