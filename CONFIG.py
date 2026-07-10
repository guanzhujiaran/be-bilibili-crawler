import os
from dataclasses import dataclass
from enum import Enum, StrEnum
from fake_useragent import UserAgent
from pydantic import BaseModel, ConfigDict, Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy import AsyncAdaptedQueuePool

from Service.BaseCrawler.config import (
    CrawlerConfig,
    GetProxyMethodsConfig,
    DynDetailScrapyConfig,
    GetRmFollowingListV2Config,
    ReserveScrapyRobotConfig,
    TopicRobotConfig,
    LotteryApiRobotConfig,
    RefreshBiliLotDatabaseConfig,
    SamsClubCrawlerConfig,
    SamsClubSPUDetailCrawlerConfig,
    BiliLiveCrawlerConfig,
)

_current_dir = os.path.dirname(os.path.abspath(__file__))


class PushChannelConfig(BaseModel):
    """全局推送渠道配置（pydantic 模型）。

    字段与 message-service 的 PushChannelConfig 保持一致，以便原样序列化后
    经 RabbitMQ 投递给 message-service 解析，未知字段一律忽略。
    """

    model_config = ConfigDict(extra="ignore")

    # 一言（随机句子）
    hitokoto: bool = True

    # Bark
    bark_push: str = ""
    bark_archive: str = ""
    bark_group: str = ""
    bark_sound: str = ""
    bark_icon: str = ""
    bark_level: str = ""
    bark_url: str = ""

    # 钉钉机器人
    dd_bot_secret: str = ""
    dd_bot_token: str = ""

    # 飞书机器人
    fskey: str = ""

    # go-cqhttp
    gobot_url: str = ""
    gobot_qq: str = ""
    gobot_token: str = ""

    # Gotify
    gotify_url: str = ""
    gotify_token: str = ""
    gotify_priority: int = 0

    # iGot
    igot_push_key: str = ""

    # Server 酱
    push_key: str = ""

    # PushDeer
    deer_key: str = ""
    deer_url: str = ""

    # Synology Chat
    chat_url: str = ""
    chat_token: str = ""

    # PushPlus
    push_plus_token: str = ""
    push_plus_url: str = ""
    push_plus_user: str = ""
    push_plus_template: str = "html"
    push_plus_channel: str = "wechat"
    push_plus_webhook: str = ""
    push_plus_callbackurl: str = ""
    push_plus_to: str = ""

    # 微加机器人
    we_plus_bot_token: str = ""
    we_plus_bot_receiver: str = ""
    we_plus_bot_version: str = "pro"

    # Qmsg 酱
    qmsg_key: str = ""
    qmsg_type: str = ""

    # 企业微信
    qywx_origin: str = ""
    qywx_am: str = ""
    qywx_key: str = ""

    # Telegram
    tg_bot_token: str = ""
    tg_user_id: str = ""
    tg_api_host: str = ""
    tg_proxy_auth: str = ""
    tg_proxy_host: str = ""
    tg_proxy_port: str = ""

    # 智能微秘书
    aibotk_key: str = ""
    aibotk_type: str = ""
    aibotk_name: str = ""

    # SMTP 邮件
    smtp_server: str = ""
    smtp_ssl: str = "false"
    smtp_email: str = ""
    smtp_password: str = ""
    smtp_name: str = ""

    # PushMe
    pushme_key: str = ""
    pushme_url: str = ""

    # Chronocat
    chronocat_qq: str = ""
    chronocat_token: str = ""
    chronocat_url: str = ""

    # 自定义 Webhook
    webhook_url: str = ""
    webhook_body: str = ""
    webhook_headers: str = ""
    webhook_method: str = ""
    webhook_content_type: str = ""

    # Ntfy
    ntfy_url: str = ""
    ntfy_topic: str = ""
    ntfy_priority: str = "3"
    ntfy_token: str = ""
    ntfy_username: str = ""
    ntfy_password: str = ""
    ntfy_actions: str = ""

    # WxPusher
    wxpusher_app_token: str = ""
    wxpusher_topic_ids: str = ""
    wxpusher_uids: str = ""


class GetOthersLotDynConfig(BaseModel):
    """第三方抽奖动态获取配置 —— 作为 Settings 的嵌套子模型，
    部署时可整个对象填 JSON，也可逐字段用双下划线覆盖。
    例如：get_others_lot='{"space_dyn_concurrency":3}'
    或：  get_others_lot__space_dyn_concurrency=3
    """
    space_dyn_concurrency: int = 1     # 空间动态并发数
    judge_dyn_concurrency: int = 1     # 抽奖判定并发数
    spare_time: int = 86400 * 7            # 多久以前的动态不再获取(秒)，默认7天
    get_dyn_interval: int = 86400 * 2      # 两次完整采集的最小间隔(秒)，默认2天
    dyn_time_limit: int = 1728000      # 返回数据的时间范围(秒)，默认20天
    max_user_list_size: int = 20       # 用户列表最大长度
    remove_check_days: int = 14        # 剔除用户时检查最近N天内的抽奖数
    min_valid_lot_threshold: int = 10  # 低于此阈值的用户将被剔除
    hot_lot_dyn_count: int = 5        # 从评论区挖掘用户时选取的高互动动态数量
    hot_lot_dyn_days: int = 7         # 高互动动态的时间范围(天)
    # 用户列表为空且无法从评论区补充时使用的默认用户 uid 列表
    default_user_uids: list[int] = [
        319857159, 14017844, 1234306704, 31497476, 2147319744,
        410550169, 646686238, 71583520, 279262754, 275744172,
        332793152, 1397970246, 3493092200024392, 386051299, 381282283,
        20958956, 1869690859, 1183157743, 4586734, 1741486871,
        266223923, 646327721, 1803790683, 8544035, 1123570168,
        3494361237031878, 223712517, 480906586, 1040677577, 471565816,
        343104186, 2204166, 290089137, 1855888816, 691536906,
        6477408, 1586295950, 1369967146, 40809204, 1992326018,
        649407876, 256316789, 143412922, 1278208248, 499023056,
        565064296, 693445761, 7538278,
    ]


class LLMApiConfig(BaseModel):
    """OpenAI 兼容 API 配置 —— 作为 Settings 的嵌套子模型列表元素，
    部署时可整个列表填 JSON，也可逐字段用双下划线覆盖。
    例如：llm_apis='[{"base_url":"https://...","model_name":"gpt-3.5","token":"sk-xxx"}]'
    或：  llm_apis__0__base_url=https://...  llm_apis__0__model_name=gpt-3.5
    """
    base_url: str = ""
    model_name: str = ""
    token: str = ""


class Settings(BaseSettings):
    MYSQL_HOST: str
    MYSQL_PORT: str
    MYSQL_USER: str
    MYSQL_PASSWORD: str
    REDIS_HOST: str
    REDIS_PORT: str
    REDIS_PWD: str
    RABBITMQ_HOST: str
    RABBITMQ_PORT: str
    RABBITMQ_USER: str
    RABBITMQ_PASSWORD: str
    # 全局推送渠道配置（PushChannelConfig pydantic 模型，与 message-service / rpa-browser 共用同一份）
    message_config: PushChannelConfig = PushChannelConfig()
    UNIDBG_HOST: str
    UNIDBG_PORT: str
    V2RAY_HOST: str
    V2RAY_PORT: str
    LLAMA_HOST: str  # llama 开个网络服务
    LLAMA_PORT: str
    PROXY_SERVER: str
    MILVUS_HOST: str
    MILVUS_PORT: str
    # message-service 健康检查地址
    MESSAGE_SERVICE_HOST: str = "message-service"
    MESSAGE_SERVICE_PORT: str = "18739"

    model_config = SettingsConfigDict(
        env_file=(
            os.path.join(_current_dir, ".env.fastapi.prod"),
            os.path.join(_current_dir, ".env.fastapi.dev"),
        )
    )
    SHOW_LOG: int = 0
    IS_DEV: int = 1  # 默认开发环境

    # ===== 第三方抽奖动态获取 =====
    get_others_lot: GetOthersLotDynConfig = GetOthersLotDynConfig()

    # 外部 LLM API 列表（按顺序优先使用，全部失败后回退到本地 lmstuidio）
    llm_apis: list[LLMApiConfig] = []

    # ===== 爬虫通用配置（集中管理，部署时用双下划线环境变量覆盖） =====
    # 例如：DYN_DETAIL__MAX_SEM=30  ->  settings.dyn_detail.max_sem
    proxy: GetProxyMethodsConfig = GetProxyMethodsConfig()
    dyn_detail: DynDetailScrapyConfig = DynDetailScrapyConfig()
    rm_following: GetRmFollowingListV2Config = GetRmFollowingListV2Config()
    reserve: ReserveScrapyRobotConfig = ReserveScrapyRobotConfig()
    topic: TopicRobotConfig = TopicRobotConfig()
    lottery_api: LotteryApiRobotConfig = LotteryApiRobotConfig()
    refresh_lot: RefreshBiliLotDatabaseConfig = RefreshBiliLotDatabaseConfig()
    sams_club: SamsClubCrawlerConfig = SamsClubCrawlerConfig()
    sams_club_spu: SamsClubSPUDetailCrawlerConfig = SamsClubSPUDetailCrawlerConfig()
    bili_live: BiliLiveCrawlerConfig = BiliLiveCrawlerConfig()


settings = Settings()


class ModelName(StrEnum):
    """模型名称枚举，集中管理所有使用的 LLM / 嵌入模型名"""

    TEXT_EMBEDDING_MULTILINGUAL_E5_BASE = "text-embedding-multilingual-e5-base"
    QWEN3_5_0_8_Q4_K_M_GGUF = "Qwen3.5-0.8B-SpectralQuant-Q4_K_M"


class PlaywrightUserDir(StrEnum):
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
    url: str = "https://www.pushplus.plus/send"
    token: str = ""


class PushNotifyConfig(BaseModel):
    """统一推送渠道配置（pydantic），数据全部从全局 message_config 提取。"""

    pushme: PushMeChannel = PushMeChannel()
    pushplus: PushPlusChannel = PushPlusChannel()

    @classmethod
    def from_message_config(cls, cfg: PushChannelConfig) -> "PushNotifyConfig":
        return cls(
            pushme=PushMeChannel(
                url=cfg.pushme_url or "https://push.i-i.me",
                token=cfg.pushme_key,
            ),
            pushplus=PushPlusChannel(
                url=cfg.push_plus_url or "https://www.pushplus.plus/send",
                token=cfg.push_plus_token,
            ),
        )


class DataBaseConfig:
    @dataclass
    class _MYSQL:
        _base_url: str = f"{settings.MYSQL_HOST}:{settings.MYSQL_PORT}"
        _pwd: str = settings.MYSQL_PASSWORD
        _user: str = settings.MYSQL_USER
        proxy_db_URI: str = (
            f"mysql+aiomysql://{_user}:{_pwd}@{_base_url}/proxy_db?charset=utf8mb4&autocommit=true"
        )
        bili_db_URI: str = (
            # 话题抽奖
            f"mysql+aiomysql://{_user}:{_pwd}@{_base_url}/bilidb?charset=utf8mb4&autocommit=true"
        )
        bili_reserve_URI: str = (
            f"mysql+aiomysql://{_user}:{_pwd}@{_base_url}/bili_reserve?charset=utf8mb4&autocommit=true"
        )
        get_other_lot_URI: str = (
            f"mysql+aiomysql://{_user}:{_pwd}@{_base_url}/biliopusdb?charset=utf8mb4&autocommit=true"
        )
        dyn_detail_URI: str = (
            f"mysql+aiomysql://{_user}:{_pwd}@{_base_url}/dyndetail?charset=utf8mb4&autocommit=true"
        )
        sams_club_URI: str = (
            f"mysql+aiomysql://{_user}:{_pwd}@{_base_url}/samsclub?charset=utf8mb4&autocommit=true"
        )

    @dataclass
    class _REDISINFO:
        def __init__(self, db: int = 15):
            self.host: str = settings.REDIS_HOST
            self.port: str = settings.REDIS_PORT
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
    # 业务连接池配置 - 供 router/service 等业务使用，保留足够的连接处理外部请求
    engine_config = dict(
        echo=False,
        pool_size=100,
        max_overflow=40,
        pool_use_lifo=True,
    )
    session_config = dict(
        expire_on_commit=False,
        autoflush=False,
    )


class CrawlerSqlAlchemyConfig:
    """
    爬虫专用连接池配置 - 与业务连接池完全隔离
    即使爬虫并发高占用大量连接，也不会影响业务请求
    池子大小与业务池相同，但使用独立的连接池实例
    """
    engine_config = dict(
        echo=False,
        pool_size=100,  # 与业务池大小相同，独立使用
        max_overflow=40,
        pool_use_lifo=True,
    )
    session_config = dict(
        expire_on_commit=False,
        autoflush=False,
    )


class RabbitMQConfig:
    class QueueName(Enum):
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
    pushnotify = PushNotifyConfig.from_message_config(settings.message_config)  # 推送设置
    database = DataBaseConfig()
    my_ipv6_addr = settings.PROXY_SERVER
    unidbg_addr = f"http://{settings.UNIDBG_HOST}:{settings.UNIDBG_PORT}"
    RabbitMQConfig = RabbitMQConfig()
    sql_alchemy_config = SqlAlchemyConfig()
    crawler_sql_alchemy_config = CrawlerSqlAlchemyConfig()
    playwright_user_dir = PlaywrightUserDir
    _pc_ua = UserAgent(platforms=["desktop", "tablet"])
    _mobile_ua = UserAgent(platforms=["mobile"])

    # 爬虫配置注册表：config 类 -> 由全局 settings 注入的实例（集中管理）
    _crawler_config_registry: dict[type[CrawlerConfig], CrawlerConfig] = {
        GetProxyMethodsConfig: settings.proxy,
        DynDetailScrapyConfig: settings.dyn_detail,
        GetRmFollowingListV2Config: settings.rm_following,
        ReserveScrapyRobotConfig: settings.reserve,
        TopicRobotConfig: settings.topic,
        LotteryApiRobotConfig: settings.lottery_api,
        RefreshBiliLotDatabaseConfig: settings.refresh_lot,
        SamsClubCrawlerConfig: settings.sams_club,
        SamsClubSPUDetailCrawlerConfig: settings.sams_club_spu,
        BiliLiveCrawlerConfig: settings.bili_live,
    }

    def get_crawler_config(self, config_cls: type[CrawlerConfig]) -> CrawlerConfig:
        """按爬虫的 ``Config`` 类返回集中管理的配置实例。

        未知类（如测试用的临时子类）回退为默认实例。
        """
        return self._crawler_config_registry.get(config_cls, config_cls())

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
