# -*- coding: utf-8 -*-
# 由redis和mysql实现数据的统一
import asyncio
import random
import time
from dataclasses import dataclass
from functools import reduce
from json import JSONDecodeError
from ssl import SSLError

import curl_cffi
from exceptiongroup import ExceptionGroup
from pydantic import BaseModel, Field
from httpx import (
    ProxyError,
    RemoteProtocolError,
    ConnectError,
    ConnectTimeout,
    ReadTimeout,
    ReadError,
    InvalidURL,
    WriteError,
    NetworkError,
    TooManyRedirects,
    HTTPError,
)
from python_socks._errors import (
    ProxyConnectionError,
    ProxyTimeoutError,
    ProxyError as SocksProxyError,
)
from socksio import ProtocolError

from CONFIG import CONFIG
from Service.GrpcModule.Models.CustomRequestErrorModel import (
    Request412Error,
    Request352Error,
    RequestKnownError,
    RequestProxyNetworkError,
    RequestProxyResponseError,
    RequestProxyUnavailableError,
    RequestUnknownError,
)
from Service.GrpcModule.Models.RabbitmqModel import VoucherInfo
from Service.MQ.base.MQClient.BiliLotDataPublisher import BiliLotDataPublisher
from log.base_log import request_with_proxy_logger, Voucher352_logger
from Models.AntiRisk.Bili.WebCookie import CookieWrapper
from Utils.代理.SealedRequests import my_async_httpx
from Utils.代理.mdoel.ProxyTier import ProxyTier
from Utils.代理.mdoel.RequestConf import RequestConf
from Utils.代理.mdoel.request_context import (
    excluded_proxy_keys,
    note_failed_proxy,
    resolve_tier,
)
from Utils.代理.数据库操作.ProxyCommOp import get_available_proxy
from Utils.代理.数据库操作.ProxyEvent import (
    handle_proxy_412,
    handle_proxy_352,
    handle_proxy_request_fail,
    handle_proxy_succ,
)
from Utils.代理.数据库操作.ProxyHealth import (
    ProxyFailureKind,
    proxy_health_store,
    proxy_prefix_of,
)
from Utils.代理.数据库操作.SqlAlcheyObj.ProxyModel import ProxyTab
from Utils.代理.数据库操作.async_proxy_op_alchemy_mysql_ver import SQLHelper, to_proxy_key
from bili_common.core.log_mask import mask_text, response_brief

#: 代理鉴权失败（407）在响应层而不是异常层出现，需要单独判一次
_PROXY_AUTH_FAILED_STATUS = 407
#: 专线闸门告警的最小间隔（秒）：冷却 / 超配额期间每个请求都刷一行会把日志打爆
_LOCAL_GATE_WARN_INTERVAL = 60.0


def _clip(text: str, limit: int) -> str:
    """日志 / 异常消息用的截断：保留开头（url、参数名），避免整段 body 落盘。"""
    return text if len(text) <= limit else text[:limit] + "...(truncated)"


def _curl_code_of(err: BaseException) -> int | None:
    """尽量从 curl_cffi 异常里取 curl 错误码（不同版本字段位置不同）。"""
    for attr in ("code", "errno", "curl_code"):
        value = getattr(err, attr, None)
        if isinstance(value, int):
            return value
    args = getattr(err, "args", ())
    for value in args:
        if isinstance(value, int):
            return value
    return None


def classify_proxy_failure(err: BaseException) -> ProxyFailureKind:
    """把异常分流成「网络层 / 风控层 / 上游响应 / 未知」。

    网络层（含 curl 28/7/35、407、连接失败、超时）→ 扣分 + 连续失败 + 10~30min 冷却；
    风控层（412/352）由调用方在专门的 except 里处理，不走这里。
    """
    if isinstance(err, (Request412Error, Request352Error)):
        return ProxyFailureKind.RISK_CONTROL
    # 注意顺序：RequestProxyNetworkError 继承 RequestProxyResponseError（兼容旧 except），
    # 必须先判子类，否则 407 / curl 网络错误会被误归到「上游响应」。
    if isinstance(err, RequestProxyNetworkError):
        return ProxyFailureKind.NETWORK
    if isinstance(err, RequestProxyResponseError):
        return ProxyFailureKind.RESPONSE
    return ProxyFailureKind.NETWORK


@dataclass(frozen=True, slots=True)
class LocalExitGate:
    """自建出口（专线）闸门的判定结果。

    ``proxy is None`` 时 ``reason`` 说明是哪一道闸门挡下来的（未配置 / 本任务内已失败 /
    前缀风控冷却 / 超 QPS 配额），供调用方决定「降级到池」还是「专线独占直接失败」，
    并让异常文案能带出通道名与出口地址。
    """

    proxy: ProxyTab | None = None
    reason: str = ""
    channel: str = ""
    qps: float = 0.0


class LocalExitGateStats(BaseModel):
    """自建出口（专线）闸门计数：说明「业务请求最后到底走了哪条出口」。

    专线被风控冷却时会静默降级到免费代理池（权重 10 : 1000），
    只看零散的 ``_warn_local_gate`` 无法判断「现在还有多少请求走专线」，
    因此这里按累计次数给出占比，由周期日志输出（冷却期间为 WARNING）。
    """

    local_used: int = Field(default=0, description="实际走自建出口的次数")
    pool_used: int = Field(default=0, description="实际走代理池的次数")
    blocked_risk_cooldown: int = Field(default=0, description="专线因风控冷却被挡次数")
    blocked_quota: int = Field(default=0, description="专线因 QPS 配额被挡次数")
    blocked_excluded: int = Field(default=0, description="专线因本任务内已失败被挡次数")
    degraded_to_pool: int = Field(default=0, description="专线不可用后降级到代理池的次数")
    local_failed: int = Field(default=0, description="专线请求本身失败（网络层）次数")

    @property
    def total(self) -> int:
        return self.local_used + self.pool_used

    @property
    def local_share(self) -> float:
        return 0.0 if self.total <= 0 else self.local_used / self.total

    def to_log_str(self) -> str:
        if self.total <= 0:
            return "出口占比：暂无统计样本"
        return (
            f"出口占比（累计 {self.total} 次）：专线 {self.local_share:.1%}"
            f"（{self.local_used} 次）/ 代理池 {1 - self.local_share:.1%}"
            f"（{self.pool_used} 次）；专线被挡：风控冷却 {self.blocked_risk_cooldown} / "
            f"QPS 配额 {self.blocked_quota} / 本任务内已失败 {self.blocked_excluded}；"
            f"降级到池 {self.degraded_to_pool} 次；专线请求失败 {self.local_failed} 次"
        )


class RequestWithProxy:
    def __init__(self):
        self.use_p_dict_flag = False
        self.channel = "bili"
        self.log = request_with_proxy_logger
        # 代理请求超时：免费代理大多在 5~10s 内要么连通要么直接失败，
        # 30s 只会让坏代理白占 worker，改成 15s 后失败能更快被打分淘汰。
        self.timeout = 15
        self.available_proxy_timeout = 15
        self.task_set = set()
        # 专线闸门（冷却 / 超配额）告警的限流时间戳
        self._local_gate_warn_ts = 0.0
        # 专线闸门计数 / 状态迁移（冷却开始与结束各打一条 WARNING）
        self._local_gate_stats = LocalExitGateStats()
        self._local_exit_cooling = False
        self._local_exit_degraded_in_cooldown = 0
        # 出口占比周期日志的时间戳（monotonic）
        self._local_gate_share_ts = 0.0

    def _timeshift(self, timestamp):
        local_time = time.localtime(timestamp)
        realtime = time.strftime("%Y-%m-%d %H:%M:%S", local_time)
        return realtime

    async def get_one_rand_proxy(self, tier: ProxyTier = ProxyTier.FAST) -> ProxyTab:
        return await SQLHelper.select_proxy("rand", channel=self.channel, tier=tier)

    # region 取代理
    def _local_ipv6_proxy(self) -> ProxyTab:
        """自建 IPv6 出口（专线）的代理行。

        它是配置项而不是代理池成员：MySQL 侧不会写它（``_is_local_ipv6_proxy`` 跳过）。
        以前这里伪造成 ``score=10000 / success_times=10000``，等于对外宣称
        「历史表现完美的代理」，再叠加健康度豁免，结果是**永不降权、永不冷却**；
        现在如实填 0：它的健康度按所在 /64 前缀统计（与普通代理同一套前缀规则）。
        """
        return ProxyTab(
            **{
                "proxy_id": 1,
                "proxy": CONFIG.custom_proxy,
                "status": 0,
                "update_ts": 0,
                "score": 0,
                "add_ts": 0,
                "success_times": 0,
                "zhihu_status": 0,
                "computed_proxy_str": CONFIG.my_ipv6_addr,
            }
        )

    def _warn_local_gate(self, msg: str) -> None:
        """专线闸门告警限流：冷却 / 超配额期间每个请求都会走到这里，不能刷屏。"""
        now = time.monotonic()
        if now - self._local_gate_warn_ts < _LOCAL_GATE_WARN_INTERVAL:
            self.log.debug(msg)
            return
        self._local_gate_warn_ts = now
        self.log.warning(msg)

    def _note_local_exit_gate(self, cooling: bool, detail: str = "") -> None:
        """专线闸门状态迁移：冷却**开始 / 结束各打一条 WARNING**。

        历史问题：专线被风控冷却后业务请求静默降级到免费池，日志里只有零散的
        ``_warn_local_gate``，无法判断「现在到底走哪条出口、为什么走这条」。
        这里把「降级策略 + 当前出口占比」一并写进一条日志，恢复时再写一条。
        """
        if cooling == self._local_exit_cooling:
            return
        self._local_exit_cooling = cooling
        cfg = CONFIG.proxy_health
        if cooling:
            if cfg.local_ipv6_cooling_pool_fallback:
                policy = (
                    "降级策略：强制专线的请求改走代理池"
                    f"（local_ipv6_cooling_pool_fallback=true，抽样权重 "
                    f"local_ipv6_weight={cfg.local_ipv6_weight} : pool_weight={cfg.pool_weight}）；"
                    "local_exit_only 请求不降级，直接失败交回队列"
                )
            else:
                policy = (
                    "降级策略：不降级（local_ipv6_cooling_pool_fallback=false），"
                    "专线请求直接失败交回队列等冷却结束"
                )
            self.log.warning(
                f"自建出口（专线）进入风控冷却：{detail or '前缀冷却生效'}；{policy}；"
                f"{self._local_gate_stats.to_log_str()}"
            )
            return
        self.log.warning(
            f"自建出口（专线）风控冷却结束，恢复使用；冷却期间降级到代理池 "
            f"{self._local_exit_degraded_in_cooldown} 次；"
            f"{self._local_gate_stats.to_log_str()}"
        )
        self._local_exit_degraded_in_cooldown = 0

    async def _report_exit_share(self) -> None:
        """周期输出「专线 / 代理池」出口占比，并顺带探测冷却状态迁移。

        冷却状态靠一次 ``is_prefix_cooling``（单次 HGET）懒探测：每
        ``local_exit_gate_report_interval`` 秒一次，既有明确的起止 WARNING，
        又不会在热路径上每个请求都查 Redis。
        """
        interval = max(30.0, float(CONFIG.proxy_health.local_exit_gate_report_interval))
        now = time.monotonic()
        if now - self._local_gate_share_ts < interval:
            return
        self._local_gate_share_ts = now
        local_addr = (CONFIG.my_ipv6_addr or "").strip()
        cooling = self._local_exit_cooling
        if local_addr:
            proxy_key = to_proxy_key(CONFIG.custom_proxy) or local_addr
            prefix = proxy_prefix_of(proxy_key)
            try:
                cooling = await proxy_health_store.is_prefix_cooling(prefix)
            except Exception as e:  # noqa: BLE001 观测失败不能影响业务请求
                self.log.debug(f"读取自建出口冷却状态失败：{type(e).__name__} {e}")
                cooling = self._local_exit_cooling
            self._note_local_exit_gate(
                cooling,
                detail=f"前缀 {prefix} 处于冷却" if cooling else "",
            )
        message = (
            f"自建出口用量观测（每 {interval:.0f}s 一条）："
            f"{self._local_gate_stats.to_log_str()}；当前专线冷却中={cooling}"
        )
        # 冷却期间用 WARNING（需要持续可见），正常状态下降到 INFO，避免长期刷屏
        if cooling:
            self.log.warning(message)
        else:
            self.log.info(message)

    @staticmethod
    def _local_exit_quota(request_conf: RequestConf | None) -> tuple[str, float]:
        """本次请求该用的专线配额：``(通道名, QPS)``。

        专线独占请求（cookie / metadata 激活）走 ``local_exit_only_*`` 独立通道，
        业务请求仍走 ``local_ipv6_*``：两条通道在 Redis 里分开计数，
        激活类请求再多也不会把业务请求的额度抢走，反之亦然。
        """
        cfg = CONFIG.proxy_health
        if request_conf is not None and request_conf.local_exit_only:
            return cfg.local_exit_only_channel, cfg.local_exit_only_qps
        return cfg.local_ipv6_channel, cfg.local_ipv6_qps

    @staticmethod
    def _check_risk_cooldown(request_conf: RequestConf | None) -> bool:
        """本次请求是否仍受「自建出口前缀风控冷却」约束。

        ``ignore_risk_cooldown`` 是通道语义（这条链路按业务判断不受风控冷却影响），
        总闸 ``local_exit_ignore_risk_cooldown`` 可在风控异常时全局收回该豁免。
        """
        if request_conf is None or not request_conf.ignore_risk_cooldown:
            return True
        return not CONFIG.proxy_health.local_exit_ignore_risk_cooldown

    async def _acquire_local_exit(
        self, tier: ProxyTier, request_conf: RequestConf | None = None
    ) -> LocalExitGate:
        """自建出口（专线）闸门：未冷却 + 拿到独立 QPS 配额才放行。

        闸门按顺序判定（任一不通过就返回 ``proxy=None`` + 原因，由调用方决定兜底 / 快速失败）：

        1. 本任务内已经失败过（或其前缀失败过）→ 不再重试，同段换地址没有意义；
        2. 前缀处于风控冷却（412/352 记在 /64 上）→ 冷却期内一律不用，
           **但 ``ignore_risk_cooldown`` 的请求跳过这一道**（冷却只对业务通道有意义）；
        3. 独立通道 QPS 配额（专线独占走 ``local_exit_only_qps``，业务走 ``local_ipv6_qps``）。
        """
        cfg = CONFIG.proxy_health
        channel, qps = self._local_exit_quota(request_conf)
        local_addr = (CONFIG.my_ipv6_addr or "").strip()
        if not local_addr:
            return LocalExitGate(
                reason="自建出口未配置（CONFIG.my_ipv6_addr 为空）",
                channel=channel,
                qps=qps,
            )
        proxy = self._local_ipv6_proxy()
        proxy_key = to_proxy_key(proxy.proxy) or local_addr
        prefix = proxy_prefix_of(proxy_key)

        excluded = excluded_proxy_keys()
        if proxy_key in excluded or (
            prefix and prefix in {proxy_prefix_of(key) for key in excluded}
        ):
            self._local_gate_stats.blocked_excluded += 1
            self.log.debug(f"自建出口（前缀 {prefix}）本任务内已失败过，本次不再重试")
            return LocalExitGate(
                reason=f"自建出口（前缀 {prefix}）本任务内已失败过",
                channel=channel,
                qps=qps,
            )

        if prefix and self._check_risk_cooldown(request_conf):
            cooling = await proxy_health_store.is_prefix_cooling(prefix)
            # 冷却开始 / 结束各一条 WARNING（状态迁移才打，不刷屏）
            self._note_local_exit_gate(
                cooling, detail=f"前缀 {prefix} 处于风控冷却"
            )
            if cooling:
                self._local_gate_stats.blocked_risk_cooldown += 1
                self._warn_local_gate(
                    f"自建出口前缀 {prefix} 处于风控冷却中，本次不使用"
                    f"（不换地址绕 412/352，等冷却自然到期）"
                )
                return LocalExitGate(
                    reason=f"自建出口前缀 {prefix} 处于风控冷却中",
                    channel=channel,
                    qps=qps,
                )

        if not await proxy_health_store.acquire_qps(
            channel,
            qps,
            cfg.local_ipv6_qps_wait,
        ):
            self._local_gate_stats.blocked_quota += 1
            self._warn_local_gate(
                f"自建出口超出 QPS 配额（通道 {channel}，{qps} req/s），本次不使用"
            )
            return LocalExitGate(
                reason=f"自建出口通道 {channel} 超出 QPS 配额（{qps} req/s）",
                channel=channel,
                qps=qps,
            )
        return LocalExitGate(proxy=proxy, channel=channel, qps=qps)

    async def _pool_proxy(self, tier: ProxyTier) -> ProxyTab | None:
        """从代理池按档位取一条（池内已按前缀去重 / 配额）。"""
        proxy, _ = await get_available_proxy(
            tier=tier, exclude=excluded_proxy_keys()
        )
        return proxy

    async def _local_exit_state(self) -> tuple[bool, str]:
        """自建出口（专线）当前状态：``(是否可用, 人话原因)``。

        用于把「出口不可用」与「池子没货」两类故障在异常信息里彻底分开
        （历史文案把两者合成一句，线上只能看到「没有可用代理」，无法判断该修哪边）。
        """
        cfg = CONFIG.proxy_health
        local_addr = (CONFIG.my_ipv6_addr or "").strip()
        if not local_addr:
            return False, "自建出口未配置（CONFIG.my_ipv6_addr 为空）"
        proxy_key = to_proxy_key(CONFIG.custom_proxy) or local_addr
        prefix = proxy_prefix_of(proxy_key)
        if prefix and await proxy_health_store.is_prefix_cooling(prefix):
            return False, f"自建出口风控冷却中（前缀 {prefix}，冷却期内不换地址绕行）"
        return True, f"自建出口未取到（QPS 配额 {cfg.local_ipv6_qps}/s 已满或本任务内已失败）"

    async def _unavailable_error(
        self, tier: ProxyTier, *, local_tried: bool, extra: str = ""
    ) -> RequestProxyUnavailableError:
        """构造「取不到代理」异常：分别写清出口侧与池子侧的原因，并给出 reason 分类。"""
        local_ok, local_reason = await self._local_exit_state()
        counts = await proxy_health_store.get_counts()
        # 文案口径（2026-11-16 起）：爬虫默认走 slow 档，取免费池的默认口径是
        # 「随机取一条 status 可用的就用」—— EWMA / 冷却 / 在途只记账，不再是闸门。
        # 因此能走到这里，只可能是**池子真空**（或候选全被本任务的失败代理/前缀排除），
        # 不再写「候选均被在途配额 / EWMA 门槛挡掉」这类已经不会发生的措辞，
        # 那种文案会把运维引向错误的排查方向。
        pool_empty = (counts.prefix_hot + counts.prefix_warm) <= 0
        pool_reason = (
            "代理池真空（prefix_hot=0、prefix_warm=0，且没有 status 可用的行）"
            if pool_empty
            else (
                f"代理池分档还有 {counts.prefix_hot} 个 hot / {counts.prefix_warm} 个 warm 网段，"
                f"但本次随机取到的候选全部被「本任务内已失败的代理 / 前缀」排除"
                f"（prefix_cooling={counts.prefix_cooling}，仅观测用）"
            )
        )
        if not local_ok and pool_empty:
            reason = RequestProxyUnavailableError.REASON_BOTH
        elif not local_ok and local_tried:
            reason = RequestProxyUnavailableError.REASON_LOCAL_EXIT
        else:
            reason = RequestProxyUnavailableError.REASON_POOL_EMPTY
        message = (
            f"没有可用代理（tier={tier.value}，已排除 {len(excluded_proxy_keys())} 个失败代理）："
            f"{local_reason}；{pool_reason}"
        )
        if extra:
            message += f"；{extra}"
        return RequestProxyUnavailableError(
            message, tier=tier.value, reason=reason
        )

    @staticmethod
    def _local_exit_tag(request_conf: RequestConf) -> str:
        """专线独占请求的失败标记（非专线独占返回空串）。

        只做可观测性：日志 / 异常里带上 ``reason=local_exit_only`` 与出口地址，
        便于线上直接判断「是自建出口挂了还是本任务把它去重掉了」，
        不需要再去翻代理池的健康统计。
        """
        if not request_conf.local_exit_only:
            return ""
        exit_addr = (CONFIG.my_ipv6_addr or "").strip() or "<未配置>"
        return (
            f"（reason={RequestProxyUnavailableError.REASON_LOCAL_EXIT_ONLY}，"
            f"出口={exit_addr}，未降级到公共池 / 直连）"
        )

    def _local_exit_only_error(
        self, tier: ProxyTier, gate: LocalExitGate
    ) -> RequestProxyUnavailableError:
        """专线独占请求取不到自建出口：明确报错，**绝不**降级到代理池 / 直连。

        文案带上 reason=local_exit_only 与出口地址，便于线上直接判断该修出口还是改配置；
        告警沿用 ``_warn_local_gate`` 限流，避免冷却 / 超配额期间每个请求刷一行。
        """
        exit_addr = (CONFIG.my_ipv6_addr or "").strip() or "<未配置>"
        reason = RequestProxyUnavailableError.REASON_LOCAL_EXIT_ONLY
        message = (
            f"专线独占请求且自建出口不可用（reason={reason}，tier={tier.value}，"
            f"出口={exit_addr}，通道={gate.channel}，配额 {gate.qps} req/s）："
            f"{gate.reason}；按 local_exit_only 语义不降级到代理池、也不直连，"
            f"交由上层重建 / 重新入队"
        )
        self._warn_local_gate(message)
        return RequestProxyUnavailableError(
            message, tier=tier.value, reason=reason
        )

    async def _select_proxy(
        self, request_conf: RequestConf, tier: ProxyTier
    ) -> tuple[ProxyTab, bool]:
        """选一个代理并记账「本次出口是专线还是池」，返回 ``(代理行, 是否自建出口)``。

        记账口径只影响可观测性（周期 WARNING 的出口占比），不改变选取行为。
        """
        # 周期观测：出口占比 + 专线冷却状态（内部按 interval 节流，热路径只是一次比较）
        await self._report_exit_share()
        proxy, is_local = await self._select_proxy_impl(request_conf, tier)
        if is_local:
            self._local_gate_stats.local_used += 1
        else:
            self._local_gate_stats.pool_used += 1
        return proxy, is_local

    async def _select_proxy_impl(
        self, request_conf: RequestConf, tier: ProxyTier
    ) -> tuple[ProxyTab, bool]:
        """选一个代理，返回 ``(代理行, 是否自建出口)``。

        两条通道：

        - **自建出口（专线）**：独立通道 + 独立 QPS 配额；不参与「换 IP 绕 412/352」，
          冷却中直接不可用（见 ``_acquire_local_exit``）；
        - **真实代理池**：按 ``tier`` 取（fast 只用已验证的，slow 允许掺 cold 探测），
          池内按前缀去重 / 在途配额（见 ``select_random_proxy``）。

        ``is_use_custom_proxy`` 的请求强制走专线；池子空时才用专线兜底（同样受限流约束）。
        ``local_exit_only`` 是更强的语义：**只走自建出口**，闸门不通过就直接
        ``RequestProxyUnavailableError(reason=local_exit_only)``，不走池、也不直连
        （``local_ipv6_cooling_pool_fallback`` 对它无效）。
        池里没有合格代理时**不在请求里现场抓一轮**（那会把 worker 挂住几分钟）：
        抛 ``RequestProxyUnavailableError`` 让上层快速失败 / 延迟重入队，后台补池照常进行。
        """
        cfg = CONFIG.proxy_health

        if request_conf.is_use_custom_proxy or request_conf.local_exit_only:
            gate = await self._acquire_local_exit(tier, request_conf)
            if gate.proxy is not None:
                return gate.proxy, True
            if request_conf.local_exit_only:
                raise self._local_exit_only_error(tier, gate)
            if not cfg.local_ipv6_cooling_pool_fallback:
                raise await self._unavailable_error(
                    tier,
                    local_tried=True,
                    extra="按配置 local_ipv6_cooling_pool_fallback=false，不降级到代理池",
                )
            fallback = await self._pool_proxy(tier)
            if fallback is not None:
                self._local_gate_stats.degraded_to_pool += 1
                self._local_exit_degraded_in_cooldown += 1
                self._warn_local_gate(
                    "自建出口不可用（风控冷却 / 超出 QPS 配额），本次降级到代理池"
                )
                return fallback, False
            raise await self._unavailable_error(
                tier, local_tried=True, extra="强制专线的请求已尝试降级到代理池但池内无候选"
            )

        use_local = random.choices(
            [True, False],
            weights=[max(0, cfg.local_ipv6_weight), max(1, cfg.pool_weight)],
            k=1,
        )[0]
        if use_local:
            gate = await self._acquire_local_exit(tier, request_conf)
            if gate.proxy is not None:
                return gate.proxy, True

        proxy = await self._pool_proxy(tier)
        if proxy is not None:
            return proxy, False

        fallback = await self._acquire_local_exit(tier, request_conf)
        if fallback.proxy is not None:
            self._warn_local_gate(
                f"代理池无符合 tier={tier.value} 的代理，本次使用自建出口兜底"
                f"（配额 {fallback.qps} req/s）"
            )
            return fallback.proxy, True
        raise await self._unavailable_error(tier, local_tried=True)

    # endregion

    async def request_with_proxy(
        self,
        request_conf: RequestConf = RequestConf(),
        cookie_data: CookieWrapper | None = None,
        **kwargs,
    ) -> dict | list[dict] | curl_cffi.requests.models.Request:
        """
        :param request_conf:
        :param cookie_data:
        :param kwargs:
        :param request_conf: 请求配置（档位 / 是否强制走自建出口等）
        取代理与失败记账的口径见 ``_select_proxy`` / ``ProxyHealth``：
        健康度按前缀（IPv6 /64、IPv4 /24）判定，地址级只作日志明细。
        """
        tier = resolve_tier(request_conf)
        proxy, is_local = await self._select_proxy(request_conf, tier)
        proxy_key = to_proxy_key(proxy.proxy) or ""
        prefix = proxy_prefix_of(proxy_key)
        # 在途计数：
        # - 池代理：地址级 + 前缀级都记（fast 档 in_flight<2，
        #   同段 in_flight<prefix_inflight_limit，避免整段并发打满被整段封）；
        # - 自建出口：并发只由 QPS 配额控制，不记在途 —— 否则 15s 超时 × 3 req/s
        #   会让 prefix_inflight_limit 常驻打满，把专线自己锁死。
        track_flight = bool(proxy_key) and not is_local
        if track_flight:
            await proxy_health_store.acquire_in_flight(proxy_key)
            if prefix:
                await proxy_health_store.acquire_prefix_in_flight(prefix)
        try:
            return await self._dispatch_request(
                proxy=proxy,
                proxy_key=proxy_key,
                request_conf=request_conf,
                cookie_data=cookie_data,
                is_local=is_local,
                **kwargs,
            )
        finally:
            if track_flight:
                await proxy_health_store.release_in_flight(proxy_key)
                if prefix:
                    await proxy_health_store.release_prefix_in_flight(prefix)

    async def _dispatch_request(
        self,
        proxy: ProxyTab,
        proxy_key: str,
        request_conf: RequestConf,
        cookie_data: CookieWrapper | None = None,
        is_local: bool = False,
        **kwargs,
    ) -> dict | list[dict] | curl_cffi.requests.models.Request:
        is_use_custom_proxy: bool = request_conf.is_use_custom_proxy
        is_return_raw_response: bool = request_conf.is_return_raw_response
        origin = kwargs.get("headers", {}).get("origin", "https://www.bilibili.com")
        referer = kwargs.get("headers", {}).get("referer", "https://www.bilibili.com/")
        req_dict = False
        req_text = ""
        request_timeout = (
            self.timeout if is_use_custom_proxy else self.available_proxy_timeout
        )
        # 自建出口同样参与健康记账：它是「专线」，健康度记在所在 /64 前缀上
        # （MySQL 侧仍是 no-op，因为它不是 proxy_tab 成员）。
        # 例外：**专线独占请求**（cookie / metadata 激活）不写池健康 / 前缀冷却 ——
        # 它们的 412/352 不代表公共池网段质量，写进去只会污染池子，
        # 还把自建前缀打进 2h 冷却、连带拖累业务通道。
        # 「本任务内已失败」去重（note_failed_proxy）仍然保留，只影响本次任务的后续重试。
        account_health = not request_conf.local_exit_only
        pool_proxy = proxy
        # 专线独占请求的失败标记：日志 / 异常里能直接 grep 到 reason=local_exit_only 与出口地址
        local_tag = self._local_exit_tag(request_conf)

        try:
            # 双保险：既把超时透传给 curl_cffi，又在外层用 asyncio.timeout 兜底，
            # 保证单次请求不会超出 request_timeout（+2s 宽限）而长期挂死。
            # 之前只有 curl 自身的超时生效，一旦它没生效，请求会一直占着 worker
            # 直到 worker_max_timeout（300s）才被取消，期间代理也无法及时标记失败；
            # 这里超时抛出 TimeoutError 会走下面的网络错误分支，标记坏代理后抛出。
            async with asyncio.timeout(request_timeout + 2):
                req = await my_async_httpx.request(
                    **kwargs,
                    timeout=request_timeout,
                    proxies=proxy.proxy,
                )
            req_text = req.text
            # 407 = 代理需要认证：HTTP 层是「成功」的，但代理不可用，必须当作网络层失败
            if getattr(req, "status_code", None) == _PROXY_AUTH_FAILED_STATUS:
                raise RequestProxyNetworkError(
                    f"代理返回 407（需要认证，该代理不可用）\n{proxy.proxy}\n{kwargs}{local_tag}",
                    -407,
                    proxy_key=proxy_key,
                    curl_code=_PROXY_AUTH_FAILED_STATUS,
                )
            if "code" not in req_text and "bili" in str(
                req.url
            ):  # 如果返回的不是json那么就打印出来看看是什么
                # self.log.info(req_text.replace('\n', ''))
                ...
            if (
                '<div class="txt-item err-text">由于触发哔哩哔哩安全风控策略，该次访问请求被拒绝。</div>'
                in req_text
            ):
                raise Request412Error(req_text + local_tag, -412)
            if not is_return_raw_response:
                req_dict = req.json()
                if type(req_dict) is list:
                    if account_health and proxy:
                        await handle_proxy_succ(
                            proxy_tab=proxy,
                        )
                    return req_dict
                if type(req_dict) is not dict:
                    self.log.warning(f"请求获取的req_dict类型出错！{req_dict}")
                if (
                    (
                        req_dict.get("code") is None
                        or type(req_dict.get("code")) is not int
                        or req_dict == {"code": 5, "message": "Not Found"}
                    )
                    or req_dict.get("msg") == "system error"
                    and "bili" in req.url.host
                ):
                    raise RequestProxyResponseError(
                        "代理返回真实响应错误！"
                        f"\nresp={response_brief(req_dict)}"
                        f"\nreq={mask_text(_clip(str(kwargs), 500))}",
                        -500,
                    )

                if (
                    req_dict.get("code") == -412
                    or req_dict.get("code") == -352
                    or req_dict.get("code") == 65539
                ):
                    if cookie_data:
                        # 按「出口前缀」去重计数：同一个坏出口连续命中不会把整个 cookie 池废掉
                        cookie_data.note_352(proxy_prefix_of(proxy_key))
                    # 异常消息会被上层写进 ERROR 日志：只带 code/关键 id 与脱敏后的请求摘要，
                    # 不再拼整个响应体 / 完整 kwargs（里面有 cookie 头）。
                    err_msg = (
                        f"{req_dict.get('code')}报错,换个ip\t{proxy}\t"
                        f"{self._timeshift(time.time())}\t"
                        f"{response_brief(req_dict)}\t"
                        f"{mask_text(_clip(str(kwargs), 500))}\t"
                        f"{mask_text(_clip(str(req.headers), 300))}"
                        f"{local_tag}"
                    )
                    if req_dict.get("code") == 65539:
                        pass
                    if req_dict.get("code") == -412:
                        raise Request412Error(err_msg, -412)
                    elif req_dict.get("code") == -352:
                        voucher = req.headers.get("x-bili-gaia-vvoucher")
                        ua = req.request.headers.get("user-agent")
                        task = asyncio.create_task(
                            BiliLotDataPublisher.pub_bili_voucher(
                                body=VoucherInfo(
                                    voucher=voucher,
                                    ua=ua,
                                    generate_ts=int(time.time()),
                                    ck=cookie_data.ck.to_str() if cookie_data else "",
                                    origin=origin,
                                    referer=referer,
                                    ticket="",
                                    version="",
                                    session_id="",
                                )
                            )
                        )
                        self.task_set.add(task)
                        task.add_done_callback(self.task_set.discard)
                        raise Request352Error(err_msg, -352)
                    raise Request412Error(err_msg, req_dict.get("code"))
        except (Request412Error, Request352Error) as _err:
            if account_health and pool_proxy:
                match _err.code:
                    case -412:
                        await handle_proxy_412(
                            proxy_tab=pool_proxy,
                        )
                    case -352:
                        Voucher352_logger.warning(
                            f"代理{pool_proxy.proxy} 报错-352 被封禁\n"
                            f"{mask_text(_clip(str(kwargs), 500))}"
                        )
                        # HTTP 通道沿用既有语义：352 仍写回 status（412/352 的
                        # 表现与 refresh_proxy 复位节奏不变），只有 gRPC 通道改了记账口径。
                        await handle_proxy_352(
                            proxy_tab=pool_proxy, mark_db_invalid=True
                        )
                    case _:
                        await handle_proxy_request_fail(
                            proxy_tab=pool_proxy,
                        )
            note_failed_proxy(proxy_key)
            raise _err
        except curl_cffi.requests.exceptions.ImpersonateError as _err:
            # 客户端指纹（impersonate）问题：异常在 set_curl_options 阶段抛出，
            # 此时请求还未发出，代理根本没有参与，因此不能惩罚代理。
            # 同时按“已知可忽略错误”上抛，使其走重试而不是计入上层 3 次硬失败上限，
            # 避免因为一次随机指纹取错而让整个业务任务被跳过。
            self.log.warning(
                f"\n代理：{str(proxy)}\n请求时发生客户端指纹错误（与代理无关，不惩罚代理，直接重试）："
                f"\n{type(_err)}\n{_err}"
            )
            raise RequestKnownError(_err)
        except (
            TooManyRedirects,
            SSLError,
            JSONDecodeError,
            ProxyError,
            RemoteProtocolError,
            ConnectError,
            ConnectTimeout,
            HTTPError,
            ReadTimeout,
            ReadError,
            WriteError,
            InvalidURL,
            NetworkError,
            RequestProxyNetworkError,
            RequestProxyResponseError,
            ExceptionGroup,
            ProxyConnectionError,
            ProxyTimeoutError,
            SocksProxyError,
            ValueError,
            ProtocolError,
            curl_cffi.requests.exceptions.ConnectionError,
            curl_cffi.requests.exceptions.ProxyError,
            curl_cffi.requests.exceptions.SSLError,
            curl_cffi.requests.exceptions.Timeout,
            curl_cffi.requests.exceptions.HTTPError,
            TimeoutError,
        ) as _err:
            kind = classify_proxy_failure(_err)
            self.log.debug(
                f"\n代理：{str(proxy)}\n请求时发生网络错误（{kind.value}）："
                f"{type(_err)}\n{_err}"
            )
            if is_local:
                # 专线自身的网络失败单独计数：池子好坏与它无关，排障时要能分开看
                self._local_gate_stats.local_failed += 1
                self._warn_local_gate(
                    f"自建出口请求失败（网络层，{kind.value}）："
                    f"{type(_err).__name__}: {_err}"
                )
            if account_health and pool_proxy:
                await handle_proxy_request_fail(proxy_tab=pool_proxy, kind=kind)
            note_failed_proxy(proxy_key)
            raise RequestProxyNetworkError(
                f"{type(_err).__name__}: {_err}{local_tag}",
                -1,
                proxy_key=proxy_key,
                curl_code=_curl_code_of(_err),
            )
        except AttributeError as _err:
            self.log.exception(f"\n代理：{str(proxy)}\n请求时出错，一般错误：{_err}{local_tag}")
            if account_health and pool_proxy:
                await handle_proxy_request_fail(proxy_tab=pool_proxy)
            note_failed_proxy(proxy_key)
            raise RequestUnknownError(f"{_err}{local_tag}")
        except Exception as _err:
            self.log.exception(
                f"\n代理：{str(proxy)}\n未知请求错误！请求：\n{kwargs}"
                f"\n结束，报错了！"
                f"\n{type(_err)}"
                f"\n{_err}\n{req_text}{local_tag}"
            )
            if account_health and pool_proxy:
                await handle_proxy_request_fail(proxy_tab=pool_proxy)
            note_failed_proxy(proxy_key)
            raise RequestUnknownError(f"{_err}{local_tag}")
        if req_dict is False and is_return_raw_response is False:
            raise ValueError("请求返回的req_dict是False")
        if account_health and proxy:
            # self.log.critical(
            #     f'获取请求成功代理：{proxy.proxy}\n{kwargs.get("url")}')
            await handle_proxy_succ(
                proxy_tab=proxy,
            )
        if is_return_raw_response:
            return req
        return req_dict

    def _remove_list_dict_duplicate(self, list_dict_data):
        """
        对list格式的dict进行去重

        """
        run_function = lambda x, y: x if y in x else x + [y]
        return reduce(
            run_function,
            [
                [],
            ]
            + list_dict_data,
        )

    async def _remove_proxy_list(self, proxy_dict):
        """
        移除代理
        :param proxy_dict:
        :return:
        """
        await SQLHelper.remove_proxy(proxy_dict["proxy"])

    async def update_to_proxy_dict(self, proxy_dict: ProxyTab, change_score_num=10):
        """
        修改所选的proxy，如果不存在则新增在第一个
        最多只记录   score: -50 ~ 100分    success_times: -10 ~ 100
        :param proxy_dict:
        :return:
        """
        if proxy_dict.proxy.get("http") == CONFIG.my_ipv6_addr:
            return
        proxy_dict.update_ts = int(time.time())
        if proxy_dict.score > 10000:
            proxy_dict.score = 10000
        if proxy_dict.score < -10000:
            proxy_dict.score = -10000
        if proxy_dict.success_times > 100:
            proxy_dict.success_times = 100
        if proxy_dict.success_times < -10:
            proxy_dict.success_times = -10
        await SQLHelper.update_to_proxy_list(proxy_dict, change_score_num)

    async def get_proxy_by_ip(self, ip: str) -> ProxyTab | None:
        """

        :param ip:传个ip地址进去查找
        :return:
        """
        while 1:
            ret_proxy = await SQLHelper.get_proxy_by_ip(ip)
            if ret_proxy:
                return ret_proxy
            else:
                return None


request_with_proxy_internal = RequestWithProxy()
