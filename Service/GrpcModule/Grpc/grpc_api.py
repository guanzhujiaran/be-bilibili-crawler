# -*- coding: utf-8 -*-
# 成功代理：\{'http': 'http://(?!.*(192)) 查找非192本地代理
import asyncio
import base64
import gzip
import json
import os
import random
import time
import uuid
from ssl import SSLError
from typing import Union
from urllib.parse import urlparse
import curl_cffi
import grpc
from exceptiongroup import ExceptionGroup
from google.protobuf.json_format import MessageToDict  # 这是第三方包，不用管
from google.protobuf.message import DecodeError, EncodeError
from httpx import (
    ProxyError,
    RemoteProtocolError,
    ConnectError,
    ConnectTimeout,
    ReadTimeout,
    ReadError,
    InvalidURL,
    Response,
    WriteError,
    NetworkError,
    HTTPStatusError,
    HTTPError,
)
from python_socks._errors import (
    ProxyConnectionError,
    ProxyTimeoutError,
    ProxyError as SocksProxyError,
)
from socksio import ProtocolError
from CONFIG import CONFIG
from Utils.GrpcUtils.CONST import AppChannels
from Utils.GrpcUtils.GrpcMsgTools import raw_resp_content_2_dict
from Utils.推送.PushMe import a_push_error
from log.base_log import BiliGrpcApi_logger, Voucher352_logger
from Service.GrpcModule.Models.CustomRequestErrorModel import Request352Error
from Service.GrpcModule.Models.GrpcApiBaseModel import MetaDataWrapper
from Utils.GrpcUtils.metadata.device_pool import get_pool
from Utils.GrpcUtils.metadata.makeMetaData import (
    make_metadata,
    is_useable_Dalvik,
    gen_trace_id,
)
from Utils.GrpcUtils.极验.极验点击验证码 import geetest_v3_breaker
from Service.GrpcModule.Grpc.Bapi.BiliApi import (
    get_latest_version_builds,
    resource_abtest_abserver,
)
from Service.GrpcModule.Grpc.Bapi.models import LatestVersionBuild
from Service.GrpcModule.Grpc.GrpcProto.bilibili.app.archive.middleware.v1.preload_pb2 import (
    PlayerArgs,
)
from Service.GrpcModule.Grpc.GrpcProto.bilibili.app.dynamic.v2.dynamic_pb2 import (
    Config,
    DynDetailReq,
    DynDetailReply,
    DynDetailsReq,
    DynSpaceReq,
    DynSpaceRsp,
)
from Service.GrpcModule.Grpc.GrpcProto.bilibili.app.dynamic.v2.dynamic_pb2_grpc import (
    DynamicStub,
)
from Utils.数据库.SqlalchemyTool import sqlalchemy_model_2_dict
from Utils.代理.SealedRequests import my_async_httpx
from Utils.代理.数据库操作.ProxyCommOp import get_available_proxy
from Utils.代理.数据库操作.ProxyEvent import (
    NETWORK_FAIL_STATUS,
    handle_proxy_succ,
    handle_proxy_request_fail,
    handle_proxy_352,
    handle_proxy_unknown_err,
)
from Utils.代理.数据库操作.SqlAlcheyObj.ProxyModel import ProxyTab
from Utils.代理.数据库操作.async_proxy_op_alchemy_mysql_ver import (
    SQLHelper,
    to_proxy_key,
)
from Utils.代理.数据库操作.ProxyHealth import (
    ProxyFailureKind,
    proxy_health_store,
    proxy_prefix_of,
)
from Utils.代理.数据库操作.comm import get_scheme_ip_port_form_proxy_dict
from Utils.代理.mdoel.request_context import (
    excluded_proxy_keys,
    note_failed_proxy,
)
from Utils.代理.mdoel.RiskScope import risk_scope_of, scope_label
from bili_common.core.log_mask import mask_text

ad_extra = "979CD936E441AD18D4DA41A86BACC8168EBE79AA118B33339F80B75C0CD4A1992D36232462A56F4CC7C7E93BFAE33C2EDEA22F19D1DB9C021604DAB304035F8FD09CC00070E1751C322FDA073FE81362163A60D48EF19F79929E98E56202A64E9CC418923EBCC72B8D676AA9423D243CBA9F7F544456356D3F20CC8EF065EB485098B21A7C39249AAA2944F0878CEB6400A58D841A31395E563CC9C9D0EC24F85A956FC3C0BEBBD28A04F20CA973344137F8583324E9EA32FE172917A0F5068F6C711E0EE360CB39DF943E09D7479CDA7584FB8AAECF4207C2AE5CCB652D1B0E445CCF13E1DAC3DFE45D86190945FECFBD82ED8DFE3BC7182313203A9DF1D93BDB0B32F3542EF35D78A806F29C5F1A66D94790E1B3077361C48F0F6C62202E21E7ACB125B18093EA08237831BB23E545610141CEEDC7D7D3F1B2D8AA4E305B91C22C112EF4E12C0C90034FFE36E32304A3926E7AD04BB7A23C3068DBF1DD757C6B43837275DF468FB0AFA297B71A8C9422175EB48877DA262EF6A41351614E89C05AB4E47292E4B405E49BE54E5F0D30D6FBCB8B441433444A8C0E4B37EDF281170B66BA8CDF704CFEBC6C9C8A8AFD9B35B55A716DEF754409DE4300E5F7A609D6EFC5FF10F253513971D6D8F09552184C7ECF6A62358BB3DBF2DA3B2FFFB77A2F8D34E8DADECDAA842A5A4966797C03"


def grpc_error(err):
    status = grpc.StatusCode.UNKNOWN
    details = str(err)
    if isinstance(err, grpc.RpcError):
        status = err.code()
        if err.details():
            details = err.details()
    return status, details


#: gRPC 通道从代理池取代理的最大尝试次数：socks / 无协议的代理在这里被丢弃重取
_GRPC_PROXY_PICK_TRIES = 5


class BiliGrpc:
    #: -352 之后「专线优先」的时间窗（秒）：与 ``MetaDataWrapper`` 的 352 冷却同口径
    _352_PREFER_LOCAL_WINDOW = 30 * 60
    #: 池代理「连不上」计数的衰减窗口（秒）：窗口外的失败不再影响出口决策
    _POOL_FAIL_DECAY_WINDOW = 300

    def __init__(self):
        self.base_uri = "https://grpc.biliapi.net"
        self.debug_mode = False
        # 池代理连续连接失败计数 / 最后一次失败时刻：免费池对 HTTP/2 经常整片不可用，
        # 连续几条都连不上就该回专线，而不是把每条请求都拖到 30s 超时（见 _decide_use_proxy）
        self._pool_grpc_fail_streak = 0
        self._pool_grpc_fail_ts = 0
        self.metadata_pool_size = 30  # 元数据（headers）池大小
        self.metadata_list = []  # 元数据（headers）池大小列表
        self.queue_num = 0
        self.my_proxy_addr = CONFIG.my_ipv6_addr
        self.grpc_api_any_log = BiliGrpcApi_logger
        # 版本号根据 ```https://app.bilibili.com/x/v2/version?mobi_app=android```这个api获取
        self.version_name_build_list: list[LatestVersionBuild] = [
            LatestVersionBuild(**x)
            for x in [
                {"build": 8000200, "version": "8.0.0"},
                {"build": 7810200, "version": "7.81.0"},
                {"build": 7800300, "version": "7.80.0"},
                {"build": 7790400, "version": "7.79.0"},
                {"build": 7780300, "version": "7.78.0"},
                {"build": 7770300, "version": "7.77.0"},
                {"build": 7760700, "version": "7.76.0"},
                {"build": 7750300, "version": "7.75.0"},
                {"build": 7740200, "version": "7.74.0"},
                {"build": 7730300, "version": "7.73.0"},
                {"build": 7720200, "version": "7.72.0"},
                {"build": 7710300, "version": "7.71.0"},
            ]
        ]
        try:
            self.version_name_build_list: list[LatestVersionBuild] = (
                get_latest_version_builds()[:70]
            )  # 获取最新的build
        except Exception as e:
            self.grpc_api_any_log.exception(e)
        # 安装包渠道列表：与 makeMetaData 共用同一渠道池，随机取用
        self.channel_list = list(AppChannels)
        with open(
            os.path.join(
                CONFIG.root_dir,
                "Utils/GrpcUtils/user-agents_dalvik_application_2-1.json",
            ),
            "r",
            encoding="utf-8",
        ) as f:
            self.Dalvik_list = json.loads(f.read())
            self.Dalvik_list = list(
                filter(
                    lambda x: (
                        "Dalvik/2.1.0" in x
                        and "[ip:" not in x
                        and "AppleWebKit" not in x
                    ),
                    self.Dalvik_list,
                )
            )
        self.brand_list = [
            "Xiaomi",
            "Huawei",
            "Samsung",
            "Vivo",
            "Oppo",
            "Oneplus",
            "Meizu",
            "Nubia",
            "Sony",
            "Zte",
            "Honor",
            "Lenovo",
            "Lg",
            "Blu",
            "Asus",
            "Panasonic",
            "Htc",
            "Nokia",
            "Motorola",
            "Realme",
            "Alcatel",
            "BlackBerry",
        ]
        self.channel = None
        self.proxy: ProxyTab | None = None

        self.timeout = 30
        self.latest_352_ts = 0

    @property
    def proxy_id(self):
        return self.proxy.proxy_id if self.proxy else 0

    # region 准备工作
    async def _prepare_channel_proxy(self):
        proxy = self.proxy
        channel = self.channel
        if not channel:
            proxy, channel = await self._get_random_channel(is_need_channel=True)
        return proxy, channel

    async def _get_random_channel(
        self, is_need_channel: bool = False, is_use_available_proxy: bool = False
    ) -> tuple[ProxyTab, grpc.aio.Channel | None]:
        # 档位跟随当前爬虫上下文（is_use_available_proxy 已废弃为死参数：
        # 「是否用探测档」由 CrawlerConfig.proxy_tier / RequestConf.tier 决定）
        proxy_tab = await self._pick_pool_proxy_for_grpc()
        if proxy_tab is None:
            # 池里没有 gRPC 可用的 http/https 代理：回退本地 ipv6 代理（它不是池成员，
            # 不参与打分），不能让 proxy 为 None —— 下面的 using_proxy 会直接取 proxy.proxy。
            proxy_tab = self._local_exit_proxy_tab()
        channel = None
        if is_need_channel:
            channel = grpc.aio.secure_channel(
                f"{self.base_uri}:443",
                grpc.ssl_channel_credentials(),
                options=[],
                # FIRERPA 真机抓包：grpc-encoding: gzip 且请求体确实压缩。
                # 与 make_metadata 里手写的 grpc-encoding 保持一致。
                compression=grpc.Compression.Gzip,
            )  # Connect to the gRPC server
        return proxy_tab, channel

    def _local_exit_proxy_tab(self) -> ProxyTab:
        """专线（自建出口）的代理行：它不是池成员，score / success_times 如实填 0。

        以前伪造成 10000（等于宣称「历史表现完美」），配合健康度豁免就是永不降权、
        永不冷却；它的健康度按 /64 前缀记账（见 ``ProxyHealth.proxy_prefix_of``）。
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

    def _local_exit_proxy_key(self) -> str:
        """专线的代理串（与 ``proxy_tab.computed_proxy_str`` 同口径）。"""
        return to_proxy_key(CONFIG.custom_proxy) or (CONFIG.my_ipv6_addr or "").strip()

    def _is_local_exit_proxy(self, proxy: ProxyTab | None) -> bool:
        """本次用的出口是不是专线（自建出口）。"""
        return bool(proxy) and (
            get_scheme_ip_port_form_proxy_dict(proxy.proxy) == self.my_proxy_addr
        )

    @staticmethod
    def _grpc_proxy_allows(proxy_tab: ProxyTab | None) -> bool:
        """该代理行能否承载 gRPC（HTTP/2 + TLS）。

        gRPC 必须经 http CONNECT 隧道，socks4 / socks5 / 无协议的明文代理一定失败，
        因此只看协议：``proxy`` 字典里的每个地址都必须以允许的 scheme 开头
        （默认 ``http://`` / ``https://``，见 ``CONFIG.grpc_device.grpc_proxy_schemes``）。
        """
        if proxy_tab is None:
            return False
        allowed = tuple(
            f"{scheme.strip().lower().rstrip(':/')}://"
            for scheme in CONFIG.grpc_device.grpc_proxy_schemes
            if scheme and scheme.strip()
        )
        if not allowed:
            return True
        proxy = proxy_tab.proxy
        if not isinstance(proxy, dict) or not proxy:
            return False
        values = [
            str(value).strip()
            for value in proxy.values()
            if isinstance(value, str) and str(value).strip()
        ]
        if not values:
            return False
        return all(value.lower().startswith(allowed) for value in values)

    async def _pick_pool_proxy_for_grpc(self) -> ProxyTab | None:
        """从代理池取一条 **gRPC 可用** 的代理（只接受 http / https 协议）。

        取到不合协议的行（socks4 / socks5 / 无协议）就丢弃重取，最多
        ``_GRPC_PROXY_PICK_TRIES`` 次；仍取不到返回 None，由调用方回退专线。
        线上教训：这条过滤缺失时，gRPC 请求会拿着 socks4 代理去发 HTTP/2 + TLS，
        每条都 curl 28/35/97 超时，补数零产出。
        """
        skipped: list[str] = []
        seen: set[str] = set()
        for _ in range(max(1, _GRPC_PROXY_PICK_TRIES)):
            proxy_tab, _ = await get_available_proxy()
            if proxy_tab is None:
                break
            if self._grpc_proxy_allows(proxy_tab):
                return proxy_tab
            key = to_proxy_key(proxy_tab.proxy) or str(proxy_tab.proxy)
            if key in seen:
                continue
            seen.add(key)
            skipped.append(key)
            self.grpc_api_any_log.debug(
                f"池代理 {mask_text(key)} 协议不被 gRPC 支持"
                f"（允许 {CONFIG.grpc_device.grpc_proxy_schemes}），丢弃重取"
            )
        if skipped:
            self.grpc_api_any_log.warning(
                f"代理池连续 {len(skipped)} 条都不是 http/https 代理，回退专线"
                f"（已跳过：{mask_text('、'.join(skipped[:5]))}）"
            )
        return None

    # endregion

    async def metadata_productor(self, proxy) -> MetaDataWrapper:
        """
        metadata生产者
        :param proxy:
        :return:
        """
        if self.queue_num < self.metadata_pool_size:
            self.queue_num += 1
            metadata: Union[MetaDataWrapper, None] = None
        else:
            while 1:
                if len(self.metadata_list) > 0:
                    metadata = random.choice(self.metadata_list)
                    if metadata.is_need_delete:
                        self.queue_num -= 1
                        self.metadata_list.remove(metadata)
                        continue
                    # if not metadata.able(num_add=False):
                    #     await asyncio.sleep(10)
                    #     continue
                    break
                if len(self.metadata_list) == 0 and self.queue_num == 0:
                    self.queue_num += 1
                    metadata = None
                    break
                await asyncio.sleep(1)
        if not metadata:
            while 1:
                brand = random.choice(self.brand_list)
                Dalvik = random.choice(self.Dalvik_list)
                while not is_useable_Dalvik(Dalvik):
                    Dalvik = random.choice(self.Dalvik_list)
                version_name_build: LatestVersionBuild = random.choice(
                    self.version_name_build_list
                )
                version_name = version_name_build.version
                build = version_name_build.build
                channel = random.choice(self.channel_list)
                # self.grpc_api_any_log.debug(
                #     f'当前metadata池数量：{len(self.metadata_list)}，总共{self.queue_num}个meta信息，前往获取新的metadata')
                # 设备池：优先复用池里已有的设备（保留其 buvid/指纹/region），没有就新建。
                # Redis 异常/超时不让爬取挂住：降级为本次新建一台
                try:
                    device = await asyncio.wait_for(
                        get_pool().acquire(
                            brand=brand,
                            Dalvik=Dalvik,
                            version_name=version_name,
                            build=build,
                            channel=channel,
                        ),
                        timeout=10,
                    )
                except Exception as pool_err:
                    self.grpc_api_any_log.error(
                        f"设备池取设备失败（降级为新建）：{type(pool_err).__name__} {pool_err}"
                    )
                    device = None
                md, ticket_resp, metadat_basic_info = await make_metadata(
                    "",
                    brand=brand,
                    Dalvik=Dalvik,
                    version_name=version_name,
                    build=build,
                    channel=channel,
                    proxy=proxy,
                    device=device,
                )
                session_id = uuid.uuid4().hex[0:8]
                md_dict = dict(md)
                if not md_dict.get("x-bili-ticket"):
                    self.grpc_api_any_log.error(f"bili-ticket获取失败！{md}")
                    await asyncio.sleep(30)
                    continue
                else:
                    try:
                        await resource_abtest_abserver(
                            buvid=metadat_basic_info.buvid,
                            fp_local=metadat_basic_info.fp_local,
                            fp_remote=metadat_basic_info.fp_remote,
                            session_id=session_id,
                            guestid=metadat_basic_info.guestid,
                            app_version_name=metadat_basic_info.app_version_name,
                            model=metadat_basic_info.model,
                            app_build=metadat_basic_info.app_build,
                            channel=metadat_basic_info.channel,
                            osver=metadat_basic_info.osver,
                            ticket=metadat_basic_info.ticket,
                            brand=metadat_basic_info.brand,
                        )
                        self.grpc_api_any_log.debug(f"激活metadata成功！{md}")
                    except Exception:
                        self.grpc_api_any_log.exception("激活metadata失败！")
                    break
            metadata = MetaDataWrapper(
                md=md,
                buvid=metadat_basic_info.buvid,
                expire_ts=ticket_resp.created_at + ticket_resp.ttl,
                version_name=version_name,
                session_id=session_id,
                guestid=metadat_basic_info.guestid,
                device=metadat_basic_info.device,
            )
            self.metadata_list.append(metadata)
            # 回写设备：本次请求可能让它学到了新的 region 等信息（失败不影响主流程）
            try:
                await asyncio.wait_for(get_pool().save(device), timeout=10)
            except Exception as pool_err:
                self.grpc_api_any_log.error(
                    f"设备池回写失败：{type(pool_err).__name__} {pool_err}"
                )
        # self.grpc_api_any_log.debug(f'当前metadata池数量：{len(self.metadata_list)}')
        return metadata

    # region 出口决策（专线 / 代理池）
    async def _local_exit_blocked_reason(self, scope: str | None = None) -> str:
        """专线为什么不能用于本次请求（``""`` = 可用）。

        闸门口径与 HTTP 通道 ``RequestWithProxy._acquire_local_exit`` 一致：

        - 本任务内专线（或它所在的前缀）已经失败过 → 同段换地址没有意义；
        - 专线前缀在**当前 gRPC 风控域**处于风控冷却（412/352 记在 /64 上）→ 冷却期内不用。
        """
        proxy_key = self._local_exit_proxy_key()
        if not proxy_key:
            return "专线未配置（CONFIG.my_ipv6_addr 为空）"
        prefix = proxy_prefix_of(proxy_key)
        excluded = excluded_proxy_keys()
        if proxy_key in excluded:
            return "本任务内专线已失败"
        if prefix and prefix in {proxy_prefix_of(key) for key in excluded}:
            return f"本任务内专线前缀 {prefix} 已失败"
        if prefix:
            try:
                if await proxy_health_store.is_prefix_cooling(prefix, scope):
                    return (
                        f"专线前缀 {prefix} 在风控域 {scope_label(scope)} 处于风控冷却"
                    )
            except Exception as e:  # noqa: BLE001 读冷却失败不能挡住业务请求
                self.grpc_api_any_log.debug(
                    f"读取专线前缀冷却状态失败：{type(e).__name__} {e}"
                )
        return ""

    async def _decide_use_proxy(
        self,
        *,
        force_proxy: bool,
        validate_token: str,
        ipv6_proxy_weights: int,
        real_proxy_weights: int,
        scope: str | None = None,
    ) -> bool:
        """本次请求走代理池（True）还是专线（False）。

        优先级（2026-11-16 修正：原实现里「最近 30 分钟有 352」先置 False，
        紧接着又被一句 ``if 最近 30 分钟有 352: proxy_flag = True`` 覆盖，
        结果 352 之后 30 分钟内**专线被排除**、请求全部降级到免费代理池，
        每条请求都等到 300s 超时，补数零产出）：

        1. ``debug_mode`` → 专线；``force_proxy`` → 代理池；
        2. 最近 30 分钟内有 -352 → **专线优先**（-352 是出口风控，正确处置是
           换设备 + 收敛出口，把请求甩给免费池只是把每条请求都拖到超时）；
        3. 池代理连续连不上（免费池对 HTTP/2 整片不可用）→ 专线；
        4. 已经拿到 ``x-bili-gaia-vtoken`` → 继续专线（换出口等于重新挑战）；
        5. 其余按历史权重随机；``ipv6_proxy_weights`` 只影响这一步的随机，
           **不再覆盖**上面的 352 / 池不可用决策；
        6. 选定专线后仍要过闸门（前缀冷却 / 本任务内已失败）才允许降级到池。
        """
        if self.debug_mode:
            return False
        if force_proxy:
            return True
        recent_352 = (
            int(time.time()) - self.latest_352_ts
            <= self._352_PREFER_LOCAL_WINDOW
        )
        if recent_352:
            proxy_flag = False
            self.grpc_api_any_log.debug(
                f"最近 {self._352_PREFER_LOCAL_WINDOW}s 内出现过 -352，本次优先专线"
                f"（剩余 {self._352_PREFER_LOCAL_WINDOW - (int(time.time()) - self.latest_352_ts)}s）"
            )
        elif self._pool_grpc_pinned():
            proxy_flag = False
            self.grpc_api_any_log.warning(
                f"gRPC 池代理连续 {self._pool_grpc_fail_streak} 次连不上"
                f"（阈值 {CONFIG.grpc_device.pool_fail_streak_to_local}），本次改用专线"
            )
        elif validate_token:
            # 手上已有有效的 gaia token：继续用同一出口，换出口等于重新被挑战
            proxy_flag = False
        else:
            proxy_flag = random.choices(
                [True, False],
                weights=[
                    real_proxy_weights if real_proxy_weights >= 0 else 0,
                    ipv6_proxy_weights * 2 if ipv6_proxy_weights >= 0 else 0,
                ],
                k=1,
            )[0]  # True 用真实代理池，False 用专线
        if proxy_flag:
            return True
        blocked = await self._local_exit_blocked_reason(scope)
        if blocked:
            self.grpc_api_any_log.warning(
                f"专线不可用（{blocked}），本次降级到 gRPC 代理池"
            )
            return True
        return False

    # region 池代理可用性（gRPC 通道口径）
    def _note_pool_grpc_fail(self) -> None:
        """记一次「池代理连不上」（只统计连接层失败，352 不算）。"""
        self._pool_grpc_fail_streak += 1
        self._pool_grpc_fail_ts = int(time.time())

    def _note_pool_grpc_ok(self) -> None:
        """池代理成功一次：说明池子对 gRPC 还能用，清空连不上计数。"""
        self._pool_grpc_fail_streak = 0
        self._pool_grpc_fail_ts = 0

    def _pool_grpc_pinned(self) -> bool:
        """池代理是否已被判定「对 gRPC 不可用」（连续失败超阈值，且未超出衰减窗口）。

        为什么需要：免费池默认权重是 1000:2，少数几条坏代理不会改变随机结果；
        一旦整片不可用，每条请求都要等满 30s 超时（线上补数零产出就是这么来的）。
        这里按「连续失败」快速切到专线，成功后或窗口过期自动回到原口径。
        """
        threshold = max(1, CONFIG.grpc_device.pool_fail_streak_to_local)
        if self._pool_grpc_fail_streak < threshold:
            return False
        if int(time.time()) - self._pool_grpc_fail_ts > self._POOL_FAIL_DECAY_WINDOW:
            self._note_pool_grpc_ok()
            return False
        return True

    # endregion

    # region 设备（metadata）重建
    async def _note_device_prefix_risk(
        self, md: MetaDataWrapper | None, proxy: ProxyTab | None
    ) -> None:
        """-352 设备风险记账：按「设备 + 出口前缀」去重（同段重复命中只算一次）。

        见 ``device_pool.note_prefix_risk``：坏的是出口前缀，不是设备；
        多个不同前缀都对它报 -352 才判定设备本身不可用。
        """
        device = getattr(md, "device", None) if md else None
        if not device:
            return
        try:
            prefix = proxy_prefix_of(to_proxy_key(proxy.proxy)) if proxy else ""
            dropped = await asyncio.wait_for(
                get_pool().note_prefix_risk(
                    device,
                    prefix,
                    max_prefixes=CONFIG.grpc_device.max_352_prefixes,
                    ttl=CONFIG.grpc_device.risk_ttl_seconds,
                ),
                timeout=10,
            )
            if not dropped:
                self.grpc_api_any_log.debug(
                    f"设备 {md.buvid} 在出口前缀 {prefix} 触发-352，"
                    f"仅冷却该前缀，设备保留"
                )
        except Exception as pool_err:  # noqa: BLE001 设备池故障不能拖垮请求
            self.grpc_api_any_log.error(
                f"设备池 -352 记账失败：{type(pool_err).__name__} {pool_err}"
            )

    async def _recreate_metadata(
        self, md: MetaDataWrapper | None
    ) -> MetaDataWrapper:
        """丢弃当前 metadata（连同它的设备）并**立刻重建**一台。

        -352 的第一处置是换设备而不是换 IP：同一台设备在任意出口都会被拒，
        继续换 IP 只会把池子里的出口一条条打成 -352。
        """
        if md is not None:
            if md in self.metadata_list:
                self.metadata_list.remove(md)
                self.queue_num = max(0, self.queue_num - 1)
            device = getattr(md, "device", None)
            if device:
                try:
                    await asyncio.wait_for(get_pool().drop(device), timeout=10)
                except Exception as pool_err:  # noqa: BLE001
                    self.grpc_api_any_log.error(
                        f"设备池剔除失败：{type(pool_err).__name__} {pool_err}"
                    )
        # 重建走与首建完全相同的链路（专线建 metadata，保证出口一致）
        return await self.metadata_productor(
            {"proxy": {"http": self.my_proxy_addr, "https": self.my_proxy_addr}}
        )

    # endregion

    async def handle_grpc_request(
        self,
        url: str,
        grpc_req_message,
        grpc_resp_msg,
        func_name: str = "",
        force_proxy: bool = False,
        force_non_proxy: bool = False,
    ):  # 连续请求20次出现-352
        """
        处理grpc请求
        :param force_non_proxy:  强制不使用代理，也就是直连
        :param force_proxy: 强制使用真实代理
        :param func_name:
        :param url:
        :param grpc_req_message: dynamic_pb2.DynDetailReq(**data_dict)
        :param grpc_resp_msg: dynamic_pb2.DynDetailReply()
        :return:
        """
        md: MetaDataWrapper | None = None
        validate_token: str = ""
        channel = None
        ipv6_proxy_weights = 1
        real_proxy_weights = 1000
        # 风控域：gRPC 方法与 web 接口的风控相互独立（DynDetail -352 不应冷却 web 接口）。
        # func_name 为空时用 url（含 ``/bilibili.app.dynamic.v2.Dynamic/DynDetail``）推导。
        grpc_scope = risk_scope_of(func_name or url, channel="grpc")
        while 1:
            # self.grpc_api_any_log.debug(f'距离上次352时间：{int(time.time()) - self.latest_352_ts}秒')
            proxy_flag = await self._decide_use_proxy(
                force_proxy=force_proxy,
                validate_token=validate_token,
                ipv6_proxy_weights=ipv6_proxy_weights,
                real_proxy_weights=real_proxy_weights,
                scope=grpc_scope,
            )
            if proxy_flag:
                proxy, channel = await self._get_random_channel()
            else:
                proxy: ProxyTab = self._local_exit_proxy_tab()

            if force_non_proxy or not proxy:
                proxy_flag = False
            msg = grpc_req_message
            proto_bytes = msg.SerializeToString()
            headers = {
                "content-type": "application/Grpc",
                # 'Connection': 'close',
                # "user-agent": ua,
                # 'user-agent': random.choice(CONFIG.UA_LIST),
            }
            if (not md or not md.able(num_add=False)) and not validate_token:
                md: MetaDataWrapper = await self.metadata_productor(
                    {"proxy": {"http": self.my_proxy_addr, "https": self.my_proxy_addr}}
                )
            md_dict = dict(md.md)
            headers |= md_dict
            if not headers.get("x-bili-ticket"):
                raise ValueError("headers中没有有效的x-bili-ticket！")
            new_headers = []
            for k, v in md.md:
                if isinstance(v, bytes):
                    new_headers.append(
                        (k, base64.b64encode(v).decode("utf-8").strip("="))
                    )
                    continue
                if k == "x-bili-trace-id":
                    new_headers.append((k, gen_trace_id()))
                    continue
                if k == "x-bili-gaia-vtoken":
                    # if validate_token:
                    #     self.grpc_api_any_log.debug(f'x-bili-gaia-vtoken被覆盖！{validate_token}')
                    new_headers.append((k, validate_token if validate_token else ""))
                    continue
                if isinstance(v, str):
                    new_headers.append((k, v))
                else:
                    new_headers.append((k, ""))
                    self.grpc_api_any_log.warning(f"headers中出现了非法类型！{k}:{v}")
            headers.update(dict(new_headers))
            resp = Response(status_code=114514)
            try:
                if "gzip" in headers.get("grpc-encoding"):
                    compressed_proto_bytes = gzip.compress(proto_bytes, compresslevel=6)
                    data = (
                        b"\01"
                        + len(compressed_proto_bytes).to_bytes(4, "big")
                        + compressed_proto_bytes
                    )
                else:
                    data = b"\01" + len(proto_bytes).to_bytes(4, "big") + proto_bytes
                using_proxy = (
                    {"http": self.my_proxy_addr, "https": self.my_proxy_addr}
                    if force_proxy
                    else proxy.proxy
                )
                self.grpc_api_any_log.debug(
                    f"请求url：{url}\n请求body：{grpc_req_message}\n请求headers：{new_headers}\n请求代理：{using_proxy}"
                )
                resp = await my_async_httpx.request(
                    method="post",
                    url=url,
                    data=data,
                    headers=tuple(new_headers),
                    # headers=dict(new_headers),
                    timeout=self.timeout,
                    proxies=using_proxy,
                )
                self.grpc_api_any_log.debug(
                    f"响应url：{url}\n响应body：{resp.text}\n响应headers：{resp.headers}"
                )
                # 让本次请求所属的设备从响应里学习 region
                # （APK: kntr.base.region.impl.h.b，先响应头再 trailer）
                device_env = getattr(md, "device", None)
                learned_region = (
                    device_env.learn_region(
                        resp.headers, getattr(resp, "trailers", None)
                    )
                    if device_env
                    else {}
                )
                if learned_region:
                    self.grpc_api_any_log.debug(f"更新region缓存：{learned_region}")
                resp.raise_for_status()
                md.able(num_add=True)
                if (
                    type(resp.headers.get("Grpc-status")) is not str
                    and type(resp.headers.get("Grpc-status")) is not bytes
                ):
                    # 响应异常同样视为设备有问题，从设备池剔除
                    if getattr(md, "device", None):
                        try:
                            await asyncio.wait_for(
                                get_pool().drop(md.device), timeout=10
                            )
                        except Exception as pool_err:
                            self.grpc_api_any_log.error(
                                f"设备池剔除失败：{type(pool_err).__name__} {pool_err}"
                            )
                    raise MY_Error(resp.text.replace("\n", ""))
                if (
                    "-352" in str(resp.headers.get("bili-status-code"))
                    or "-352" in str(resp.headers.get("Grpc-Message"))
                    or "-412" in str(resp.headers.get("bili-status-code"))
                    or "-412" in str(resp.headers.get("Grpc-Message"))
                ):  # -352的话尝试把这个metadata丢弃
                    self.grpc_api_any_log.warning(
                        mask_text(
                            f"\n-352报错！\n"
                            f"url:{url}\n"
                            f"body:{grpc_req_message}"
                            f"headers:{new_headers}\n"
                            f"token:{validate_token}\n"
                            f"{sqlalchemy_model_2_dict(proxy)}"
                        )
                    )
                    if not validate_token:
                        # self.grpc_api_any_log.debug(f'未携带validate_token报错-352')
                        parsed_url = urlparse(url)
                        validate_token = (
                            await geetest_v3_breaker.a_validate_form_voucher_ua(
                                v_voucher=resp.headers.get("x-bili-gaia-vvoucher"),
                                ua=headers.get("user-agent"),
                                ck=md.buvid,
                                ori=f"https://{parsed_url.netloc}",
                                ref=url,
                                ticket=headers.get("x-bili-ticket"),
                                version=md.version_name,
                                session_id=md.session_id,
                            )
                        )
                        if validate_token:
                            self.grpc_api_any_log.debug(
                                f"获取到-352验证token:{validate_token}"
                            )
                            continue
                        else:
                            # 极验这条链路自己就没拿到 token：属于「验证码服务不可用 /
                            # 验证码获取失败」，不能算作设备风险信号（见捕获分支）。
                            raise Request352Error(
                                f"{func_name}\t{url} metadata已经发起了{md.used_times}次有效请求，遇到-352，未获取到-352验证token",
                                -352,
                                token_acquired=False,
                            )
                    else:
                        raise Request352Error(
                            f"{func_name}\t{url} metadata已经发起了{md.used_times}次有效请求，携带validate_token{validate_token}请求依旧 -352报错-{proxy}\n{str(resp.headers)}\n{str(new_headers)}\n{str(data)}",
                            -352,
                            token_acquired=True,
                        )
                resp_dict = raw_resp_content_2_dict(
                    raw_resp=resp,
                    protobuf_msg=grpc_resp_msg,
                    is_gzip="gzip" in headers.get("grpc-encoding"),
                )
                if proxy:
                    await handle_proxy_succ(
                        proxy_tab=proxy,
                        scope=grpc_scope,
                    )
                if proxy_flag:
                    # 池代理对 gRPC 可用：清空「连不上」计数
                    self._note_pool_grpc_ok()
                # self.grpc_api_any_log.critical(
                #     f'{func_name}\t{url} \n获取grpc动态请求成功代理：{proxy.proxy} \n{grpc_req_message}\n{new_headers}')  # 成功代理：\{'http': 'http://(?!.*(192)) 查找非192本地代理
                md.times_352 = 0
                return resp_dict
            except (
                ConnectionError,
                ProxyError,
                RemoteProtocolError,
                ConnectError,
                ConnectTimeout,
                ReadTimeout,
                ReadError,
                WriteError,
                InvalidURL,
                NetworkError,
                ValueError,
                OverflowError,
                ExceptionGroup,
                ProxyConnectionError,
                ProxyTimeoutError,
                SocksProxyError,
                ProtocolError,
                SSLError,
                HTTPStatusError,
                HTTPError,
                curl_cffi.requests.exceptions.ConnectionError,
                curl_cffi.curl.CurlError,
                curl_cffi.requests.exceptions.ProxyError,
                curl_cffi.requests.exceptions.SSLError,
                curl_cffi.requests.exceptions.Timeout,
                curl_cffi.requests.exceptions.HTTPError,
                TimeoutError,
            ) as connect_error:
                self.grpc_api_any_log.debug(
                    f"请求url：{url}\n请求body：{grpc_req_message}\n请求headers：{new_headers}\n请求代理：{using_proxy}"
                    f"连接错误：{connect_error}"
                )
                await asyncio.sleep(3)
                if proxy_flag:
                    await handle_proxy_request_fail(
                        proxy_tab=proxy,
                    )
                    ipv6_proxy_weights += 1
                    # 池代理连不上：连续几条都这样就直接回专线（别把每条请求都拖到超时）
                    self._note_pool_grpc_fail()
                else:
                    real_proxy_weights += 20
                    # 专线自己也连不上：池子重新给一次机会（不是每次都要等衰减窗口）
                    self._note_pool_grpc_ok()
                if self._is_local_exit_proxy(proxy):
                    # 本任务内不再重试专线（下一轮才允许降级到池）；池代理的记账
                    # 已经在 handle_proxy_request_fail 里做，这里不重复排除网段。
                    note_failed_proxy(to_proxy_key(proxy.proxy) or "")
            except Request352Error as _352_err:
                self.grpc_api_any_log.debug(
                    f"请求url：{url}\n请求body：{grpc_req_message}\n请求headers：{new_headers}\n请求代理：{using_proxy}"
                    f"352错误：{_352_err}"
                )
                await asyncio.sleep(3)
                # token 已被消费（或压根没取到）：下一轮必须重新取，
                # 否则「携带旧 token + 同一个 md」会一直原地 352。
                validate_token = ""
                md.times_352 += 1
                if _352_err.token_acquired:
                    # -352 是**出口风控**信号：cookie / 设备 / 出口前缀三者耦合，
                    # 不能一遇 -352 就剔除设备（一个坏前缀会把设备池逐台清空）。
                    # 按「出口前缀 + 设备」记账：同一前缀只记一次，
                    # 多个不同前缀都对它报 -352 才判定设备本身不可用。
                    await self._note_device_prefix_risk(md, proxy)
                else:
                    # 极验环节就没拿到 token（服务不可用 / 验证码获取失败）：
                    # 挑战都没做完，不能据此判定设备有问题，只记 WARNING（不记设备风险）。
                    # 否则验证码服务抖动会把设备池整批清空，重建出来还是同一类设备。
                    self.grpc_api_any_log.warning(
                        f"{func_name}\t{url} 极验未取得 token"
                        f"（服务不可用 / 验证码获取失败），本次不计入设备风险：{_352_err}"
                    )
                ipv6_proxy_weights -= 10
                if proxy:
                    if self._is_local_exit_proxy(proxy):
                        self.latest_352_ts = int(time.time())
                        self.grpc_api_any_log.debug(
                            f"设置本地代理最后-352时间为：{self.latest_352_ts}"
                        )
                        # 这里**不**把专线记进「本任务已失败」：-352 的正确处置是换设备
                        # 继续用专线，出口本身没坏（降级到免费池只会把每条请求拖到超时）。
                        # 专线的节流由前缀冷却负责（上面 handle_proxy_352）；
                        # 只有网络层连不上（专线端口黑洞）才算「本任务内专线已失败」。
                    Voucher352_logger.warning(
                        f"代理{proxy.proxy} 报错-352 被封禁\n{url}\n{new_headers}\n{grpc_req_message}"
                    )
                    # gRPC 通道不再把代理在 MySQL 里永久置为不可用（否则几轮 352
                    # 就把池子打空），只在 Redis 健康度上做**当前 gRPC 风控域**的前缀冷却。
                    await handle_proxy_352(
                        proxy_tab=proxy, mark_db_invalid=False, scope=grpc_scope
                    )
                    ipv6_proxy_weights += 1
                else:
                    real_proxy_weights += 20
                if (
                    md
                    and CONFIG.grpc_device.recreate_device_on_352
                    and md.times_352 <= max(1, CONFIG.grpc_device.max_352_times)
                ):
                    # 先换设备（而不是继续用同一个 md 换 IP）：极验拿不到 token 时
                    # 换出口也没用（挑战本身失败），换台设备才是唯一可能解开的变量
                    self.grpc_api_any_log.warning(
                        f"{func_name}\t{url} -352 后重建设备：buvid={md.buvid} "
                        f"累计 {md.times_352} 次（阈值 "
                        f"{CONFIG.grpc_device.max_352_times}），丢弃当前 metadata"
                    )
                    try:
                        md = await self._recreate_metadata(md)
                    except Exception as rebuild_err:  # noqa: BLE001
                        # 重建链路（buvid / ticket / abserver）本身也可能超时：
                        # 不能让它把整条请求打成异常，交给下一轮循环重新建
                        self.grpc_api_any_log.error(
                            f"-352 后重建设备失败，下一轮重新建："
                            f"{type(rebuild_err).__name__} {rebuild_err}"
                        )
                        md = None
            except Exception as err:
                self.grpc_api_any_log.debug(
                    f"请求url：{url}\n请求body：{grpc_req_message}\n请求headers：{new_headers}\n请求代理：{using_proxy}"
                    f"未知错误：{err}"
                )
                await asyncio.sleep(3)
                if type(err) is DecodeError or type(err) is EncodeError:
                    self.grpc_api_any_log.error(
                        f"{func_name}\t"
                        f"解析grpc消息失败！\n"
                        f"{url}\n"
                        f"{resp.headers}"
                        f"{grpc_req_message}\n"
                        f"{resp.text}\n"
                        f"{resp.content.hex()}"
                    )
                    await a_push_error(
                        subject="运行异常",
                        content=f"[Bili]解析grpc消息失败，解析结果为空\n解析grpc消息失败！{url}{grpc_req_message}{resp.text}{resp.content.hex()}",
                    )
                    if proxy:
                        await handle_proxy_succ(
                            proxy_tab=proxy,
                            scope=grpc_scope,
                        )
                    return {}

                self.grpc_api_any_log.exception(
                    f"{func_name}\n{grpc_req_message}\n{proxy.proxy}\n{url} grpc_get_dynamic_detail_by_type_and_rid\nBiliGRPC error: {err}\n"
                    f"{new_headers}\n"
                    f"{proxy.proxy}"
                )
                if proxy:
                    await handle_proxy_unknown_err(
                        proxy_tab=proxy,
                    )
                    ipv6_proxy_weights += 1
                    if self._is_local_exit_proxy(proxy):
                        note_failed_proxy(to_proxy_key(proxy.proxy) or "")
                else:
                    real_proxy_weights += 20
                validate_token = ""

    # region 第三方grpc库发起的请求
    async def grpc_api_get_DynDetails(self, dyn_ids: list[int]) -> dict:
        """
        通过grpc客户端请求的，不太好一起统一处理
        通过动态id的列表批量获取动态详情，但是需要有所有的动态id，不能用，很难受
        :param dyn_ids:
        :return:
        """
        if type(dyn_ids) is not list:
            raise TypeError(f"dyn_ids must be a list!{dyn_ids}")
        if len(dyn_ids) == 0:
            return {}
        dyn_ids = [int(x) for x in dyn_ids]
        # proxy_server_address = sqlhelper.select_rand_proxy()['proxy']['https']
        # intercept_channel = grpc.intercept_channel(
        #     channel,
        #     # RequestInterceptor()
        # )

        while 1:
            proxy, channel = await self._prepare_channel_proxy()
            dyn_details_req = DynDetailsReq(
                dynamic_ids=json.dumps({"dyn_ids": dyn_ids}),
            )
            try:
                dynamic_client = DynamicStub(channel)
                # print(dyn_details_req.SerializeToString())
                # ack = gen_random_access_key()
                ack = ""
                md, ticket, metadat_basic_info = await make_metadata(ack)

                dyn_all_resp = await dynamic_client.DynDetails(
                    dyn_details_req, metadata=md, timeout=self.timeout
                )
                ret_dict = MessageToDict(dyn_all_resp)
                if proxy.proxy_id != self.proxy_id:
                    proxy.status = 0
                    await SQLHelper.update_to_proxy_list(proxy, 10)
                return ret_dict
            except grpc.RpcError as e:
                stat, det = grpc_error(e)
                self.grpc_api_any_log.warning(
                    f"\nBiliGRPC error: {stat} - {proxy['proxy']}"
                )
                score_change = -10
                # 400状态码表示代理可能是http1.1协议，不支持grpc的http2.0
                if (
                    "HTTP proxy returned response code 400" in det
                    or "OPENSSL_internal" in det
                ):
                    score_change = -10
                # 已知的不重要的错误
                if det == "Deadline Exceeded":
                    pass
                elif "failed to connect to all addresses" in det:
                    pass
                elif "OPENSSL_internal:TLSV1_ALERT_NO_APPLICATION_PROTOCOL." in det:
                    pass
                elif "OPENSSL_internal:WRONG_VERSION_NUMBER." in det:
                    pass
                else:
                    self.grpc_api_any_log.warning(
                        # 重大错误！
                        f"{dyn_ids} grpc_api_get_DynDetails\n BiliGRPC error: {stat} - {det}\n{dyn_details_req}\n{type(e)}"
                    )
                # gRPC RpcError 无法区分网络 / 风控：按 unknown 记账（status=-1），
                # 不再复用 -412 —— 后者只代表真正的 B 站风控码。
                proxy.status = NETWORK_FAIL_STATUS
                await SQLHelper.update_to_proxy_list(
                    proxy, score_change, kind=ProxyFailureKind.UNKNOWN
                )

    # endregion

    # region grpc请求接口
    async def grpc_get_dynamic_detail_by_type_and_rid(
        self, rid: Union[int, str], dynamic_type: int = 2, force_proxy: bool = False
    ) -> dict:
        """
        通过rid和动态类型特定获取一个动态详情
        :param force_proxy:是否强制使用代理
        :param dynamic_type:动态类型
        :param rid:动态rid
        :return:
        """
        if type(rid) is str and str.isdigit(rid):
            rid = int(rid)
        if type(rid) is not int:
            raise TypeError(f"rid must be number! rid:{rid}")
        url = f"{self.base_uri}/bilibili.app.dynamic.v2.Dynamic/DynDetail"
        data_dict = {
            # 'uid': random.randint(1, 3537105317792299),
            "dyn_type": dynamic_type,
            "rid": rid,
            # "ad_param": AdParam(
            #     ad_extra=ad_extra
            # ),
            "player_args": PlayerArgs(qn=112, fnval=17360, voice_balance=1),
            "share_id": "dt.dt-detail.0.0.pv",
            "share_mode": 3,
            "local_time": 8,
            "config": Config(),
        }
        msg = DynDetailReq(**data_dict)
        gresp = DynDetailReply()
        return await self.handle_grpc_request(url, msg, gresp, force_proxy=force_proxy)

    async def grpc_get_dynamic_detail_by_dynamic_id(
        self, dynamic_id: int | str, force_proxy: bool = False
    ) -> dict:
        """
        通过rid和动态类型特定获取一个动态详情
        :param dynamic_id:动态id
        :param force_proxy:
        :return:
        """
        if type(dynamic_id) is int:
            dynamic_id = str(dynamic_id)
        if type(dynamic_id) is not str or not str.isdigit(dynamic_id):
            raise TypeError(
                f"dynamic_id must be string type number! dynamic_id:{dynamic_id}"
            )
        url = f"{self.base_uri}/bilibili.app.dynamic.v2.Dynamic/DynDetail"
        data_dict = {
            # 'uid': random.randint(1, 3537105317792299),
            "dynamic_id": dynamic_id,
            # "ad_param": AdParam(
            #     ad_extra=ad_extra
            # ),
            "player_args": PlayerArgs(qn=112, fnval=17360, voice_balance=1),
            "share_id": "dt.dt-detail.0.0.pv",
            "share_mode": 3,
            "local_time": 8,
            "config": Config(),
        }
        msg = DynDetailReq(**data_dict)
        gresp = DynDetailReply()
        return await self.handle_grpc_request(url, msg, gresp, force_proxy=force_proxy)

    async def grpc_get_space_dyn_by_uid(
        self,
        uid: Union[str, int],
        history_offset: str = "",
        page: int = 1,
        force_non_proxy: bool = False,
    ) -> dict:
        """
         获取up空间
        :param force_non_proxy:
        :param proxy_flag:
        :param uid:
        :param history_offset:
        :param page:
        :return:
        """
        if type(uid) is str and str.isdigit(uid):
            uid = int(uid)
        if type(uid) is not int or type(history_offset) is not str:
            raise TypeError(
                f"uid must be a number and history_offset must be str! uid:{uid} history_offset:{history_offset}"
            )
        url = f"{self.base_uri}/bilibili.app.dynamic.v2.Dynamic/DynSpace"
        data_dict = {
            "host_uid": int(uid),
            "history_offset": history_offset,
            "local_time": 8,
            "page": page,
            "from": "space",
        }
        msg = DynSpaceReq(**data_dict)
        gresp = DynSpaceRsp()
        return await self.handle_grpc_request(
            url, msg, gresp, force_non_proxy=force_non_proxy
        )

    # endregion


class MY_Error(ValueError):
    pass


bili_grpc = BiliGrpc()

if __name__ == "__main__":

    async def _test():
        bili_grpc.my_proxy_addr = "http://192.168.1.200:8080"  # mitm代理默认地址
        result1 = await bili_grpc.grpc_get_dynamic_detail_by_type_and_rid(
            dynamic_type=2, rid=343543865, force_proxy=True
        )
        print(result1)

    asyncio.run(_test())
