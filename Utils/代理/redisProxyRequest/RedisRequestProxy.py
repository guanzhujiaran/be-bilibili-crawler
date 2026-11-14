# -*- coding: utf-8 -*-
# 由redis和mysql实现数据的统一
import asyncio
import random
import time
from functools import reduce
from json import JSONDecodeError
from ssl import SSLError

import curl_cffi
from exceptiongroup import ExceptionGroup
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
)
from Utils.代理.数据库操作.SqlAlcheyObj.ProxyModel import ProxyTab
from Utils.代理.数据库操作.async_proxy_op_alchemy_mysql_ver import SQLHelper, to_proxy_key

#: 代理鉴权失败（407）在响应层而不是异常层出现，需要单独判一次
_PROXY_AUTH_FAILED_STATUS = 407
#: 本地 ipv6 代理在随机取代理时的权重（相对真实代理池，约 1%）
_LOCAL_PROXY_WEIGHT = 10
#: 真实代理池的权重
_REAL_PROXY_WEIGHT = 1000


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

    def _timeshift(self, timestamp):
        local_time = time.localtime(timestamp)
        realtime = time.strftime("%Y-%m-%d %H:%M:%S", local_time)
        return realtime

    async def get_one_rand_proxy(self, tier: ProxyTier = ProxyTier.FAST) -> ProxyTab:
        return await SQLHelper.select_proxy("rand", channel=self.channel, tier=tier)

    # region 取代理
    def _local_ipv6_proxy(self) -> ProxyTab:
        """本地 ipv6 代理：它是配置项而不是代理池成员，不参与打分 / 健康统计。"""
        return ProxyTab(
            **{
                "proxy_id": 1,
                "proxy": CONFIG.custom_proxy,
                "status": 0,
                "update_ts": 0,
                "score": 10000,
                "add_ts": 0,
                "success_times": 10000,
                "zhihu_status": 0,
                "computed_proxy_str": CONFIG.my_ipv6_addr,
            }
        )

    async def _select_proxy(
        self, request_conf: RequestConf, tier: ProxyTier
    ) -> tuple[ProxyTab, bool]:
        """选一个代理，返回 ``(代理行, 是否本地 ipv6 代理)``。

        - ``is_use_custom_proxy``：直接用本地 ipv6 代理（原本地行为）；
        - 否则按权重在「本地 ipv6 代理 / 代理池」之间随机，代理池按 ``tier`` 取
          （fast 只用已验证的，slow 允许掺 cold 探测）；
        - 池里没有合格代理时**不再在请求里现场抓一轮**（那会把 worker 挂住几分钟），
          而是快速抛 ``RequestProxyUnavailableError``：fast 档直接失败交回队列，
          slow 档由重试预算的指数退避等待后台补池；本地 ipv6 可用时优先用它兜底。
        """
        if request_conf.is_use_custom_proxy:
            return self._local_ipv6_proxy(), True

        use_local = random.choices(
            [True, False],
            weights=[_LOCAL_PROXY_WEIGHT, _REAL_PROXY_WEIGHT],
            k=1,
        )[0]
        if use_local:
            return self._local_ipv6_proxy(), True

        excluded = excluded_proxy_keys()
        proxy, _ = await get_available_proxy(tier=tier, exclude=excluded)
        if proxy:
            return proxy, False
        if CONFIG.my_ipv6_addr:
            self.log.debug(
                f"代理池无符合 tier={tier.value} 的代理，本次使用本地 ipv6 代理兜底"
            )
            return self._local_ipv6_proxy(), True
        raise RequestProxyUnavailableError(
            f"没有可用代理（tier={tier.value}，已排除 {len(excluded)} 个失败代理）",
            tier=tier.value,
        )

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
        :mode single|rand 设置代理是否选择最高的单一代理还是随机
        :hybrid 是否将本地ipv6代理加入随机选择中
        :return:
        """
        tier = resolve_tier(request_conf)
        proxy, is_local = await self._select_proxy(request_conf, tier)
        proxy_key = "" if is_local else to_proxy_key(proxy.proxy)
        # 在途计数：fast 档要求 in_flight<2，靠它把热门代理的并发压在低位，
        # TTL 兜底，进程崩溃也不会永久占住代理。
        if proxy_key:
            await proxy_health_store.acquire_in_flight(proxy_key)
        try:
            return await self._dispatch_request(
                proxy=proxy,
                proxy_key=proxy_key,
                is_local=is_local,
                request_conf=request_conf,
                cookie_data=cookie_data,
                **kwargs,
            )
        finally:
            if proxy_key:
                await proxy_health_store.release_in_flight(proxy_key)

    async def _dispatch_request(
        self,
        proxy: ProxyTab,
        proxy_key: str,
        is_local: bool,
        request_conf: RequestConf,
        cookie_data: CookieWrapper | None = None,
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
        pool_proxy = None if is_local else proxy

        def _mark_failed(kind: ProxyFailureKind):
            """登记一次代理失败：MySQL 打分 + Redis 健康状态机 + 下一轮换代理。"""
            if not pool_proxy:
                return
            note_failed_proxy(proxy_key)

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
                    f"代理返回 407（需要认证，该代理不可用）\n{proxy.proxy}\n{kwargs}",
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
                raise Request412Error(req_text, -412)
            if not is_return_raw_response:
                req_dict = req.json()
                if type(req_dict) is list:
                    if proxy:
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
                        f"代理返回真实响应错误！\n{req.text}\n{kwargs}\n", -500
                    )

                if (
                    req_dict.get("code") == -412
                    or req_dict.get("code") == -352
                    or req_dict.get("code") == 65539
                ):
                    if cookie_data:
                        cookie_data.times_352 += 1
                    err_msg = f"{req_dict.get('code')}报错,换个ip\t{proxy}\t{self._timeshift(time.time())}\t{req_dict}\n{kwargs}\n{req.headers}"
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
            if pool_proxy:
                match _err.code:
                    case -412:
                        await handle_proxy_412(
                            proxy_tab=pool_proxy,
                        )
                    case -352:
                        Voucher352_logger.warning(
                            f"代理{pool_proxy.proxy} 报错-352 被封禁\n{kwargs}"
                        )
                        await handle_proxy_352(
                            proxy_tab=pool_proxy,
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
            if pool_proxy:
                await handle_proxy_request_fail(proxy_tab=pool_proxy, kind=kind)
                note_failed_proxy(proxy_key)
            raise RequestProxyNetworkError(
                f"{type(_err).__name__}: {_err}",
                -1,
                proxy_key=proxy_key,
                curl_code=_curl_code_of(_err),
            )
        except AttributeError as _err:
            self.log.exception(f"\n代理：{str(proxy)}\n请求时出错，一般错误：{_err}")
            if pool_proxy:
                await handle_proxy_request_fail(proxy_tab=pool_proxy)
                note_failed_proxy(proxy_key)
            raise RequestUnknownError(_err)
        except Exception as _err:
            self.log.exception(
                f"\n代理：{str(proxy)}\n未知请求错误！请求：\n{kwargs}"
                f"\n结束，报错了！"
                f"\n{type(_err)}"
                f"\n{_err}\n{req_text}"
            )
            if pool_proxy:
                await handle_proxy_request_fail(proxy_tab=pool_proxy)
                note_failed_proxy(proxy_key)
            raise RequestUnknownError(_err)
        if req_dict is False and is_return_raw_response is False:
            raise ValueError("请求返回的req_dict是False")
        if proxy:
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
