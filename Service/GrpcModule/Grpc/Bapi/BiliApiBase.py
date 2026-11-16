import json
import time

import curl_cffi

from CONFIG import CONFIG
from Models.AntiRisk.Bili.WebCookie import BiliWebCookie
from Service.GrpcModule.Models.ApiResponseModel import (
    FrontendFingerSpiResp,
    BiliBaseResp,
)
from Utils.GrpcUtils.UserAgentParser import UserAgentParser
from Service.GrpcModule.Grpc.Bapi.Constants import (
    URL_FRONTEND_FINGER_SPI,
    URL_BILI_MAIN_PAGE,
    URL_GEN_WEB_TICKET,
    URL_GAIA_EXCLIMB_WUZHI,
    URL_GAIA_EXCLIMB_CONGLING,
)
from Service.GrpcModule.Grpc.Bapi.GaiaActivation import (
    GaiaCongLingEnvelope,
    build_congling_body,
    log_missing_encryptor,
    validate_envelope,
)
from Service.GrpcModule.Grpc.Bapi.BapiUtils import request_wrapper
from Utils.代理.mdoel.RequestConf import RequestConf
from Utils.代理.redisProxyRequest.RedisRequestProxy import request_with_proxy_internal
from Utils.加密.utils import hmac_sha256


def _gen_headers_base(bili_web_cookie: BiliWebCookie, exheaders: dict = None):
    ua_parser = UserAgentParser(
        user_agent=bili_web_cookie.gen_web_cookie_params.ua, is_mobile=False
    )
    base_headers = ua_parser.get_headers_dict(extra_headers_dict=exheaders)
    return base_headers


@request_wrapper
async def get_frontend_finger_spi(
    cookie_str: str,
    bili_web_cookie: BiliWebCookie,
    request_conf=RequestConf(
        is_use_custom_proxy=True,
        local_exit_only=True,
        ignore_risk_cooldown=True,
        is_use_available_proxy=False,
        is_use_cookie=False,
        is_return_raw_response=False,
    ),
) -> BiliBaseResp[FrontendFingerSpiResp] | curl_cffi.Response:
    url = URL_FRONTEND_FINGER_SPI
    headers = _gen_headers_base(
        bili_web_cookie=bili_web_cookie,
        exheaders={
            "origin": "https://www.bilibili.com",
            "referer": URL_BILI_MAIN_PAGE,
            "cookie": cookie_str,
        },
    )
    cookie_data = None
    resp = await request_with_proxy_internal.request_with_proxy(
        method="GET",
        url=url,
        headers=headers,
        request_conf=request_conf,
        cookie_data=cookie_data,
    )
    if request_conf.is_return_raw_response:
        return resp
    return BiliBaseResp[FrontendFingerSpiResp].model_validate(resp)


@request_wrapper
async def gen_web_ticket(
    cookie_str: str,
    bili_web_cookie: BiliWebCookie,
    request_conf=RequestConf(
        is_use_custom_proxy=True,
        local_exit_only=True,
        ignore_risk_cooldown=True,
        is_use_available_proxy=False,
        is_use_cookie=False,
        is_return_raw_response=False,
    ),
):
    url = URL_GEN_WEB_TICKET
    headers = _gen_headers_base(
        bili_web_cookie=bili_web_cookie,
        exheaders={
            "origin": "https://www.bilibili.com",
            "referer": URL_BILI_MAIN_PAGE,
            "cookie": cookie_str,
        },
    )
    cookie_data = None
    o = hmac_sha256("XgwSnGZ1p", f"ts{int(time.time())}")
    params = {
        "key_id": "ec02",
        "hexsign": o,
        "context[ts]": f"{int(time.time())}",
        "csrf": "",
    }
    resp = await request_with_proxy_internal.request_with_proxy(
        method="POST",
        url=url,
        params=params,
        headers=headers,
        request_conf=request_conf,
        cookie_data=cookie_data,
    )
    return resp


@request_wrapper
async def get_bili_main_page_raw_resp(
    bili_web_cookie: BiliWebCookie,
    request_conf=RequestConf(
        is_use_custom_proxy=True,
        local_exit_only=True,
        ignore_risk_cooldown=True,
        is_use_available_proxy=False,
        is_use_cookie=False,
        is_return_raw_response=True,
    ),
) -> curl_cffi.requests.models.Response:
    url = URL_BILI_MAIN_PAGE
    headers = _gen_headers_base(
        bili_web_cookie=bili_web_cookie,
        exheaders={
            "origin": "https://www.bilibili.com",
            "referer": URL_BILI_MAIN_PAGE,
        },
    )
    cookie_data = None
    resp = await request_with_proxy_internal.request_with_proxy(
        method="GET",
        url=url,
        headers=headers,
        request_conf=request_conf,
        cookie_data=cookie_data,
    )
    return resp


@request_wrapper
async def gaia_gateway_ExClimbWuzhi(
    cookie_str: str,
    bili_web_cookie: BiliWebCookie,
    payload: str,
    request_conf=RequestConf(
        is_use_custom_proxy=True,
        local_exit_only=True,
        ignore_risk_cooldown=True,
        is_use_available_proxy=False,
        is_use_cookie=False,
        is_return_raw_response=False,
    ),
):
    url = URL_GAIA_EXCLIMB_WUZHI
    headers = _gen_headers_base(
        bili_web_cookie=bili_web_cookie,
        exheaders={
            "origin": "https://www.bilibili.com",
            "referer": URL_BILI_MAIN_PAGE,
            "cookie": cookie_str,
        },
    )
    cookie_data = None
    resp = await request_with_proxy_internal.request_with_proxy(
        url=url,
        method="post",
        data=payload,
        headers=headers,
        request_conf=request_conf,
        cookie_data=cookie_data,
    )

    return resp


@request_wrapper
async def gaia_gateway_ExClimbCongling(
    envelope: GaiaCongLingEnvelope | None = None,
    cookie_str: str = "",
    ua: str | None = None,
    request_conf=RequestConf(
        is_use_custom_proxy=True,
        local_exit_only=True,
        ignore_risk_cooldown=True,
        is_use_available_proxy=False,
        is_use_cookie=False,
        is_return_raw_response=False,
    ),
):
    """ExClimbCongLing：只有拿到 WASM 加密结果（``envelope``）才会真正发送。

    没有加密器时返回 ``None``（不伪造请求体）。schema 与证据见
    ``Service/GrpcModule/Grpc/Bapi/GaiaActivation.py`` 与 docs/gaia_evidence/。
    """
    if envelope is None:
        log_missing_encryptor()
        return None
    if not validate_envelope(envelope):
        log_missing_encryptor()
        return None

    headers = {
        "origin": "https://www.bilibili.com",
        "referer": URL_BILI_MAIN_PAGE,
        "accept": "*/*",
        "content-type": "application/json",
    }
    if ua:
        headers["user-agent"] = ua
    if cookie_str:
        headers["cookie"] = cookie_str

    return await request_with_proxy_internal.request_with_proxy(
        url=URL_GAIA_EXCLIMB_CONGLING,
        method="POST",
        data=json.dumps(build_congling_body(envelope), ensure_ascii=False),
        headers=UserAgentParser(
            ua or CONFIG.rand_ua, is_mobile=False
        ).get_headers_dict(extra_headers_dict=headers),
        request_conf=request_conf,
        cookie_data=None,
    )
