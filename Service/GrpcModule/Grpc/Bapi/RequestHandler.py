from Models.AntiRisk.Bili.WebCookie import BiliWebCookie
from Utils.GrpcUtils.UserAgentParser import UserAgentParser
from Service.GrpcModule.Grpc.Bapi.BiliCookie import get_bili_cookie, CookieWrapper
from Utils.代理.mdoel.RequestConf import RequestConf
from Utils.代理.session_profile import (
    profile_for_session_key,
    session_key_from_headers,
)


async def prepare_request_data(
    request_conf: RequestConf,
    /,
    extra_headers: dict | None = None,
    cookie_include_list: list[BiliWebCookie.__pydantic_fields_set__] | None = None,
) -> tuple[CookieWrapper | None, dict]:
    """
    准备请求所需的 Cookie 和 Headers

    UA 与 TLS 指纹必须同源：带 cookie 时用 cookie 创建时绑定的 UA；不带 cookie
    时按匿名会话键固定一份 UA，保证二者都指向同一个 ``chromeNNN`` 主版本。
    """
    cookie_data = None
    headers = extra_headers or {}

    if request_conf.is_use_cookie:
        cookie_data = await get_bili_cookie()
        ua = cookie_data.ua
        headers.update(
            {"cookie": cookie_data.ck.to_str(include_keys=cookie_include_list)}
        )
    else:
        ua = profile_for_session_key(
            request_conf.session_key or session_key_from_headers(headers)
        ).ua

    ua_parser = UserAgentParser(ua, is_mobile=False)
    base_headers = ua_parser.get_headers_dict(extra_headers_dict=headers)

    return cookie_data, base_headers
