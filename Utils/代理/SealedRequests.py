import asyncio
import random
import ssl
import typing
from typing import Union
from curl_cffi import Curl, Response, CurlHttpVersion, BrowserTypeLiteral, requests
from curl_cffi.fingerprints import FingerprintManager
from curl_cffi.requests.utils import _is_native_impersonate_target
from curl_cffi.requests.impersonate import resolve_latest_browser_type
from httpx._types import (
    RequestContent,
    RequestFiles,
    QueryParamTypes,
    HeaderTypes,
    CookieTypes,
    RequestData,
)

from Utils.代理.数据库操作.comm import get_scheme_ip_port_form_proxy_dict

# curl_cffi 内部实现变动导致探测失败时的兜底目标（别名，会被解析成最新版本）
_FALLBACK_IMPERSONATE_TARGETS = ("chrome", "edge", "safari", "firefox")


def _probe_impersonate_target(target: str, named_fingerprints: set[str]) -> bool:
    """实测单个 impersonate 目标是否真的可用。

    必须复刻 curl_cffi 在 set_curl_options 里的分支口径，只看任何一边都会误判：

    - 原生分支：库会先 resolve_latest_browser_type() 再调 Curl.impersonate()，
      返回非 0 表示底层 .so 确实不支持。注意不能用裸名去调 Curl.impersonate，
      例如 "chrome" 裸调返回 43，但库先解析成 chrome150 之后是成功的。
    - 扩展分支：名字不在原生名单里时，库改查指纹库（内置 + impersonate.pro 下发），
      查不到就直接抛 ImpersonateError。典型例子正是 safari18_4 / safari18_4_ios：
      底层 .so 其实支持（Curl.impersonate 返回 0），但 Python 侧名单没登记、
      指纹库也没有，库永远进不了原生分支 → 实际仍然不可用。
    """

    if _is_native_impersonate_target(target):
        curl = Curl()
        try:
            if curl.impersonate(resolve_latest_browser_type(target)) == 0:
                return True
        finally:
            try:
                curl.close()
            except Exception:  # noqa: BLE001 句柄关闭失败不影响判定结果
                pass
    return target in named_fingerprints


def _build_supported_impersonate_targets() -> list[str]:
    """对候选目标逐个实测，构建 curl_cffi 当前构建真正支持的 impersonate 白名单。

    ``BrowserTypeLiteral.__args__`` 只是给类型检查器看的字面量清单：它既可能残留
    底层已移除的 deprecated 别名，也可能漏掉底层其实支持的名字，因此不能当作支持
    清单使用。这里对全部候选逐个实测（每个目标走一遍库的真实分支口径），只保留
    真正能用的，避免依赖任何硬编码的黑白名单。
    """
    try:
        named_fingerprints = set(FingerprintManager.load_fingerprints())
        targets = [
            target
            for target in BrowserTypeLiteral.__args__
            if _probe_impersonate_target(target, named_fingerprints)
        ]
    except Exception:  # curl_cffi 内部实现变动时静默退回兜底白名单
        targets = []
    return targets or list(_FALLBACK_IMPERSONATE_TARGETS)


# 进程启动时确定一次即可，请求路径上只做随机取值，不再重复探测
SUPPORTED_IMPERSONATE_TARGETS = _build_supported_impersonate_targets()


class SSLFactory:
    @property
    def bili_cipher(self):
        base_cipher = ["ECDHE-RSA-AES128-GCM-SHA256"]
        cipher_suites = [
            "ECDHE-RSA-AES256-GCM-SHA384",
            "ECDHE-ECDSA-AES128-GCM-SHA256",
            "ECDHE-ECDSA-AES256-GCM-SHA384",
            "DHE-RSA-AES128-GCM-SHA256",
            "DHE-RSA-AES256-GCM-SHA384",
            "ECDHE-RSA-AES128-SHA256",
            "ECDHE-RSA-AES256-SHA384",
            "DHE-RSA-AES128-SHA256",
            "DHE-RSA-AES256-SHA256",
            "DHE-RSA-AES256-SHA384",
            "DHE-RSA-AES128-CCM",
            "DHE-RSA-AES256-CCM",
            "DHE-RSA-AES128-CCM8",
            "ECDHE-RSA-CHACHA20-POLY1305",
            "ECDHE-ECDSA-CHACHA20-POLY1305",
            "DHE-RSA-CHACHA20-POLY1305",
            "TLS_AES_128_GCM_SHA256",
            "TLS_AES_256_GCM_SHA384",
            "TLS_CHACHA20_POLY1305_SHA256",
            "TLS_AES_128_CCM_SHA256",
            "TLS_AES_128_CCM_8_SHA256",
        ]
        for i in range(random.choice(range(4))):
            base_cipher.append(
                cipher_suites.pop(random.choice(range(len(cipher_suites))))
            )
        common_cipher = [
            "ECDH+AESGCM",
            "ECDH+CHACHA20",
            "DH+AESGCM",
            "DH+CHACHA20",
            "ECDH+AES256",
            "DH+AES256",
            "ECDH+AES128",
            "DH+AES",
            "ECDH+HIGH",
            "DH+HIGH",
            "RSA+AESGCM",
            "RSA+AES",
            "RSA+HIGH",
        ]
        random.shuffle(common_cipher)
        return (
            ":".join(common_cipher)
            + ":"
            + ":".join(base_cipher)
            + ":!aNULL:!eNULL:!MD5"
        )

    def __call__(self) -> ssl.SSLContext:
        context = ssl.SSLContext(ssl.PROTOCOL_TLS)
        context.minimum_version = ssl.TLSVersion.TLSv1
        context.maximum_version = ssl.TLSVersion.TLSv1_3
        context.set_alpn_protocols(["h2"])
        context.set_ciphers(self.bili_cipher)
        return context


sslgen = SSLFactory()
asyncio_timeout = 60


def format_transport(request_proxy: dict | None):
    # if format_ip_str := get_scheme_ip_port_form_proxy_dict(request_proxy):
    #     if 'sock' in format_ip_str or 'socks' in format_ip_str:
    #         return AsyncProxyTransport.from_url(
    #             url=format_ip_str,
    #         )
    return None


def format_httpx_proxy(request_proxy: dict | None) -> str | None:
    if not request_proxy:
        return None
    if format_ip_str := get_scheme_ip_port_form_proxy_dict(request_proxy):
        return format_ip_str
    else:
        return None


# class MYASYNCHTTPX:
#     async def get(self, url, headers=None, verify=False, proxies: Union[dict, None] = None, timeout=10, params=None,
#                   *args, **kwargs):
#         """
#
#         :param url:
#         :param headers:
#         :param verify:
#         :param proxies: like {
#             'http':'http://1.1.1.1',
#             'https':'http://1.1.1.1'
#         }
#         :param timeout:
#         :param params:
#         :param args:
#         :param kwargs:
#         :return:
#         """
#         format_proxy_str = format_httpx_proxy(proxies)
#         async with AsyncClient(
#                 transport=format_transport(proxies),
#                 proxy=format_proxy_str,
#                 http2=True,
#                 verify=True,
#                 timeout=timeout
#         ) as client:
#             client.headers.clear()
#             resp = await asyncio.wait_for(
#                 client.get(url=url, headers=headers, params=params, timeout=timeout, follow_redirects=True,
#                            ),
#                 timeout=asyncio_timeout)
#             return resp
#
#     async def post(self, url, data=None, headers=None, verify=False, proxies=None, timeout=10, *args, **kwargs):
#         format_proxy_str = format_httpx_proxy(proxies)
#         async with AsyncClient(
#                 transport=format_transport(proxies),
#                 proxy=format_proxy_str,
#                 http2=True,
#                 verify=True,
#                 timeout=timeout
#         ) as client:
#             client.headers.clear()
#             resp = await asyncio.wait_for(
#                 client.post(url=url, data=data, headers=headers, timeout=timeout, follow_redirects=True),
#                 timeout=asyncio_timeout
#             )
#             return resp
#
#     async def request(self, url,
#                       data: typing.Optional[RequestData] = None,
#                       method='GET',
#                       headers: typing.Optional[HeaderTypes] = None,
#                       verify=False,
#                       proxies=None,
#                       timeout=10,
#                       content: typing.Optional[RequestContent] = None,
#                       files: typing.Optional[RequestFiles] = None,
#                       json: typing.Optional[typing.Any] = None,
#                       params: typing.Optional[QueryParamTypes] = None,
#                       cookies: typing.Optional[CookieTypes] = None,
#                       extensions: typing.Optional[dict] = None, *args, **kwargs):
#         """
#
#         :param url:
#         :param data:
#         :param method:
#         :param headers:
#         :param verify:
#         :param proxies: {"http":"xxx.xxx.xxx.xxx", "https":"xxx.xxx.xxx.xxx"}
#         :param timeout:
#         :param content:
#         :param files:
#         :param json:
#         :param params:
#         :param cookies:
#         :param extensions:
#         :return:
#         """
#         ca = True
#         if (
#                 'api.bilibili.com/x/gaia-vgate/v1/register' in url or
#                 'api.bilibili.com/x/gaia-vgate/v1/validate' in url
#         ):
#             ca = sslgen()
#             format_proxy_str = None
#             format_transport_ins = None
#         else:
#             format_proxy_str = format_httpx_proxy(proxies)
#             format_transport_ins = format_transport(proxies)
#         async with AsyncClient(
#                 transport=format_transport_ins,
#                 proxy=format_proxy_str,
#                 verify=ca,
#                 http2=True,
#                 http1=False,
#                 follow_redirects=True,
#                 timeout=timeout
#         ) as client:
#             client.headers.clear()
#             resp = await asyncio.wait_for(
#                 client.request(url=url, data=data, method=method, headers=headers, timeout=timeout,
#                                content=content, files=files, json=json, params=params, cookies=cookies,
#                                extensions=extensions, follow_redirects=True
#                                ),
#                 timeout=asyncio_timeout
#             )
#             return resp
#


class MYASYNCHTTPX:
    async def get(
        self,
        url,
        headers=None,
        verify=False,
        proxies: Union[dict, None] = None,
        timeout=10,
        params=None,
        *args,
        **kwargs,
    ) -> Response:
        """

        :param url:
        :param headers:
        :param verify:
        :param proxies: like {
            'http':'http://1.1.1.1',
            'https':'http://1.1.1.1'
        }
        :param timeout:
        :param params:
        :param args:
        :param kwargs:
        :return:
        """
        format_proxy_str = format_httpx_proxy(proxies)
        if type(headers) is tuple:
            headers = list(headers)
        impersonate = random.choice(SUPPORTED_IMPERSONATE_TARGETS)
        async with requests.AsyncSession(
            max_clients=1000,
            allow_redirects=True,
            timeout=30,
            verify=True,
            trust_env=True,
            impersonate="chrome",
            http_version=CurlHttpVersion.V2_0,
        ) as s:
            resp = await s.get(
                url=url,
                # headers=headers,
                timeout=timeout,
                params=params,
                proxy=format_proxy_str,
                verify=False,
                # default_headers=False,
                impersonate=impersonate,
            )
        return resp

    async def post(
        self,
        url,
        data=None,
        headers=None,
        verify=False,
        proxies: dict = None,
        timeout=10,
        json: dict | list | None = None,
        *args,
        **kwargs,
    ) -> Response:
        format_proxy_str = format_httpx_proxy(proxies)
        if type(headers) is tuple:
            headers = list(headers)
        impersonate = random.choice(SUPPORTED_IMPERSONATE_TARGETS)
        async with requests.AsyncSession(
            max_clients=1000,
            allow_redirects=True,
            timeout=30,
            verify=True,
            trust_env=True,
            impersonate="chrome",
            http_version=CurlHttpVersion.V2_0,
        ) as s:
            resp = await s.post(
                url=url,
                data=data,
                headers=headers,
                timeout=timeout,
                proxy=format_proxy_str,
                verify=False,
                default_headers=False,
                json=json,
                impersonate=impersonate,
            )
        return resp

    async def request(
        self,
        url,
        data: typing.Optional[RequestData] = None,
        method: typing.Literal[
            "GET", "POST", "PUT", "DELETE", "OPTIONS", "HEAD", "TRACE", "PATCH", "QUERY"
        ] = "GET",
        headers: typing.Optional[HeaderTypes] = None,
        verify=False,
        proxies=None,
        timeout=10,
        content: typing.Optional[RequestContent] = None,
        files: typing.Optional[RequestFiles] = None,
        json: typing.Optional[typing.Any] = None,
        params: typing.Optional[QueryParamTypes] = None,
        cookies: typing.Optional[CookieTypes] = None,
        extensions: typing.Optional[dict] = None,
        *args,
        **kwargs,
    ) -> Response:
        """

        :param url:
        :param data:
        :param method:
        :param headers:
        :param verify:
        :param proxies: {"http":"xxx.xxx.xxx.xxx", "https":"xxx.xxx.xxx.xxx"}
        :param timeout:
        :param content:
        :param files:
        :param json:
        :param params:
        :param cookies:
        :param extensions:
        :return:
        """
        if (
            "api.bilibili.com/x/gaia-vgate/v1/register" in url
            or "api.bilibili.com/x/gaia-vgate/v1/validate" in url
        ):
            format_proxy_str = None
        else:
            format_proxy_str = format_httpx_proxy(proxies)
        if type(headers) is tuple:
            headers = list(headers)
        impersonate = random.choice(SUPPORTED_IMPERSONATE_TARGETS)
        async with requests.AsyncSession(
            max_clients=1000,
            allow_redirects=True,
            timeout=30,
            verify=True,
            trust_env=True,
            impersonate="chrome",
            http_version=CurlHttpVersion.V2_0,
        ) as s:
            resp = await s.request(
                url=url,
                data=data,
                method=method,
                headers=headers,
                timeout=timeout,
                files=files,
                json=json,
                params=params,
                cookies=cookies,
                proxy=format_proxy_str,
                verify=False,
                default_headers=False,
                impersonate=impersonate,
            )
        return resp


my_async_httpx = MYASYNCHTTPX()
if __name__ == "__main__":
    MyAsyncReq = MYASYNCHTTPX()
    loop = asyncio.get_event_loop()
    task = loop.create_task(
        MyAsyncReq.request(
            method="GET",
            url="https://test.ipw.cn",
            headers=(
                ("Referer", "https://www.bilibili.com/"),
                (
                    "User-Agent",
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
                ),
                ("Cookie", "1"),
            ),
            proxies={
                "sock4": "socks4://1.10.133.155:4145",
                "socks4": "socks4://1.10.133.155:4145",
            },
        )
    )
    loop.run_until_complete(task)
    print(task.result().text)
