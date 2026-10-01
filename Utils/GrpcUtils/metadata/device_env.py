# -*- coding: utf-8 -*-
"""设备环境对象：一台「设备」的全部身份、硬件参数、请求身份与 region 状态。

对应 APK 的实际形态：一台设备拥有一整套自洽的身份（buvid / 指纹 / 机型 / UA / 会话），
且服务端按设备下发的 region（``x-bili-metadata-recent-region`` 是绑定 buvid 的
ES256 JWT，24h 有效）也属于这台设备（APK: ``kntr.base.region.impl.e``）。

:class:`DeviceEnv` 把「这台设备需要的一切」收成一个对象，方法即它的行为：

- 身份：``buvid`` / ``fp_local`` / ``fp_remote`` / ``guest_id`` / ``session_id``；
- 硬件：机型 / 系统 / 版本 / 渠道（决定 UA 与 ``x-bili-device-bin``）；
- 请求身份：三套 UA（gRPC 新格式 / buvid 老格式 / 普通 HTTP 的 BiliDroid 格式）；
- region：``ip_region`` / ``legal_region`` / ``recent_region`` 三个字段 +
  :meth:`DeviceEnv.learn_region`（被动学习）、:meth:`DeviceEnv.fetch_region`（主动领取）、
  :meth:`DeviceEnv.region_headers`（回带）；recent-region 按 JWT 的 ``exp`` 判过期。

不同设备之间互不共享任何状态。
"""

import base64
import gzip  # noqa: F401  (保持与历史导入一致，供同包模块直接使用)
import hashlib
import json
import random
import re
import time
import uuid
from dataclasses import dataclass, field
from dataclasses import fields as dc_fields
from datetime import datetime

from CONFIG import CONFIG
from Service.GrpcModule.Grpc.Bapi.BapiUtils import appsign
from Service.GrpcModule.Grpc.GrpcProto.bilibili.metadata.locale.locale_pb2 import (
    Locale,
    LocaleIds,
)
from Service.GrpcModule.Grpc.GrpcProto.bilibili.metadata.network.network_pb2 import (
    Network,
    NetworkType,
    NetQuality,
)
from Utils.GrpcUtils.CONST import AppChannels
from Utils.代理.SealedRequests import my_async_httpx

# —— region 请求头（抓包中的三个头名）——
IP_REGION_HEADER = "x-bili-metadata-ip-region"
LEGAL_REGION_HEADER = "x-bili-metadata-legal-region"
RECENT_REGION_HEADER = "x-bili-metadata-recent-region"

# recent-region 的 JWT 过期提前量（秒），避免在边界上刚好失效
_EXPIRE_SKEW = 60

# 直连哨兵：显式要求不走任何代理（排查风控 / 本机可直连时使用）
DIRECT = "direct"


def resolve_proxies(proxy=None):
    """把各处形态不一的 proxy 统一成请求库的 ``proxies`` 参数。

    - ``DIRECT`` / ``False`` → ``None``：直连；
    - 空值 → ``CONFIG.custom_proxy``：沿用配置里的代理（既有行为）；
    - ``{"proxy": {...}}`` → 解开包装；
    - ``{"http": ..., "https": ...}`` → 原样使用。
    """
    if proxy is False or proxy == DIRECT:
        return None
    if not proxy:
        return CONFIG.custom_proxy
    if isinstance(proxy, dict) and isinstance(proxy.get("proxy"), dict):
        return proxy["proxy"]
    return proxy


DEFAULT_DALVIK = (
    "Dalvik/2.1.0 (Linux; U; Android 11; ONEPLUS A6000 Build/RKQ1.201217.002)"
)


class Fp:
    """设备指纹生成器（buvid + 机型 + radio 版本 → fp_local / fp_remote / fp）。"""

    def __init__(self, buvid_auth, device_model, device_radio_ver):
        self.buvid_auth = buvid_auth
        self.device_model = device_model
        self.device_radio_ver = device_radio_ver

    def gen(self, timestamp):
        device_fp = f"{self.buvid_auth}{self.device_model}{self.device_radio_ver}"
        device_fp_md5 = hashlib.md5(device_fp.encode()).hexdigest()

        fp_raw = device_fp_md5
        fp_raw += datetime.fromtimestamp(timestamp).strftime("%Y%m%d%H%M%S")
        fp_raw += self.gen_random_string(16)

        veri_code_str = format(
            "%02x"
            % (
                sum(
                    int(fp_raw[i : i + 2], 16)
                    for i in range(0, min(len(fp_raw), 62), 2)
                )
                % 256
            )
        )

        fp_raw += veri_code_str

        return fp_raw

    @staticmethod
    def gen_random_string(length):
        charset = "0123456789abcdef"
        return "".join(random.choice(charset) for _ in range(length))


def random_id() -> str:
    """8 位会话 id（gRPC 的 x-bili-fawkes-req-bin 与 HTTP 的 session_id 共用）。"""
    return "".join(random.sample("0123456789abcdefghijklmnopqrstuvwxyz", 8))


def fake_buvid() -> str:
    """按「随机 mac → md5」的规则造一个 buvid（每台设备一个）。"""
    mac_list = []
    for _ in range(1, 7):
        rand_str = "".join(random.sample("0123456789abcdef", 2))
        mac_list.append(rand_str)
    rand_mac = ":".join(mac_list)
    md5 = hashlib.md5()
    md5.update(rand_mac.encode())
    md5_mac_str = md5.hexdigest()
    md5_mac = list(md5_mac_str)
    return f"XY{md5_mac[2]}{md5_mac[12]}{md5_mac[22]}{md5_mac_str}".upper()


def gen_trace_id() -> str:
    """与抓包一致的 trace id：``<32位hex>:<后16位hex>:0:0``。"""
    trace_id_uid = uuid.uuid4().hex
    return f"{trace_id_uid}:{trace_id_uid[-16:]}:0:0"


def encode_metadata_headers(headers) -> tuple:
    """把 ``(k, v)`` 请求头里的 bytes 值编码成 base64（**去掉 ``=`` 填充**）。

    抓包与 APK 都证实 ``x-bili-*-bin`` 头用的是 gRPC 的
    ``BASE64_ENCODING_OMIT_PADDING``（见 ``MetadataCodeC``）。
    """
    return tuple(
        (
            k,
            base64.b64encode(v).decode("utf-8").strip("=")
            if isinstance(v, bytes)
            else str(v),
        )
        for k, v in headers
    )


def jwt_payload(token: str) -> dict | None:
    """解析 JWT 的 payload（只做 base64url 解码，不校验签名）。"""
    parts = token.split(".")
    if len(parts) != 3:
        return None
    try:
        raw = base64.urlsafe_b64decode(parts[1] + "=" * (-len(parts[1]) % 4))
        data = json.loads(raw)
    except (ValueError, TypeError):
        return None
    return data if isinstance(data, dict) else None


def is_expired(token: str) -> bool:
    """按 JWT 的 ``exp`` 判断是否过期；解析不出 exp 时视为未过期。"""
    payload = jwt_payload(token)
    if not payload:
        return False
    exp = payload.get("exp")
    if not isinstance(exp, (int, float)):
        return False
    return time.time() + _EXPIRE_SKEW >= exp


def _header(headers, name: str) -> str | None:
    """从 dict / httpx.Headers / (k, v) 序列里大小写无关地取一个头。"""
    if not headers:
        return None
    value = headers.get(name) if hasattr(headers, "get") else dict(headers).get(name)
    if value is None:
        items = headers.items() if hasattr(headers, "items") else headers
        for key, val in items:
            if str(key).lower() == name.lower():
                value = val
                break
    if value is None:
        return None
    if isinstance(value, bytes):
        try:
            value = value.decode("utf-8")
        except UnicodeDecodeError:
            return None
    return str(value).strip() or None


@dataclass
class DeviceEnv:
    """一台设备的完整环境：身份 + 硬件 + 请求身份 + region。"""

    # —— 硬件/系统（抓包机型：Redmi 23113RKC6C / Android 12 / 9.13.0 build 9130500）——
    build: int = 9130500
    inner_ver: str = "9130510"
    device_model: str = "ONEPLUS A6000"
    osver: str = "11"
    version_name: str = "9.13.0"
    brand: str = "OnePlus"
    channel: str = field(default_factory=lambda: random.choice(AppChannels))
    # —— 设备身份（一台设备一套，互不共享）——
    buvid: str = field(default_factory=fake_buvid)
    fp_local: str = ""
    fp_remote: str = ""
    # fp 的生成时间戳：x-bili-device-bin 的 fts 必须与之一致（抓包：fts 比 fp 内嵌时间早几秒）
    fp_ts: int = 0
    guest_id: str = field(
        default_factory=lambda: str(random.randint(10000000000000, 99999999999999))
    )
    session_id: str = field(default_factory=random_id)
    # —— 请求身份：三套 UA ——
    ua: str = ""
    ua_http: str = ""
    ua_bapi: str = ""
    # —— region：服务端按 buvid 下发，属于这台设备自己 ——
    ip_region: str | None = None
    legal_region: str | None = None
    recent_region: str | None = None

    def __post_init__(self) -> None:
        if not self.fp_remote:
            # 抓包里 fp_local / fp_remote / fp 三者一致
            self.fp_local = self.fp_remote = self._gen_fp()
        if not (self.ua and self.ua_http and self.ua_bapi):
            self.gen_ua()

    # 序列化（供 Redis 设备池持久化复用）----------------------------------
    def to_dict(self) -> dict:
        """导出为可 JSON 序列化的字典（字段与构造参数一致）。"""
        return {f.name: getattr(self, f.name) for f in dc_fields(self)}

    @classmethod
    def from_dict(cls, data: dict) -> "DeviceEnv":
        """从字典还原设备；已过期的 recent-region 直接丢掉。"""
        valid = {f.name for f in dc_fields(cls)}
        device = cls(**{k: v for k, v in data.items() if k in valid})
        if device.recent_region and is_expired(device.recent_region):
            device.recent_region = None
        return device

    # 身份 ---------------------------------------------------------------
    def _gen_fp(self) -> str:
        # 上报时间往前挪一点，避免服务端认为时间异常；同时记下该时间戳，
        # 供 x-bili-device-bin 的 fts 复用（二者必须同源，见 device_params）
        self.fp_ts = int(time.time()) - random.randint(600, 60000)
        return Fp(self.buvid, self.device_model, "").gen(self.fp_ts)

    def fp_timestamp(self) -> int:
        """fp 的生成时间戳：优先取字段，其次从 fp 串内嵌时间反解（兼容旧存量设备）。"""
        if self.fp_ts:
            return self.fp_ts
        try:
            return int(
                datetime.strptime(self.fp_local[32:46], "%Y%m%d%H%M%S").timestamp()
            )
        except (ValueError, IndexError):
            return int(time.time())

    # UA -----------------------------------------------------------------
    def adopt_dalvik(self, Dalvik: str) -> None:
        """从 Dalvik 字符串反解机型/系统版本（抓包里的 Dalvik 就带这两项）。"""
        model = "".join(re.findall(r"Android.*?\d+; (.*?) (?:Build|MIUI)", Dalvik))
        osver = "".join(re.findall(r"Android (.*?[\w]);", Dalvik))
        if model:
            self.device_model = model
        if osver:
            self.osver = osver

    def gen_ua(self, Dalvik: str | None = None) -> None:
        """生成三套 UA：gRPC(新格式) / buvid·老格式 HTTP / 普通 HTTP API(BiliDroid)。"""
        Dalvik = Dalvik or DEFAULT_DALVIK
        tail = (
            f"os/android "
            f"model/{self.device_model} "
            f"mobi_app/android "
            f"build/{self.build} "
            f"channel/{self.channel} "
            f"innerVer/{self.inner_ver} "
            f"osVer/{self.osver} "
            f"network/2"
        )
        self.ua_http = f"{Dalvik} {self.version_name} {tail}"
        self.ua_bapi = (
            f"Mozilla/5.0 BiliDroid/{self.version_name} (bbcallen@gmail.com) "
            f"{self.version_name} {tail}"
        )
        self.ua = (
            f"grpc-c++/1.66.2 {Dalvik} "
            f"ignet_http/5.66.4 "
            f"(bilibili-android-client/BiliApp/{self.build} "
            f"com.bilibili.app.in/{self.build} "
            f"channel/{self.channel} "
            f"grpc-c/43.0.0) ct/1"
        )

    def adopt_http_ua(self, ua: str, brand: str | None = None) -> None:
        """从外部（Dalvik 池）UA 反向解析硬件信息，并派生出其余 UA。"""
        build = "".join(re.findall(r"(?:build/|BiliApp/)(\d+)", ua))
        if build.isdigit():
            self.build = int(build)
            self.inner_ver = str(int(build) + 10)
        model = "".join(re.findall(r"Android.*?\d+; (.*?) (?:Build|MIUI)", ua))
        osver = "".join(re.findall(r"Android (.*?[\w]);", ua))
        version_name = "".join(re.findall(r"\(.*?\) (\d+\.\d+\.\d+) ", ua))
        channel = "".join(re.findall(r"channel/(\w+)", ua))
        if model:
            self.device_model = model
        if osver:
            self.osver = osver
        if version_name:
            self.version_name = version_name
        if channel:
            self.channel = channel
        if brand:
            self.brand = brand
        if ua.startswith("grpc-c++"):
            self.gen_ua()
            self.ua = ua
        else:
            dalvik = ua.split(f" {self.version_name} ")[0] or ua
            self.gen_ua(dalvik)
            self.ua_http = ua

    # 请求体构造 ---------------------------------------------------------
    def device_params(self, fts: int | None = None) -> dict:
        """``x-bili-device-bin``（Device）的参数字典。

        :param fts: 指纹上报时间戳。不传则与 ``fp`` 内嵌的生成时间对齐
            （抓包：fts 比 fp 内嵌时间早几秒，二者同源；若各随机一次会被风控识别为伪造设备）
        """
        if fts is None:
            fts = self.fp_timestamp() - random.randint(0, 15)
        return {
            "app_id": 1,
            "build": self.build,
            "buvid": self.buvid,
            "mobi_app": "android",
            "platform": "android",
            "channel": self.channel,
            "brand": self.brand,
            "model": self.device_model,
            "osver": self.osver,
            "fp_local": self.fp_local,
            "fp_remote": self.fp_remote,
            "version_name": self.version_name,
            "fp": self.fp_remote,
            "fts": fts,
            "guest_id": self.guest_id,
        }

    def metadata_params(self) -> dict:
        """``x-bili-metadata-bin``（Metadata）的参数字典。"""
        return {
            "mobi_app": "android",
            "build": self.build,
            "channel": self.channel,
            "buvid": self.buvid,
            "platform": "android",
        }

    def locale_header(self) -> bytes:
        """``x-bili-locale-bin``（抓包固定：zh-Hans_CN / Asia/Shanghai / +08:00）。"""
        return Locale(
            c_locale=LocaleIds(language="zh", script="Hans", region="CN"),
            s_locale=LocaleIds(language="zh", script="Hans", region="CN"),
            timezone="Asia/Shanghai",
            utc_offset="+08:00",
        ).SerializeToString()

    def network_header(self, oid: str = "", success_rate: float = -1.0) -> bytes:
        """``x-bili-network-bin``：WIFI + 可选运营商 + 成功率（-1 表示未采样）。"""
        return Network(
            type=NetworkType.WIFI,
            oid=oid,
            quality=NetQuality(success_rate=success_rate),
        ).SerializeToString()

    # region -------------------------------------------------------------
    def recent_region_valid(self) -> bool:
        """recent-region 是否存在且未过期（过期即清掉）。"""
        if not self.recent_region:
            return False
        if is_expired(self.recent_region):
            self.recent_region = None
            return False
        return True

    def learn_region(self, response_headers, trailers=None) -> dict[str, str]:
        """被动学习：从响应里读 region（先响应头、再 gRPC trailer）。

        对应 APK ``kntr.base.region.impl.h.b``：只在取到非空值且与当前不同时更新。
        """
        changed: dict[str, str] = {}
        pairs = (
            ("ip_region", IP_REGION_HEADER),
            ("legal_region", LEGAL_REGION_HEADER),
            ("recent_region", RECENT_REGION_HEADER),
        )
        for attr, name in pairs:
            value = _header(response_headers, name) or _header(trailers, name)
            if value and value != getattr(self, attr):
                setattr(self, attr, value)
                changed[name] = value
        return changed

    def region_headers(self) -> list[tuple[str, str]]:
        """要回带的 region 请求头（顺序照 APK ``h.a``：ip → legal → recent）。

        没学到（或 recent 已过期）就不带该头 —— 与冷启动 buvid/get 抓包一致。
        """
        headers = []
        if self.ip_region:
            headers.append((IP_REGION_HEADER, self.ip_region))
        if self.legal_region:
            headers.append((LEGAL_REGION_HEADER, self.legal_region))
        if self.recent_region_valid():
            headers.append((RECENT_REGION_HEADER, self.recent_region))
        return headers

    async def fetch_region(self, proxy=None) -> dict[str, str]:
        """主动领取 region：``GET /x/resource/show/tab/v2``，从响应里学。

        抓包证实该接口的响应头会下发 ``x-bili-metadata-ip-region`` 与
        ``x-bili-metadata-recent-region``（JWT 的 ``iat`` 与响应 ``date`` 同一秒），
        请求参数/请求头按抓包对齐（query 走 appsign 补 appkey/ts/sign）。
        """
        params = appsign(
            {
                "build": self.build,
                "c_locale": "zh-Hans_CN",
                "channel": self.channel,
                "disable_rcmd": 0,
                "mobi_app": "android",
                "platform": "android",
                "s_locale": "zh-Hans_CN",
                "statistics": json.dumps(
                    {
                        "appId": 1,
                        "platform": 3,
                        "version": self.version_name,
                        "abtest": "",
                    },
                    separators=(",", ":"),
                ),
                "ts": int(time.time()),
            }
        )
        headers = encode_metadata_headers(
            (
                ("accept", "*/*"),
                ("accept-encoding", "gzip, deflate, br"),
                ("app-key", "android64"),
                ("bili-http-engine", "ignet"),
                ("buvid", self.buvid),
                ("env", "prod"),
                ("fp_local", self.fp_local),
                ("session_id", self.session_id),
                ("user-agent", self.ua_bapi),
                ("x-bili-locale-bin", self.locale_header()),
                # 抓包：WIFI + 运营商 460000 + 未采样
                ("x-bili-network-bin", self.network_header(oid="460000")),
                ("x-bili-redirect", "1"),
                ("x-bili-trace-id", gen_trace_id()),
            )
        )
        resp = await my_async_httpx.request(
            method="GET",
            url="https://app.bilibili.com/x/resource/show/tab/v2",
            params=params,
            headers=headers,
            proxies=resolve_proxies(proxy),
            verify=False,
        )
        return self.learn_region(resp.headers, getattr(resp, "trailers", None))


def new_device_env(
    Dalvik: str | None = None,
    version_name: str = "9.13.0",
    build: int = 9130500,
    channel: str | None = None,
    brand: str = "OnePlus",
    ua: str | None = None,
) -> DeviceEnv:
    """造一台新设备（新 buvid / 新指纹 / 新会话），或按传入的 UA 还原一台。

    :param ua: 传外部 UA（Dalvik 池）时按它反解硬件信息并派生其余 UA
    :param channel: 不传则从渠道池随机取
    """
    device = DeviceEnv(
        build=build,
        inner_ver=str(int(build) + 10),
        version_name=version_name,
        brand=brand,
        channel=channel or random.choice(AppChannels),
    )
    if ua:
        device.adopt_http_ua(ua, brand=brand)
    else:
        if Dalvik:
            device.adopt_dalvik(Dalvik)
        device.gen_ua(Dalvik)
    return device
