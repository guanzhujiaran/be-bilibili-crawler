# -*- coding: utf-8 -*-
import asyncio
import base64
import gzip
import hashlib
import hmac
import json
import os
import random
import re
import string
import time
import uuid
from datetime import datetime, timedelta

from CONFIG import CONFIG
from log.base_log import BiliGrpcApi_logger
from Service.GrpcModule.Models.GrpcApiBaseModel import MetaDataBasicInfo
from Utils.GrpcUtils.CONST import (
    MemSizes,
    CPUFreqs,
    ProductDevices,
    Languages,
    Countries,
    NetworkTypes,
    UsbStates,
    CPUAbiLists,
    CPUHardwares,
    ANDROID_VERSIONS,
    BatteryStates,
    ANDROID_KERNELS,
    ScreenDPIs,
)
from Service.GrpcModule.Grpc.Bapi.BapiUtils import appsign
from Service.GrpcModule.Grpc.GrpcProto.bilibili.api.ticket.v1.ticket_pb2 import (
    GetTicketResponse,
    GetTicketRequest,
)
from Service.GrpcModule.Grpc.GrpcProto.bilibili.metadata.device.device_pb2 import Device
from Service.GrpcModule.Grpc.GrpcProto.bilibili.metadata.fawkes.fawkes_pb2 import (
    FawkesReq,
)
from Service.GrpcModule.Grpc.GrpcProto.bilibili.metadata.metadata_pb2 import Metadata
from Service.GrpcModule.Grpc.GrpcProto.bilibili.metadata.restriction.restriction_pb2 import (
    Restriction,
)
from Service.GrpcModule.Grpc.GrpcProto.datacenter.hakase.protobuf.android_device_info_pb2 import (
    AndroidDeviceInfo,
)
from Utils.GrpcUtils.metadata.device_env import (
    RECENT_REGION_HEADER,
    DeviceEnv,
    encode_metadata_headers,
    gen_trace_id,
    new_device_env,
    resolve_proxies,
)
from Utils.代理.SealedRequests import my_async_httpx


def gen_random_access_key() -> str:
    charset = "abcdefghijklmnopqrstuvwxyz0123456789"
    shift_charset = "ABCDEFGHIJKLMNOPQRSTUVWXYabcdefghijklmnopqrstuvwxyz0123456789"
    return (
        "".join([random.choice(charset) for _ in range(32)])
        + "".join([random.choice(shift_charset) for _ in range(34)])
        + "_"
        + "".join([random.choice(shift_charset) for _ in range(14)])
        + "-"
        + "".join([random.choice(shift_charset) for _ in range(25)])
    )


def gen_aurora_eid(uid: int) -> str:
    if uid == 0:
        raise ValueError("uid must not be 0")
    result_byte = bytearray()
    mid_byte = bytearray(str(uid), "utf-8")
    key = bytearray(b"ad1va46a7lza")
    for i, v in enumerate(mid_byte):
        result_byte.append(v ^ key[i % len(key)])
    return base64.b64encode(result_byte).decode("utf-8").rstrip("=")


class gen_x_bili_ticket:
    def __init__(
        self, device_info: bytes, fingerprint: bytes, exbadbasket: bytes = b""
    ):
        """
        :param device_info: # context, generated with `com.bapis.bilibili.metadata.device.Device`
        :param fingerprint:        /// x-fingerprint, generated with `datacenter.hakase.protobuf.AndroidDeviceInfo`
        :param exbadbasket:        /// x-exbadbasket, can leave it empty but should with it
        """
        self.device_info: bytes = device_info
        self.fingerprint: bytes = fingerprint
        self.exbadbasket: bytes = exbadbasket
        self.App_key = b"Ezlc3tgtl"

    def gen(self) -> bytes:
        mac = hmac.new(self.App_key, digestmod=hashlib.sha256)
        mac.update(self.device_info)
        mac.update(b"x-exbadbasket")
        mac.update(self.exbadbasket)
        mac.update(b"x-fingerprint")
        mac.update(self.fingerprint)
        return mac.digest()


async def make_metadata(
    access_key,
    brand="OnePlus",
    Dalvik="Dalvik/2.1.0 (Linux; U; Android 11; ONEPLUS A6000 Build/RKQ1.201217.002)",
    version_name="9.13.0",
    build=9130500,
    channel=None,
    proxy=None,
    mid=0,
    device: DeviceEnv | None = None,
) -> tuple[tuple, GetTicketResponse | None, MetaDataBasicInfo]:
    """
    根据ua自动生成包含ua信息的MetaData
    :param mid:
    :param brand:
    :param Dalvik:
    :param version_name:
    :param build:
    :param channel:
    :param access_key:
    :param proxy:
    :return:
    """
    proxy = {"proxy": {"http": CONFIG.my_ipv6_addr, "https": CONFIG.my_ipv6_addr}}
    # 一台设备 = 一个 DeviceEnv：buvid/指纹/guest_id/会话、硬件参数、三套 UA，
    # 以及该设备自己的 region（服务端按 buvid 签发）全都挂在它上面。
    # 传了 device 就复用它（设备池），否则新建一台。
    if device is None:
        device = new_device_env(
            Dalvik=Dalvik,
            version_name=version_name,
            build=build,
            channel=channel,
            brand=brand,
        )
    # fts 由设备对象按 fp 内嵌生成时间派生，保证「设备身份时间」自洽
    device_info_bytes = Device(**device.device_params()).SerializeToString()
    metadata: tuple = (
        ("accept", "*/*"),
        ("accept-encoding", "gzip, deflate, br"),
        ("buvid", device.buvid),
        ("content-type", "application/grpc"),
        ("grpc-accept-encoding", "identity, deflate, gzip"),
        ("grpc-encoding", "gzip"),
        ("grpc-timeout", "8S"),
        ("te", "trailers"),
        ("user-agent", device.ua),
        ("x-bili-device-bin", device_info_bytes),
        (
            "x-bili-fawkes-req-bin",
            FawkesReq(
                appkey="android64", env="prod", session_id=device.session_id
            ).SerializeToString(),
        ),
        ("x-bili-locale-bin", device.locale_header()),
        (
            "x-bili-metadata-bin",
            Metadata(**device.metadata_params()).SerializeToString(),
        ),
        # 引擎标识：抓包中 gRPC over HTTP/2 固定为 1；服务端不校验，可安全省略
        ("x-bili-moss-engine-type", "1"),
        # 抓包为 WIFI 且不带 oid（oid 仅免流场景下发）；success_rate 是客户端上报的采样值
        (
            "x-bili-network-bin",
            device.network_header(success_rate=random.uniform(0.95, 0.99)),
        ),
        ("x-bili-restriction-bin", Restriction(teenagers_age=16).SerializeToString()),
        ("x-bili-ticket", ""),
        # 抓包未发送 x-bili-trace-id，故不发送（HTTP 侧仍会带）
    )
    try:
        await active_buvid(device, proxy=proxy)
    except Exception as e:
        # 失败不影响后续 ticket 流程，但必须留痕，不能静默吞掉
        BiliGrpcApi_logger.error(f"激活buvid失败：{type(e).__name__}\t{e}")
    bili_ticket_resp = await get_bili_ticket(
        device, device_info=device_info_bytes, md=metadata, proxy=proxy
    )
    if bili_ticket_resp:
        metadata = tuple(
            (k, bili_ticket_resp.ticket if k == "x-bili-ticket" else v)
            for k, v in metadata
        )
    # region 属于设备：没有 recent-region 就让它自己去领一次
    # （抓包证实由 GET /x/resource/show/tab/v2 的响应头下发）
    if not device.recent_region_valid():
        try:
            learned = await device.fetch_region(proxy=proxy)
            if learned:
                BiliGrpcApi_logger.info(f"tab/v2 学到 region：{sorted(learned)}")
        except Exception as e:
            # 领不到不影响主流程，但必须留痕
            BiliGrpcApi_logger.error(
                f"tab/v2 获取 region 失败：{type(e).__name__}\t{e}"
            )
    # 设备回带自己学到的 region 头（ip → legal → recent，没学到就不带）
    region_headers = device.region_headers()
    forced_recent = os.environ.get("BILI_RECENT_REGION", "")
    if forced_recent and not device.recent_region_valid():
        region_headers = [
            (k, v) for k, v in region_headers if k != RECENT_REGION_HEADER
        ] + [(RECENT_REGION_HEADER, forced_recent)]
    if region_headers:
        metadata = metadata + tuple(region_headers)
    if access_key:
        metadata = metadata + (("authorization", f"identify_v1 {access_key}"),)

    metadata_basic_info = MetaDataBasicInfo(
        buvid=device.buvid,
        fp_local=device.fp_local,
        fp_remote=device.fp_remote,
        guestid=int(device.guest_id),
        app_version_name=device.version_name,
        model=device.device_model,
        app_build=device.build,
        channel=device.channel,
        osver=device.osver,
        ticket=bili_ticket_resp.ticket if bili_ticket_resp else "",
        brand=device.brand,
        session_id=device.session_id,
        device=device,
    )
    return metadata, bili_ticket_resp, metadata_basic_info


def is_useable_Dalvik(Dalvik: str):
    """
    检查Dalvik是否可用
    :param Dalvik:
    :return:
    """
    device_model = "".join(re.findall("Android.*?\d+; (.*?) (?:Build|MIUI)", Dalvik))
    osver = "".join(re.findall("Android (.*?[\w]);", Dalvik))
    if device_model and osver:
        return True
    else:
        return False


def generate_app_info(
    android_version: str, is_sys_app: bool = True, app_ver_name: str = "8.15.0"
) -> str:
    sdk_ver = ANDROID_VERSIONS.get(
        android_version,
    )
    if is_sys_app:
        apps = [
            "com.android.settings",
            "com.android.phone",
            "com.android.contacts",
            "com.android.messaging",
            "com.android.documentsui",
            "com.android.dreams.phototable",
            "com.android.calendar",
            "com.android.browser",
            "com.android.gallery",
            "com.android.music",
            "com.android.launcher",
            "com.android.camera",
        ]
        data_list = []
    else:
        apps = [
            "com.android.chrome",
            "com.android.contacts",
            "com.android.dialer",
            "com.android.gallery",
            "com.android.messaging",
            "com.android.settings",
            "com.android.calendar",
            "com.android.calculator2",
            "com.android.music",
            "com.facebook.katana",
            "com.instagram.android",
            "com.snapchat.android",
            "com.twitter.android",
            "com.linkedin.android",
            "com.tinder",
            "com.spotify.music",
            "com.netflix.mediaclient",
            "com.hulu.plus",
            "com.amazon.mShop.android.shopping",
            "com.ebay.mobile",
            "com.walmart.android",
            "com.target",
            "com.kroger.mobile",
            "com.alibaba.aliexpresshd",
            "com.booking",
            "com.airbnb",
            "com.expedia",
            "com.tripadvisor",
            "com.yelp.android",
            "com.zomato",
            "com.ubereats",
            "com.doordash",
            "com.postmates",
            "com.swiggy",
            "com.dunzo",
            "com.fitbit.FitbitMobile",
            "com.strava",
            "com.myfitnesspal.android",
            "com.headspace",
            "com.calm.android",
            "com.duolingo",
            "com.memrise",
            "com.babbel.mobile",
            "com.khanacademy.android",
            "com.coursera",
            "com.edx.mobile",
            "com.udemy",
            "com.quora",
            "com.reddit",
            "com.stackoverflow",
            "com.discord",
            "com.zoom.us",
            "com.microsoft.teams",
            "com.skype",
            "com.googleclassroom",
            "com.schoology",
            "com.blackboard",
            "com.canva",
            "com.adobe.psmobile",
            "com.pinterest",
            "com.etsy.android",
            "com.zillow",
            "com.trulia",
            "com.realtor.com",
            "com.indeed.android.jobsearch",
            "com.linkedin.jobs",
            "com.glassdoor",
            "com.monster.android",
            "com.simplyhired",
            "com.trello",
            "com.asana",
            "com.jira.mobile",
            "com.evernote",
            "com.microsoft.onenote",
            "com.dropbox.android",
            "com.box.android",
            "com.google.docs",
            "com.google.sheets",
            "com.google.slides",
            "com.microsoft.word",
            "com.microsoft.excel",
            "com.microsoft.powerpoint",
            "com.adobe.acrobat.reader",
            "com.kindle",
            "com.nook.android",
            "com.scribd",
            "com.pandora.android",
            "com.spotify.music",
            "com.apple.music",
            "com.tidal",
            "com.deezer",
            "com.soundcloud",
            "com.shazam",
            "com.spotify.tuner",
            "com.netflix.mediaclient",
            "com.hulu.plus",
            "com.amazon.avod.thirdpartyclient",
            "com.hbo.max",
            "com.disneyplus",
            "com.paramountplus",
            "com.peacocktv",
            "com.appletv.app",
            "com.fandango",
            "com.movietickets",
            "com.atomtickets",
            "com.stubhub",
            "com.eventbrite",
            "com.meetup",
            "com.ticketmaster",
            "com.lyft",
            "com.taxify",
            "com.ola.cabs",
            "com.uber",
            "com.didi",
            "com.grab",
            "com.gojek",
            "com.bolt",
            "com.inshorts",
            "com.flipboard.android",
            "com.pulse",
            "com.news360",
            "com.feedly",
            "com.smule",
            "com.yokee",
            "com.singplay",
            "com.soundhound",
            "com.shazam.en",
            "com.spotify.tuner.en",
            "com.netflix.mediaclient.en",
            "com.hulu.plus.en",
            "com.amazon.avod.thirdpartyclient.en",
            "com.hbo.max.en",
            "com.disneyplus.en",
            "com.paramountplus.en",
            "com.peacocktv.en",
            "com.appletv.app.en",
            "com.fandango.en",
            "com.movietickets.en",
            "com.atomtickets.en",
            "com.stubhub.en",
            "com.eventbrite.en",
            "com.meetup.en",
            "com.ticketmaster.en",
        ]
        random_time_delta = random.randint(1000000000, 5000000000)
        timestamp = int(time.time() * 1000) - random_time_delta
        data_list = [
            f"{timestamp},tv.danmaku.bili,{1 if is_sys_app else app_ver_name},{android_version},{sdk_ver},{timestamp}"
        ]

    max_data_len = (
        20
        if is_sys_app
        else random.choice(
            [4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20]
        )
    )
    while len(data_list) < max_data_len:
        random_app = random.choice(apps)
        existing_data_with_app = [data for data in data_list if random_app in data]
        if existing_data_with_app:
            generated_data = existing_data_with_app[0]
        else:
            # 生成几个月或者几年前的时间戳
            random_time_delta = random.randint(1000000000, 5000000000)
            timestamp = int(time.time() * 1000) - random_time_delta
            generated_data = f"{timestamp},{random_app},{1 if is_sys_app else app_ver_name},{android_version},{sdk_ver},{timestamp}"
        data_list.append(generated_data)
    return json.dumps(data_list, separators=(",", " "))


async def get_bili_ticket(
    device: DeviceEnv, device_info: bytes, md, proxy=None
) -> GetTicketResponse | None:
    # 指纹/版本信息全部取自设备对象
    app_version = device.version_name
    app_version_code = str(device.build)
    chid = device.channel
    osver = device.osver
    model = device.device_model
    brand = device.brand
    fp_local = device.fp_local
    android_build_id_moc = f"{''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(4))}.{(datetime.now() - timedelta(days=random.randint(365, 365 * 5))).strftime('%y%m%d')}.{str(random.randint(10000000, 99999999))}"
    rand_memory = MemSizes[random.choice(list(MemSizes.keys()))]
    rand_boot_id = random.randint(100000, 948576)
    rand_cpu_freq = CPUFreqs[random.choice(list(CPUFreqs.keys()))]
    rand_brightness = random.randint(30, 255)
    rand_ro_build_date_utc = int(
        (
            datetime.now() - timedelta(days=random.randint(int(0.3 * 365), 5 * 365))
        ).timestamp()
    )
    rand_ro_product_device = random.choice(ProductDevices)
    rand_persist_sys_language = random.choice(Languages)
    rand_serial_no = "".join(
        random.choices(string.ascii_lowercase + string.digits, k=8)
    )
    rand_country = random.choice(Countries)
    rand_network_type = random.choice(NetworkTypes)
    rand_usb_state = random.choice(UsbStates)
    rand_cpu_abi_list = random.choice(CPUAbiLists)
    rand_cpu_headrware = random.choice(CPUHardwares)
    sys_apps = generate_app_info(osver, is_sys_app=True)
    android_apps = generate_app_info(osver, is_sys_app=False, app_ver_name=app_version)
    rand_battery_state = random.choice(BatteryStates)
    rand_kernel = random.choice(
        ANDROID_KERNELS.get(random.choice(list(ANDROID_KERNELS.keys())), ["4.4.146"])
    )
    rand_battery = random.randint(30, 100)
    rand_screen = random.choice(ScreenDPIs)
    rand_light_intensity = str(round(random.uniform(50.0, 600.0), 3))
    x_fingerprint = AndroidDeviceInfo()
    x_fingerprint.sdkver = "0.2.4"
    x_fingerprint.app_id = "1"
    x_fingerprint.app_version = app_version
    x_fingerprint.app_version_code = app_version_code
    x_fingerprint.chid = chid
    x_fingerprint.fts = 1712822061
    x_fingerprint.buvid_local = fp_local
    x_fingerprint.proc = "tv.danmaku.bili"
    x_fingerprint.osver = osver
    x_fingerprint.t = int(time.time() * 1000)
    x_fingerprint.cpu_count = random.choice([4, 6, 8, 10, 12])
    x_fingerprint.model = model
    x_fingerprint.brand = brand
    x_fingerprint.screen = rand_screen
    x_fingerprint.boot = rand_boot_id
    x_fingerprint.emu = random.choice(
        ["000", "001", "010", "011", "100", "101", "110", "111"]
    )
    x_fingerprint.oid = random.choice(["46000", "46002", "46007", "46008"])
    x_fingerprint.network = "WIFI"
    x_fingerprint.mem = rand_memory
    x_fingerprint.sensor = '["LSM330 Accelerometer,STMicroelectronics", "Linear Acceleration,QTI", "Magnetometer,AKM", "Orientation,Yamaha", "Gravity,QTI", "Gyroscope,STMicroelectronics", "Proximity sensor,AMS TAOS", "Light sensor,AMS TAOS", "Game Rotation Vector Sensor,AOSP", "GeoMag Rotation Vector Sensor,AOSP", "Rotation Vector Sensor,AOSP", "Orientation Sensor,AOSP"]'
    x_fingerprint.cpu_freq = rand_cpu_freq
    x_fingerprint.cpu_vendor = "ARM"
    x_fingerprint.brightness = rand_brightness
    x_fingerprint.props.update(
        {
            "net.hostname": "",
            "ro.boot.hardware": "qcom",
            "gsm.sim.state": "LOADED",
            "ro.build.date.utc": f"{rand_ro_build_date_utc}",
            "ro.product.device": rand_ro_product_device,
            "persist.sys.language": rand_persist_sys_language,
            "ro.debuggable": "1",
            "net.gprs.local-ip": "",
            "ro.build.tags": "release-keys",
            "http.proxy": "",
            "ro.serialno": rand_serial_no,
            "persist.sys.country": rand_country,
            "ro.boot.serialno": rand_serial_no,
            "gsm.network.type": rand_network_type,
            "net.eth0.gw": "",
            "net.dns1": f"192.168.{random.randint(0, 255)}.{random.randint(0, 255)}",
            "sys.usb.state": rand_usb_state,
            "http.agent": "",
            "product": model,
            "cpu_model_name": "",
            "display": f"{android_build_id_moc} release-keys",
            "cpu_abi_list": rand_cpu_abi_list,
            "cpu_abi_libc": "X86_64",
            "manufacturer": brand,
            "cpu_hardware": rand_cpu_headrware,
            "cpu_processor": "AArch64 Processor rev 12 (aarch64)",
            "cpu_abi_libc64": "arm64-v8a",
            "cpu_abi": "arm64-v8a",
            "serial": "unknown",
            "cpu_features": "fp asimd evtstrm aes pmull sha1 sha2 crc32 atomics fphp asimdhp",
            "fingerprint": f"{brand}/{model}/{rand_ro_product_device}:{osver}/{android_build_id_moc}/{random.randint(100000, 9999999999)}:user/release-keys",
            "cpu_abi2": "",
            "device": rand_ro_product_device,
            "hardware": "qcom",
        }
    )
    x_fingerprint.adid = hashlib.md5(str(time.time()).encode()).hexdigest()[:16]
    x_fingerprint.os = "android"
    x_fingerprint.total_space = random.randint(10**8, 10**10)
    x_fingerprint.axposed = "false"
    x_fingerprint.files = "/data/user/0/tv.danmaku.bili/files"
    x_fingerprint.virtual = "0"
    x_fingerprint.virtualproc = "[]"
    x_fingerprint.apps = sys_apps
    x_fingerprint.guid = str(uuid.uuid4())
    x_fingerprint.uid = str(random.randint(10000, 10053))
    x_fingerprint.root = 0
    x_fingerprint.androidapp20 = android_apps
    x_fingerprint.androidappcnt = random.randint(20, 70)
    x_fingerprint.androidsysapp20 = sys_apps
    x_fingerprint.battery = rand_battery  # 63
    x_fingerprint.battery_state = rand_battery_state  # 64
    x_fingerprint.build_id = f"{android_build_id_moc} release-keys"  # 67
    x_fingerprint.country_iso = rand_country  # 68
    x_fingerprint.free_memory = random.randint(10**8, 10**10)  # 70
    x_fingerprint.fstorage = f"{random.randint(10 ** 8, 10 ** 10)}"  # 71
    x_fingerprint.kernel_version = rand_kernel  # 74
    x_fingerprint.languages = rand_persist_sys_language  # 75
    x_fingerprint.systemvolume = random.choice([0, 1, 2, 3, 4, 5, 6, 7])  # 80
    x_fingerprint.memory = rand_memory  # 82
    x_fingerprint.str_battery = str(rand_battery)  # 83
    x_fingerprint.is_root = False  # 84
    x_fingerprint.str_brightness = str(rand_brightness)  # 85
    x_fingerprint.str_app_id = "1"  # 86
    x_fingerprint.light_intensity = rand_light_intensity  # 89
    x_fingerprint.device_angle.extend(
        [round(random.uniform(-180.0, 180.0), 3) for _ in range(3)]
    )  # 90
    x_fingerprint.gps_sensor = 1  # 91
    x_fingerprint.speed_sensor = 1  # 92
    x_fingerprint.linear_speed_sensor = 1  # 93
    x_fingerprint.gyroscope_sensor = 1  # 94
    x_fingerprint.biometric = 1  # 95
    x_fingerprint.biometrics.extend(["touchid"]),  # 96
    x_fingerprint.last_dump_ts = int(time.time() * 1000) - random.randint(
        3 * 3600 * 1000, 5000000000
    )  #
    x_fingerprint.ui_version = f"{android_build_id_moc} release-keys"  # 108
    x_fingerprint.sensors_info.extend([])  # 110
    x_fingerprint.battery_present = True  # 112
    x_fingerprint.battery_technology = "Li-ion"  # 113
    x_fingerprint.battery_temperature = random.choice(
        [322, 323, 324, 325, 326, 327, 328, 329, 330]
    )  # 114
    x_fingerprint.battery_voltage = random.choice(
        [
            3000,
            3150,
            3250,
            3450,
            3550,
            3650,
            3750,
            3850,
            3950,
            4050,
            4150,
            4250,
            4350,
            4450,
            4650,
            4550,
            4660,
            5000,
        ]
    )  # 115
    x_fingerprint.battery_plugged = 1  # 116
    x_fingerprint.battery_health = 2  # 117
    x_fingerprint.adb_info = json.dumps(
        {
            "ro.product.model": model,
            "ro.bootmode": "unknown",
            "qemu.sf.lcd_density": "",
            "qemu.hw.mainkeys": "",
            "init.svc.qemu-props": "",
            "ro.hardware": "qcom",
            "ro.product.device": rand_ro_product_device,
            "init.svc.qemud": "",
            "ro.kernel.android.qemud": "",
            "ro.kernel.qemu.gles": "",
            "ro.serialno": rand_serial_no,
            "ro.kernel.qemu": "",
            "ro.product.name": model,
            "qemu.sf.fake_camera": "",
            "ro.bootloader": "unknown",
        }
    )

    sign = gen_x_bili_ticket(
        device_info=device_info,
        fingerprint=x_fingerprint.SerializeToString(),
        exbadbasket=b"",
    ).gen()
    reqdata = GetTicketRequest(
        context={
            "x-fingerprint": x_fingerprint.SerializeToString(),
            "x-exbadbasket": b"",
        },
        key_id="ec01",
        sign=sign,
    )
    new_headers = []
    for k, v in md:
        if isinstance(v, bytes):
            new_headers.append((k, base64.b64encode(v).decode("utf-8").strip("=")))
            continue
        if k == "x-bili-trace-id":
            new_headers.append((k, gen_trace_id()))
            continue
        new_headers.append((k, v))
    proto_bytes = reqdata.SerializeToString()
    compressed_proto_bytes = gzip.compress(proto_bytes, compresslevel=6)
    data = (
        b"\01" + len(compressed_proto_bytes).to_bytes(4, "big") + compressed_proto_bytes
    )
    proxies = resolve_proxies(proxy)
    resp = None
    while 1:
        try:
            resp = await my_async_httpx.request(
                url="http://app.bilibili.com/bilibili.api.ticket.v1.Ticket/GetTicket",
                method="POST",
                data=data,
                headers=tuple(new_headers),
                proxies=proxies,
                verify=False,
            )
            gresp = GetTicketResponse()
            if "gzip" in dict(new_headers).get("grpc-encoding"):
                gresp.ParseFromString(gzip.decompress(resp.content[5:]))
            else:
                gresp.ParseFromString(resp.content[5:])
            # 顺带从响应里学习 region（APK: kntr.base.region.impl.h.b）
            device.learn_region(resp.headers, getattr(resp, "trailers", None))
            if not gresp.ticket:
                BiliGrpcApi_logger.error(
                    f"获取ticket失败！\n{resp.content}\n{resp.headers}"
                )
            return gresp
        except Exception as e:
            err_resp = None if resp is None else resp.content
            BiliGrpcApi_logger.exception(
                f"获取bili_ticket失败！\nproxy：{proxies}\n{err_resp}\n{type(e)}\t{e}"
            )
            # 代理与直连互为兜底，因此处会来回切换
            proxies = None if proxies else CONFIG.custom_proxy


async def active_buvid(device: DeviceEnv, proxy=None, build=28799195):
    """
    激活buvid（POST https://app.bilibili.com/x/polymer/buvid/get），顺序在 get_bili_ticket 之前

    参数与请求头均按该接口抓包对齐：
    - app-key/bili-http-engine/session_id/x-bili-redirect 等头部照抓包下发；
    - user-agent 用 HTTP 老格式（不含 grpc-c++ 包装）；
    - 表单字段 androidId/drmId/mac/imei/oaid/build/internalVersionCode 与抓包一致；
      appkey/ts/sign 由 appsign 补齐。
    """
    url = "https://app.bilibili.com/x/polymer/buvid/get"
    data = {
        "androidId": "".join(
            random.choice(string.ascii_lowercase + string.digits) for _ in range(16)
        ),
        "brand": device.brand,
        "build": build,
        "buvid": device.buvid,
        "channel": device.channel,
        "drmId": "".join(random.choice("0123456789abcdef") for _ in range(32)),
        "fawkesAppKey": "android64",
        "first": 1,
        "firstStart": 1,
        "imei": "",
        "internalVersionCode": device.inner_ver,
        "mac": "",
        "model": device.device_model,
        "neuronAppId": 1,
        "neuronPlatformId": 3,
        "oaid": "",
        "ts": int(time.time()),
        "versionCode": device.build,
        "versionName": device.version_name,
    }
    signed_data = appsign(data)
    headers = (
        ("accept", "*/*"),
        ("accept-encoding", "gzip, deflate, br"),
        ("app-key", "android64"),
        ("bili-http-engine", "ignet"),
        ("buvid", device.buvid),
        ("content-type", "application/x-www-form-urlencoded; charset=utf-8"),
        ("env", "prod"),
        ("session_id", device.session_id),
        ("user-agent", device.ua_http),
        ("x-bili-locale-bin", device.locale_header()),
        # 抓包 CAEqBQ0AAIC/：WIFI + success_rate=-1.0（-1 表示尚未采样），不带 oid
        ("x-bili-network-bin", device.network_header()),
        ("x-bili-redirect", "1"),
        ("x-bili-trace-id", gen_trace_id()),
    )
    headers = encode_metadata_headers(headers)

    req = await my_async_httpx.request(
        url=url,
        method="POST",
        data=signed_data,
        headers=headers,
        proxies=resolve_proxies(proxy),
        verify=False,
    )
    device.learn_region(req.headers)
    BiliGrpcApi_logger.debug(f" {url} 激活buvid：{req.text}")


if __name__ == "__main__":
    __ = asyncio.run(make_metadata(""))
    print(__)
