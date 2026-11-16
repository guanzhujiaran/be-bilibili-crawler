import random
import re

from Utils.GrpcUtils.CONST import ANDROID_VERSIONS

# 顺序即优先级：Edge/Opera 的 UA 里也含 Chrome，必须先匹配。
_PLATFORM_PATTERNS: tuple[tuple[str, str], ...] = (
    (r"Windows NT", "Windows"),
    (r"CrOS", "Chrome OS"),
    (r"Android", "Android"),
    (r"iPhone|iPad", "iOS"),
    (r"Macintosh", "macOS"),
    (r"Linux", "Linux"),
)


def _client_hints_platform(user_agent: str) -> str | None:
    for pattern, platform in _PLATFORM_PATTERNS:
        if re.search(pattern, user_agent, re.IGNORECASE):
            return platform
    return None


def _client_hints_brands(user_agent: str) -> str | None:
    """Chromium 系才发 sec-ch-ua；Firefox / Safari 不发，宁缺勿错。"""
    brand: str | None = None
    major: str | None = None
    if m := re.search(r"\bEdg/(\d+)", user_agent):
        brand, major = "Microsoft Edge", m.group(1)
    elif m := re.search(r"\bOPR/(\d+)", user_agent):
        brand, major = "Opera", m.group(1)
    elif m := re.search(r"\bChrome/(\d+)", user_agent):
        brand, major = "Google Chrome", m.group(1)
    if not brand or not major:
        return None
    return f'"{brand}";v="{major}", "Chromium";v="{major}", "Not_A Brand";v="8"'


class UserAgentParser:
    def __init__(self, user_agent, is_mobile=False):
        self.user_agent = user_agent
        self.is_mobile = is_mobile

    def _fetch_metadata_headers(self) -> dict[str, str]:
        """浏览器 XHR/fetch 的 Fetch Metadata + Client Hints 头。"""
        headers: dict[str, str] = {
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-site",
            "priority": "u=1, i",
        }
        brands = _client_hints_brands(self.user_agent)
        if brands:
            headers["sec-ch-ua"] = brands
            headers["sec-ch-ua-mobile"] = "?1" if self.is_mobile else "?0"
            platform = _client_hints_platform(self.user_agent)
            if platform:
                headers["sec-ch-ua-platform"] = f'"{platform}"'
        return headers

    def get_headers_dict(self, extra_headers_dict: dict | None = None) -> dict:
        """按 REA 抓包观测到的字典序组装浏览器 XHR 头。

        证据：REA 抓包（Chromium）里 17 个 ``api.bilibili.com`` XHR 的头顺序
        全部是字典序：``accept, accept-encoding, accept-language, cookie, origin,
        priority, referer, sec-ch-ua, sec-ch-ua-mobile, sec-ch-ua-platform,
        sec-fetch-dest, sec-fetch-mode, sec-fetch-site, user-agent``。

        注意：curl_cffi 在发请求时会自行重排 header，这里排序的收益是
        「代码可审计 + 单测可锁定」，不宣称改变 wire 顺序
        （见 docs/be-bilibili-crawler-风控对抗加固计划书.md §1.3）。
        """
        origin_headers = {
            "user-agent": self.user_agent,
            "accept": "*/*",
            "accept-language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
            "accept-encoding": "gzip, deflate, br, zstd",
        }
        origin_headers.update(self._fetch_metadata_headers())
        filtered_headers_dict = {
            key: value for key, value in origin_headers.items() if value
        }
        if extra_headers_dict:
            filtered_headers_dict.update(extra_headers_dict)
        return dict(sorted(filtered_headers_dict.items()))

    @staticmethod
    def parse_h5_ua(dalvik_ua: str, buvid: str, session_id: str) -> str:
        def get_sdk_int(_android_version):
            # 根据Android版本获取对应的SDK Int
            for _version, _sdk_int in ANDROID_VERSIONS.items():
                if _android_version.startswith(_version):
                    return _sdk_int
            return None

        def generate_random_versions():
            # 生成合理的WebKit、Chrome和Safari版本号
            webkit_version = "537.36"  # WebKit版本通常固定为537.36
            chrome_major = random.randint(70, 100)  # 选择一个合理的Chrome主版本号范围
            chrome_minor = random.randint(0, 99)
            chrome_patch = random.randint(0, 9999)
            chrome_version = f"{chrome_major}.0.{chrome_minor}.{chrome_patch}"

            safari_version = "537.36"  # Safari版本通常与WebKit版本相同

            return webkit_version, chrome_version, safari_version

        if "Mozilla/5.0" in dalvik_ua:  # 本来就是Mozilla的ua就直接返回
            return dalvik_ua
        device_info_match = re.search(r"\(([^)]+)\)", dalvik_ua)
        if not device_info_match:
            return dalvik_ua

        device_info = device_info_match.group(1)
        # 提取其他参数
        app_version_match = re.search(r"(?<=\))\s*(\d+\.\d+\.\d+)", dalvik_ua)
        if not app_version_match:
            app_version = "8.13.0"
        else:
            app_version = app_version_match.group(1)
        build_number = (
            re.search(r"build/(\d+)", dalvik_ua).group(1)
            if "build/" in dalvik_ua
            else "8130300"
        )
        channel = (
            re.search(r"channel/(\w+)", dalvik_ua).group(1)
            if "channel/" in dalvik_ua
            else "master"
        )

        # 提取Android版本
        android_version_match = re.search(r"Android (\d+(\.\d+)?(\.\d+)?)", dalvik_ua)
        if not android_version_match:
            android_version = "9"
        else:
            android_version = android_version_match.group(1)
        sdk_int = get_sdk_int(android_version)

        # 提取设备型号
        model_match = re.search(r"Android \d+(\.\d+)?(\.\d+)?; ([^ ;]+)", device_info)
        if not model_match:
            model = "23113RKC6C"
        else:
            model = model_match.group(3)

        # 生成随机版本号
        webkit_version, chrome_version, safari_version = generate_random_versions()
        # 构建Mozilla/5.0格式的UA字符串
        mozilla_ua = (
            f"Mozilla/5.0 ({device_info}) "
            f"AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/91.0.4472.114 Mobile Safari/537.36 "
            f"os/android model/{model} build/{build_number} osVer/{android_version} sdkInt/{sdk_int} network/2 BiliApp/{build_number} "
            f"mobi_app/android channel/{channel} Buvid/{buvid} sessionID/{session_id} innerVer/{build_number} c_locale/zh_CN s_locale/zh_CN "
            f"disable_rcmd/0 themeId/1 sh/24 {app_version} os/android model/{model} mobi_app/android build/{build_number} "
            f"channel/{channel} innerVer/{build_number} osVer/{android_version} network/2"
        )

        return mozilla_ua


if __name__ == "__main__":
    # 使用示例
    user_agent = "Dalvik/2.1.0 (Linux; U; Android 5.1; Impress Style Build/LMY47I) 8.2.0 os/android model/Impress Style mobi_app/android build/8020300 channel/360 innerVer/8020300 osVer/5.1 network/2"
    ua_parser = UserAgentParser.parse_h5_ua(user_agent, "114514", "1919810")
    print(ua_parser)
