"""B 站 Web API 请求头 / 参数对齐浏览器真实形态的回归测试。

证据来源：REA capture_browser_scenario（Chromium 153，locale=zh-CN）
- https://space.bilibili.com/1/dynamic -> feed/space 的 query
- https://t.bilibili.com/463864834570585963 -> detail 的 query
- 所有 api.bilibili.com XHR 的 sec-* / priority 头

运行:
    uv run python -m pytest test/test_web_bapi_headers.py -v
"""

import json
import sys
from pathlib import Path

import pytest

_project_root = Path(__file__).resolve().parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from Service.GrpcModule.Grpc.Bapi.Constants import (
    BILI_WEB_LOCALE_JSON,
    FEATURES_DYNAMIC_DETAIL,
    FEATURES_SPACE_DYNAMIC,
)
from Utils.GrpcUtils.UserAgentParser import UserAgentParser
from Utils.加密.wbi加密 import gen_bili_device_req_json

CHROME_WIN_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/153.0.0.0 Safari/537.36"
)
EDGE_MAC_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/153.0.0.0 Safari/537.36 Edg/153.0.0.0"
)
FIREFOX_LINUX_UA = (
    "Mozilla/5.0 (X11; Linux x86_64; rv:128.0) Gecko/20100101 Firefox/128.0"
)


@pytest.mark.parametrize(
    ("user_agent", "expected_platform"),
    [
        (CHROME_WIN_UA, '"Windows"'),
        (EDGE_MAC_UA, '"macOS"'),
        (FIREFOX_LINUX_UA, None),
        (
            "Mozilla/5.0 (Linux; Android 13; SM-S918B) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
            '"Android"',
        ),
        (
            "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) "
            "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
            None,
        ),
    ],
)
def test_sec_ch_ua_platform_matches_user_agent(
    user_agent: str, expected_platform: str | None
) -> None:
    headers = UserAgentParser(user_agent).get_headers_dict()
    assert headers.get("sec-ch-ua-platform") == expected_platform


def test_fetch_metadata_headers_present_on_every_web_api_call() -> None:
    headers = UserAgentParser(CHROME_WIN_UA).get_headers_dict()
    assert headers["sec-fetch-dest"] == "empty"
    assert headers["sec-fetch-mode"] == "cors"
    assert headers["sec-fetch-site"] == "same-site"
    assert headers["priority"] == "u=1, i"
    assert headers["sec-ch-ua-mobile"] == "?0"
    assert '"Chromium";v="153"' in headers["sec-ch-ua"]
    assert '"Google Chrome";v="153"' in headers["sec-ch-ua"]


def test_edge_ua_uses_edge_brand_not_chrome() -> None:
    headers = UserAgentParser(EDGE_MAC_UA).get_headers_dict()
    assert '"Microsoft Edge";v="153"' in headers["sec-ch-ua"]
    assert "Google Chrome" not in headers["sec-ch-ua"]


def test_firefox_does_not_send_client_hints() -> None:
    headers = UserAgentParser(FIREFOX_LINUX_UA).get_headers_dict()
    assert "sec-ch-ua" not in headers
    assert "sec-ch-ua-mobile" not in headers
    assert "sec-ch-ua-platform" not in headers
    # Fetch Metadata 与浏览器类型无关，仍然要发。
    assert headers["sec-fetch-site"] == "same-site"


def test_extra_headers_can_override_defaults() -> None:
    headers = UserAgentParser(CHROME_WIN_UA).get_headers_dict(
        extra_headers_dict={
            "sec-fetch-site": "none",
            "sec-fetch-mode": "navigate",
        }
    )
    assert headers["sec-fetch-site"] == "none"
    assert headers["sec-fetch-mode"] == "navigate"


def test_gen_bili_device_req_json_matches_capture() -> None:
    assert gen_bili_device_req_json("333.1387", include_mobi_app=True) == json.dumps(
        {
            "platform": "web",
            "device": "pc",
            "spmid": "333.1387",
            "mobi_app": "web_cn",
        },
        separators=(",", ":"),
    )
    assert gen_bili_device_req_json("333.1368") == (
        '{"platform":"web","device":"pc","spmid":"333.1368"}'
    )


def test_locale_json_and_features_match_capture() -> None:
    assert BILI_WEB_LOCALE_JSON == (
        '{"c_locale":{"language":"zh","script":"Hans"},"always_translate":false}'
    )
    assert FEATURES_SPACE_DYNAMIC.split(",") == [
        "itemOpusStyle",
        "listOnlyfans",
        "opusBigCover",
        "onlyfansVote",
        "forwardListHidden",
        "decorationCard",
        "commentsNewVersion",
        "onlyfansAssetsV2",
        "ugcDelete",
        "onlyfansQaCard",
        "avatarAutoTheme",
        "sunflowerStyle",
        "cardsEnhance",
        "eva3CardOpus",
        "eva3CardVideo",
        "eva3CardComment",
        "eva3CardUser",
    ]
    # detail 的 18 个 flag 必须保持原值，不能被 feed 的列表误改。
    assert len(FEATURES_DYNAMIC_DETAIL.split(",")) == 18
    assert "endFooterHidden" in FEATURES_DYNAMIC_DETAIL
    assert "eva3CardVote" in FEATURES_DYNAMIC_DETAIL
