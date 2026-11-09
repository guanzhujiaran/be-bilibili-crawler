# -*- coding: utf-8 -*-
"""代理源「访问路径」相关单测（不发起真实网络请求）。

覆盖：
1. jsdelivr gh 路径 → raw.githubusercontent 归一化；
2. GitHub 源的候选路径顺序（V2Ray → 镜像 → 原生直连）；
3. 非 GitHub 源保持原样、不加镜像；
4. MirrorAwareHttpx 在 V2Ray 失败时自动改走镜像；
5. 补充源解析（lines / geonode_json）。
"""

import pytest

import Utils.代理.redisProxyRequest.GetProxyFromNet as mod
from Utils.代理.redisProxyRequest.GetProxyFromNet import (
    ProxySourceSpec,
    build_source_url_candidates,
    is_github_source,
    parse_supplement_payload,
    to_raw_github_url,
)

_RAW_URL = "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt"
_JSD_URL = "https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/all/data.txt"


def test_is_github_source():
    assert is_github_source(_RAW_URL) is True
    assert is_github_source(_JSD_URL) is True
    assert is_github_source("https://www.66ip.cn/areaindex_1/1.html") is False


def test_to_raw_github_url():
    assert (
        to_raw_github_url(_JSD_URL)
        == "https://raw.githubusercontent.com/proxifly/free-proxy-list/main/proxies/all/data.txt"
    )
    # 非 jsdelivr：不需要归一化
    assert to_raw_github_url(_RAW_URL) is None
    # 缺 ref 的 jsdelivr 路径无法还原
    assert to_raw_github_url("https://cdn.jsdelivr.net/gh/owner/repo/file.txt") is None


def test_candidates_mirror_fallback_order():
    candidates = build_source_url_candidates(
        _RAW_URL,
        {"https": "http://v2ray:10809"},
        mirror_urls=["https://m1/", None, "https://m2/"],
        max_attempts=3,
    )
    assert candidates[0] == (_RAW_URL, True)  # 原行为：先走 V2Ray
    assert candidates[1] == (f"https://m1/{_RAW_URL}", False)
    assert candidates[2] == (_RAW_URL, False)  # 配置里的 None = 直连
    assert candidates[3] == (f"https://m2/{_RAW_URL}", False)


def test_candidates_capped_by_max_attempts():
    mirrors = [f"https://m{i}/" for i in range(100)]
    candidates = build_source_url_candidates(
        _RAW_URL, None, mirror_urls=mirrors, max_attempts=5
    )
    # 100+ 个镜像只按顺序试前 5 个，避免把一轮抓取拖死
    assert [u for u, _ in candidates] == [f"https://m{i}/{_RAW_URL}" for i in range(5)]


def test_candidates_dedup_repeated_mirrors():
    candidates = build_source_url_candidates(
        _RAW_URL,
        None,
        mirror_urls=["https://m1/", "https://m1/", "https://m2/"],
        max_attempts=5,
    )
    assert [u for u, _ in candidates] == [
        f"https://m1/{_RAW_URL}",
        f"https://m2/{_RAW_URL}",
    ]


def test_default_mirror_list_is_general_config():
    """镜像清单来自通用配置（与 RPA-Browser 同语义的百项列表），不再硬编码。"""
    from CONFIG import settings

    assert settings.github_proxy_urls
    assert None in settings.github_proxy_urls  # None = 直连，RPA 列表同样把它放首位
    assert len(settings.github_proxy_urls) >= 50
    assert settings.github_proxy_max_attempts >= 1
    assert settings.github_proxy_timeout > 0

    candidates = build_source_url_candidates(_RAW_URL, None)
    assert len(candidates) <= settings.github_proxy_max_attempts + 1


def test_non_github_source_untouched():
    url = "https://www.66ip.cn/areaindex_1/1.html"
    assert build_source_url_candidates(url, None) == [(url, True)]
    assert build_source_url_candidates(url, {"https": "http://v2ray:10809"}) == [
        (url, True)
    ]


class _FakeResponse:
    def __init__(self, text: str, status_code: int = 200):
        self.text = text
        self.status_code = status_code

    def __bool__(self) -> bool:
        return True


class _FakeClient:
    def __init__(self, fail_urls: set[str], body: str = "1.1.1.1:80"):
        self.fail_urls = fail_urls
        self.body = body
        self.calls: list[tuple[str, dict | None]] = []

    async def get(self, url, headers=None, verify=False, proxies=None, timeout=10, params=None, **kwargs):
        self.calls.append((url, proxies))
        if url in self.fail_urls:
            raise RuntimeError("connect failed")
        return _FakeResponse(self.body)


async def test_mirror_aware_httpx_falls_back_to_mirror(monkeypatch):
    monkeypatch.setattr(
        mod.settings, "github_proxy_urls", ["https://m1/", "https://m2/"], raising=False
    )
    fake = _FakeClient(fail_urls={_RAW_URL})
    monkeypatch.setattr(mod, "_sealed_httpx", fake)

    resp = await mod.MirrorAwareHttpx().get(
        url=_RAW_URL, proxies=mod._github_proxy, timeout=5
    )

    assert resp.text == "1.1.1.1:80"
    # 第一次走 V2Ray 失败，第二次改走镜像且不带代理
    assert fake.calls[0] == (_RAW_URL, mod._github_proxy)
    assert fake.calls[1] == (f"https://m1/{_RAW_URL}", None)


async def test_mirror_aware_httpx_returns_falsy_when_all_paths_fail(monkeypatch):
    monkeypatch.setattr(
        mod.settings, "github_proxy_urls", ["https://m1/", "https://m2/"], raising=False
    )
    fake = _FakeClient(
        fail_urls={
            _RAW_URL,
            f"https://m1/{_RAW_URL}",
            f"https://m2/{_RAW_URL}",
        }
    )
    monkeypatch.setattr(mod, "_sealed_httpx", fake)

    resp = await mod.MirrorAwareHttpx().get(url=_RAW_URL, proxies=mod._github_proxy)

    # 调用方的 `if req: ... else: self.log.info(req.text)` 依赖这个语义
    assert bool(resp) is False
    assert resp.text == ""
    assert len(fake.calls) == 3


async def test_non_github_source_passes_through(monkeypatch):
    url = "https://www.66ip.cn/areaindex_1/1.html"
    fake = _FakeClient(fail_urls=set())
    monkeypatch.setattr(mod, "_sealed_httpx", fake)

    resp = await mod.MirrorAwareHttpx().get(url=url, proxies=None)

    assert resp.text == "1.1.1.1:80"
    assert fake.calls == [(url, None)]


def test_parse_lines_payload():
    spec = ProxySourceSpec(name="t", url="u", protocol="socks5")
    payload = "1.1.1.1:1080\n\n# comment\nbad-line\n2.2.2.2:1080\n"

    proxies = parse_supplement_payload(spec, payload)

    assert proxies == [
        {"sock5": "socks5://1.1.1.1:1080", "socks5": "socks5://1.1.1.1:1080"},
        {"sock5": "socks5://2.2.2.2:1080", "socks5": "socks5://2.2.2.2:1080"},
    ]


@pytest.mark.parametrize(
    ("line", "expected"),
    [
        ("1.2.3.4:8080", "1.2.3.4:8080"),
        ("http://1.2.3.4:8080", "http://1.2.3.4:8080"),
        ("socks5://1.2.3.4:1080", "socks5://1.2.3.4:1080"),
        # hideip.me 的 socks4 列表带国家后缀
        ("31.43.63.70:4145:Ukraine", "31.43.63.70:4145"),
        ("  1.2.3.4:8080 高匿  ", "1.2.3.4:8080"),
        ("<html>", None),
        ("error code: 1015", None),
    ],
)
def test_extract_proxy_text(line, expected):
    assert mod.extract_proxy_text(line) == expected


def test_parse_lines_payload_with_country_suffix():
    spec = ProxySourceSpec(name="hideip", url="u", protocol="socks4")
    payload = "31.43.63.70:4145:Ukraine\n83.143.29.5:4145:Germany\n"

    proxies = parse_supplement_payload(spec, payload)

    assert [list(p.values())[0] for p in proxies] == [
        "socks4://31.43.63.70:4145",
        "socks4://83.143.29.5:4145",
    ]


def test_parse_geonode_payload():
    spec = ProxySourceSpec(name="g", url="u", kind="geonode_json")
    payload = (
        '{"data":[{"ip":"1.1.1.1","port":"8080","protocols":["socks4","http"]},'
        '{"ip":"2.2.2.2","port":"3128","protocols":["https"]},'
        '{"ip":"3.3.3.3","port":"","protocols":["http"]}]}'
    )

    proxies = parse_supplement_payload(spec, payload)

    # 每个条目只取第一个受支持的协议；无端口的条目丢弃
    assert proxies == [
        {"http": "http://1.1.1.1:8080", "https": "http://1.1.1.1:8080"},
        {"http": "https://2.2.2.2:3128", "https": "https://2.2.2.2:3128"},
    ]


def test_parse_geonode_payload_invalid_json():
    spec = ProxySourceSpec(name="g", url="u", kind="geonode_json")
    assert parse_supplement_payload(spec, "<html>502</html>") == []


async def test_wrapper_no_longer_truncates(monkeypatch):
    """不再按行数截断：源站给多少就返回多少（由流式入库控制内存）。"""
    payload = "\n".join(f"10.0.{i // 250}.{i % 250}:8080" for i in range(5000))
    fake = _FakeClient(fail_urls=set(), body=payload)
    monkeypatch.setattr(mod, "_sealed_httpx", fake)

    resp = await mod.MirrorAwareHttpx().get(url=_RAW_URL, proxies=None)

    assert len(resp.text.splitlines()) == 5000


def test_supplement_sources_discovered_by_runner():
    """所有源方法都必须是 async generator（__get_proxy 用 isasyncgenfunction 发现）。"""
    import inspect

    methods = inspect.getmembers(
        mod.get_proxy_methods, predicate=inspect.isasyncgenfunction
    )
    names = [name for name, _ in methods if name.startswith("get_proxy_from")]
    assert "get_proxy_from_supplement_sources" in names
    # 一个都不能漏：漏掉的源会永远不被调度
    assert len(names) >= 90

    sync_style = [
        name
        for name, _ in inspect.getmembers(mod.get_proxy_methods)
        if name.startswith("get_proxy_from")
        and not inspect.isasyncgenfunction(getattr(mod.get_proxy_methods, name))
    ]
    assert sync_style == [], f"这些源还不是 async generator：{sync_style}"


@pytest.mark.parametrize("spec", list(mod.SUPPLEMENT_SOURCES))
def test_supplement_spec_valid(spec: ProxySourceSpec):
    assert spec.url.startswith("http")
    assert spec.protocol in ("http", "https", "socks4", "socks5")
