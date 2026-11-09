"""
从网上获取代理
"""

import asyncio
import inspect
import json
import re
import time
from datetime import datetime
from typing import AsyncGenerator, Literal

import bs4
import curl_cffi.requests.exceptions
from pydantic import BaseModel, Field

from CONFIG import CONFIG, settings
from log.base_log import sql_log
from Utils.通用.Common import asyncio_gather
from Utils.代理.SealedRequests import my_async_httpx as _sealed_httpx
from Utils.代理.数据库操作.SqlAlcheyObj.ProxyModel import ProxyTab
from Utils.代理.数据库操作.async_proxy_op_alchemy_mysql_ver import (
    ProxyKeyLookup,
    SQLHelper,
    is_zombie_success_times,
    to_proxy_key,
)
from Utils.代理.数据库操作.comm import (
    format_proxy,
    get_scheme_ip_port_form_proxy_dict,
)

_github_proxy = {"http": CONFIG.V2ray_proxy, "https": CONFIG.V2ray_proxy}

# ===== 抓取/入库的容量常量 =====
# 必须放在类定义之前：`iter_lines()` / `_iter_raw_list_source()` 的默认参数
# 在函数定义时求值，放在文件后段会直接 NameError。
# 单个源任务的超时：大文件（517k 行 ≈ 5MB）要完整下载并解析
_SOURCE_TASK_TIMEOUT = 300
# 并发数统一来自 CONFIG.concurrency（部署时只改一处）
# 同时进行流式读取的源数量：每个源会一直持有连接直到读完，96 个源全开会给镜像站
# 和连接池压力，默认 32 足够把整体时长压在几分钟内。
_SOURCE_CONCURRENCY = settings.concurrency.proxy_source_fetch
# 单批入库条数：大源一次能返回 50 万条，整批 gather 会同时挂起几十万个协程，
# 这里按 2000 条一批「预筛 + 限并发入库」，处理完一批再进下一批。
_INGEST_CHUNK_SIZE = 2000
# 入库并发（逐条 INSERT）
_INGEST_CONCURRENCY = settings.concurrency.proxy_source_ingest

# ---------------------------------------------------------------------------
# GitHub 源的备用访问路径
# ---------------------------------------------------------------------------
# 线上实测（2026-11-09，容器内）：
#   raw.githubusercontent.com      → 连不通
#   cdn.jsdelivr.net               → 连不通
#   V2Ray（host.docker.internal:10809）→ 连不通
#   https://ghproxy.net/<原 URL>   → 200，返回完整列表
#   https://gh-proxy.com/<原 URL>  → 200
# 也就是说本模块里 76 个 GitHub 源原本全都拿不到数据，只能靠国内几个站兜底。
# 镜像清单是**通用配置**（settings.github_proxy_urls，与 RPA-Browser 同语义），
# 不在代码里硬编码域名；这里只负责给「GitHub 源」叠加候选路径与上限。
# 判定 URL 是否属于 GitHub 生态
_GITHUB_HOST_HINTS: tuple[str, ...] = (
    "raw.githubusercontent.com",
    "cdn.jsdelivr.net",
    "githubusercontent.com",
)


def is_github_source(url: str) -> bool:
    """该 URL 是否属于 GitHub 生态（直连经常不可用，需要镜像）。"""
    return any(hint in url for hint in _GITHUB_HOST_HINTS)


def to_raw_github_url(url: str) -> str | None:
    """把 jsdelivr 的 gh 路径还原成 raw.githubusercontent 形式。

    ``https://cdn.jsdelivr.net/gh/OWNER/REPO@REF/PATH``
    → ``https://raw.githubusercontent.com/OWNER/REPO/REF/PATH``

    非 jsdelivr 的 URL 返回 None（镜像前缀可直接拼原始 URL）。
    """
    prefix = "https://cdn.jsdelivr.net/gh/"
    if not url.startswith(prefix):
        return None
    rest = url[len(prefix) :]
    # OWNER/REPO 自身就带一个 "/"，所以先按 "@" 切 ref，再按 "/" 切 path
    owner_repo, at_sep, tail = rest.partition("@")
    if not at_sep:
        return None
    ref, _, path = tail.partition("/")
    if "/" not in owner_repo or not ref or not path:
        return None
    return f"https://raw.githubusercontent.com/{owner_repo}/{ref}/{path}"


def build_source_url_candidates(
    url: str,
    proxies: dict | None,
    mirror_urls: list[str | None] | None = None,
    max_attempts: int | None = None,
) -> list[tuple[str, bool]]:
    """给出「(URL, 是否走 GitHub 代理)」的尝试顺序。

    - 非 GitHub 源：原样返回一次，行为与改造前完全一致；
    - GitHub 源：V2Ray 直连（原行为）→ 按 ``settings.github_proxy_urls`` 顺序尝试镜像
      / 直连（``None`` 表示直连原 URL）→ 原生 URL 兜底。

    镜像清单是通用配置（默认参考 RPA-Browser 的百项镜像列表），
    ``max_attempts`` 限制本轮实际尝试的候选数，避免 100+ 个镜像把一轮抓取拖死。
    """
    if not is_github_source(url):
        return [(url, True)]
    mirrors = settings.github_proxy_urls if mirror_urls is None else mirror_urls
    limit = settings.github_proxy_max_attempts if max_attempts is None else max_attempts
    raw_url = to_raw_github_url(url) or url
    candidates: list[tuple[str, bool]] = []
    if proxies:
        candidates.append((url, True))
    # 空列表按 RPA 的语义等同「只直连」
    seen: set[str] = set()
    attempted = 0
    for mirror in mirrors or [None]:
        if attempted >= limit:
            break
        candidate_url = raw_url if mirror is None else f"{mirror}{raw_url}"
        if candidate_url in seen:
            continue
        seen.add(candidate_url)
        attempted += 1
        candidates.append((candidate_url, False))
    if raw_url != url and url not in seen:
        # jsdelivr 之类被归一化过的原始 URL，最后再直连试一次
        candidates.append((url, False))
    return candidates


class _EmptySourceResponse:
    """所有访问路径都失败时的占位响应。

    现有 ``get_proxy_from_*`` 统一写成 ``if req: ... else: self.log.info(req.text)``，
    这里提供 ``text`` 属性并让布尔值为 False，调用方即可走 else 分支并标记失败
    （改造前这种情况下 req 为 None，走到 else 会抛 AttributeError）。
    """

    status_code: int = 0
    text: str = ""

    def __bool__(self) -> bool:
        return False


# 行首的 [协议://]ip:port（容忍尾部追加内容，如 hideip.me 的 "ip:port:国家"）
_PROXY_PREFIX_RE = re.compile(
    r"^((?:[a-z][a-z0-9]{3,6}://)?(?:\d{1,3}\.){3}\d{1,3}:\d{1,5})(?:\b.*)?$"
)


class MirrorAwareHttpx:
    """本模块专用的请求入口：GitHub 源自动叠加镜像回退。

    与 ``Utils.代理.SealedRequests.my_async_httpx`` 同名同签名（模块末尾以
    ``my_async_httpx = MirrorAwareHttpx()`` 覆盖局部名字），这样 76 个
    ``get_proxy_from_*`` 方法与其它调用点都无需改动，非 GitHub 源行为不变。
    """

    async def get(
        self,
        url,
        headers=None,
        verify=False,
        proxies: dict | None = None,
        timeout=10,
        params=None,
        *args,
        **kwargs,
    ):
        for candidate_url, use_github_proxy in build_source_url_candidates(
            url, proxies
        ):
            # 镜像候选的超时由通用配置兜底（部分仓库单文件 1MB+，调用方默认的 10s 不够）
            attempt_timeout = (
                timeout
                if use_github_proxy
                else max(timeout, settings.github_proxy_timeout)
            )
            try:
                resp = await _sealed_httpx.get(
                    url=candidate_url,
                    headers=headers,
                    verify=verify,
                    proxies=proxies if use_github_proxy else None,
                    timeout=attempt_timeout,
                    params=params,
                    **kwargs,
                )
            except Exception as e:  # noqa: BLE001 单个路径失败就换下一个
                sql_log.debug(f"代理源请求失败（{candidate_url}）：{e!r}")
                continue
            if (
                resp is not None
                and getattr(resp, "status_code", 0) == 200
                and resp.text.strip()
            ):
                if candidate_url != url:
                    sql_log.info(f"代理源改走镜像成功：{candidate_url}")
                return resp
            sql_log.debug(
                f"代理源无有效内容（{candidate_url}），状态码="
                f"{getattr(resp, 'status_code', None)}"
            )
        return _EmptySourceResponse()

    async def iter_lines(
        self,
        url: str,
        headers: dict | None = None,
        proxies: dict | None = None,
        timeout: float = _SOURCE_TASK_TIMEOUT,
    ) -> AsyncGenerator[str, None]:
        """按行流式读取文本源：拿到一行就交出去，不在内存里攒完整文件。

        与 ``get()`` 共用镜像候选与超时策略；某个候选读失败就换下一个候选，
        已经 yield 出去的行无法撤回，由调用方的跨源去重兜底。
        """
        for candidate_url, use_github_proxy in build_source_url_candidates(
            url, proxies
        ):
            attempt_timeout = (
                timeout
                if use_github_proxy
                else max(timeout, settings.github_proxy_timeout)
            )
            proxy_str = (
                get_scheme_ip_port_form_proxy_dict(proxies)
                if use_github_proxy
                else None
            )
            try:
                async with curl_cffi.requests.AsyncSession(
                    max_clients=10,
                    allow_redirects=True,
                    timeout=attempt_timeout,
                    verify=True,
                    trust_env=True,
                    impersonate="chrome",
                ) as session:
                    resp = await session.get(
                        candidate_url,
                        stream=True,
                        timeout=attempt_timeout,
                        proxy=proxy_str,
                    )
                    if resp.status_code != 200:
                        sql_log.debug(
                            f"代理源无有效内容（{candidate_url}），"
                            f"状态码={resp.status_code}"
                        )
                        continue
                    if candidate_url != url:
                        sql_log.info(f"代理源改走镜像成功：{candidate_url}")
                    async for line in resp.aiter_lines():
                        if isinstance(line, bytes):
                            line = line.decode("utf-8", errors="ignore")
                        text = line.strip()
                        if text:
                            yield text
                    return
            except Exception as e:  # noqa: BLE001 单个候选失败就换下一个
                sql_log.debug(f"代理源流式读取失败（{candidate_url}）：{e!r}")
                continue


# 覆盖本模块内的请求入口：GitHub 源自动叠加镜像回退。
# 只影响本文件里的 `my_async_httpx.get(...)`；其它模块仍直接用 SealedRequests。
my_async_httpx = MirrorAwareHttpx()


class ProxyFetchRoundStats(BaseModel):
    """一轮代理抓取的统计（只用于 WARNING 观测，不参与业务决策）。"""

    raw_total: int = Field(default=0, description="各源返回的原始代理条数")
    dedup_total: int = Field(default=0, description="去重后条数")
    inserted: int = Field(default=0, description="实际新增入库条数")
    existed: int = Field(default=0, description="库中已存在条数（源站重新列出）")
    dead_skipped: int = Field(default=0, description="已存在但已被判死的条数（跳过）")
    ingest_failures: int = Field(default=0, description="入库失败条数")
    usable_pool: int = Field(default=0, description="本轮结束时 MySQL 可用池规模")
    elapsed_seconds: int = Field(default=0, description="本轮耗时（秒）")

    def to_log_str(self) -> str:
        return (
            f"源站返回={self.raw_total} 去重后={self.dedup_total} "
            f"新增入库={self.inserted} 已存在={self.existed}"
            f"（其中判死={self.dead_skipped}） "
            f"入库失败={self.ingest_failures} "
            f"可用池={self.usable_pool} 耗时={self.elapsed_seconds}s"
        )


class ProxySourceSpec(BaseModel):
    """一个补充代理源的声明。

    新增源只需在 ``SUPPLEMENT_SOURCES`` 里加一行，不用再复制一份
    ``get_proxy_from_xxx`` 方法体。GitHub 源会自动走镜像回退。
    """

    name: str = Field(..., description="源名称（日志标识）")
    url: str = Field(..., description="列表地址")
    protocol: Literal["http", "https", "socks4", "socks5"] = Field(
        default="http", description="该列表的协议（geonode_json 会按条目自带协议取）"
    )
    kind: Literal["lines", "geonode_json"] = Field(
        default="lines", description="lines=一行一个 ip:port；geonode_json=geonode API"
    )


# 补充源清单（2026-11-09 实测：全部可通过 ghproxy 镜像拿到数据）。
# 选源原则：优先「仓库自身每日更新、量级可控（几百~几千条）」，
# 体量过大的仓库（单文件 600KB+）不收录，避免一轮抓取被单个源拖长。
SUPPLEMENT_SOURCES: tuple[ProxySourceSpec, ...] = (
    ProxySourceSpec(
        name="monosans_http",
        url="https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt",
        protocol="http",
    ),
    ProxySourceSpec(
        name="monosans_socks4",
        url="https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks4.txt",
        protocol="socks4",
    ),
    ProxySourceSpec(
        name="monosans_socks5",
        url="https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks5.txt",
        protocol="socks5",
    ),
    ProxySourceSpec(
        name="clarketm_http",
        url="https://raw.githubusercontent.com/clarketm/proxy-list/master/proxy-list-raw.txt",
        protocol="http",
    ),
    ProxySourceSpec(
        name="shiftytr_http",
        url="https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/http.txt",
        protocol="http",
    ),
    ProxySourceSpec(
        name="shiftytr_https",
        url="https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/https.txt",
        protocol="https",
    ),
    ProxySourceSpec(
        name="shiftytr_socks4",
        url="https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/socks4.txt",
        protocol="socks4",
    ),
    ProxySourceSpec(
        name="shiftytr_socks5",
        url="https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/socks5.txt",
        protocol="socks5",
    ),
    ProxySourceSpec(
        name="vakhov_http",
        url="https://raw.githubusercontent.com/vakhov/fresh-proxy-list/master/http.txt",
        protocol="http",
    ),
    ProxySourceSpec(
        name="vakhov_https",
        url="https://raw.githubusercontent.com/vakhov/fresh-proxy-list/master/https.txt",
        protocol="https",
    ),
    ProxySourceSpec(
        name="vakhov_socks4",
        url="https://raw.githubusercontent.com/vakhov/fresh-proxy-list/master/socks4.txt",
        protocol="socks4",
    ),
    ProxySourceSpec(
        name="vakhov_socks5",
        url="https://raw.githubusercontent.com/vakhov/fresh-proxy-list/master/socks5.txt",
        protocol="socks5",
    ),
    ProxySourceSpec(
        name="thespeedx_socks4",
        url="https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks4.txt",
        protocol="socks4",
    ),
    ProxySourceSpec(
        name="thespeedx_socks5",
        url="https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks5.txt",
        protocol="socks5",
    ),
    # --- 2026-11-10 扩张：已实测可通过镜像拿到数据（HTTP 200 + 列表格式） ---
    ProxySourceSpec(
        name="ercin_http",
        url="https://raw.githubusercontent.com/ErcinDedeoglu/proxies/main/proxies/http.txt",
        protocol="http",
    ),
    ProxySourceSpec(
        name="ercin_https",
        url="https://raw.githubusercontent.com/ErcinDedeoglu/proxies/main/proxies/https.txt",
        protocol="https",
    ),
    ProxySourceSpec(
        name="ercin_socks4",
        url="https://raw.githubusercontent.com/ErcinDedeoglu/proxies/main/proxies/socks4.txt",
        protocol="socks4",
    ),
    ProxySourceSpec(
        name="ercin_socks5",
        url="https://raw.githubusercontent.com/ErcinDedeoglu/proxies/main/proxies/socks5.txt",
        protocol="socks5",
    ),
    ProxySourceSpec(
        name="jetkai_http",
        url=(
            "https://raw.githubusercontent.com/jetkai/proxy-list/main/"
            "online-proxies/txt/proxies-http.txt"
        ),
        protocol="http",
    ),
    ProxySourceSpec(
        name="jetkai_https",
        url=(
            "https://raw.githubusercontent.com/jetkai/proxy-list/main/"
            "online-proxies/txt/proxies-https.txt"
        ),
        protocol="https",
    ),
    ProxySourceSpec(
        name="jetkai_socks4",
        url=(
            "https://raw.githubusercontent.com/jetkai/proxy-list/main/"
            "online-proxies/txt/proxies-socks4.txt"
        ),
        protocol="socks4",
    ),
    ProxySourceSpec(
        name="jetkai_socks5",
        url=(
            "https://raw.githubusercontent.com/jetkai/proxy-list/main/"
            "online-proxies/txt/proxies-socks5.txt"
        ),
        protocol="socks5",
    ),
    ProxySourceSpec(
        name="hookzof_socks5",
        url="https://raw.githubusercontent.com/hookzof/socks5_list/master/proxy.txt",
        protocol="socks5",
    ),
    ProxySourceSpec(
        name="proxifly_socks4",
        url=(
            "https://raw.githubusercontent.com/proxifly/free-proxy-list/main/"
            "proxies/protocols/socks4/data.txt"
        ),
        protocol="socks4",
    ),
    ProxySourceSpec(
        name="proxifly_socks5",
        url=(
            "https://raw.githubusercontent.com/proxifly/free-proxy-list/main/"
            "proxies/protocols/socks5/data.txt"
        ),
        protocol="socks5",
    ),
    ProxySourceSpec(
        name="zloi_socks4",
        url="https://raw.githubusercontent.com/zloi-user/hideip.me/master/socks4.txt",
        protocol="socks4",
    ),
    ProxySourceSpec(
        name="roosterkid_socks4",
        url="https://raw.githubusercontent.com/roosterkid/openproxylist/main/SOCKS4_RAW.txt",
        protocol="socks4",
    ),
    ProxySourceSpec(
        name="roosterkid_socks5",
        url="https://raw.githubusercontent.com/roosterkid/openproxylist/main/SOCKS5_RAW.txt",
        protocol="socks5",
    ),
    ProxySourceSpec(
        name="mmpx12_socks4",
        url="https://raw.githubusercontent.com/mmpx12/proxy-list/master/socks4.txt",
        protocol="socks4",
    ),
    ProxySourceSpec(
        name="mmpx12_socks5",
        url="https://raw.githubusercontent.com/mmpx12/proxy-list/master/socks5.txt",
        protocol="socks5",
    ),
    ProxySourceSpec(
        name="murongpig_socks5",
        url="https://raw.githubusercontent.com/MuRongPIG/Proxy-Master/main/socks5.txt",
        protocol="socks5",
    ),
    # 非 GitHub：公共 API（纯文本列表，国内可直连），作为「不走镜像」的独立路径
    ProxySourceSpec(
        name="geonode_api",
        url=(
            "https://proxylist.geonode.com/api/proxy-list"
            "?limit=500&page=1&sort_by=lastChecked&sort_type=desc"
        ),
        kind="geonode_json",
    ),
    ProxySourceSpec(
        name="openproxylist_socks4",
        url="https://api.openproxylist.xyz/socks4.txt",
        protocol="socks4",
    ),
    ProxySourceSpec(
        name="openproxylist_socks5",
        url="https://api.openproxylist.xyz/socks5.txt",
        protocol="socks5",
    ),
    ProxySourceSpec(
        name="proxyspace_socks4",
        url="https://proxyspace.pro/socks4.txt",
        protocol="socks4",
    ),
    ProxySourceSpec(
        name="proxyspace_socks5",
        url="https://proxyspace.pro/socks5.txt",
        protocol="socks5",
    ),
)
# 单个补充源的请求超时（秒）：各源并行，整体不超过这个量级
_SUPPLEMENT_SOURCE_TIMEOUT = 20
# 补充源内部并发上限（统一配置）：源多了（30+）之后全并发容易被镜像限流
# （实测 mmpx12 会返回 200 + "error code: 1015"，解析 0 条），默认压到 12 更稳
_SUPPLEMENT_SOURCE_CONCURRENCY = settings.concurrency.proxy_source_supplement
# geonode 的单条目可能同时挂多个协议，按此顺序取第一个可用的
_GEONODE_PROTOCOL_PREFERENCE = ("http", "https", "socks5", "socks4")


def _parse_geonode_json(payload: str) -> list[dict[str, str]]:
    """解析 geonode 的 ``{"data":[{ip, port, protocols}]}``。"""
    try:
        data = json.loads(payload)
    except json.JSONDecodeError:
        return []
    items = data.get("data") if isinstance(data, dict) else None
    if not isinstance(items, list):
        return []
    proxies: list[dict[str, str]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        ip, port = item.get("ip"), item.get("port")
        if not ip or not port:
            continue
        # 同一条目可能同时支持多种协议：按「http → https → socks5 → socks4」优先取一个，
        # 避免把能用 http 的代理录成 socks（B 站请求对 http 代理兼容性最好）。
        protocols = set(item.get("protocols") or [])
        for protocol in _GEONODE_PROTOCOL_PREFERENCE:
            if protocol not in protocols:
                continue
            if proxy_dict := format_proxy(f"{ip}:{port}", protocol=protocol):
                proxies.append(proxy_dict)
            break
    return proxies


def parse_supplement_payload(
    spec: ProxySourceSpec, payload: str
) -> list[dict[str, str]]:
    """把补充源返回的文本解析成代理字典列表（与其它源同一格式）。"""
    if spec.kind == "geonode_json":
        return _parse_geonode_json(payload)
    proxies: list[dict[str, str]] = []
    for line in payload.splitlines():
        text = line.strip()
        if not text or text.startswith("#"):
            continue
        proxy_text = extract_proxy_text(text)
        if not proxy_text:
            continue
        if proxy_dict := format_proxy(proxy_text, protocol=spec.protocol):
            proxies.append(proxy_dict)
    return proxies


def extract_proxy_text(line: str) -> str | None:
    """从一行文本里取出 ``[协议://]ip:port`` 前缀。

    免费源常见的附带信息形式都能兼容：
    ``31.43.63.70:4145:Ukraine``（hideip.me）、``1.2.3.4:8080 高匿``、
    ``http://1.2.3.4:8080``。
    """
    if matched := _PROXY_PREFIX_RE.match(line.strip()):
        return matched.group(1)
    return None


class GetProxyMethods:
    """代理抓取器。

    不再继承 ``UnlimitedCrawler``：抓取与入库已经合并成一条流式流水线
    （见 ``__get_proxy`` / ``_ingest_proxies``），不需要 worker 池、任务队列与插件。
    调度器只调用 ``main()``，并用 ``self.log`` / ``self.crawler_name`` 记日志。
    """

    async def main(self, *args, **kwargs):
        start_ts = int(time.time())
        self._round_stats = ProxyFetchRoundStats()
        self.proxy_key_lookup = None
        self.log.debug(f"{self.__class__.__name__}开始获取代理")
        # 抓取与入库已经合并成一条流水线（每个源一返回就入库并释放），
        # 不再先攒出全量列表再交给 run()，避免 250 万条同时驻留内存。
        await self.get_proxy()
        self.log.debug(f"{self.__class__.__name__}获取代理并入库完成")
        self.log.debug(f"{self.__class__.__name__}开始移除重复代理")
        await SQLHelper.remove_list_dict_data_by_proxy()
        self.log.debug(f"{self.__class__.__name__}移除重复代理完成")
        # 生产 LOG_LEVEL=WARNING，这里必须用 WARNING 才能在 docker logs 里看到，
        # 用于区分「源站没产出」和「选取策略不对」。
        stats = self._round_stats
        stats.elapsed_seconds = int(time.time()) - start_ts
        stats.usable_pool = await SQLHelper.get_usable_proxy_num()
        self.log.warning(f"代理抓取轮次结束：{stats.to_log_str()}")

    def __init__(self):
        self.log = sql_log
        self.crawler_name: str = type(self).__name__
        self._lock = asyncio.Lock()
        self._round_stats = ProxyFetchRoundStats()
        # 本轮候选的批量存在性检查结果；None 表示不在抓取轮次内（回退单条查询）
        self.proxy_key_lookup: ProxyKeyLookup | None = None
        # 单源翻页上限：20~30 页，失败源由单源超时自然跳过
        self.get_proxy_page = 25
        self.get_proxy_timestamp: int = 0
        # 抓取间隔：调度每 5 分钟触发，45 分钟真正抓一次（原 2 小时产出跟不上消耗）
        self.get_proxy_sep_time = 45 * 60
        self.GetProxy_Flag = False

    # region a从代理网站获取代理

    # region a从免费代理网站获取代理，每个网站的表格不一样，需要测试！网站按照表格的样式填充代理信息

    # async def get_proxy_from_cn_proxy_tools(self) -> tuple[list, bool]:
    #     headers = {
    #         'user-agent': CONFIG.rand_ua,
    #     }
    #     get_proxy_success = True
    #     proxy_queue = []
    #     for page in range(1, self.get_proxy_page + 1):
    #
    #         url = f'https://cn.proxy-tools.com/proxy?page={page}'
    #         headers.update({'Referer': url})
    #         req = await my_async_httpx.get(url=url, verify=False, headers=headers, )
    #         if req:
    #             html = bs4.BeautifulSoup(req.text, 'html.parser')
    #             td = html.select('tr>td')
    #             proxies = []
    #             for i in range(len(td) // 9):
    #                 proxies.append(f'{td[i * 9 + 2].text.strip().lower()}://{td[i * 9].text}')
    #
    #             # have_proxy = [x['proxy'] for x in self.proxy_list]
    #
    #             for i in proxies:
    #                 if i:
    #                     append_dict = format_proxy(i)
    #                     if not append_dict:
    #                         self.log.debug(f'代理格式错误！{i}\n{td}')
    #                         continue
    #                     # if append_dict not in have_proxy:
    #                     proxy_queue.append(append_dict)
    #             if len(proxy_queue) < 10:
    #                 self.log.info(f'{req.text}, {url}')
    #             self.log.info(f'总共有{len(proxy_queue)}个代理需要检查')
    #         else:
    #             self.log.info(f'{req.text}, {url}')
    #             get_proxy_success = False
    #     return proxy_queue, get_proxy_success

    async def get_proxy_from_kuaidaili_dps(self) -> AsyncGenerator[list, None]:
        headers = {
            "cookie": "channelid=0; sid=1744472570698409; _ga=GA1.1.1164732364.1744473880; _gcl_au=1.1.1549492445.1744473880; _ss_s_uid=b40a1d7896a217a1d7db1800aad1c025; _ga_DC1XM0P4JL=GS1.1.1746241898.3.1.1746241905.53.0.0",
            "Sec-Fetch-Site": "same-origin",
            "user-agent": CONFIG.rand_ua,
        }
        proxy_queue = []
        for page in range(1, self.get_proxy_page + 1):
            url = f"https://www.kuaidaili.com/free/dps/{page}"
            headers.update({"Referer": url})
            req = await my_async_httpx.get(
                url=url,
                verify=False,
                headers=headers,
            )
            if req:
                html = bs4.BeautifulSoup(req.text, "html.parser")
                script = html.select('script[type="text/javascript"]')
                json_str = ""
                for x in filter(lambda y: y.text and "valid_minute" in y.text, script):
                    json_str = "".join(re.findall("const fpsList =(.*);", x.text))
                if not json_str:
                    return
                proxies = json.loads(json_str)

                for i in proxies:
                    if i:
                        append_dict = format_proxy(
                            f"{i.get('ip')}:{i.get('port')}", protocol="http"
                        )
                        if not append_dict:
                            self.log.debug(f"代理格式错误！{i}")
                            continue
                        # if append_dict not in have_proxy:
                        proxy_queue.append(append_dict)
                if len(proxy_queue) < 10:
                    self.log.info(f"代理获取失败！\n{req.text}, {url}")
                self.log.info(f"总共有{len(proxy_queue)}个代理需要检查")
            else:
                self.log.info(f"{req.text}, {url}")

            await asyncio.sleep(6)
        if proxy_queue:
            yield proxy_queue

    async def get_proxy_from_kuaidaili_fps(self) -> AsyncGenerator[list, None]:
        headers = {
            "cookie": "channelid=0; sid=1744472570698409; _ga=GA1.1.1164732364.1744473880; _gcl_au=1.1.1549492445.1744473880; _ss_s_uid=b40a1d7896a217a1d7db1800aad1c025; _ga_DC1XM0P4JL=GS1.1.1746241898.3.1.1746241905.53.0.0",
            "Sec-Fetch-Site": "same-origin",
            "user-agent": CONFIG.rand_ua,
        }
        proxy_queue = []
        for page in range(1, self.get_proxy_page + 1):
            url = f"https://www.kuaidaili.com/free/fps/{page}"
            headers.update({"Referer": url})
            req = await my_async_httpx.get(
                url=url,
                verify=False,
                headers=headers,
            )
            if req:
                html = bs4.BeautifulSoup(req.text, "html.parser")
                script = html.select('script[type="text/javascript"]')
                json_str = ""
                for x in filter(
                    lambda y: y.text and "last_check_time" in y.text, script
                ):
                    json_str = "".join(re.findall("const fpsList =(.*);", x.text))
                if not json_str:
                    return
                proxies = json.loads(json_str)
                for i in proxies:
                    if i:
                        append_dict = format_proxy(
                            f"{i.get('ip')}:{i.get('port')}", protocol="http"
                        )
                        if not append_dict:
                            self.log.debug(f"代理格式错误！{i}")
                            continue
                        # if append_dict not in have_proxy:
                        proxy_queue.append(append_dict)
                if len(proxy_queue) < 10:
                    self.log.info(f"代理获取失败！\n{req.text}, {url}")
                self.log.info(f"总共有{len(proxy_queue)}个代理需要检查")
            else:
                self.log.info(f"{req.text}, {url}")
            await asyncio.sleep(6)
        if proxy_queue:
            yield proxy_queue

    async def get_proxy_from_zdayip(self) -> AsyncGenerator[list, None]:
        headers = {
            "User-Agent": CONFIG.rand_ua,
        }

        proxy_queue = []
        for page in range(1, self.get_proxy_page + 1):
            url = f"https://www.zdaye.com/free/{page}/"
            req = await my_async_httpx.get(
                url=url,
                headers=headers,
                verify=False,
                proxies=(await SQLHelper.select_score_top_proxy()).proxy,
            )
            if req.status_code == 200:
                html = bs4.BeautifulSoup(req.text, "html.parser")
                td = html.select("tr>td")
                proxies = []
                for i in range(len(td) // 9):
                    proxies.append(f"{td[i * 9].text}:{td[i * 9 + 1].text}")

                # have_proxy = [x['proxy'] for x in self.proxy_list]

                for i in proxies:
                    if i:
                        append_dict = format_proxy(i)
                        if not append_dict:
                            self.log.debug(f"代理格式错误！{i}\n{td}")
                            continue
                        proxy_queue.append(append_dict)
                if len(proxy_queue) < 10:
                    self.log.info(f"{req.text}, {url}")
                self.log.info(f"总共有{len(proxy_queue)}个代理需要检查")
            else:
                self.log.info(f"{req.text}, {url}")

        if proxy_queue:
            yield proxy_queue

    async def get_proxy_from_66daili(self) -> AsyncGenerator[list, None]:
        headers = {
            "user-agent": CONFIG.rand_ua,
        }
        proxy_queue = []
        for page in range(1, self.get_proxy_page + 1):
            url = f"http://www.66ip.cn/{page}.html"
            req = await my_async_httpx.get(
                url=url,
                headers=headers,
                verify=False,
                proxies=(await SQLHelper.select_score_top_proxy()).proxy,
            )

            if req:
                html = bs4.BeautifulSoup(req.text, "html.parser")
                td = html.select("tr>td")[6:]
                proxies = []
                for i in range(len(td) // 5):
                    proxies.append(f"{td[i * 5].text}:{td[i * 5 + 1].text}")

                # have_proxy = [x['proxy'] for x in self.proxy_list]

                for i in proxies:
                    if i:
                        append_dict = format_proxy(i)
                        if not append_dict:
                            self.log.debug(f"代理格式错误！{i}\n{td}")
                            continue
                        proxy_queue.append(append_dict)
                if len(proxy_queue) < 5:
                    self.log.info(f"{req.text}, {url}")
                self.log.info(f"总共有{len(proxy_queue)}个代理需要检查")
            else:
                self.log.info(f"{req.text}, {url}")


        if proxy_queue:
            yield proxy_queue

    async def get_proxy_from_89daili(self) -> AsyncGenerator[list, None]:
        def fr(YH):
            oo = [
                0x91,
                0x66,
                0xBC,
                0xE1,
                0x53,
                0x94,
                0xC5,
                0x00,
                0x40,
                0x93,
                0x52,
                0x97,
                0xD4,
                0x12,
                0xDC,
                0x12,
                0x4B,
                0x9C,
                0x7D,
                0x2A,
                0x65,
                0xC3,
                0x37,
                0x8A,
                0xF5,
                0x5B,
                0x78,
                0xD1,
                0x58,
                0x8B,
                0xD0,
                0x14,
                0x45,
                0x8E,
                0xF5,
                0xFE,
                0x28,
                0x67,
                0xAC,
                0xFD,
                0xDE,
                0xA9,
                0x23,
                0x0E,
                0x5D,
                0x8E,
                0xD3,
                0x1B,
                0x42,
                0x14,
                0xEC,
                0xD7,
                0xAA,
                0x9D,
                0xC6,
                0x0C,
                0x06,
                0x55,
                0x30,
                0x61,
                0xAA,
                0xEF,
                0x37,
                0x9D,
                0x68,
                0x55,
                0x26,
                0x1F,
                0xED,
                0xAE,
                0x78,
                0xA7,
                0xA0,
                0x73,
                0xBA,
                0x7B,
                0x4E,
                0xAB,
                0x9C,
                0xC3,
                0xB6,
                0x77,
                0x52,
                0x83,
                0x61,
                0x50,
                0x3B,
                0xFB,
                0x25,
                0x6A,
                0x59,
                0xDC,
                0xAD,
                0xFC,
                0xD5,
                0xA8,
                0xF7,
                0xD0,
                0xD5,
                0xC0,
                0x8F,
                0x76,
                0x37,
                0x20,
                0xEC,
                0x0E,
                0x00,
                0xCF,
                0xBA,
                0x9F,
                0x7A,
                0x47,
                0x7C,
                0x4D,
                0x38,
                0x0D,
                0xD7,
                0x09,
                0x64,
                0x0B,
                0x76,
                0xAF,
                0x17,
                0x48,
                0xAD,
                0x8E,
                0x7C,
                0x89,
                0xDA,
                0x0E,
                0xD0,
                0x9B,
                0x80,
                0x7A,
                0x45,
                0x06,
                0x0F,
                0x7A,
                0xC1,
                0x82,
                0x0F,
                0xF5,
                0xC6,
                0x91,
                0x7A,
                0x55,
                0x2C,
                0x25,
                0x18,
                0xE8,
                0xDF,
                0xB0,
                0x9B,
                0x46,
                0x17,
                0x04,
                0x05,
                0x38,
                0x0D,
                0xD7,
                0x0B,
                0x49,
                0x86,
                0xC7,
                0x11,
                0x4A,
                0x89,
                0x6A,
                0x4C,
                0x0B,
                0xE5,
                0xDE,
                0x18,
                0x63,
                0x22,
                0x62,
                0xA7,
                0xE6,
                0xDB,
                0x86,
                0xF1,
                0x3B,
                0xF5,
                0x49,
                0x84,
                0x65,
                0x22,
                0x17,
                0xC1,
                0x4A,
                0x6F,
                0xC0,
                0xE5,
                0x57,
                0xBE,
                0xEF,
                0x95,
                0x42,
                0x37,
                0x84,
                0xDD,
                0x1D,
                0x50,
                0x30,
                0x6D,
                0xDA,
                0x99,
                0xCC,
                0x0A,
                0x4F,
                0x0D,
                0x7E,
                0xBF,
                0xF0,
                0x30,
                0x83,
                0x42,
                0x81,
                0xC4,
                0x02,
                0x47,
                0x90,
                0xE3,
                0x3D,
                0xE7,
                0x25,
                0x64,
                0x45,
                0x12,
                0x4D,
                0xC0,
                0xCD,
                0x21,
                0x8C,
                0xF1,
                0xC8,
                0x85,
                0x42,
                0x4A,
                0xB7,
                0x05,
                0x72,
                0x31,
                0x0C,
                0xE4,
                0xC7,
                0x01,
                0x6C,
                0x2B,
                0x70,
                0xAF,
                0x6C,
                0x04,
                0x5D,
                0x9C,
                0xCF,
                0x01,
                0x7C,
                0x99,
                0x01,
                0xE9,
                0xA8,
                0xE3,
                0x57,
                0x98,
                0xDB,
                0x38,
                0x04,
                0x3B,
            ]

            # 第一轮处理
            qo = 267
            while qo >= 2:
                oo[qo] = (-oo[qo]) & 0xFF
                oo[qo] = (((oo[qo] >> 5) | ((oo[qo] << 3) & 0xFF)) - 12) & 0xFF
                qo -= 1

            # 第二轮处理
            qo = 266
            while qo >= 3:
                oo[qo] = (oo[qo] - oo[qo - 1]) & 0xFF
                qo -= 1

            # 第三轮处理
            for qo in range(1, len(oo)):
                oo[qo] = ((((oo[qo] + 110) & 0xFF) + 181) & 0xFF) << 4 & 0xFF | (
                    (((oo[qo] + 110) & 0xFF) + 181) & 0xFF
                ) >> 4

            # 构建输出字符串
            po = ""
            for qo in range(1, len(oo) - 1):
                if qo % 7 != 0:
                    po += chr(oo[qo] ^ YH)

            return po

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36 Edg/114.0.1823.67",
            "cookie": re.search(r"https_ydclearance=[^;]+", fr(252)).group(0),
        }
        proxy_queue = []
        for page in range(1, self.get_proxy_page + 1):
            url = f"https://www.89ip.cn/index_{page}.html"
            req = await my_async_httpx.get(
                url=url,
                verify=False,
                headers=headers,
            )

            if req:
                html = bs4.BeautifulSoup(req.text, "html.parser")
                td = html.select("tr>td")
                proxies = []
                for i in range(len(td) // 5):
                    proxies.append(
                        f"{td[i * 5].text.strip()}:{td[i * 5 + 1].text.strip()}"
                    )

                # have_proxy = [x['proxy'] for x in self.proxy_list]

                for i in proxies:
                    if i:
                        append_dict = format_proxy(i)
                        if not append_dict:
                            self.log.debug(f"代理格式错误！{i}\n{td}")
                            continue
                        proxy_queue.append(append_dict)
                if len(proxy_queue) < 5:
                    self.log.info(f"{req.text}, {url}")
                self.log.info(f"总共有{len(proxy_queue)}个代理需要检查")
            else:
                self.log.info(f"{req.text}, {url}")

        if proxy_queue:
            yield proxy_queue

    async def get_proxy_from_ip3366_1(self) -> AsyncGenerator[list, None]:
        headers = {
            "User-Agent": CONFIG.rand_ua,
        }

        proxy_queue = []
        for page in range(1, self.get_proxy_page + 1):
            url = f"http://www.ip3366.net/free/?stype=1&page={page}"
            req = await my_async_httpx.get(
                url=url,
                headers=headers,
                verify=False,
            )

            if req:
                html = bs4.BeautifulSoup(req.text, "html.parser")
                td = html.select("tr>td")
                proxies = []
                for i in range(len(td) // 7):
                    proxies.append(f"{td[i * 7].text}:{td[i * 7 + 1].text}")

                # have_proxy = [x['proxy'] for x in self.proxy_list]

                for i in proxies:
                    if i:
                        append_dict = format_proxy(i, protocol="http")
                        if not append_dict:
                            self.log.debug(f"代理格式错误！{i}\n{td}")
                            continue
                        proxy_queue.append(append_dict)
                if len(proxy_queue) < 10:
                    self.log.info(f"{req.text}, {url}")
                self.log.info(f"总共有{len(proxy_queue)}个代理需要检查")
            else:
                self.log.info(f"{req.text}, {url}")

        if proxy_queue:
            yield proxy_queue

    async def get_proxy_from_ip3366_2(self) -> AsyncGenerator[list, None]:
        headers = {
            "User-Agent": CONFIG.rand_ua,
        }

        proxy_queue = []
        for page in range(1, self.get_proxy_page + 1):
            url = f"http://www.ip3366.net/free/?stype=2&page={page}"
            req = await my_async_httpx.get(
                url=url,
                headers=headers,
                verify=False,
            )
            if req:
                html = bs4.BeautifulSoup(req.text, "html.parser")
                td = html.select("tr>td")
                proxies = []
                for i in range(len(td) // 7):
                    proxies.append(f"{td[i * 7].text}:{td[i * 7 + 1].text}")

                # have_proxy = [x['proxy'] for x in self.proxy_list]

                for i in proxies:
                    if i:
                        append_dict = format_proxy(i, protocol="https")
                        if not append_dict:
                            self.log.debug(f"代理格式错误！{i}\n{td}")
                            continue
                        proxy_queue.append(append_dict)
                if len(proxy_queue) < 10:
                    self.log.info(f"{req.text}, {url}")
                self.log.info(f"总共有{len(proxy_queue)}个代理需要检查")
            else:
                self.log.info(f"{req.text}, {url}")


        if proxy_queue:
            yield proxy_queue

    async def get_proxy_from_qiyun(self) -> AsyncGenerator[list, None]:
        headers = {
            "User-Agent": CONFIG.rand_ua,
        }

        proxy_queue = []
        for page in range(1, self.get_proxy_page + 1):
            url = f"https://www.qiyunip.com/freeProxy/{page}.html"
            req = await my_async_httpx.get(
                url=url,
                headers=headers,
                verify=False,
            )

            if req:
                html = bs4.BeautifulSoup(req.text, "html.parser")
                td = html.select("tbody>tr>th")
                proxies = []
                for i in range(len(td) // 7):
                    proxies.append(
                        f"{td[i * 7 + 3].text}://{td[i * 7].text}:{td[i * 7 + 1].text}"
                    )
                for i in proxies:
                    if i:
                        append_dict = format_proxy(i)
                        if not append_dict:
                            self.log.debug(f"代理格式错误！{i}\n{td}")
                            continue
                        proxy_queue.append(append_dict)
                if len(proxy_queue) < 10:
                    self.log.info(f"{req.text}, {url}")
                self.log.info(f"总共有{len(proxy_queue)}个代理需要检查")
            else:
                self.log.info(f"{req.text}, {url}")

        if proxy_queue:
            yield proxy_queue

    async def get_proxy_from_ihuan(self) -> AsyncGenerator[list, None]:
        headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
            "cache-control": "max-age=0",
            "content-type": "",
            "sec-ch-ua": '"Microsoft Edge";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "same-origin",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
        }
        proxy_queue = []
        url = f"https://ip.ihuan.me/"
        req = await my_async_httpx.get(
            url=url,
            headers=headers,
            verify=False,
        )
        if req:
            html = bs4.BeautifulSoup(req.text, "html.parser")
            td = html.select("tr>td")
            proxies = []
            for i in range(len(td) // 10):
                proxies.append(f"{td[i * 10].text}:{td[i * 10 + 1].text}")

            # have_proxy = [x['proxy'] for x in self.proxy_list]

            for i in proxies:
                if i:
                    append_dict = format_proxy(i)
                    if not append_dict:
                        self.log.debug(f"代理格式错误！{i}\n{td}")
                        continue
                    proxy_queue.append(append_dict)
            if len(proxy_queue) < 10:
                self.log.info(f"{req.text}, {url}")
            self.log.info(f"总共有{len(proxy_queue)}个代理需要检查")
        else:
            self.log.info(f"{req.text}, {url}")


        if proxy_queue:
            yield proxy_queue

    async def get_proxy_from_docip(self) -> AsyncGenerator[list, None]:
        headers = {
            "User-Agent": CONFIG.rand_ua,
        }

        proxy_queue = []
        gmt_format = "%a %b %d %Y %H:%M:%S GMT 0800 (中国标准时间)"
        gmt = datetime.now().strftime(gmt_format)

        url = f"https://www.docip.net/data/free.json?t={gmt}"
        req = await my_async_httpx.get(
            url=url,
            headers=headers,
            verify=False,
        )

        if req:
            for i in req.json().get("data"):
                if ip := i.get("ip"):
                    append_dict = format_proxy(ip)
                    if not append_dict:
                        self.log.debug(f"代理格式错误！{i}")
                        continue
                    proxy_queue.append(append_dict)
            if len(proxy_queue) < 10:
                self.log.info(f"{req.text}, {url}")
            self.log.info(f"总共有{len(proxy_queue)}个代理需要检查")
        else:
            self.log.info(f"{req.text}, {url}")


        if proxy_queue:
            yield proxy_queue

    async def get_proxy_from_openproxylist(self) -> AsyncGenerator[list, None]:
        url = f"https://openproxylist.xyz/http.txt"
        async for batch in self._iter_raw_list_source(
            url, protocol="http"
        ):
            yield batch

    async def get_proxy_from_proxyhub(self) -> AsyncGenerator[list, None]:
        headers = {
            "user-agent": CONFIG.rand_ua,
        }
        req = ""
        proxy_queue = []
        for page in range(1, self.get_proxy_page + 1):
            url = f"https://proxyhub.me/"
            headers.update({"cookie": f"page={page};"})
            req = await my_async_httpx.get(
                url=url,
                verify=False,
                headers=headers,
            )

            if req:
                html = bs4.BeautifulSoup(req.text, "html.parser")
                td = html.select("tr>td")
                proxies = []
                for i in range(len(td) // 6):
                    proxies.append(f"{td[i * 6].text}:{td[i * 6 + 1].text}")

                # have_proxy = [x['proxy'] for x in self.proxy_list]

                for i in proxies:
                    if i:
                        append_dict = format_proxy(i)
                        if not append_dict:
                            self.log.debug(f"代理格式错误！{i}\n{td}")
                            continue
                        proxy_queue.append(append_dict)
                if len(proxy_queue) < 10:
                    self.log.info(f"{req.text}, {url}")
                self.log.info(f"总共有{len(proxy_queue)}个代理需要检查")
            else:
                self.log.info(f"{req.text}, {url}")


        if proxy_queue:
            yield proxy_queue

    # endregion

    # region Github获取的text格式的代理，每行格式为ip:port
    async def get_proxy_from_proxy_scdn_http(self) -> AsyncGenerator[list, None]:
        url = f"https://proxy.scdn.io/text.php"
        async for batch in self._iter_raw_list_source(
            url, protocol="http"
        ):
            yield batch

    async def get_proxy_from_proxy_list_download_http(self) -> AsyncGenerator[list, None]:
        url = f"https://www.proxy-list.download/api/v1/get?type=http"
        async for batch in self._iter_raw_list_source(
            url, protocol="http"
        ):
            yield batch

    async def get_proxy_from_proxy_list_download_socks5(self) -> AsyncGenerator[list, None]:
        url = f"https://www.proxy-list.download/api/v1/get?type=socks5"
        async for batch in self._iter_raw_list_source(
            url, protocol="socks5"
        ):
            yield batch

    async def get_proxy_from_sunny9577_socks5(self) -> AsyncGenerator[list, None]:
        url = f"https://raw.githubusercontent.com/sunny9577/proxy-scraper/refs/heads/master/generated/socks5_proxies.txt"
        async for batch in self._iter_raw_list_source(
            url, protocol="socks5"
        ):
            yield batch

    async def get_proxy_from_sunny9577_http(self) -> AsyncGenerator[list, None]:
        url = f"https://raw.githubusercontent.com/sunny9577/proxy-scraper/refs/heads/master/generated/http_proxies.txt"
        async for batch in self._iter_raw_list_source(
            url, protocol="http"
        ):
            yield batch

    async def get_proxy_from_Vadim287_socks5(self) -> AsyncGenerator[list, None]:
        url = f"https://raw.githubusercontent.com/Vadim287/free-proxy/refs/heads/main/proxies/socks5.txt"
        async for batch in self._iter_raw_list_source(
            url, protocol="socks5"
        ):
            yield batch

    async def get_proxy_from_Vadim287_http(self) -> AsyncGenerator[list, None]:
        url = f"https://raw.githubusercontent.com/Vadim287/free-proxy/refs/heads/main/proxies/http.txt"
        async for batch in self._iter_raw_list_source(
            url, protocol="http"
        ):
            yield batch

    async def get_proxy_from_VMHeaven_socks5(self) -> AsyncGenerator[list, None]:
        url = f"https://raw.githubusercontent.com/vmheaven/VMHeaven-Free-Proxy-Updated/refs/heads/main/socks5.txt"
        async for batch in self._iter_raw_list_source(
            url, protocol="socks5"
        ):
            yield batch

    async def get_proxy_from_VMHeaven_https(self) -> AsyncGenerator[list, None]:
        url = f"https://raw.githubusercontent.com/vmheaven/VMHeaven-Free-Proxy-Updated/refs/heads/main/https.txt"
        async for batch in self._iter_raw_list_source(
            url, protocol="https"
        ):
            yield batch

    async def get_proxy_from_VMHeaven_http(self) -> AsyncGenerator[list, None]:
        url = f"https://raw.githubusercontent.com/vmheaven/VMHeaven-Free-Proxy-Updated/refs/heads/main/http.txt"
        async for batch in self._iter_raw_list_source(
            url, protocol="http"
        ):
            yield batch

    async def get_proxy_from_RioMMO_http(self) -> AsyncGenerator[list, None]:
        url = f"https://raw.githubusercontent.com/RioMMO/ProxyFree/refs/heads/main/HTTP.txt"
        async for batch in self._iter_raw_list_source(
            url, protocol="http"
        ):
            yield batch

    async def get_proxy_from_RioMMO_socks5(self) -> AsyncGenerator[list, None]:
        url = f"https://raw.githubusercontent.com/RioMMO/ProxyFree/refs/heads/main/SOCKS5.txt"
        async for batch in self._iter_raw_list_source(
            url, protocol="socks5"
        ):
            yield batch

    async def get_proxy_from_databay_labs_lists_socks5(self) -> AsyncGenerator[list, None]:
        url = f"https://raw.githubusercontent.com/databay-labs/free-proxy-list/refs/heads/master/socks5.txt"
        async for batch in self._iter_raw_list_source(
            url, protocol="socks5"
        ):
            yield batch

    async def get_proxy_from_saisuiu_Lionkings_Http_Proxys_Proxies(self) -> AsyncGenerator[list, None]:
        url = f"https://raw.githubusercontent.com/saisuiu/Lionkings-Http-Proxys-Proxies/refs/heads/main/free.txt"
        async for batch in self._iter_raw_list_source(
            url, protocol="http"
        ):
            yield batch

    async def get_proxy_from_Xnidada_proxylist(self) -> AsyncGenerator[list, None]:
        headers = {"user-agent": CONFIG.rand_ua}
        req = ""
        proxy_queue = []
        url = f"https://raw.githubusercontent.com/Xnidada/proxylist/refs/heads/main/proxylist.json"
        req = await my_async_httpx.get(
            url=url, headers=headers, verify=False, proxies=_github_proxy
        )
        if req:
            proxies = []
            for i in req.json():
                if _ := i:
                    append_dict = format_proxy(_.get("proxy"), protocol="http")
                    if not append_dict:
                        self.log.debug(f"代理格式错误！{_}")
                        continue
                    proxy_queue.append(append_dict)
            if len(proxy_queue) < 10:
                self.log.info(f"{req.text}, {url}")
            self.log.info(f"总共有{len(proxy_queue)}个代理需要检查")
        else:
            self.log.info(f"{req.text}, {url}")

        if proxy_queue:
            yield proxy_queue

    async def get_proxy_from_databay_labs_lists_http(self) -> AsyncGenerator[list, None]:
        url = f"https://raw.githubusercontent.com/databay-labs/free-proxy-list/refs/heads/master/http.txt"
        async for batch in self._iter_raw_list_source(
            url, protocol="http"
        ):
            yield batch

    async def get_proxy_from_Tsprnay_Proxy_lists_socks5(self) -> AsyncGenerator[list, None]:
        url = f"https://raw.githubusercontent.com/Tsprnay/Proxy-lists/master/proxies/socks5.txt"
        async for batch in self._iter_raw_list_source(
            url, protocol="socks5"
        ):
            yield batch

    async def get_proxy_from_Tsprnay_Proxy_lists_https(self) -> AsyncGenerator[list, None]:
        url = f"https://raw.githubusercontent.com/Tsprnay/Proxy-lists/master/proxies/https.txt"
        async for batch in self._iter_raw_list_source(
            url, protocol="https"
        ):
            yield batch

    async def get_proxy_from_Tsprnay_Proxy_lists_http(self) -> AsyncGenerator[list, None]:
        url = f"https://raw.githubusercontent.com/Tsprnay/Proxy-lists/master/proxies/http.txt"
        async for batch in self._iter_raw_list_source(
            url, protocol="http"
        ):
            yield batch

    async def get_proxy_from_M_logique_Proxies_socks5(self) -> AsyncGenerator[list, None]:
        url = f"https://raw.githubusercontent.com/M-logique/Proxies/refs/heads/main/proxies/regular/socks5.txt"
        async for batch in self._iter_raw_list_source(
            url, protocol="socks5"
        ):
            yield batch

    async def get_proxy_from_M_logique_Proxies_http(self) -> AsyncGenerator[list, None]:
        url = f"https://raw.githubusercontent.com/M-logique/Proxies/refs/heads/main/proxies/regular/http.txt"
        async for batch in self._iter_raw_list_source(
            url, protocol="http"
        ):
            yield batch

    async def get_proxy_from_ALIILAPRO_Proxy_http(self) -> AsyncGenerator[list, None]:
        url = f"https://raw.githubusercontent.com/ALIILAPRO/Proxy/refs/heads/main/http.txt"
        async for batch in self._iter_raw_list_source(
            url, protocol="http"
        ):
            yield batch

    async def get_proxy_from_theriturajps_proxy_list_http(self) -> AsyncGenerator[list, None]:
        url = f"https://raw.githubusercontent.com/theriturajps/proxy-list/refs/heads/main/proxies.txt"
        async for batch in self._iter_raw_list_source(
            url, protocol="http"
        ):
            yield batch

    async def get_proxy_from_SevenworksDev_proxy_list_socks5(self) -> AsyncGenerator[list, None]:
        url = f"https://raw.githubusercontent.com/SevenworksDev/proxy-list/refs/heads/main/proxies/socks5.txt"
        async for batch in self._iter_raw_list_source(
            url, protocol="socks5"
        ):
            yield batch

    async def get_proxy_from_SevenworksDev_proxy_list_https(self) -> AsyncGenerator[list, None]:
        url = f"https://raw.githubusercontent.com/SevenworksDev/proxy-list/refs/heads/main/proxies/https.txt"
        async for batch in self._iter_raw_list_source(
            url, protocol="https"
        ):
            yield batch

    async def get_proxy_from_SevenworksDev_proxy_list_http(self) -> AsyncGenerator[list, None]:
        url = f"https://raw.githubusercontent.com/SevenworksDev/proxy-list/refs/heads/main/proxies/http.txt"
        async for batch in self._iter_raw_list_source(
            url, protocol="http"
        ):
            yield batch

    async def get_proxy_from_gh_proxifly_free_proxy_list(self) -> AsyncGenerator[list, None]:
        url = f"https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/all/data.txt"
        async for batch in self._iter_raw_list_source(
            url, protocol="http"
        ):
            yield batch

    async def get_proxy_from_fyvri_fresh_proxy_list_socks5(self) -> AsyncGenerator[list, None]:
        url = f"https://raw.githubusercontent.com/fyvri/fresh-proxy-list/archive/storage/classic/socks5.txt"
        async for batch in self._iter_raw_list_source(
            url, protocol="socks5"
        ):
            yield batch

    async def get_proxy_from_fyvri_fresh_proxy_list_https(self) -> AsyncGenerator[list, None]:
        url = f"https://raw.githubusercontent.com/fyvri/fresh-proxy-list/archive/storage/classic/https.txt"
        async for batch in self._iter_raw_list_source(
            url, protocol="https"
        ):
            yield batch

    async def get_proxy_from_fyvri_fresh_proxy_list_http(self) -> AsyncGenerator[list, None]:
        url = f"https://raw.githubusercontent.com/fyvri/fresh-proxy-list/archive/storage/classic/http.txt"
        async for batch in self._iter_raw_list_source(
            url, protocol="http"
        ):
            yield batch

    async def get_proxy_from_BreakingTechFr_Proxy_Free_socks5(self) -> AsyncGenerator[list, None]:
        url = f"https://raw.githubusercontent.com/BreakingTechFr/Proxy_Free/refs/heads/main/proxies/socks5.txt"
        async for batch in self._iter_raw_list_source(
            url, protocol="socks5"
        ):
            yield batch

    async def get_proxy_from_BreakingTechFr_Proxy_Free_http(self) -> AsyncGenerator[list, None]:
        url = f"https://raw.githubusercontent.com/BreakingTechFr/Proxy_Free/refs/heads/main/proxies/http.txt"
        async for batch in self._iter_raw_list_source(
            url, protocol="http"
        ):
            yield batch

    async def get_proxy_from_zloi_user_hideip_me_socks5(self) -> AsyncGenerator[list, None]:
        url = f"https://raw.githubusercontent.com/zloi-user/hideip.me/refs/heads/master/socks5.txt"
        async for batch in self._iter_raw_list_source(
            url, protocol="socks5"
        ):
            yield batch

    async def get_proxy_from_zloi_user_hideip_me_https(self) -> AsyncGenerator[list, None]:
        url = f"https://raw.githubusercontent.com/zloi-user/hideip.me/refs/heads/master/https.txt"
        async for batch in self._iter_raw_list_source(
            url, protocol="https"
        ):
            yield batch

    async def get_proxy_from_zloi_user_hideip_me_http(self) -> AsyncGenerator[list, None]:
        url = f"https://raw.githubusercontent.com/zloi-user/hideip.me/refs/heads/master/http.txt"
        async for batch in self._iter_raw_list_source(
            url, protocol="http"
        ):
            yield batch

    async def get_proxy_from_zloi_user_hideip_me_connect(self) -> AsyncGenerator[list, None]:
        url = f"https://raw.githubusercontent.com/zloi-user/hideip.me/refs/heads/master/connect.txt"
        async for batch in self._iter_raw_list_source(
            url, protocol="http"
        ):
            yield batch

    async def get_proxy_from_trio666_proxy_checker(self) -> AsyncGenerator[list, None]:
        url = f"https://raw.githubusercontent.com/trio666/proxy-checker/refs/heads/main/all.txt"
        async for batch in self._iter_raw_list_source(
            url, protocol="http"
        ):
            yield batch

    async def get_proxy_from_MohammadHosseinkargar_proxy_https2(self) -> AsyncGenerator[list, None]:
        url = f"https://raw.githubusercontent.com/MohammadHosseinkargar/proxylist/refs/heads/main/https2.txt"
        async for batch in self._iter_raw_list_source(
            url, protocol="https"
        ):
            yield batch

    async def get_proxy_from_MohammadHosseinkargar_proxy_socks5_2(self) -> AsyncGenerator[list, None]:
        url = f"https://raw.githubusercontent.com/MohammadHosseinkargar/proxylist/refs/heads/main/socks5_2.txt"
        async for batch in self._iter_raw_list_source(
            url, protocol="socks5"
        ):
            yield batch

    async def get_proxy_from_MohammadHosseinkargar_proxy_http2(self) -> AsyncGenerator[list, None]:
        url = f"https://raw.githubusercontent.com/MohammadHosseinkargar/proxylist/refs/heads/main/http2.txt"
        async for batch in self._iter_raw_list_source(
            url, protocol="http"
        ):
            yield batch

    async def get_proxy_from_MohammadHosseinkargar_proxy_socks5(self) -> AsyncGenerator[list, None]:
        url = f"https://raw.githubusercontent.com/MohammadHosseinkargar/proxylist/refs/heads/main/socks5.txt"
        async for batch in self._iter_raw_list_source(
            url, protocol="socks5"
        ):
            yield batch

    async def get_proxy_from_MohammadHosseinkargar_proxy_https(self) -> AsyncGenerator[list, None]:
        url = f"https://raw.githubusercontent.com/MohammadHosseinkargar/proxylist/refs/heads/main/https.txt"
        async for batch in self._iter_raw_list_source(
            url, protocol="https"
        ):
            yield batch

    async def get_proxy_from_MohammadHosseinkargar_proxy_http(self) -> AsyncGenerator[list, None]:
        url = f"https://raw.githubusercontent.com/MohammadHosseinkargar/proxylist/refs/heads/main/http.txt"
        async for batch in self._iter_raw_list_source(
            url, protocol="http"
        ):
            yield batch

    async def get_proxy_from_nhan0o22_proxy(self) -> AsyncGenerator[list, None]:
        url = f"https://raw.githubusercontent.com/nhan0o22/proxy/refs/heads/master/proxy.txt"
        async for batch in self._iter_raw_list_source(
            url, protocol="http"
        ):
            yield batch

    async def get_proxy_from_parserpp_ip_ports(self) -> AsyncGenerator[list, None]:
        headers = {"user-agent": CONFIG.rand_ua}
        req = ""
        proxy_queue = []
        url = f"https://cdn.jsdelivr.net/gh/parserpp/ip_ports/proxyinfo.json"
        req = await my_async_httpx.get(
            url=url, headers=headers, verify=False, proxies=_github_proxy
        )
        if req:
            proxies = []
            for i in req.text.split("}"):
                if i.strip():
                    i_dict = json.loads(i + "}")
                    port = i_dict.get("port")
                    host = i_dict.get("host")
                    protocol = i_dict.get("type")
                    append_dict = format_proxy(
                        f"{protocol}://{host}:{port}", protocol=protocol
                    )
                    if not append_dict:
                        self.log.debug(f"代理格式错误！{i}")
                        continue
                    proxy_queue.append(append_dict)
            if len(proxy_queue) < 10:
                self.log.info(f"{req.text}, {url}")
            self.log.info(f"总共有{len(proxy_queue)}个代理需要检查")
        else:
            self.log.info(f"{req.text}, {url}")

        if proxy_queue:
            yield proxy_queue

    async def get_proxy_from_gitrecon1455_fresh_proxy_list(self) -> AsyncGenerator[list, None]:
        url = f"https://raw.githubusercontent.com/gitrecon1455/fresh-proxy-list/refs/heads/main/proxylist.txt"
        async for batch in self._iter_raw_list_source(
            url, protocol="http"
        ):
            yield batch

    async def get_proxy_from_api_openproxylist_xyz_https(self) -> AsyncGenerator[list, None]:
        url = f"https://api.openproxylist.xyz/http.txt"
        async for batch in self._iter_raw_list_source(
            url, protocol="https"
        ):
            yield batch

    async def get_proxy_from_dpangestuw_Free_Proxy(self) -> AsyncGenerator[list, None]:
        url = f"https://raw.githubusercontent.com/dpangestuw/Free-Proxy/refs/heads/main/All_proxies.txt"
        async for batch in self._iter_raw_list_source(
            url, protocol="http"
        ):
            yield batch

    async def get_proxy_from_casa_ls_proxy_list_socks5(self) -> AsyncGenerator[list, None]:
        url = f"https://raw.githubusercontent.com/casa-ls/proxy-list/refs/heads/main/socks5"
        async for batch in self._iter_raw_list_source(
            url, protocol="socks5"
        ):
            yield batch

    async def get_proxy_from_casa_ls_proxy_list_http(self) -> AsyncGenerator[list, None]:
        url = f"https://raw.githubusercontent.com/casa-ls/proxy-list/refs/heads/main/http"
        async for batch in self._iter_raw_list_source(
            url, protocol="http"
        ):
            yield batch

    async def get_proxy_from_r00tee_Proxy_List_socks5(self) -> AsyncGenerator[list, None]:
        url = f"https://raw.githubusercontent.com/r00tee/Proxy-List/main/Socks5.txt"
        async for batch in self._iter_raw_list_source(
            url, protocol="socks5"
        ):
            yield batch

    async def get_proxy_from_r00tee_Proxy_List(self) -> AsyncGenerator[list, None]:
        url = f"https://raw.githubusercontent.com/r00tee/Proxy-List/main/Https.txt"
        async for batch in self._iter_raw_list_source(
            url, protocol="https"
        ):
            yield batch

    async def get_proxy_from_themiralay_Proxy_List_World(self) -> AsyncGenerator[list, None]:
        url = f"https://raw.githubusercontent.com/themiralay/Proxy-List-World/refs/heads/master/data.txt"
        async for batch in self._iter_raw_list_source(
            url, protocol="http"
        ):
            yield batch

    async def get_proxy_from_lalifeier_proxy_scraper_https(self) -> AsyncGenerator[list, None]:
        url = f"https://raw.githubusercontent.com/lalifeier/proxy-scraper/main/proxies/https.txt"
        async for batch in self._iter_raw_list_source(
            url, protocol="https"
        ):
            yield batch

    async def get_proxy_from_lalifeier_proxy_scraper_http(self) -> AsyncGenerator[list, None]:
        url = f"https://raw.githubusercontent.com/lalifeier/proxy-scraper/main/proxies/http.txt"
        async for batch in self._iter_raw_list_source(
            url, protocol="http"
        ):
            yield batch

    async def get_proxy_from_claude89757_free_https_proxies(self) -> AsyncGenerator[list, None]:
        url = f"https://raw.githubusercontent.com/claude89757/free_https_proxies/refs/heads/main/free_https_proxies.txt"
        async for batch in self._iter_raw_list_source(
            url, protocol="https"
        ):
            yield batch

    async def get_proxy_from_Simatwa_free_proxies_http(self) -> AsyncGenerator[list, None]:
        headers = {"user-agent": CONFIG.rand_ua}
        req = ""
        proxy_queue = []
        url = f"https://raw.githubusercontent.com/Simatwa/free-proxies/master/files/http.json"
        req = await my_async_httpx.get(
            url=url, headers=headers, verify=False, proxies=_github_proxy
        )
        if req:
            proxies = []
            for i in req.json().get("proxies"):
                if i.strip():
                    append_dict = format_proxy(i)
                    if not append_dict:
                        self.log.debug(f"代理格式错误！{i}")
                        continue
                    proxy_queue.append(append_dict)
            if len(proxy_queue) < 10:
                self.log.info(f"{req.text}, {url}")
            self.log.info(f"总共有{len(proxy_queue)}个代理需要检查")
        else:
            self.log.info(f"{req.text}, {url}")

        if proxy_queue:
            yield proxy_queue

    # async def get_proxy_from_elliottophellia_proxylist_http(self) -> tuple[list, bool]:
    #     headers = {
    #         'user-agent': CONFIG.rand_ua
    #     }
    #     get_proxy_success = True
    #     req = ''
    #     proxy_queue = []
    #     url = f'https://cdn.rei.my.id/proxy/pHTTP'
    #     req = await my_async_httpx.get(
    #         url=url,
    #         headers=headers,
    #         verify=False,
    #
    #         proxies=_github_proxy
    #     )
    #     if req:
    #         proxies = []
    #         for i in req.text.split('\n'):
    #             if _ := i.strip():
    #                 append_dict = format_proxy(_)
    #                 if not append_dict:
    #                     self.log.debug(f'代理格式错误！{_}')
    #                     continue
    #                 proxy_queue.append(append_dict)
    #         if len(proxy_queue) < 10:
    #             self.log.info(f'{req.text}, {url}')
    #         self.log.info(f'总共有{len(proxy_queue)}个代理需要检查')
    #     else:
    #         self.log.info(f'{req.text}, {url}')
    #
    #         get_proxy_success = False
    #     return proxy_queue, get_proxy_success

    # async def get_proxy_from_elliottophellia_proxylist_socks5(self) -> tuple[list, bool]:
    #     headers = {
    #         'user-agent': CONFIG.rand_ua
    #     }
    #     get_proxy_success = True
    #     req = ''
    #     proxy_queue = []
    #     url = f'https://cdn.rei.my.id/proxy/pSOCKS5'
    #     req = await my_async_httpx.get(url=url, headers=headers, verify=False, )
    #     if req:
    #         proxies = []
    #         for i in req.text.split('\n'):
    #             if _ := i.strip():
    #                 append_dict = format_proxy(_, protocol='socks5')
    #                 if not append_dict:
    #                     self.log.debug(f'代理格式错误！{_}')
    #                     continue
    #                 proxy_queue.append(append_dict)
    #         if len(proxy_queue) < 10:
    #             self.log.info(f'{req.text}, {url}')
    #         self.log.info(f'总共有{len(proxy_queue)}个代理需要检查')
    #     else:
    #         self.log.info(f'{req.text}, {url}')
    #         get_proxy_success = False
    #     return proxy_queue, get_proxy_success

    async def get_proxy_from_officialputuid_KangProxy_KangProxy_https(self) -> AsyncGenerator[list, None]:
        url = f"https://raw.githubusercontent.com/officialputuid/KangProxy/KangProxy/https/https.txt"
        async for batch in self._iter_raw_list_source(
            url, protocol="https"
        ):
            yield batch

    async def get_proxy_from_officialputuid_KangProxy_KangProxy_http(self) -> AsyncGenerator[list, None]:
        url = f"https://raw.githubusercontent.com/officialputuid/KangProxy/KangProxy/http/http.txt"
        async for batch in self._iter_raw_list_source(
            url, protocol="http"
        ):
            yield batch

    async def get_proxy_from_MuRongPIG_Proxy_Master(self) -> AsyncGenerator[list, None]:
        url = f"https://raw.githubusercontent.com/MuRongPIG/Proxy-Master/main/http.txt"
        async for batch in self._iter_raw_list_source(
            url, protocol="http"
        ):
            yield batch

    async def get_proxy_from_roosterkid_openproxylist_main_HTTPS_RAW(self) -> AsyncGenerator[list, None]:
        url = f"https://raw.githubusercontent.com/roosterkid/openproxylist/main/HTTPS_RAW.txt"
        async for batch in self._iter_raw_list_source(
            url, protocol="http"
        ):
            yield batch

    async def get_proxy_from_proxy_casals_ar_main_http(self) -> AsyncGenerator[list, None]:
        url = f"https://raw.githubusercontent.com/casals-ar/proxy.casals.ar/main/http"
        async for batch in self._iter_raw_list_source(
            url, protocol="http"
        ):
            yield batch

    async def get_proxy_from_Zaeem20_FREE_PROXIES_LIST_master_socks5(self) -> AsyncGenerator[list, None]:
        url = f"https://raw.githubusercontent.com/Zaeem20/FREE_PROXIES_LIST/master/socks5.txt"
        async for batch in self._iter_raw_list_source(
            url, protocol="socks5"
        ):
            yield batch

    async def get_proxy_from_Zaeem20_FREE_PROXIES_LIST_master_http(self) -> AsyncGenerator[list, None]:
        url = f"https://raw.githubusercontent.com/Zaeem20/FREE_PROXIES_LIST/master/http.txt"
        async for batch in self._iter_raw_list_source(
            url, protocol="http"
        ):
            yield batch

    async def get_proxy_from_Zaeem20_FREE_PROXIES_LIST_master_https(self) -> AsyncGenerator[list, None]:
        url = f"https://raw.githubusercontent.com/Zaeem20/FREE_PROXIES_LIST/master/https.txt"
        async for batch in self._iter_raw_list_source(
            url, protocol="https"
        ):
            yield batch

    async def get_proxy_from_TheSpeedX_PROXY_List_master_http(self) -> AsyncGenerator[list, None]:
        url = f"https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt"
        async for batch in self._iter_raw_list_source(
            url, protocol="http"
        ):
            yield batch

    async def get_proxy_from_yemixzy_proxy_list_main_proxies_http(self) -> AsyncGenerator[list, None]:
        url = f"https://raw.githubusercontent.com/yemixzy/proxy-list/main/proxies/http.txt"
        async for batch in self._iter_raw_list_source(
            url, protocol="http"
        ):
            yield batch

    async def get_proxy_from_Free_Proxies_blob_main_proxy_files_http_proxies(self) -> AsyncGenerator[list, None]:
        url = f"https://raw.githubusercontent.com/Anonym0usWork1221/Free-Proxies/main/proxy_files/http_proxies.txt"
        async for batch in self._iter_raw_list_source(
            url, protocol="http"
        ):
            yield batch

    async def get_proxy_from_Free_Proxies_blob_main_proxy_files_https_proxies(self) -> AsyncGenerator[list, None]:
        url = f"https://raw.githubusercontent.com/Anonym0usWork1221/Free-Proxies/main/proxy_files/https_proxies.txt"
        async for batch in self._iter_raw_list_source(
            url, protocol="http"
        ):
            yield batch

    async def get_proxy_from_proxifly_free_proxy_list_main_proxies_protocols_http_data(self) -> AsyncGenerator[list, None]:
        url = f"https://raw.githubusercontent.com/proxifly/free-proxy-list/main/proxies/protocols/http/data.txt"
        async for batch in self._iter_raw_list_source(
            url, protocol="http"
        ):
            yield batch

    async def get_proxy_from_sarperavci_freeCheckedHttpProxies(self) -> AsyncGenerator[list, None]:
        url = f"https://raw.githubusercontent.com/sarperavci/freeCheckedHttpProxies/main/freshHttpProxies.txt"
        async for batch in self._iter_raw_list_source(
            url, protocol="http"
        ):
            yield batch

    async def get_proxy_from_prxchk_proxy_list(self) -> AsyncGenerator[list, None]:
        url = f"https://raw.githubusercontent.com/prxchk/proxy-list/main/http.txt"
        async for batch in self._iter_raw_list_source(
            url, protocol="http"
        ):
            yield batch

    async def get_proxy_from_andigwandi_free_proxy(self) -> AsyncGenerator[list, None]:
        url = f"https://raw.githubusercontent.com/andigwandi/free-proxy/main/proxy_list.txt"
        async for batch in self._iter_raw_list_source(
            url, protocol="http"
        ):
            yield batch

    async def get_proxy_from_elliottophellia_yakumo(self) -> AsyncGenerator[list, None]:
        url = f"https://raw.githubusercontent.com/elliottophellia/yakumo/master/results/http/global/http_checked.txt"
        async for batch in self._iter_raw_list_source(
            url, protocol="http"
        ):
            yield batch

    async def get_proxy_from_im_razvan_proxy_list(self) -> AsyncGenerator[list, None]:
        url = f"https://raw.githubusercontent.com/im-razvan/proxy_list/main/http.txt"
        async for batch in self._iter_raw_list_source(
            url, protocol="http"
        ):
            yield batch

    async def get_proxy_from_proxy4parsing_proxy_list(self) -> AsyncGenerator[list, None]:
        url = f"https://raw.githubusercontent.com/proxy4parsing/proxy-list/main/http.txt"
        async for batch in self._iter_raw_list_source(
            url, protocol="http"
        ):
            yield batch

    async def get_proxy_from_mmpx12_proxy_list(self) -> AsyncGenerator[list, None]:
        url = f"https://raw.githubusercontent.com/mmpx12/proxy-list/master/http.txt"
        async for batch in self._iter_raw_list_source(
            url, protocol="http"
        ):
            yield batch

    # endregion

    # region json格式代理（每个函数的json响应可能都不一样，要换里面解析json的方式）
    async def get_proxy_from_kgtools(self) -> AsyncGenerator[list, None]:
        headers = {
            "User-Agent": CONFIG.rand_ua,
        }
        req = ""
        proxy_queue = []
        url = "https://www.kgtools.cn/api/proxy/ops/list/"
        for pn in range(1, self.get_proxy_page + 1):
            params = {"label": 1, "name": "全国代理ip", "page": pn}
            req = await my_async_httpx.get(
                url=url,
                headers=headers,
                params=params,
                verify=False,
            )
            if req:
                req_dict = req.json()
                http_p = req_dict.get("data", {}).get("data")
                if not http_p:
                    break
                for da in http_p:
                    ip = da.get("proxy_ip")
                    port = da.get("port")
                    protocol_type = da.get("type")
                    if protocol_type == "HTTP":
                        protocol = "http"
                    elif protocol_type == "HTTPS":
                        protocol = "https"
                    elif protocol_type == "SOCKS5":
                        protocol = "socks5"
                    else:
                        protocol = "http"
                    proxy_queue.append(format_proxy(f"{ip}:{port}", protocol=protocol))
                self.log.info(f"总共有{len(proxy_queue)}个代理需要检查")
            else:
                self.log.info(f"{req.text}, {url}")
                break
        if proxy_queue:
            yield proxy_queue

    async def get_proxy_from_lumiproxy(self) -> AsyncGenerator[list, None]:
        headers = {
            "User-Agent": CONFIG.rand_ua,
        }
        req = ""
        proxy_queue = []
        url = "https://api.lumiproxy.com/web_v1/free-proxy/list?page_size=6000&page=1&language=zh-hans"
        req = await my_async_httpx.get(
            url=url,
            headers=headers,
            verify=False,
        )
        if req:
            req_dict = req.json()
            http_p = req_dict.get("data").get("list")
            for da in http_p:
                ip = da.get("ip")
                port = da.get("port")
                protocol_type = da.get("protocol")
                if protocol_type == 1:
                    protocol = "http"
                elif protocol_type == 2:
                    protocol = "https"
                elif protocol_type == 4:
                    protocol = "socks4"
                elif protocol_type == 8:
                    protocol = "socks5"
                else:
                    protocol = "http"
                proxy_queue.append(format_proxy(f"{ip}:{port}", protocol=protocol))
            if len(proxy_queue) < 10:
                self.log.info(f"{req.text}, {url}")
            self.log.info(f"总共有{len(proxy_queue)}个代理需要检查")
        else:
            self.log.info(f"{req.text}, {url}")
        if proxy_queue:
            yield proxy_queue

    async def get_proxy_from_proxylist_geonode_com(self) -> AsyncGenerator[list, None]:
        headers = {
            "User-Agent": CONFIG.rand_ua,
        }
        req = ""
        proxy_queue = []
        url = f"https://proxylist.geonode.com/api/proxy-list"
        limit = 500
        for page in range(1, self.get_proxy_page + 1):
            params = {
                "limit": limit,
                "page": page,
                "sort_by": "lastChecked",
                "sort_type": "desc",
            }
            req = await my_async_httpx.get(
                url=url,
                params=params,
                headers=headers,
                verify=False,
            )
            if req:
                req_dict = req.json()
                http_p = req_dict.get("data")
                total_count = req_dict.get("total")
                for da in http_p:
                    ip = da.get("ip")
                    port = da.get("port")
                    protocols = da.get("protocols")
                    for protocol in protocols:
                        if ip and port and protocol:
                            proxy_queue.append(
                                format_proxy(
                                    f"{protocol}://{ip}:{port}", protocol=protocol
                                )
                            )
                if len(proxy_queue) < 10:
                    self.log.info(f"{req.text}, {url}")
                self.log.info(f"总共有{len(proxy_queue)}个代理需要检查")
            else:
                self.log.info(f"{req.text}, {url}")
                break
            if page * limit >= total_count:
                break
        if proxy_queue:
            yield proxy_queue

    async def get_proxy_from_proxydb_net(self) -> AsyncGenerator[list, None]:
        headers = {
            "User-Agent": CONFIG.rand_ua,
        }
        req = ""
        proxy_queue = []
        for pn in range(0, self.get_proxy_page):
            offset = 30 * pn
            url = f"https://proxydb.net/?country=&offset={offset}&protocol=http&protocol=https"
            req = await my_async_httpx.get(
                url=url,
                headers=headers,
                verify=False,
            )
            if req:
                html = bs4.BeautifulSoup(req.text, "html.parser")
                td = html.select("tr>td")
                for i in range(len(td) // 9):
                    proxy_str = (
                        td[i * 9].text.strip()
                        + ":"
                        + td[i * 9 + 1].text.strip().split("\n")[0]
                    )
                    if "HTTP" in td[i * 9 + 2].text:
                        proxy_queue.append(format_proxy(proxy_str, protocol="http"))
                    elif "HTTPS" in td[i * 9 + 2].text:
                        proxy_queue.append(format_proxy(proxy_str, protocol="https"))
                if len(proxy_queue) < 10:
                    self.log.info(f"{req.text}, {url}")
                self.log.info(f"总共有{len(proxy_queue)}个代理需要检查")
            else:
                self.log.info(f"{req.text}, {url}")
                break
        if proxy_queue:
            yield proxy_queue

    async def get_proxy_from_proxyshare(self) -> AsyncGenerator[list, None]:
        headers = {
            "User-Agent": CONFIG.rand_ua,
        }
        req = ""
        proxy_queue = []
        url = f"https://www.proxyshare.com/detection/proxyList?limit=500&page=1&sort_by=lastChecked&sort_type=desc&protocols=http"
        req = await my_async_httpx.get(
            url=url,
            headers=headers,
            verify=False,
        )
        if req:
            req_dict = req.json()
            http_p = req_dict.get("data")
            for da in http_p:
                if ip := da.get("ip"):
                    if port := da.get("port"):
                        proxy_queue.append(
                            {
                                "http": f"http://{ip}:{port}",
                                "https": f"http://{ip}:{port}",
                            }
                        )
            if len(proxy_queue) < 10:
                self.log.info(f"{req.text}, {url}")
            self.log.info(f"总共有{len(proxy_queue)}个代理需要检查")
        else:
            self.log.info(f"{req.text}, {url}")

        if proxy_queue:
            yield proxy_queue

    async def get_proxy_from_t0mer_free_proxies(self) -> AsyncGenerator[list, None]:
        headers = {
            "User-Agent": CONFIG.rand_ua,
        }
        req = ""
        proxy_queue = []
        url = f"https://raw.githubusercontent.com/t0mer/free-proxies/main/proxies.json"
        req = await my_async_httpx.get(
            url=url, headers=headers, verify=False, proxies=_github_proxy
        )
        if req:
            req_dict = json.loads(
                req.text.replace("None", "null")
                .replace("False", "false")
                .replace("True", "true")
            )
            http_p = req_dict.get("http")
            for i in list(http_p.keys()):
                proxy_queue.append({"http": f"http://{i}", "https": f"http://{i}"})
            if len(proxy_queue) < 10:
                self.log.info(f"{req.text}, {url}")
            self.log.info(f"总共有{len(proxy_queue)}个代理需要检查")
        else:
            self.log.info(f"{req.text}, {url}")

        if proxy_queue:
            yield proxy_queue

    # endregion

    async def get_proxy_from_supplement_sources(self) -> AsyncGenerator[list, None]:
        """补充源（数据驱动，见 ``SUPPLEMENT_SOURCES``）：GitHub 镜像列表 + 公共 API。

        与其它 ``get_proxy_from_*`` 一样返回 ``(代理字典列表, 是否成功)``，
        由 ``__get_proxy`` 统一收集去重。各源并行抓取、单源超时
        ``_SUPPLEMENT_SOURCE_TIMEOUT``，单源失败不影响其它源。
        """
        headers = {"user-agent": CONFIG.rand_ua}
        semaphore = asyncio.Semaphore(_SUPPLEMENT_SOURCE_CONCURRENCY)

        async def _fetch_one(spec: ProxySourceSpec) -> list[dict]:
            source_proxies = _github_proxy if is_github_source(spec.url) else None
            async with semaphore:
                try:
                    async with asyncio.timeout(_SUPPLEMENT_SOURCE_TIMEOUT):
                        req = await my_async_httpx.get(
                            url=spec.url,
                            headers=headers,
                            verify=False,
                            proxies=source_proxies,
                            timeout=_SUPPLEMENT_SOURCE_TIMEOUT,
                        )
                except Exception as e:  # noqa: BLE001 单源失败只丢这一份数据
                    self.log.debug(f"补充源 {spec.name} 请求失败：{e!r}")
                    return []
            if not req:
                self.log.debug(f"补充源 {spec.name} 无数据")
                return []
            proxies = parse_supplement_payload(spec, req.text)
            self.log.debug(f"补充源 {spec.name} 解析到 {len(proxies)} 条")
            return proxies

        results = await asyncio_gather(
            *(_fetch_one(spec) for spec in SUPPLEMENT_SOURCES), log=None
        )
        proxy_queue: list[dict] = []
        for result in results:
            if isinstance(result, Exception) or not result:
                continue
            proxy_queue.extend(result)
        self.log.info(
            f"补充源共解析到 {len(proxy_queue)} 个代理"
            f"（源数 {len(SUPPLEMENT_SOURCES)}）"
        )
        if proxy_queue:
            yield proxy_queue

    # endregion
    # region 获取代理主函数
    async def _pump_source(
        self,
        fn,
        queue: "asyncio.Queue[list[dict] | None]",
        semaphore: asyncio.Semaphore,
    ) -> None:
        """跑一个源，把它的每一批代理塞进队列；无论成功失败最后都塞一个 None 表示收尾。"""
        try:
            async with semaphore:
                async with asyncio.timeout(_SOURCE_TASK_TIMEOUT):
                    async for batch in fn():
                        if batch:
                            await queue.put(batch)
        except Exception as e:  # noqa: BLE001 单源失败不影响其它源
            self.log.debug(f"代理源任务失败：{e!r}")
        finally:
            await queue.put(None)

    async def _iter_raw_list_source(
        self,
        url: str,
        protocol: str = "http",
        batch_size: int = _INGEST_CHUNK_SIZE,
    ) -> AsyncGenerator[list, None]:
        """流式读取「一行一个代理」的源：边下边解析，凑满一批就 yield 一批。

        内存峰值只有一批（默认 2000 条），与源文件大小无关；
        大文件（50 万行 / 5MB）也不会在内存里攒成完整 list。
        """
        headers = {"user-agent": CONFIG.rand_ua}
        source_proxies = _github_proxy if is_github_source(url) else None
        batch: list[dict] = []
        total = 0
        async for line in my_async_httpx.iter_lines(
            url, headers=headers, proxies=source_proxies
        ):
            proxy_text = extract_proxy_text(line)
            if not proxy_text:
                continue
            proxy_dict = format_proxy(proxy_text, protocol=protocol)
            if not proxy_dict:
                continue
            batch.append(proxy_dict)
            total += 1
            if len(batch) >= batch_size:
                yield batch
                batch = []
        if batch:
            yield batch
        self.log.info(f"{url} 共解析到 {total} 个代理")

    async def __get_proxy(self):
        if (
            self.GetProxy_Flag
            or time.time() - self.get_proxy_timestamp < self.get_proxy_sep_time
        ):
            return
        async with self._lock:
            if (
                self.GetProxy_Flag
                or time.time() - self.get_proxy_timestamp < self.get_proxy_sep_time
            ):
                self.log.info(
                    f"获取代理时间过短！返回！（冷却剩余：{self.get_proxy_sep_time - (int(time.time() - self.get_proxy_timestamp))}）"
                )
                return
            else:
                self.get_proxy_timestamp = time.time()
                self.GetProxy_Flag = True
        # 常规的周期性获取代理，属正常流程，用 debug 记录即可，
        # 避免污染 CRITICAL 级别（只应保留真正的严重故障）。
        self.log.debug(
            f"开始获取代理\t上次获取代理时间：{datetime.fromtimestamp(self.get_proxy_timestamp)}\t{datetime.now()}"
        )
        funcs = inspect.getmembers(
            get_proxy_methods, predicate=inspect.isasyncgenfunction
        )
        queue: asyncio.Queue[list[dict] | None] = asyncio.Queue()
        semaphore = asyncio.Semaphore(_SOURCE_CONCURRENCY)
        tasks = [
            asyncio.create_task(self._pump_source(fn, queue, semaphore))
            for name, fn in funcs
            if name.startswith("get_proxy_from")
        ]
        # 边下边解析边入库：每个源按批塞进队列，处理完一批即释放，
        # 内存峰值 = 一批（2000 条）+ 去重集合，与源文件大小无关。
        seen: set[str] = set()
        finished = 0
        while finished < len(tasks):
            batch = await queue.get()
            if batch is None:
                finished += 1
                if finished % 20 == 0:
                    self.log.info(
                        f"代理抓取进度 {finished}/{len(tasks)}："
                        f"去重后累计 {len(seen)} 条，已入库 {self._round_stats.inserted} 条"
                    )
                continue
            self._round_stats.raw_total += len(batch)
            deduped = self._dedup_batch(batch, seen)
            if deduped:
                await self._ingest_proxies(deduped)
        self._round_stats.dedup_total = len(seen)
        self.proxy_list = []
        if not seen:
            self.log.warning("警告：未能获取到任何代理，proxy_list为空")
        else:
            self.log.info(
                f"本轮去重后共 {len(seen)} 个代理，已边收边存完成"
                f"（新增 {self._round_stats.inserted} / 已存在 {self._round_stats.existed}）"
            )

    @staticmethod
    def _dedup_batch(proxies: list[dict], seen: set[str]) -> list[dict]:
        """把单个源返回的代理去重后放入批次（``seen`` 跨源共享）。"""
        batch: list[dict] = []
        for proxy_dict in proxies:
            if not proxy_dict:
                continue
            proxy_key = list(proxy_dict.values())[0]
            if proxy_key in seen:
                continue
            seen.add(proxy_key)
            batch.append(proxy_dict)
        return batch

    async def _ingest_proxies(self, proxy_dicts: list[dict]) -> None:
        """把一批候选代理预筛后限并发入库，处理完一块即可释放这块数据。"""
        if not proxy_dicts:
            return
        semaphore = asyncio.Semaphore(_INGEST_CONCURRENCY)
        for start in range(0, len(proxy_dicts), _INGEST_CHUNK_SIZE):
            chunk = proxy_dicts[start : start + _INGEST_CHUNK_SIZE]
            lookup = await SQLHelper.lookup_proxy_keys(
                [
                    key
                    for proxy_dict in chunk
                    if (key := to_proxy_key(proxy_dict))
                ]
            )

            async def _one(
                proxy_dict: dict, chunk_lookup: ProxyKeyLookup = lookup
            ) -> Exception | None:
                async with semaphore:
                    try:
                        # 源方法返回的是裸代理字典（{"http": ..., "https": ...}），
                        # 入库需要 proxy/status/update_ts/score 这套结构
                        await self._add_to_proxy_list(
                            self._proxy_warrper(proxy_dict), lookup=chunk_lookup
                        )
                    except Exception as e:  # noqa: BLE001 交由调用方统计，避免静默丢批
                        return e
                return None

            results = await asyncio_gather(
                *(_one(proxy_dict) for proxy_dict in chunk), log=None
            )
            # asyncio_gather 内部会把异常吞成 None，这里靠 _one 自己把异常返回来统计
            failures = [r for r in results if isinstance(r, Exception)]
            if failures:
                self._round_stats.ingest_failures += len(failures)
                self.log.warning(
                    f"本批 {len(chunk)} 条代理入库失败 {len(failures)} 条：{failures[0]!r}"
                )

    async def get_proxy(self):
        task = asyncio.create_task(self.__get_proxy())
        task.add_done_callback(lambda _: self.set_GetProxy_Flag(False))
        await task

    def set_GetProxy_Flag(self, boolean: bool = False):
        self.GetProxy_Flag = boolean

    # endregion
    def _proxy_warrper(self, proxy, status=0, score=50):
        return {
            "proxy": proxy,
            "status": status,
            "update_ts": int(time.time()),
            "score": score,
        }

    async def _add_to_proxy_list(
        self, proxy_dict: dict, lookup: ProxyKeyLookup | None = None
    ):
        """新增代理入库（已存在的只统计，不再写任何缓存）。

        Redis 池废弃后，抓取轮次只负责「把源站列表落进 MySQL」；
        取代理时直接随机查 MySQL，所以这里不需要再维护新鲜度池。
        已被业务请求判死的代理（status!=0 或 success_times<0）只计入 existed，
        不会因为源站再次列出就复活——复活由 ``refresh_proxy()`` 的冷却逻辑负责。

        存在性判断用调用方传入的批次预筛结果（``lookup``，内存 set）；
        没有时回退到 ``self.proxy_key_lookup``，再没有才单条查库。
        :param proxy_dict: 代理字典
        :param lookup: 该批次的批量预筛结果
        :return:
        """
        now = int(time.time())
        proxy_key = to_proxy_key(proxy_dict["proxy"])
        if not proxy_key:
            return
        lookup = lookup or self.proxy_key_lookup
        if lookup is not None:
            is_new = lookup.is_new(proxy_key)
            is_alive = lookup.is_alive(proxy_key)
        else:
            existing = await SQLHelper.get_proxy_by_key(proxy_key)
            is_new = existing is None
            is_alive = (
                existing is not None
                and existing.status == 0
                and not is_zombie_success_times(existing.success_times)
            )
        if is_new:
            proxy_dict.update({"add_ts": now})
            proxy_tab = ProxyTab(**proxy_dict)
            await SQLHelper.add_to_proxy_tab_database(proxy_tab)
            self._round_stats.inserted += 1
            return
        self._round_stats.existed += 1
        if not is_alive:
            # 已被真实业务请求判死：不做任何处理（取代理时会按条件过滤掉）
            self._round_stats.dead_skipped += 1


get_proxy_methods = GetProxyMethods()

if __name__ == "__main__":
    asyncio.run(get_proxy_methods.main())
