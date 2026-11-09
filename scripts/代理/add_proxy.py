"""手动把单个源抓到的代理入库（调试用）。

源方法现在是 async generator（流式、按批产出），入库统一走 `_ingest_proxies`
（内部做批量预筛 + 包装成 proxy/status/update_ts/score）。
"""

import asyncio

from Utils.代理.redisProxyRequest.GetProxyFromNet import get_proxy_methods


async def upsert_proxy():
    get_proxy_methods.get_proxy_page = 10
    async for batch in get_proxy_methods.get_proxy_from_saisuiu_Lionkings_Http_Proxys_Proxies():
        print(f"本批 {len(batch)} 条")
        await get_proxy_methods._ingest_proxies(batch)


if __name__ == "__main__":
    asyncio.run(upsert_proxy())
