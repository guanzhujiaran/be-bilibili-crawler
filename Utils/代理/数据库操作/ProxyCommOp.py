import asyncio
from typing import Optional

from log.base_log import sql_log
from Utils.代理.redisProxyRequest.GetProxyFromNet import get_proxy_methods
from Utils.代理.数据库操作.SqlAlcheyObj.ProxyModel import ProxyTab
from Utils.代理.数据库操作.async_proxy_op_alchemy_mysql_ver import SQLHelper


async def get_available_proxy(
    is_use_available_proxy: bool = False,
    initial_retry_delay_seconds: int = 10,  # 池子空时的重试间隔（秒）
) -> tuple[ProxyTab | None, bool]:
    """从 MySQL 随机取一条可用代理；取不到就抓一轮新代理再重试。

    Redis 代理池（zset_bili_proxy / zset_bili_proxy_fresh）已于 2026-11-10 废弃，
    取代理只有一条路径：``proxy_tab`` 里随机（随机主键起点，实测 ~15ms/次）。

    :param is_use_available_proxy: 仅为兼容旧调用签名保留。
        available_proxy 表现在只做统计，不再参与取代理。
    :param initial_retry_delay_seconds: 没取到代理时，抓取一轮后的等待秒数。
    :return: (代理行, 是否来自 available_proxy)；第二个值恒为 False。
    """
    for attempt in range(4):
        try:
            proxy_tab: Optional[ProxyTab] = await SQLHelper.select_proxy("rand")
        except Exception as e:
            sql_log.exception(f"Attempt {attempt}: 从 MySQL 随机取代理失败：{e}")
            proxy_tab = None
        if proxy_tab:
            return proxy_tab, False
        sql_log.debug(f"Attempt {attempt}: 没有可用代理，触发一轮抓取...")
        try:
            await get_proxy_methods.main()
        except Exception as e:
            sql_log.exception(f"Attempt {attempt}: 抓取新代理失败：{e}")
        await asyncio.sleep(initial_retry_delay_seconds)
    return await SQLHelper.select_proxy("rand"), False


if __name__ == "__main__":
    result = asyncio.run(get_available_proxy(is_use_available_proxy=True))
    print(str(result[0]))
