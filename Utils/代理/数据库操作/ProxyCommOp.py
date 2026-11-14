import asyncio
from typing import Optional

from log.base_log import sql_log
from Utils.代理.mdoel.ProxyTier import ProxyTier
from Utils.代理.mdoel.request_context import current_proxy_tier
from Utils.代理.redisProxyRequest.GetProxyFromNet import get_proxy_methods
from Utils.代理.数据库操作.SqlAlcheyObj.ProxyModel import ProxyTab
from Utils.代理.数据库操作.async_proxy_op_alchemy_mysql_ver import SQLHelper

# 一轮抓取（拉取 + 入库 + 去重）动辄几十秒到几分钟，绝不能挂在业务请求路径上。
# 这里用「后台任务 + 单飞」把补充代理的动作挪出 worker：请求侧取不到代理就快速失败，
# 由爬虫的延迟重入队来等下一轮，而不是让 worker 在信号量里 sleep 等待抓取完成。
_fetch_task: asyncio.Task | None = None


async def _run_proxy_fetch() -> None:
    try:
        await get_proxy_methods.main()
    except Exception as e:  # noqa: BLE001 后台补池失败只记日志，不能影响请求侧
        sql_log.exception(f"后台补充代理池失败：{type(e).__name__} {e}")


def _trigger_proxy_fetch() -> None:
    """触发一轮后台抓取（已在跑就复用，避免并发抓同一批源）。"""
    global _fetch_task
    if _fetch_task is not None and not _fetch_task.done():
        return
    try:
        _fetch_task = asyncio.create_task(_run_proxy_fetch())
    except RuntimeError as e:  # 没有运行中的事件循环（脚本 / 同步入口）
        sql_log.warning(f"没有可用事件循环，跳过后台补池：{e}")


async def get_available_proxy(
    tier: ProxyTier | None = None,
    exclude: frozenset[str] | set[str] | None = None,
) -> tuple[ProxyTab | None, bool]:
    """按档位从 MySQL 随机取一条代理；取不到就触发后台补池并立即返回 None。

    取代理只有一条路径：``proxy_tab`` 随机（随机主键起点，实测 ~15ms），
    健康状态（冷却 / EWMA / 在途并发）由 ``select_proxy`` 在候选批里筛选。
    Redis 代理池（zset_bili_proxy / zset_bili_proxy_fresh）已于 2026-11-10 废弃。

    :param tier: 代理档位；None = 跟随当前爬虫上下文（默认 fast）
    :param exclude: 本次任务内已失败的代理串（换代理用）
    :return: (代理行, 是否来自 available_proxy)；第二个值恒为 False，仅为兼容旧调用签名。
    """
    effective_tier = tier or current_proxy_tier()
    try:
        proxy_tab: Optional[ProxyTab] = await SQLHelper.select_proxy(
            "rand", tier=effective_tier, exclude=exclude
        )
    except Exception as e:  # noqa: BLE001 取代理失败不能直接把异常抛进请求路径
        sql_log.exception(f"从 MySQL 随机取代理失败：{type(e).__name__} {e}")
        proxy_tab = None
    if proxy_tab:
        return proxy_tab, False
    sql_log.debug(f"没有符合条件的代理（tier={effective_tier.value}），已触发后台补池")
    _trigger_proxy_fetch()
    return None, False


if __name__ == "__main__":
    result = asyncio.run(get_available_proxy(tier=ProxyTier.FAST))
    print(str(result[0]))
