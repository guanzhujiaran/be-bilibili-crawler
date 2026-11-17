import asyncio

from log.base_log import sql_log
from Utils.代理.mdoel.ProxyTier import ProxyTier
from Utils.代理.mdoel.request_context import current_proxy_tier
from Utils.代理.redisProxyRequest.GetProxyFromNet import get_proxy_methods
from Utils.代理.数据库操作.SqlAlcheyObj.ProxyModel import ProxyTab
from Utils.代理.数据库操作.async_proxy_op_alchemy_mysql_ver import (
    SQLHelper,
    SelectionStats,
)

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
    """按档位从 MySQL 随机取一条代理（兼容旧调用签名）。"""
    proxy_tab, _ = await get_available_proxy_detailed(tier=tier, exclude=exclude)
    return proxy_tab, False


async def get_available_proxy_detailed(
    tier: ProxyTier | None = None,
    exclude: frozenset[str] | set[str] | None = None,
) -> tuple[ProxyTab | None, SelectionStats]:
    """按档位从 MySQL 随机取一条代理，并返回选择过程的诊断计数。

    取代理只有一条路径：``proxy_tab`` 随机（随机主键起点，实测 ~15ms）。
    slow 档（爬虫默认车道）默认随机取一条 status 可用的就用：地址冷却**优先规避**
    （412/352 保护，全在冷却时回退，绝不空池），EWMA / 段级冷却 / 段级在途 / dead
    只记账与排序（见 ``ProxyHealthConfig.slow_skip_*`` 开关）。因此返回 None 的语义
    只剩「库里真的没有 status 可用的行，或候选全被本任务的失败代理 / 前缀排除」；
    fast 档仍保留严格准入，需要快速失败时显式启用。
    Redis 代理池（zset_bili_proxy / zset_bili_proxy_fresh）已于 2026-11-10 废弃。

    :param tier: 代理档位；None = 跟随当前上下文的档位（无上下文默认 slow）
    :param exclude: 本次任务内已失败的代理串（换代理用）
    """
    effective_tier = tier or current_proxy_tier()
    try:
        proxy_tab, stats = await SQLHelper.select_proxy_detailed(
            "rand", tier=effective_tier, exclude=exclude
        )
    except Exception as e:  # noqa: BLE001 取代理失败不能直接把异常抛进请求路径
        sql_log.exception(f"从 MySQL 随机取代理失败：{type(e).__name__} {e}")
        proxy_tab, stats = None, SelectionStats(tier=effective_tier.value)
    if proxy_tab:
        return proxy_tab, stats
    # slow 档的冷却/EWMA 都会回退，所以走到这里只可能是：池子真空（没有 status 可用行），
    # 或候选全被本任务内失败的代理 / 前缀排除。
    sql_log.debug(
        f"代理池没有 status 可用的行（tier={effective_tier.value}，"
        f"已排除 {len(exclude or ())} 个本任务失败代理）：{stats.to_log_str()}，"
        "已触发后台补池"
    )
    _trigger_proxy_fetch()
    return None, stats


if __name__ == "__main__":
    result = asyncio.run(get_available_proxy(tier=ProxyTier.FAST))
    print(str(result[0]))
