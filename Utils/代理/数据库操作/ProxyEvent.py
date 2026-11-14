from Utils.代理.数据库操作.ProxyHealth import (
    ProxyFailureKind,
    proxy_health_store,
)
from Utils.代理.数据库操作.SqlAlcheyObj.ProxyModel import ProxyTab
from Utils.代理.数据库操作.async_proxy_op_alchemy_mysql_ver import (
    SQLHelper,
    to_proxy_key,
)
from log.base_log import sql_log

# 代理事件的两个副作用：
# 1. proxy_tab 的 status / score / success_times（保留原有语义，读写方很多，不能改含义）；
# 2. Redis 健康状态机（连续失败 / EWMA / 冷却），见 ProxyHealth.py。
# available_proxy 表已于 2026-11-10 废弃（取代理改为 MySQL 随机），这里不再写它。
#
# 口径（2026-11-14 起）：**判定看前缀**（IPv6 /64、IPv4 /24），地址级明细只留作日志。
# 自建 IPv6 出口（PROXY_SERVER）不再豁免记账：它是一条固定出口，
# 以前被伪造成 score=10000/success_times=10000，导致永不降权、永不冷却。


def _pool_proxy_key(proxy_tab: ProxyTab | None) -> str:
    """代理事件的健康明细 key（地址级）。

    自建出口同样返回代理串：地址级只作日志明细，真正生效的是它所在的 /64 前缀。
    MySQL 侧的 ``update_to_proxy_list`` 对自建出口仍是 no-op（它不是 proxy_tab 成员），
    所以这里放开记账不会往库里写配置项。
    """
    if proxy_tab is None:
        return ""
    return to_proxy_key(proxy_tab.proxy) or ""


async def handle_proxy_succ(proxy_tab: ProxyTab | None):
    """请求成功：分数 +10，status 归零，健康状态回到 hot 并清冷却。"""
    if not proxy_tab:
        sql_log.warning("handle_proxy_succ called with None proxy_tab.")
        return
    proxy_tab.status = 0
    await SQLHelper.update_to_proxy_list(proxy_tab, change_score_num=10)
    key = _pool_proxy_key(proxy_tab)
    if key:
        await proxy_health_store.record_success(key)


async def handle_proxy_352(proxy_tab: ProxyTab | None):
    """-352（风控挑战）：按临时状态处理，分数仍 +10，健康侧按**前缀级**风控长冷却。"""
    if not proxy_tab:
        sql_log.warning("handle_proxy_352 called with None proxy_tab.")
        return
    proxy_tab.status = -352
    await SQLHelper.update_to_proxy_list(proxy_tab, change_score_num=10)
    key = _pool_proxy_key(proxy_tab)
    if key:
        await proxy_health_store.record_failure(key, ProxyFailureKind.RISK_CONTROL)


async def handle_proxy_412(proxy_tab: ProxyTab | None):
    """-412（安全风控）：按**前缀**长冷却（2h→6h→24h），分数仍 +10（能返回 412 说明网络是通的）。

    冷却记在 /64、/24 上：同段内换地址等于同一个 IP，换地址重试只会继续撞墙。
    """
    if not proxy_tab:
        sql_log.warning("handle_proxy_412 called with None proxy_tab.")
        return
    proxy_tab.status = -412
    await SQLHelper.update_to_proxy_list(proxy_tab, change_score_num=10)
    key = _pool_proxy_key(proxy_tab)
    if key:
        await proxy_health_store.record_failure(key, ProxyFailureKind.RISK_CONTROL)


async def handle_proxy_request_fail(
    proxy_tab: ProxyTab | None,
    kind: ProxyFailureKind = ProxyFailureKind.NETWORK,
):
    """请求失败（连接错误 / 超时 / 407）：分数 -10、连续失败 +1、10~30 分钟冷却。"""
    if not proxy_tab:
        sql_log.warning("handle_proxy_request_fail called with None proxy_tab.")
        return
    proxy_tab.status = -412
    await SQLHelper.update_to_proxy_list(proxy_tab, change_score_num=-10)
    key = _pool_proxy_key(proxy_tab)
    if key:
        await proxy_health_store.record_failure(key, kind)


async def handle_proxy_unknown_err(proxy_tab: ProxyTab | None):
    """未知错误：同样按失败处理（归到 unknown，冷却策略与网络层一致）。"""
    if not proxy_tab:
        sql_log.warning("handle_proxy_unknown_err called with None proxy_tab.")
        return
    proxy_tab.status = -412
    await SQLHelper.update_to_proxy_list(proxy_tab, change_score_num=-10)
    key = _pool_proxy_key(proxy_tab)
    if key:
        await proxy_health_store.record_failure(key, ProxyFailureKind.UNKNOWN)
