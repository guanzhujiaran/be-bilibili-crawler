from Utils.代理.数据库操作.ProxyHealth import (
    ProxyFailureKind,
    proxy_health_store,
)
from Utils.代理.数据库操作.SqlAlcheyObj.ProxyModel import ProxyTab
from Utils.代理.数据库操作.async_proxy_op_alchemy_mysql_ver import (
    SQLHelper,
    is_local_ipv6_proxy,
    to_proxy_key,
)
from log.base_log import sql_log

# 代理事件的两个副作用：
# 1. proxy_tab 的 status / score / success_times（保留原有语义，读写方很多，不能改含义）；
# 2. Redis 健康状态机（连续失败 / EWMA / 冷却），见 ProxyHealth.py。
# available_proxy 表已于 2026-11-10 废弃（取代理改为 MySQL 随机），这里不再写它。


def _pool_proxy_key(proxy_tab: ProxyTab) -> str:
    """池成员的代理串；本地 ipv6 代理（配置项）返回空串，不参与健康统计。"""
    key = to_proxy_key(proxy_tab.proxy) or ""
    if not key or is_local_ipv6_proxy(key):
        return ""
    return key


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
    """-352（风控挑战）：按临时状态处理，分数仍 +10，健康侧按风控层长冷却。"""
    if not proxy_tab:
        sql_log.warning("handle_proxy_352 called with None proxy_tab.")
        return
    proxy_tab.status = -352
    await SQLHelper.update_to_proxy_list(proxy_tab, change_score_num=10)
    key = _pool_proxy_key(proxy_tab)
    if key:
        await proxy_health_store.record_failure(key, ProxyFailureKind.RISK_CONTROL)


async def handle_proxy_412(proxy_tab: ProxyTab | None):
    """-412（安全风控）：按 IP 长冷却（2~6h），分数仍 +10（能返回 412 说明网络是通的）。"""
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
