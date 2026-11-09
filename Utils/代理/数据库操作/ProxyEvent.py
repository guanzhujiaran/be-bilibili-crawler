from Utils.代理.数据库操作.SqlAlcheyObj.ProxyModel import ProxyTab
from Utils.代理.数据库操作.async_proxy_op_alchemy_mysql_ver import SQLHelper
from log.base_log import sql_log

# 代理事件的唯一副作用：更新 proxy_tab 的 status / score / success_times。
# available_proxy 表已于 2026-11-10 废弃（取代理改为 MySQL 随机），这里不再写它。


async def handle_proxy_succ(proxy_tab: ProxyTab | None):
    """请求成功：分数 +10，status 归零。"""
    if not proxy_tab:
        sql_log.warning("handle_proxy_succ called with None proxy_tab.")
        return
    proxy_tab.status = 0
    await SQLHelper.update_to_proxy_list(proxy_tab, change_score_num=10)


async def handle_proxy_352(proxy_tab: ProxyTab | None):
    """-352（风控挑战）：按临时状态处理，分数仍 +10。"""
    if not proxy_tab:
        sql_log.warning("handle_proxy_352 called with None proxy_tab.")
        return
    proxy_tab.status = -352
    await SQLHelper.update_to_proxy_list(proxy_tab, change_score_num=10)


async def handle_proxy_412(proxy_tab: ProxyTab | None):
    """-412（安全风控）：按临时封禁处理，分数仍 +10（能返回 412 说明网络是通的）。"""
    if not proxy_tab:
        sql_log.warning("handle_proxy_412 called with None proxy_tab.")
        return
    proxy_tab.status = -412
    await SQLHelper.update_to_proxy_list(proxy_tab, change_score_num=10)


async def handle_proxy_request_fail(proxy_tab: ProxyTab | None):
    """请求失败（连接错误 / 超时）：分数 -10 并标记不可用。"""
    if not proxy_tab:
        sql_log.warning("handle_proxy_request_fail called with None proxy_tab.")
        return
    proxy_tab.status = -412
    await SQLHelper.update_to_proxy_list(proxy_tab, change_score_num=-10)


async def handle_proxy_unknown_err(proxy_tab: ProxyTab | None):
    """未知错误：同样按失败处理。"""
    if not proxy_tab:
        sql_log.warning("handle_proxy_unknown_err called with None proxy_tab.")
        return
    proxy_tab.status = -412
    await SQLHelper.update_to_proxy_list(proxy_tab, change_score_num=-10)
