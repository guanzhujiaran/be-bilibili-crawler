from Utils.代理.数据库操作.ProxyHealth import (
    ProxyFailureKind,
    proxy_health_store,
)
from Utils.代理.数据库操作.SqlAlcheyObj.ProxyModel import ProxyTab
from Utils.代理.数据库操作.async_proxy_op_alchemy_mysql_ver import (
    SQLHelper,
    to_proxy_key,
)
from CONFIG import CONFIG
from log.base_log import sql_log

# 代理事件的两个副作用：
# 1. proxy_tab 的 status / score / success_times + 失败原因计数（MySQL 侧可聚合）；
# 2. Redis 健康状态机（连续失败 / EWMA / 冷却），见 ProxyHealth.py。
# available_proxy 表已于 2026-11-10 废弃（取代理改为 MySQL 随机），这里不再写它。
#
# 口径（2026-11-14 起）：**判定看前缀**（IPv6 /64、IPv4 /24），地址级明细只留作日志。
# 自建 IPv6 出口（PROXY_SERVER）不再豁免记账：它是一条固定出口，
# 以前被伪造成 score=10000/success_times=10000，导致永不降权、永不冷却。
#
# status 语义（2026-11-17 起拆分）：
# - 0    可用；
# - -1   非风控失败（network / response / unknown），由 refresh_proxy 两小时后复位；
# - -412 真正的 B 站风控码（412），不再被网络失败复用；
# - -352 风控挑战（352）。
# 只有风控类失败会写 -412/-352；网络失败写 -1，MySQL 侧因此可直接区分两类失败。

#: 网络 / 响应 / 未知失败写回 MySQL 的 status（不再复用 -412）
NETWORK_FAIL_STATUS = -1


def _pool_proxy_key(proxy_tab: ProxyTab | None) -> str:
    """代理事件的健康明细 key（地址级）。

    自建出口同样返回代理串：地址级只作日志明细，真正生效的是它所在的 /64 前缀。
    MySQL 侧的 ``update_to_proxy_list`` 对自建出口仍是 no-op（它不是 proxy_tab 成员），
    所以这里放开记账不会往库里写配置项。
    """
    if proxy_tab is None:
        return ""
    return to_proxy_key(proxy_tab.proxy) or ""


async def handle_proxy_succ(proxy_tab: ProxyTab | None, scope: str | None = None):
    """请求成功：分数 +10，status 归零，健康状态回到 hot 并清该 scope 的冷却。"""
    if not proxy_tab:
        sql_log.warning("handle_proxy_succ called with None proxy_tab.")
        return
    proxy_tab.status = 0
    await SQLHelper.update_to_proxy_list(proxy_tab, change_score_num=10)
    key = _pool_proxy_key(proxy_tab)
    if key:
        await proxy_health_store.record_success(key, scope=scope)


async def handle_proxy_352(
    proxy_tab: ProxyTab | None,
    mark_db_invalid: bool = False,
    scope: str | None = None,
):
    """-352（风控挑战）：按临时状态处理，健康侧按 **(scope, 前缀)** 风控长冷却。

    :param mark_db_invalid: 是否把 ``status=-352`` 写回 MySQL。

        - ``False``（默认，gRPC / HTTP 都已对齐）：352 是**出口风控**而不是「代理坏了」。
          写回 MySQL 会让该代理永久不满足 ``status==0``（候选查询只取 0），
          几轮 352 就能把整个池子打空；这里只在 Redis 健康度上做前缀冷却
          （``ProxyFailureKind.RISK_CONTROL``），代理行本身保持可用。
        - ``True``：沿用 HTTP 通道旧行为（412/352 都置 -352 并等 ``refresh_proxy`` 复位）。
    :param scope: 风控域（如 ``grpc:dynamic``）；None = 全局桶（历史口径）。
    """
    if not proxy_tab:
        sql_log.warning("handle_proxy_352 called with None proxy_tab.")
        return
    if mark_db_invalid:
        proxy_tab.status = -352
        # risk_control 只累加 quarantine_times（观测用），不进入 fail_* 删除计数
        await SQLHelper.update_to_proxy_list(
            proxy_tab, change_score_num=10, kind=ProxyFailureKind.RISK_CONTROL
        )
    key = _pool_proxy_key(proxy_tab)
    if key:
        await proxy_health_store.record_failure(
            key, ProxyFailureKind.RISK_CONTROL, scope=scope
        )


async def handle_proxy_412(
    proxy_tab: ProxyTab | None,
    mark_db_invalid: bool | None = None,
    scope: str | None = None,
):
    """-412（安全风控）：按 **(scope, 前缀)** 长冷却（2h→6h→24h）。

    :param mark_db_invalid: 是否把 ``status=-412`` 写回 MySQL。默认取
        ``CONFIG.proxy_health.risk_write_db_status``（默认 False）：412 是出口风控，
        写 status 会让候选查询（``status==0``）在所有接口上一并剔除该代理 ——
        这正是「一个接口风控 → 整条出口不可用」的第二层放大。
    :param scope: 风控域；None = 全局桶（历史口径）。
    """
    if not proxy_tab:
        sql_log.warning("handle_proxy_412 called with None proxy_tab.")
        return
    if mark_db_invalid is None:
        mark_db_invalid = bool(CONFIG.proxy_health.risk_write_db_status)
    if mark_db_invalid:
        proxy_tab.status = -412
        # risk_control 只累加 quarantine_times（观测用），不进入 fail_* 删除计数
        await SQLHelper.update_to_proxy_list(
            proxy_tab, change_score_num=10, kind=ProxyFailureKind.RISK_CONTROL
        )
    key = _pool_proxy_key(proxy_tab)
    if key:
        await proxy_health_store.record_failure(
            key, ProxyFailureKind.RISK_CONTROL, scope=scope
        )


async def handle_proxy_request_fail(
    proxy_tab: ProxyTab | None,
    kind: ProxyFailureKind = ProxyFailureKind.NETWORK,
    scope: str | None = None,
):
    """请求失败（连接错误 / 超时 / 407 / 上游响应异常）：扣分 + 按原因记账 + 冷却。

    - status 写 ``-1``（非风控失败），与真正的 B 站风控码 -412 区分开；
    - MySQL 侧按 ``kind`` 累加 ``fail_network_times`` / ``fail_response_times`` /
      ``fail_unknown_times``（窗口内滚动），供清理任务按原因判定；
    - 网络层冷却与风控域无关（``scope`` 只透传给健康状态机）。
    """
    if not proxy_tab:
        sql_log.warning("handle_proxy_request_fail called with None proxy_tab.")
        return
    proxy_tab.status = NETWORK_FAIL_STATUS
    await SQLHelper.update_to_proxy_list(proxy_tab, change_score_num=-10, kind=kind)
    key = _pool_proxy_key(proxy_tab)
    if key:
        await proxy_health_store.record_failure(key, kind, scope=scope)


async def handle_proxy_unknown_err(proxy_tab: ProxyTab | None):
    """未知错误：按未知失败处理（status=-1，按 unknown 计数，冷却策略与网络层一致）。"""
    if not proxy_tab:
        sql_log.warning("handle_proxy_unknown_err called with None proxy_tab.")
        return
    proxy_tab.status = NETWORK_FAIL_STATUS
    await SQLHelper.update_to_proxy_list(
        proxy_tab, change_score_num=-10, kind=ProxyFailureKind.UNKNOWN
    )
    key = _pool_proxy_key(proxy_tab)
    if key:
        await proxy_health_store.record_failure(key, ProxyFailureKind.UNKNOWN)
