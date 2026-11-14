from bili_common.models import StrEnumAutoDoc


class ProxyTier(StrEnumAutoDoc):
    """代理取用档位。

    **只表达优先级语义**，不含任何站点 / 业务信息：

    - ``fast``：高价值、可以快速失败的任务。只从「已证明可用」的代理里取
      （未冷却、EWMA 成功率达标、并发未打满），重试预算小，拿不到就快速失败交回队列；
    - ``slow``：可承受慢速探测的任务。允许掺入 cold / suspect 代理做探测，
      重试预算大、指数退避封顶较久，用来把新代理「养」成 hot。

    具体哪个爬虫用哪档，由各爬虫的 ``CrawlerConfig.proxy_tier`` 声明，
    不在这里写死任何业务名称。
    """

    FAST = "fast"
    SLOW = "slow"
