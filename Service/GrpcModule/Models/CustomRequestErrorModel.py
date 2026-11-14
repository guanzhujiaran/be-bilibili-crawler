class RequestKnownError(Exception):
    def __init__(self, message="", code=-2):
        super().__init__(message)  # 👈 调用父类 Exception，便于 traceback 等工具识别
        self.code = code
        self.message = message

    def __str__(self):
        return f"{self.code}: {self.message}"


class RequestUnknownError(Exception):
    """未知错误，继承自 RequestKnownError（或你也可以让它直接继承 Exception）"""

    pass  # 不需要重写，直接继承父类行为


class Request412Error(RequestKnownError):
    def __init__(self, message="", code=-412):  # 👈 默认 code 改为 -412 更合理
        super().__init__(message, code)


class Request352Error(RequestKnownError):
    def __init__(self, message="", code=-352):
        super().__init__(message, code)


class RequestProxyResponseError(RequestKnownError):
    def __init__(self, message="", code=-1):
        super().__init__(message, code)


class RequestProxyNetworkError(RequestProxyResponseError):
    """网络层代理失败（curl 28/7/35、407、连接失败、超时）。

    继承 ``RequestProxyResponseError`` 是为了让现有 ``except`` 分支继续生效；
    额外携带 ``proxy_key`` / ``curl_code`` 便于错误分流与日志排查。
    """

    def __init__(
        self,
        message="",
        code=-1,
        proxy_key: str | None = None,
        curl_code: int | None = None,
    ):
        super().__init__(message, code)
        self.proxy_key = proxy_key
        self.curl_code = curl_code


class RequestProxyUnavailableError(RequestKnownError):
    """当前档位取不到可用代理（全部冷却 / fast 池为空）。

    单独成类是为了让调用方区分「代理坏了」和「没有代理可换」：
    前者要扣分换代理，后者应尽快把任务交回队列延迟重试。
    """

    def __init__(self, message="", code=-4, tier: str = ""):
        super().__init__(message, code)
        self.tier = tier


class ProxyRetryBudgetExhausted(RequestKnownError):
    """重试预算耗尽（fast 档 2 轮 × 3 次后快速失败）。

    **不可再被 request_wrapper 重试**：它在 wrapper 内部抛出，
    用途是把「坏代理无限重试」截断成一次失败，交给队列延迟重入队。
    """

    def __init__(self, message="", code=-5, attempts: int = 0):
        super().__init__(message, code)
        self.attempts = attempts
