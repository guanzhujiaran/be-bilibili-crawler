import hashlib
import urllib.parse
import uuid
from typing import Callable

from CONFIG import CONFIG
from log.base_log import bapi_log
from Service.GrpcModule.Models.CustomRequestErrorModel import (
    ProxyRetryBudgetExhausted,
    Request352Error,
    Request412Error,
    RequestKnownError,
    RequestProxyResponseError,
)
from Service.GrpcModule.Grpc.Bapi.Constants import APP_KEY, APP_SEC
from Utils.代理.mdoel.ProxyTier import ProxyTier
from Utils.代理.mdoel.RequestConf import RequestConf, RequestRetryPolicy
from Utils.代理.mdoel.request_context import (
    begin_retry_round,
    resolve_tier,
    retry_sleep,
)


def appsign(params: dict, appkey=APP_KEY, appsec=APP_SEC) -> dict:
    """
    为请求参数进行 APP 签名
    """
    params.update({"appkey": appkey})
    params = dict(sorted(params.items()))
    query = urllib.parse.urlencode(params)
    sign = hashlib.md5((query + appsec).encode()).hexdigest()
    params.update({"sign": sign})
    return params


def gen_trace_id() -> str:
    """
    生成 Bilibili 请求追踪 ID

    抓包格式：``<32位hex>:<后16位hex>:0:0``
    例：``18f5d24115f33f049b447e0e096abdf6:9b447e0e096abdf6:0:0``
    """
    trace_id_uid = uuid.uuid4().hex
    return f"{trace_id_uid}:{trace_id_uid[-16:]}:0:0"


#: 「已知可忽略错误」——代理 / 风控 / 上游返回体错误，按档位预算重试。
#: ``RequestProxyUnavailableError``（取不到代理）是 ``RequestKnownError`` 的子类，
#: 同样走这里：slow 档用退避等代理池恢复，fast 档快速耗尽预算。
_KNOWN_ERRORS = (
    RequestKnownError,
    Request412Error,
    Request352Error,
    RequestProxyResponseError,
)

#: 普通异常（接口 -500 等业务错误）的重试次数
_DEFAULT_MAX_ERROR_RETRIES = 3


def _extract_request_conf(*args, **kwargs) -> RequestConf | None:
    """从调用参数里找 ``RequestConf``（位置参数或关键字参数均可）。"""
    conf = kwargs.get("request_conf")
    if isinstance(conf, RequestConf):
        return conf
    for arg in args:
        if isinstance(arg, RequestConf):
            return arg
    return None


def _resolve_policy(
    args: tuple, kwargs: dict, tier: ProxyTier | None
) -> tuple[ProxyTier, RequestRetryPolicy]:
    conf = _extract_request_conf(*args, **kwargs)
    effective_tier = tier or resolve_tier(conf)
    policy = conf.retry_policy if conf and conf.retry_policy else None
    if policy is None:
        policy = CONFIG.proxy_retry.policy_for(effective_tier)
    return effective_tier, policy


def _describe(err: BaseException) -> str:
    return f"{type(err).__name__}: {err}"


def request_wrapper(
    func: Callable | None = None,
    max_error_retries: int = _DEFAULT_MAX_ERROR_RETRIES,
    *,
    tier: ProxyTier | None = None,
):
    """通用请求重试装饰器（按档位分级，重试期间不占 worker 并发额度）。

    - **已知可忽略错误**（代理失败 / 412 / 352 / 取不到代理）按 ``tier`` 的预算重试：
      * ``fast``：换 2 轮代理、每轮 3 次（共 6 次），退避 0.2~1s，仍未成功就抛
        ``ProxyRetryBudgetExhausted`` 快速失败 —— 交给队列延迟重入队，
        **不再无限重试**（历史 bug：坏代理让 20 个 worker 长期空转）；
      * ``slow``：大预算 + 指数退避封顶 60s（``PROXY_RETRY__SLOW__*``），
        允许慢慢等代理池恢复，但同样不阻塞并发；
    - **其它异常**（接口返回 -500 等业务错误）最多重试 ``max_error_retries`` 次，
      仍然失败则抛出，由上层标记任务失败并跳过。

    关键：所有 sleep 都通过 ``retry_sleep`` 走「让出并发额度」的钩子
    （由爬虫 worker 注入），**不会在信号量作用域内 sleep**。
    """

    def decorator(fn: Callable) -> Callable:
        async def wrapper(*args, **kwargs):
            error_retry_count = 0
            known_error_retry_count = 0
            waited_seconds = 0.0
            effective_tier, policy = _resolve_policy(args, kwargs, tier)
            log_interval = max(1, policy.log_interval)

            while True:
                try:
                    resp_dict = await fn(*args, **kwargs)
                    if known_error_retry_count:
                        bapi_log.warning(
                            f"方法：【{fn.__name__}】 在 {known_error_retry_count} 次可忽略错误重试后成功"
                            f"（tier={effective_tier.value}，累计等待 {waited_seconds:.1f}s）"
                        )
                    return resp_dict
                except ProxyRetryBudgetExhausted:
                    # 已经在本 wrapper 内部判定过预算，绝不能再套一层无限重试
                    raise
                except _KNOWN_ERRORS as known_err:
                    known_error_retry_count += 1
                    delay = policy.delay_for(known_error_retry_count)
                    if known_error_retry_count % log_interval == 1:
                        bapi_log.warning(
                            f"方法：【{fn.__name__}】 遇到可忽略错误"
                            f"（{_describe(known_err)}），tier={effective_tier.value}，"
                            f"已重试 {known_error_retry_count} 次"
                            f"（次数预算 {policy.max_attempts or '不限'}，"
                            f"时间预算 {policy.max_total_seconds or '不限'}s，"
                            f"已等待 {waited_seconds:.1f}s）"
                        )
                    # 两条预算：次数（fast 2 轮 × 3 次）与累计等待（slow 也不能挂几小时）
                    if policy.is_exhausted(known_error_retry_count, waited_seconds):
                        bapi_log.error(
                            f"方法：【{fn.__name__}】 重试预算耗尽"
                            f"（tier={effective_tier.value}，共 {known_error_retry_count} 次、"
                            f"累计等待 {waited_seconds:.1f}s），"
                            f"交回队列延迟重入队。最后一次错误：{_describe(known_err)}"
                        )
                        raise ProxyRetryBudgetExhausted(
                            f"{fn.__name__} 重试预算耗尽：{_describe(known_err)}",
                            attempts=known_error_retry_count,
                        ) from known_err
                    # 每跨一轮清空已排除的代理：fast 档的「换 2 轮代理」靠这里生效
                    if policy.attempts_per_round > 0 and (
                        known_error_retry_count % policy.attempts_per_round == 0
                    ):
                        begin_retry_round()
                    waited_seconds += delay
                    await retry_sleep(delay)
                    continue
                except TypeError as type_err:
                    raise type_err
                except Exception as e:
                    error_retry_count += 1
                    # 统一成「函数 + 第几次 + 异常类型 + 异常内容」，避免 e 和 type(e) 重复打印。
                    err_desc = _describe(e)
                    bapi_log.exception(
                        f"方法：【{fn.__name__}】 请求失败！(第{error_retry_count}/{max_error_retries}次) {err_desc}"
                    )
                    if error_retry_count >= max_error_retries:
                        bapi_log.error(
                            f"方法：【{fn.__name__}】 连续失败 {max_error_retries} 次，跳过该任务！"
                            f"最后一次错误：{err_desc}"
                        )
                        raise
                    # 同样让出并发额度，避免普通重试也把 worker 挂住
                    await retry_sleep(10)

        return wrapper

    # 支持 @request_wrapper 与 @request_wrapper(...) 两种写法
    if func is not None:
        return decorator(func)
    return decorator
