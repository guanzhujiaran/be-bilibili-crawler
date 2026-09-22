import hashlib
import random
import time
import urllib.parse
import uuid
import asyncio
from typing import Callable
from log.base_log import bapi_log
from Service.GrpcModule.Models.CustomRequestErrorModel import RequestKnownError, Request412Error, Request352Error, \
    RequestProxyResponseError
from Service.GrpcModule.Grpc.Bapi.Constants import APP_KEY, APP_SEC


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
    """
    trace_id_uid = str(uuid.uuid4()).replace("-", "")[0:26].lower()
    trace_id_hex = hex(int(round(time.time()) / 256)).lower().replace("0x", "")
    return f"{trace_id_uid}{trace_id_hex}:{trace_id_uid[-10:]}{trace_id_hex}:0:0"


# 已知可忽略的错误（代理/412/352 等）是无限重试的，
# 每累计这么多次就打一条告警，避免「静默无限重试」把问题藏起来。
_KNOWN_ERROR_RETRY_LOG_INTERVAL = 50
# 已知可忽略错误的退避区间（秒）：避免代理持续不可用时以最大速率反复轰上游
_KNOWN_ERROR_RETRY_BACKOFF = (0.2, 1.0)


def request_wrapper(func: Callable, max_error_retries: int = 3):
    """
    一个通用的请求重试装饰器

    - 已知可忽略的错误（代理/412/352 等）会一直重试直到成功；
      每次重试带短随机退避，并按固定间隔输出重试次数，便于观察是否卡死。
    - 其余异常（如接口返回 -500 等业务错误）最多重试 max_error_retries 次，
      仍然失败则抛出异常，交由上层标记任务失败并跳过。
    """

    async def wrapper(*args, **kwargs):
        error_retry_count = 0
        known_error_retry_count = 0
        while True:
            try:
                resp_dict = await func(*args, **kwargs)
                if known_error_retry_count:
                    bapi_log.warning(
                        f"方法：【{func.__name__}】 在 {known_error_retry_count} 次可忽略错误重试后成功"
                    )
                return resp_dict
            except (RequestKnownError, Request412Error, Request352Error, RequestProxyResponseError) as known_err:
                # 已知可忽略的错误：继续重试，但要留下可观测的痕迹
                known_error_retry_count += 1
                if known_error_retry_count % _KNOWN_ERROR_RETRY_LOG_INTERVAL == 1:
                    bapi_log.warning(
                        f"方法：【{func.__name__}】 遇到可忽略错误"
                        f"（{type(known_err).__name__}: {known_err}），"
                        f"已连续重试 {known_error_retry_count} 次，继续重试..."
                    )
                await asyncio.sleep(
                    random.uniform(*_KNOWN_ERROR_RETRY_BACKOFF)
                )
                continue
            except TypeError as type_err:
                raise type_err
            except Exception as e:
                error_retry_count += 1
                # 统一成「函数 + 第几次 + 异常类型 + 异常内容」，避免 e 和 type(e) 重复打印。
                err_desc = f"{type(e).__name__}: {e}"
                bapi_log.exception(
                    f"方法：【{func.__name__}】 请求失败！(第{error_retry_count}/{max_error_retries}次) {err_desc}"
                )
                if error_retry_count >= max_error_retries:
                    bapi_log.error(
                        f"方法：【{func.__name__}】 连续失败 {max_error_retries} 次，跳过该任务！"
                        f"最后一次错误：{err_desc}"
                    )
                    raise
                await asyncio.sleep(10)

    return wrapper
