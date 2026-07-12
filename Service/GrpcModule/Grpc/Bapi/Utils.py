import hashlib
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


def request_wrapper(func: Callable, max_error_retries: int = 3):
    """
    一个通用的请求重试装饰器

    - 已知可忽略的错误（代理/412/352 等）会一直重试直到成功。
    - 其余异常（如接口返回 -500 等业务错误）最多重试 max_error_retries 次，
      仍然失败则抛出异常，交由上层标记任务失败并跳过。
    """

    async def wrapper(*args, **kwargs):
        error_retry_count = 0
        while True:
            try:
                resp_dict = await func(*args, **kwargs)
                return resp_dict
            except (RequestKnownError, Request412Error, Request352Error, RequestProxyResponseError):
                # 已知可忽略的错误，直接重试
                continue
            except TypeError as type_err:
                raise type_err
            except Exception as e:
                error_retry_count += 1
                bapi_log.exception(
                    f"方法：【{func.__name__}】 请求失败！(第{error_retry_count}/{max_error_retries}次)\n{e}\n{type(e)}"
                )
                if error_retry_count >= max_error_retries:
                    bapi_log.error(
                        f"方法：【{func.__name__}】 连续失败 {max_error_retries} 次，跳过该任务！"
                    )
                    raise
                await asyncio.sleep(10)

    return wrapper
