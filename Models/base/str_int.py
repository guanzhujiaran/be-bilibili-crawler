"""雪花 ID 统一入参类型：StrInt（与 be-message 同口径）。

业务背景：B 站 dynId / rpid 等是 **19 位雪花 ID**，超过 JS `Number.MAX_SAFE_INTEGER`
（9007199254740991）。前端 `Number(dynId)` 或网关 `JSON.parse` 一旦经过 JS Number，
末几位会被四舍五入（如 `1174364313566052360` → `1174364313566052400`），
后端按该 ID 查询必然 404「不存在」——第三方抽奖动态详情页（2.61.0）即踩此坑。

对策（与 `be-message-service/app/models/str_int.py` 一致）：
- **入参**：JSON 中既可能是数字也可能是字符串，一律「接受 str 或 int，内部归一为 int」；
  前端统一以**字符串**传递（响应侧已有 `dynId_str` 等字符串镜像字段同理）。
- **出参**：Python `int` 任意精度无损，序列化仍为 int（前端读取用 `*_str` 镜像字段）。

OpenAPI schema 表现为 `anyOf[integer, string]`，hey-api 生成的前端 SDK 参数类型为
`number | string`，前端封装层可直接传字符串，无需 `Number()` 转换。
"""

from typing import Annotated, Union

from pydantic import BeforeValidator


def _coerce_snowflake(v):
    # bool 是 int 子类，但绝不可能是合法雪花 ID，显式拒绝避免误判
    if isinstance(v, bool):
        raise ValueError("bool is not a valid snowflake id")
    if isinstance(v, int):
        return v
    if isinstance(v, str):
        try:
            return int(v)
        except (ValueError, TypeError):
            raise ValueError(f"invalid snowflake id string: {v!r}")
    if isinstance(v, float):
        return int(v)
    raise ValueError(f"cannot coerce {type(v).__name__} to snowflake id")


StrInt = Annotated[Union[int, str], BeforeValidator(_coerce_snowflake)]


__all__ = ["StrInt"]
