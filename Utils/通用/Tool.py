import datetime


def str_2_DateTime(
    date_string: str, date_format: str = "%Y-%m-%dT%H:%M:%S", time_zone_offset=0
) -> datetime.datetime:
    datetime_object = datetime.datetime.strptime(
        date_string, date_format
    ) + datetime.timedelta(hours=time_zone_offset)
    return datetime_object


def ts_2_DateTime(timestamp: int | float, time_zone_offset=0) -> datetime.datetime:
    datetime_object = datetime.datetime.fromtimestamp(timestamp) + datetime.timedelta(
        hours=time_zone_offset
    )
    return datetime_object


def format_duration(seconds: float | int | None) -> str:
    """把秒数格式化成面向人的「X天X小时X分X秒」。

    省略为 0 的高位与末尾单位，但保留中间单位为 0 的占位（更易读）；
    最小单位必出：不足 60 秒时输出秒。

    仅用于日志 / 推送等文案；接口里的时长字段仍是秒级数值，不要用本函数替换。

    例：
        45    -> "45秒"
        65    -> "1分5秒"
        3725  -> "1小时2分5秒"
        93784 -> "1天2小时3分4秒"
        3600  -> "1小时"
        43201 -> "12小时0分1秒"
        0 / None / 负数 / 非法值 -> "0秒"
    """
    if seconds is None:
        return "0秒"
    try:
        total_seconds = int(seconds)
    except (TypeError, ValueError):
        return "0秒"
    if total_seconds <= 0:
        return "0秒"

    # timedelta 负责把秒拆成「天 + 当天内秒数」，剩下的再用 divmod 拆时分秒
    delta = datetime.timedelta(seconds=total_seconds)
    days = delta.days
    hours, rest_seconds = divmod(delta.seconds, 3600)
    minutes, secs = divmod(rest_seconds, 60)

    values = (days, hours, minutes, secs)
    labels = ("天", "小时", "分", "秒")
    # 只省略「高位」与「末尾」的 0：中间为 0 的单位保留（12小时0分1秒），
    # 这样既满足 45 -> "45秒"，也不会出现 "12小时1秒" 这种难读的省略
    first_nonzero_index = next(i for i, v in enumerate(values) if v)
    last_nonzero_index = len(values) - 1 - next(
        i for i, v in enumerate(reversed(values)) if v
    )
    return "".join(
        f"{values[i]}{labels[i]}"
        for i in range(first_nonzero_index, last_nonzero_index + 1)
    )
