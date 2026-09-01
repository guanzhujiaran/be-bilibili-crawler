from bili_common.core import IntEnumAutoDoc

# 抽奖相关共享枚举与查询模型已统一迁移至 bili_common.models.lottery_query，
# 本文件仅 re-export 供存量 `from Models.lottery_database.bili.comm import ...` 引用兼容，
# 并保留 crawler 本地业务枚举（LotteryBusinessType / BiliLotDataStatusEnum）。
from bili_common.models.lottery_query import (
    LotteryDataSortEnum,
    SortOrderEnum,
    OthersLotDynSortEnum,
    OthersLotDynSortOrderEnum,
    TimePresetEnum,
    LotteryPaginationParams,
    LotterySearchPaginationParams,
    LotteryAdvancedQueryParams,
    OthersLotDynListFilterMetadata,
)


class LotteryBusinessType(IntEnumAutoDoc):
    Official = 1
    Reserve = 10
    Charge = 12


class BiliLotDataStatusEnum(IntEnumAutoDoc):
    CANCELED = -1
    DELETED = -2
    UNFINISHED = 0
    FINISHED = 2
    UNKNOWN = 404


__all__ = [
    "LotteryDataSortEnum",
    "SortOrderEnum",
    "OthersLotDynSortEnum",
    "OthersLotDynSortOrderEnum",
    "TimePresetEnum",
    "LotteryPaginationParams",
    "LotterySearchPaginationParams",
    "LotteryAdvancedQueryParams",
    "OthersLotDynListFilterMetadata",
    "LotteryBusinessType",
    "BiliLotDataStatusEnum",
]
