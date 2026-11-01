from Models.base.custom_pydantic import CustomBaseModel
from pydantic import Field, computed_field

from Models.lottery_database.bili.comm import (
    BiliLotDataStatusEnum,
    LotteryAdvancedQueryParams,
    LotteryBusinessType,
)
from Models.lottery_database.bili.LotteryDataModels import (
    LotteryDataSortEnum,
    SortOrderEnum,
    TimePresetEnum,
)


class ReserveLotteryQueryParams(LotteryAdvancedQueryParams):
    """预约抽奖的 GET 查询参数。

    在通用高级查询参数（``LotteryAdvancedQueryParams``）之上追加「仅看有效预约」：
    预约是否已取消 / 已失效并不体现在 ``lotdata.status`` 上，而是由预约关系表
    ``t_up_reserve_relation_info`` 的 ``state / code / etime`` 决定（见
    ``Service.lottery_database.bili_lotterty._reserve_available``）。因此需要独立参数，
    不能复用 ``status``。仅加在预约抽奖端点，避免污染其它抽奖接口的 OpenAPI / 筛选元数据。
    """

    only_available: bool = Field(
        default=True,
        description="仅返回预约仍有效（未取消/未失效）的记录；仅在 status=unfinished 时生效",
        json_schema_extra={
            "filter_display_name": "仅看有效预约",
            "filter_widget": "switch",
            "filter_description": "过滤掉已取消/已失效的预约抽奖",
            "filter_default": True,
        },
    )


class BiliLotDataQueryModel(CustomBaseModel):
    business_type: LotteryBusinessType = Field(...)
    status: BiliLotDataStatusEnum | None = Field(
        default=None, description="不传则不过滤状态"
    )
    page_num: int = Field(..., ge=0)
    page_size: int = Field(..., ge=0)
    start_ts: int | None = Field(default=None, ge=0)
    end_ts: int | None = Field(default=None, ge=0)
    sender_uid: int | None = Field(default=None, ge=0)
    min_participants: int | None = Field(default=None, ge=0)
    max_participants: int | None = Field(default=None, ge=0)
    keyword: str | None = Field(
        default=None, description="关键词，对抽奖结果描述做 LIKE 过滤"
    )
    created_at_preset: TimePresetEnum | None = Field(
        default=None, description="收录时间快捷筛选"
    )
    pub_time_preset: TimePresetEnum | None = Field(
        default=None, description="发布时间快捷筛选"
    )
    sort_by: LotteryDataSortEnum | None = Field(default=None, description="排序字段")
    sort_order: SortOrderEnum | None = Field(default=None, description="排序方向")
    is_grand_prize: bool | None = Field(
        default=None, description="是否大奖筛选: True-是, False-否, None-不过滤"
    )
    only_available: bool = Field(
        default=False,
        description="仅返回预约仍有效的记录（仅预约抽奖、status=unfinished 时由业务层生效）",
    )
    business_id_in: list[int] | None = Field(
        default=None,
        description="限定 business_id 在给定集合内；跨库（预约关系表）过滤时由业务层填充",
    )

    @computed_field
    @property
    def offset(self) -> int:
        """分页 offset: (page_num - 1) * page_size，page_num 从 1 开始

        当 page_num=0 或 page_size=0 时返回 0（由 SQL 层的 if guard 跳过分页）。
        max(0, ...) 防止 page_num=0 时返回负数。
        """
        return max(0, (self.page_num - 1) * self.page_size)
