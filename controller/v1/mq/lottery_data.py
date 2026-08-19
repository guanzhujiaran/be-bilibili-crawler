"""
LotteryData RPC handlers

为 RPC 服务端提供抽奖数据库核心业务逻辑处理函数。
对应 controller/v1/lotttery_database/bili/LotteryData.py 的 15 个路由。

使用 @rpc_subscriber 装饰器注册，handler 直接接收强类型参数模型，
返回 CommonResponseModel，全程由 Pydantic 做参数校验。

不依赖 FastAPI 上下文（无 Request/BackgroundTasks/Depends）：
- RPC 调用方（RPA-Browser）通过 routing_key 定位方法，handler 内部不做鉴权
"""

import time

from bili_common.models.response import StandardResponse as CommonResponseModel
from bili_common.models.pagination import ResponsePaginationItems
from Models.lottery_database.bili.LotteryDataBaseQueryModels import BiliLotDataQueryModel
from Models.lottery_database.bili.LotteryDataModels import (
    AddDynamicLotteryResp,
    OfficialLotteryResp,
    AllLotteryResp,
    ChargeLotteryResp,
    ReserveInfoResp,
    TopicLotteryResp,
    LiveLotteryResp,
    AddDynamicLotteryReq,
    AddTopicLotteryReq,
    AddTopicLotteryResp,
    BulkAddDynamicLotteryReq,
    LotdataResp,
    OthersLotDynItem,
    LotteryFilterParamsResp,
    CommonLotExtraInfoResp,
    EndpointFilterMeta,
    pydantic_model_to_filter_params,
)
from Models.lottery_database.bili.comm import (
    BiliLotDataStatusEnum,
    LotteryBusinessType,
)
from bili_common.models import (
    LotteryPaginationParams,
    LotterySearchPaginationParams,
    LotteryAdvancedQueryParams,
    OthersLotDynListFilterMetadata,
    OthersLotDynSortEnum,
    OthersLotDynSortOrderEnum,
    TimePresetEnum,
    LotteryDataSortEnum,
    SortOrderEnum,
)
from Models.v1.background_service.background_service_model import AllLotScrapyStatusResp
from Service.BackgroundServiceStatus.GetScrapyStaus import get_scrapy_status
from Service.LangChainCompo.text_embed import (
    get_lottery_entity_num,
    search_lottery_text,
)
from Service.GrpcModule.GrpcSrc.SQLObject.models import Lotdata
from Service.lottery_database.bili_lotterty import (
    get_reserve_lottery,
    get_official_lottery,
    get_all_lottery,
    get_charge_lottery,
    get_topic_lottery,
    get_live_lottery,
    add_dynamic_lottery_by_dynamic_id,
    add_topic_lottery,
    add_others_lot_dyn_by_dynamic_id,
    process_others_lot_dyn,
)
from Service.GetOthersLotDyn.Sql.sql_helper import SqlHelper

from Utils.推送.PushMe import a_pushme
from bili_common.models import RpcMethodName
from bili_common.models import (
    GetReserveLotteryRpcParams,
    GetOfficialLotteryRpcParams,
    GetChargeLotteryRpcParams,
    GetTopicLotteryRpcParams,
    GetAllLotteryRpcParams,
    GetOthersLotDynListRpcParams,
    CheckLotteryExistRpcParams,
    CheckLotteryExistRpcResult,
    LotteryDetailItem,
)
from controller.v1.mq.rpc_server import rpc_subscriber
from sqlalchemy import select
from Service.GrpcModule.GrpcSrc.SQLObject.DynDetailSqlHelperMysqlVer import grpc_sql_helper
from Service.GrpcModule.GrpcSrc.SQLObject.models import Lotdata


def _parse_status(status: str | None) -> BiliLotDataStatusEnum | None:
    """将字符串状态转为 BiliLotDataStatusEnum，不传则返回 None（不过滤状态）"""
    if not status:
        return None
    status_map = {
        "unfinished": BiliLotDataStatusEnum.UNFINISHED,
        "finished": BiliLotDataStatusEnum.FINISHED,
        "canceled": BiliLotDataStatusEnum.CANCELED,
        "deleted": BiliLotDataStatusEnum.DELETED,
        "unknown": BiliLotDataStatusEnum.UNKNOWN,
    }
    return status_map.get(status.lower())


@rpc_subscriber(RpcMethodName.GET_RESERVE_LOTTERY, GetReserveLotteryRpcParams)
async def handle_get_reserve_lottery(params: GetReserveLotteryRpcParams) -> CommonResponseModel[ResponsePaginationItems[ReserveInfoResp]]:
    """获取必抽的预约抽奖数据"""
    result_items, total = await get_reserve_lottery(
        q=BiliLotDataQueryModel(
            business_type=LotteryBusinessType.Reserve,
            status=_parse_status(params.status),
            page_num=params.page_num,
            page_size=params.page_size,
            start_ts=params.start_ts,
            end_ts=params.end_ts,
            sender_uid=params.sender_uid,
            min_participants=params.min_participants,
            max_participants=params.max_participants,
            keyword=params.keyword,
            created_at_preset=params.created_at_preset,
            pub_time_preset=params.pub_time_preset,
            sort_by=params.sort_by,
            sort_order=params.sort_order,
        ),
        background_task=None,
    )
    return CommonResponseModel(
        data=ResponsePaginationItems[ReserveInfoResp](items=result_items, total=total)
    )


@rpc_subscriber(RpcMethodName.GET_OFFICIAL_LOTTERY, GetOfficialLotteryRpcParams)
async def handle_get_official_lottery(params: GetOfficialLotteryRpcParams) -> CommonResponseModel:
    """获取必抽的官方抽奖数据"""
    result_items, total = await get_official_lottery(
        q=BiliLotDataQueryModel(
            business_type=LotteryBusinessType.Official,
            status=_parse_status(params.status),
            page_num=params.page_num,
            page_size=params.page_size,
            start_ts=params.start_ts,
            end_ts=params.end_ts,
            sender_uid=params.sender_uid,
            min_participants=params.min_participants,
            max_participants=params.max_participants,
            keyword=params.keyword,
            created_at_preset=params.created_at_preset,
            pub_time_preset=params.pub_time_preset,
            sort_by=params.sort_by,
            sort_order=params.sort_order,
        )
    )
    return CommonResponseModel(
        data=ResponsePaginationItems[OfficialLotteryResp](
            items=result_items, total=total
        )
    )


@rpc_subscriber(RpcMethodName.GET_CHARGE_LOTTERY, GetChargeLotteryRpcParams)
async def handle_get_charge_lottery(params: GetChargeLotteryRpcParams) -> CommonResponseModel:
    """获取必抽的充电抽奖数据"""
    result_items, total = await get_charge_lottery(
        q=BiliLotDataQueryModel(
            business_type=LotteryBusinessType.Charge,
            status=_parse_status(params.status),
            page_num=params.page_num,
            page_size=params.page_size,
            start_ts=params.start_ts,
            end_ts=params.end_ts,
            sender_uid=params.sender_uid,
            min_participants=params.min_participants,
            max_participants=params.max_participants,
            keyword=params.keyword,
            created_at_preset=params.created_at_preset,
            pub_time_preset=params.pub_time_preset,
            sort_by=params.sort_by,
            sort_order=params.sort_order,
        )
    )
    return CommonResponseModel(
        data=ResponsePaginationItems[ChargeLotteryResp](items=result_items, total=total)
    )

@rpc_subscriber(RpcMethodName.GET_TOPIC_LOTTERY, GetTopicLotteryRpcParams)
async def handle_get_topic_lottery(params: GetTopicLotteryRpcParams) -> CommonResponseModel:
    """获取所有话题抽奖数据（分页+筛选）"""
    result_items, total = await get_topic_lottery(
        params.page_num, params.page_size, keyword=params.keyword,
    )
    return CommonResponseModel(
        data=ResponsePaginationItems[TopicLotteryResp](items=result_items, total=total)
    )


@rpc_subscriber(RpcMethodName.GET_ALL_LOTTERY, GetAllLotteryRpcParams)
async def handle_get_all_lottery(params: GetAllLotteryRpcParams) -> CommonResponseModel[AllLotteryResp]:
    """获取所有抽奖信息（按收录时间和发布时间过滤，支持分页）"""
    result: AllLotteryResp = await get_all_lottery(
        created_at_preset=params.created_at_preset,
        created_at_start=params.created_at_start,
        created_at_end=params.created_at_end,
        pub_time_preset=params.pub_time_preset,
        pub_time_start=params.pub_time_start,
        pub_time_end=params.pub_time_end,
        page_num=params.page_num,
        page_size=params.page_size,
    )
    return CommonResponseModel[AllLotteryResp](data=result)


@rpc_subscriber(RpcMethodName.GET_OTHERS_LOT_DYN_LIST, GetOthersLotDynListRpcParams)
async def handle_get_others_lot_dyn_list(params: GetOthersLotDynListRpcParams) -> CommonResponseModel:
    """获取第三方抽奖动态列表（分页+排序+时间筛选）

    RPC 模式下：
    - 不校验网关登录态
    - 信息缺失由脚本补全，handler 仅返回已有缓存
    - 所有筛选参数直接从强类型 params 读取，由 Pydantic 校验
    """
    # 收录时间快捷筛选：优先级高于 created_at_start
    created_at_start = params.created_at_start
    if params.created_at_preset is not None:
        days = int(params.created_at_preset.value.replace("d", ""))
        created_at_start = int(time.time() - days * 86400)

    # 发布时间快捷筛选：优先级高于 pub_time_start
    pub_time_start = params.pub_time_start
    if params.pub_time_preset is not None:
        days = int(params.pub_time_preset.value.replace("d", ""))
        pub_time_start = int(time.time() - days * 86400)
    items, total = await SqlHelper.getLotDynListPaginated(
        page_num=params.page_num,
        page_size=params.page_size,
        sort_by=params.sort_by.value,
        sort_order=params.sort_order.value,
        is_lot=params.is_lot,
        pub_time_start=pub_time_start,
        pub_time_end=params.pub_time_end,
        created_at_start=created_at_start,
        created_at_end=params.created_at_end,
    )

    # 批量获取已缓存的 t_lot_extra_info（信息缺失由脚本补全，接口直接返回已有缓存）
    dyn_ids = [item.dynId for item in items]
    cached_infos = await SqlHelper.get_extra_info_map_by_ref_ids(dyn_ids, "common")

    # 构建响应，附加 extra_info（已合并 prize_names / lottery_time）
    result_items: list[OthersLotDynItem] = []
    for item in items:
        obj = OthersLotDynItem.model_validate(item)
        cached = cached_infos.get(item.dynId)
        if cached:
            obj.extra_info = CommonLotExtraInfoResp(
                is_lot=bool(cached.is_lot),
                is_grand_prize=bool(cached.is_grand_prize),
                need_comment=bool(cached.need_comment),
                need_repost=bool(cached.need_repost),
                required_topic_text=cached.required_topic_text,
                prize_names=cached.prize_names or [],
                lottery_time=cached.lottery_time,
                lot_type=cached.lot_type,
                predicted_at=cached.predicted_at,
            )
        result_items.append(obj)

    return CommonResponseModel(
        data=ResponsePaginationItems[OthersLotDynItem](
            items=result_items,
            total=total,
        )
    )


@rpc_subscriber(RpcMethodName.CHECK_LOTTERY_EXIST, CheckLotteryExistRpcParams)
async def handle_check_lottery_exist(params: CheckLotteryExistRpcParams) -> CommonResponseModel[CheckLotteryExistRpcResult]:
    """校验 lottery 是否存在并回传基础详情（供 be-message 互动/转发时校验资源存在，2.20.0）。

    RPC 模式下不校验登录态；`params.lottery_ids` 非空时批量查询（一次 SQL `IN (...)`），
    否则按 `params.lottery_id` 单查。存在时回传 attach 卡片详情
    （title=first_prize_cmt / cover=first_prize_pic / jumpUrl=lottery_detail_url），
    供 be-message 读取动态时实时填充 RESOURCE=lottery 节点（2.20.1）。
    """
    if params.lottery_ids:
        return await _handle_check_lottery_exist_batch(params)
    return await _handle_check_lottery_exist_one(params)


async def _handle_check_lottery_exist_one(params: CheckLotteryExistRpcParams) -> CommonResponseModel[CheckLotteryExistRpcResult]:
    exists = False
    title = cover = jump_url = None
    try:
        async with grpc_sql_helper.async_session() as session:
            stmt = (
                select(
                    Lotdata.lottery_id,
                    Lotdata.first_prize_cmt,
                    Lotdata.first_prize_pic,
                    Lotdata.lottery_detail_url,
                )
                .where(Lotdata.lottery_id == params.lottery_id)
                .limit(1)
            )
            res = await session.execute(stmt)
            row = res.first()
            if row is not None:
                exists = True
                title = row.first_prize_cmt
                cover = row.first_prize_pic
                jump_url = row.lottery_detail_url
    except Exception:  # noqa: BLE001
        # 弱依赖：查询失败按不存在处理（be-message 侧校验失败会降级放行或 422，见调用方）
        exists = False
    return CommonResponseModel(
        data=CheckLotteryExistRpcResult(
            exists=exists,
            lottery_id=params.lottery_id,
            title=title or None,
            cover=cover or None,
            jumpUrl=jump_url or None,
        )
    )


async def _handle_check_lottery_exist_batch(params: CheckLotteryExistRpcParams) -> CommonResponseModel[CheckLotteryExistRpcResult]:
    items: list[LotteryDetailItem] = []
    try:
        ids = list(dict.fromkeys(params.lottery_ids))
        async with grpc_sql_helper.async_session() as session:
            stmt = select(
                Lotdata.lottery_id,
                Lotdata.first_prize_cmt,
                Lotdata.first_prize_pic,
                Lotdata.lottery_detail_url,
            ).where(Lotdata.lottery_id.in_(ids))
            res = await session.execute(stmt)
            found: dict[int, tuple[str | None, str | None, str | None]] = {}
            for row in res:
                found[int(row.lottery_id)] = (
                    row.first_prize_cmt,
                    row.first_prize_pic,
                    row.lottery_detail_url,
                )
            for lid in ids:
                detail = found.get(lid)
                items.append(
                    LotteryDetailItem(
                        lottery_id=lid,
                        exists=detail is not None,
                        title=(detail[0] if detail else None) or None,
                        cover=(detail[1] if detail else None) or None,
                        jumpUrl=(detail[2] if detail else None) or None,
                    )
                )
    except Exception:  # noqa: BLE001
        # 弱依赖：查询失败返回空 items（be-message 侧降级保留原节点）
        items = []
    return CommonResponseModel(data=CheckLotteryExistRpcResult(items=items))
