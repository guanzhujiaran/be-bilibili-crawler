"""
LotteryData RPC handlers —— **只读取数**，由 query 角色提供服务端

职责边界（2.70.0）：
- `FastapiApp.rpc.*` 全部是「只读取数」（请求-响应），由 `query_main.py`
  （SERVICE_ROLE=query，可指向只读账号 / 只读副本）注册并消费，与爬虫进程解耦：
  爬虫没起 / 迁移失败都不影响 RPA / be-message 取数；
- **写库不走 RPC**：走 `bili_data` 的普通 MQ 消息（`BiliLotDataPublisher` 发布 →
  crawler 角色的 `mq_controller` 订阅者落库），因此 crawler 角色不注册任何 RPC 服务端。

刻意**不 import** 爬虫 / LLM 侧的重依赖（`Service.LangChainCompo.text_embed`（模块级建
OpenAIEmbeddings + Milvus）、`BackgroundServiceStatus`、`Utils.推送.PushMe`、
`add_*` 写动作、`reserve_robot` 刷新链路）：query 容器的 lifespan 不初始化 Milvus /
不跑迁移 / 不启动调度器，把这些拖进来就等于把爬虫依赖重新绑回来。

读路径不做任何写入：预约状态刷新 / lotdata 回写由 crawler 角色的定时任务负责，
且这些写是纯副作用（不影响本次响应内容），见 `get_reserve_lottery(allow_active_fetch=False)`。

对应只读 HTTP 路由：controller/v1/lotttery_database/bili/LotteryData.py 的 query_router。

不依赖 FastAPI 上下文（无 Request/BackgroundTasks/Depends）：
- RPC 调用方（RPA-Browser / be-message）通过 routing_key 定位方法，handler 内部不做鉴权
"""

import time

from sqlalchemy import func, select, text
from loguru import logger

from bili_common.models.response import StandardResponse as CommonResponseModel
from bili_common.models.pagination import ResponsePaginationItems
from bili_common.models import (
    RpcMethodName,
    GetReserveLotteryRpcParams,
    GetOfficialLotteryRpcParams,
    GetChargeLotteryRpcParams,
    GetTopicLotteryRpcParams,
    GetAllLotteryRpcParams,
    GetOthersLotDynListRpcParams,
    CheckLotteryExistRpcParams,
    CheckLotteryExistRpcResult,
    LotteryDetailItem,
    CheckOthersLotDynExistRpcParams,
    CheckOthersLotDynExistRpcResult,
    OthersLotDynDetailItem,
)
from Models.lottery_database.bili.LotteryDataBaseQueryModels import (
    BiliLotDataQueryModel,
)
from Models.lottery_database.bili.LotteryDataModels import (
    OfficialLotteryResp,
    AllLotteryResp,
    ChargeLotteryResp,
    ReserveInfoResp,
    TopicLotteryResp,
    OthersLotDynItem,
    CommonLotExtraInfoResp,
)
from Models.lottery_database.bili.comm import (
    BiliLotDataStatusEnum,
    LotteryBusinessType,
)
from Service.lottery_database.bili_lotterty import (
    get_reserve_lottery,
    get_official_lottery,
    get_all_lottery,
    get_charge_lottery,
    get_topic_lottery,
)
from Service.GetOthersLotDyn.Sql.sql_helper import SqlHelper
from Service.GetOthersLotDyn.Sql.models import TLotdyninfo
from Service.GrpcModule.GrpcSrc.SQLObject.models import Lotdata
from Service.GrpcModule.GrpcSrc.SQLObject.DynDetailSqlHelperMysqlVer import (
    grpc_sql_helper,
)
from controller.v1.mq.rpc_server import rpc_subscriber


def _parse_status(status: str | None) -> BiliLotDataStatusEnum:
    """将字符串状态转为 BiliLotDataStatusEnum；不传时默认按「未开奖」过滤

    抽奖查询接口默认只返回仍可参与（未开奖且 lottery_time 未过期）的记录，
    避免把已开奖/已取消/已删除的数据返回给调用方（RPA、前端等）。
    """
    if not status:
        return BiliLotDataStatusEnum.UNFINISHED
    status_map = {
        "unfinished": BiliLotDataStatusEnum.UNFINISHED,
        "finished": BiliLotDataStatusEnum.FINISHED,
        "canceled": BiliLotDataStatusEnum.CANCELED,
        "deleted": BiliLotDataStatusEnum.DELETED,
        "unknown": BiliLotDataStatusEnum.UNKNOWN,
    }
    return status_map.get(status.lower(), BiliLotDataStatusEnum.UNFINISHED)


@rpc_subscriber(
    RpcMethodName.GET_RESERVE_LOTTERY,
    GetReserveLotteryRpcParams,
)
async def handle_get_reserve_lottery(
    params: GetReserveLotteryRpcParams,
) -> CommonResponseModel[ResponsePaginationItems[ReserveInfoResp]]:
    """获取必抽的预约抽奖数据

    RPC 是**只读取数**（RPA 等外部系统批量拉列表）：
    - 不发起预约信息刷新、不回写 lotdata（`allow_active_fetch=False`）。
      RPC 没有 BackgroundTasks，写路径会退化为同步 await，逐条访问 B 站与写库，
      page_size=1000 时必然超过服务端 60s 超时；这些刷新由爬虫角色定时任务负责，
      且属于纯副作用，不影响本次响应内容。
    """
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
            is_grand_prize=params.is_grand_prize,
            # 与只读 HTTP 端点 GetReserveLottery 默认行为一致（only_available 默认 True）：
            # 过滤掉已取消 / 已失效 / 已开奖 / 已过期的预约。之前这里漏传，
            # 模型默认 False，导致 150 条里 108 条是 available=False 的取消预约。
            only_available=params.only_available,
        ),
        background_task=None,
        allow_active_fetch=False,
    )
    return CommonResponseModel(
        data=ResponsePaginationItems[ReserveInfoResp](items=result_items, total=total)
    )


@rpc_subscriber(
    RpcMethodName.GET_OFFICIAL_LOTTERY,
    GetOfficialLotteryRpcParams,
)
async def handle_get_official_lottery(
    params: GetOfficialLotteryRpcParams,
) -> CommonResponseModel:
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
            is_grand_prize=params.is_grand_prize,
        )
    )
    return CommonResponseModel(
        data=ResponsePaginationItems[OfficialLotteryResp](
            items=result_items, total=total
        )
    )


@rpc_subscriber(
    RpcMethodName.GET_CHARGE_LOTTERY,
    GetChargeLotteryRpcParams,
)
async def handle_get_charge_lottery(
    params: GetChargeLotteryRpcParams,
) -> CommonResponseModel:
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
            is_grand_prize=params.is_grand_prize,
        )
    )
    return CommonResponseModel(
        data=ResponsePaginationItems[ChargeLotteryResp](items=result_items, total=total)
    )


@rpc_subscriber(
    RpcMethodName.GET_TOPIC_LOTTERY,
    GetTopicLotteryRpcParams,
)
async def handle_get_topic_lottery(
    params: GetTopicLotteryRpcParams,
) -> CommonResponseModel:
    """获取所有话题抽奖数据（分页+筛选）"""
    result_items, total = await get_topic_lottery(
        params.page_num,
        params.page_size,
        keyword=params.keyword,
    )
    return CommonResponseModel(
        data=ResponsePaginationItems[TopicLotteryResp](items=result_items, total=total)
    )


@rpc_subscriber(
    RpcMethodName.GET_ALL_LOTTERY,
    GetAllLotteryRpcParams,
)
async def handle_get_all_lottery(
    params: GetAllLotteryRpcParams,
) -> CommonResponseModel[AllLotteryResp]:
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


@rpc_subscriber(
    RpcMethodName.GET_OTHERS_LOT_DYN_LIST,
    GetOthersLotDynListRpcParams,
)
async def handle_get_others_lot_dyn_list(
    params: GetOthersLotDynListRpcParams,
) -> CommonResponseModel:
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
        is_grand_prize=params.is_grand_prize,
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


@rpc_subscriber(
    RpcMethodName.CHECK_LOTTERY_EXIST,
    CheckLotteryExistRpcParams,
)
async def handle_check_lottery_exist(
    params: CheckLotteryExistRpcParams,
) -> CommonResponseModel[CheckLotteryExistRpcResult]:
    """校验 lottery 是否存在并回传基础详情（供 be-message 互动/转发时校验资源存在，2.20.0）。

    RPC 模式下不校验登录态；`params.lottery_ids` 非空时批量查询（一次 SQL `IN (...)`），
    否则按 `params.lottery_id` 单查。存在时回传 attach 卡片详情
    （title=first_prize_cmt / cover=first_prize_pic / jumpUrl=lottery_detail_url），
    供 be-message 读取动态时实时填充 RESOURCE=lottery 节点（2.20.1）。
    """
    if params.lottery_ids:
        return await _handle_check_lottery_exist_batch(params)
    return await _handle_check_lottery_exist_one(params)


async def _handle_check_lottery_exist_one(
    params: CheckLotteryExistRpcParams,
) -> CommonResponseModel[CheckLotteryExistRpcResult]:
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


async def _handle_check_lottery_exist_batch(
    params: CheckLotteryExistRpcParams,
) -> CommonResponseModel[CheckLotteryExistRpcResult]:
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


# ==================== 第三方抽奖动态存在性校验（2.61.0）====================
# 第三方抽奖动态（biliopusdb.t_lotdyninfo）没有 lotdata.lottery_id，
# 若拿 dynId 走 check_lottery_exist 必然判不存在；be-message 的
# `others_lot_dyn`（bizType=15）资源类型改走本 RPC 校验，两个命名空间互不干扰。


def _others_lot_dyn_title(author_name: str | None, dyn_id: int) -> str:
    """第三方抽奖动态卡片标题（与前端 lotteryNormalization 的 THIRD_PARTY 口径一致）。"""
    return f"{author_name} 的抽奖动态" if author_name else f"第三方抽奖 #{dyn_id}"


async def _log_others_lot_dyn_miss(dyn_ids: list[int]) -> None:
    """全部未命中时的诊断日志（排查用，仅未命中才触发一次轻量查询）。

    打印**当前连接的实际库名** + 表内 dynId 的行数与取值范围，用于区分两种根因：
    - 表内行数 > 0 且范围与传入值同量级 → 同库但这些行确实不存在（数据被删 / id 来源不同）；
    - 表内为空或量级完全不同 / 库名与列表接口不同 → 查的库不是列表接口那个（配置 / 环境不一致）。
    """
    try:
        async with SqlHelper.async_session() as session:
            row = (
                await session.execute(
                    select(
                        func.min(TLotdyninfo.dynId),
                        func.max(TLotdyninfo.dynId),
                        func.count(),
                    )
                )
            ).first()
            db_name = (await session.execute(text("SELECT DATABASE()"))).scalar()
        logger.warning(
            f"[check_others_lot_dyn_exist] 全部未命中: 传入 {len(dyn_ids)} 个 "
            f"dyn_ids={dyn_ids[:3]}...; 当前库={db_name} t_lotdyninfo 行数="
            f"{row[2] if row else '?'} dynId 范围=({row[0] if row else '?'}, {row[1] if row else '?'})"
        )
    except Exception as e:  # noqa: BLE001
        logger.warning(f"[check_others_lot_dyn_exist] 未命中诊断查询失败: {e}")


@rpc_subscriber(
    RpcMethodName.CHECK_OTHERS_LOT_DYN_EXIST,
    CheckOthersLotDynExistRpcParams,
)
async def handle_check_others_lot_dyn_exist(
    params: CheckOthersLotDynExistRpcParams,
) -> CommonResponseModel[CheckOthersLotDynExistRpcResult]:
    """校验第三方抽奖动态是否存在并回传基础详情（供 be-message 互动/跳转校验，2.61.0）。

    `params.dyn_ids` 非空时批量查询（一次 SQL `IN (...)`），否则按 `params.dyn_id` 单查。
    弱依赖：查询失败按不存在处理（be-message 侧会降级放行或 400，见调用方）。
    """
    if params.dyn_ids:
        return await _handle_check_others_lot_dyn_exist_batch(params)
    return await _handle_check_others_lot_dyn_exist_one(params)


async def _handle_check_others_lot_dyn_exist_one(
    params: CheckOthersLotDynExistRpcParams,
) -> CommonResponseModel[CheckOthersLotDynExistRpcResult]:
    exists = False
    title = cover = jump_url = None
    author_mid = author_name = None
    try:
        async with SqlHelper.async_session() as session:
            stmt = (
                select(
                    TLotdyninfo.dynId,
                    TLotdyninfo.authorName,
                    TLotdyninfo.up_uid,
                    TLotdyninfo.dynamicUrl,
                )
                .where(TLotdyninfo.dynId == params.dyn_id)
                .limit(1)
            )
            row = (await session.execute(stmt)).first()
            if row is not None:
                exists = True
                author_name = row.authorName or None
                author_mid = int(row.up_uid) if row.up_uid else None
                title = _others_lot_dyn_title(author_name, int(row.dynId))
                jump_url = row.dynamicUrl or None
    except Exception as e:  # noqa: BLE001
        logger.warning(
            f"[check_others_lot_dyn_exist] 单查失败 dyn_id={params.dyn_id}: {e}"
        )
        exists = False
    if not exists and params.dyn_id is not None:
        await _log_others_lot_dyn_miss([int(params.dyn_id)])
    return CommonResponseModel(
        data=CheckOthersLotDynExistRpcResult(
            exists=exists,
            dyn_id=params.dyn_id,
            title=title,
            cover=cover,
            jumpUrl=jump_url,
            authorMid=author_mid,
            authorName=author_name,
        )
    )


async def _handle_check_others_lot_dyn_exist_batch(
    params: CheckOthersLotDynExistRpcParams,
) -> CommonResponseModel[CheckOthersLotDynExistRpcResult]:
    items: list[OthersLotDynDetailItem] = []
    try:
        ids = list(dict.fromkeys(params.dyn_ids))
        async with SqlHelper.async_session() as session:
            stmt = select(
                TLotdyninfo.dynId,
                TLotdyninfo.authorName,
                TLotdyninfo.up_uid,
                TLotdyninfo.dynamicUrl,
            ).where(TLotdyninfo.dynId.in_(ids))
            found: dict[int, tuple[str | None, int | None, str | None]] = {}
            for row in await session.execute(stmt):
                found[int(row.dynId)] = (
                    row.authorName or None,
                    int(row.up_uid) if row.up_uid else None,
                    row.dynamicUrl or None,
                )
            for dyn_id in ids:
                detail = found.get(dyn_id)
                author_name = detail[0] if detail else None
                items.append(
                    OthersLotDynDetailItem(
                        dyn_id=dyn_id,
                        exists=detail is not None,
                        title=_others_lot_dyn_title(author_name, dyn_id)
                        if detail
                        else None,
                        cover=None,
                        jumpUrl=(detail[2] if detail else None) or None,
                        authorMid=(detail[1] if detail else None),
                        authorName=author_name,
                    )
                )
    except Exception as e:  # noqa: BLE001
        logger.warning(f"[check_others_lot_dyn_exist] 批量查询失败: {e}")
        items = []
    if items and not any(item.exists for item in items):
        await _log_others_lot_dyn_miss([item.dyn_id for item in items])
    return CommonResponseModel(data=CheckOthersLotDynExistRpcResult(items=items))
