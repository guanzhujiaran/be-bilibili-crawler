import asyncio
import gc
import traceback
from datetime import datetime

from fastapi import Body
from ApiRoutes import RouterPaths, RouterNames, RouterTags
from controller.common.base import new_router
from log.base_log import myfastapi_logger
from Models.lottery_database.bili.LotteryDataModels import reserveInfo
from Service.GetOthersLotDyn import get_others_lot_dyn
from Service.GrpcModule.GrpcSrc.SQLObject.DynDetailSqlHelperMysqlVer import grpc_sql_helper
from Service.GrpcModule.GrpcSrc.获取取关对象.GetRmFollowingListV2 import gmflv2
from Service.MQ.message.message_pub import publish_message
from Service.toutiao.src.FastApiReturns.SpaceFeedLotService.ToutiaoSpaceFeedLot import \
    toutiaoSpaceFeedLotService
from Service.zhihu.获取知乎抽奖想法.根据用户空间获取想法.GetMomentsByUser import zhihu_lotScrapy
from CONFIG import settings
from Utils.推送.PushMe import a_pushme, server_label

router = new_router()


@router.get(RouterPaths.GET_LIVE_LOTS, name=RouterNames.GET_LIVE_LOTS, description='获取redis中的所有直播相关抽奖信息', )
async def v1_get_live_lots(
        get_all: bool = False
):
    return []


# region 测试类

@router.get(RouterPaths.TEST, name=RouterNames.TEST)
async def app_avaliable_api():
    await asyncio.sleep(1)
    return 'Service is running!'


@router.get(RouterPaths.GC, name=RouterNames.GC)
async def app_avaliable_api():
    await asyncio.to_thread(gc.collect)
    return 'gc完成！'


@router.get(
    RouterPaths.TEST_PUSH_ERROR,
    name=RouterNames.TEST_PUSH_ERROR,
    tags=[RouterTags.MESSAGE_SERVICE],
    description='测试接口：故意抛错，并向消息推送微服务(message-service)发送一条测试告警，'
                '用于验证「异常 -> RabbitMQ(message 队列) -> message-service -> PushMe/PushPlus」推送链路是否正常',
)
async def test_push_error():
    """故意抛错，把测试告警直接发布到 message-service，验证消息推送微服务是否正常工作。

    说明：
    - 直接调用 publish_message 发布到 message 队列（message-service 消费者监听该队列并分发），
      绕过 PushMe 的 60s 限流/去重，保证每次调用都能可靠地触发 message-service；
      这样即便服务近期已有其它推送，也不会因限流而让本次测试告警被丢弃。
    - 发布完成后重新抛出 RuntimeError，模拟未捕获异常，接口返回 500（与线上异常一致）。
    - 全局异常处理器(@app.exception_handler)会再次尝试 a_push_error，但受 60s 限流保护，不会重复推送。
    """
    err_time = datetime.now()
    try:
        raise RuntimeError(
            f"Ciallo～(∠・ω< )⌒★ 这是一条用于测试消息推送微服务(message-service)的故意抛错喵~ "
            f"时间【{err_time}】"
        )
    except RuntimeError as e:
        tb = traceback.format_exc()
        # 直接发布到 message 队列，绕过 PushMe 限流，每次调用必触发 message-service
        await publish_message(
            title=f"{server_label()} 测试抛错(消息推送微服务)",
            content=(
                "这是一条测试告警，用于验证 message-service 推送链路是否正常。\n"
                f"触发时间: {err_time}\n"
                f"错误信息: {e}\n"
                f"堆栈:\n{tb}"
            ),
            push_type="text",
            config=settings.message_config,
        )
        raise  # 重新抛出，模拟线上未捕获异常（接口返回 500）


# endregion

# region 基于Grpc api的功能实现
@router.post(RouterPaths.POST_RM_FOLLOWING_LIST, name=RouterNames.POST_RM_FOLLOWING_LIST, response_model=list, description='获取需要取关的up主列表')
async def v1_post_rm_following_list(data: list[int | str] = Body()):
    """
    取关接口 调用的是b站appp端的grpc协议接口，没那么容易被风控
    :param data: list[int] 关注列表 直接传列表即可
    :return:
    """
    return await gmflv2.get_rm_following_list(data)


# endregion

# region 获取抽奖内容接口
@router.post(RouterPaths.UPSERT_LOT_DETAIL, name=RouterNames.UPSERT_LOT_DETAIL)
async def upsert_lot_detail(request_body: dict):
    result = await grpc_sql_helper.upsert_lot_detail(request_body)
    return result


@router.get(RouterPaths.GET_OTHERS_LOT_DYN, name=RouterNames.GET_OTHERS_LOT_DYN)
async def api_get_others_lot_dyn():
    myfastapi_logger.error('GetOthersLotDyn 开始获取B站其他用户的动态抽奖！')
    result = await get_others_lot_dyn.get_new_dyn()
    return result


@router.get(RouterPaths.GET_OTHERS_OFFICIAL_LOT_DYN, name=RouterNames.GET_OTHERS_OFFICIAL_LOT_DYN)
async def api_get_others_official_lot_dyn():
    myfastapi_logger.error('GetOthersLotDyn 开始获取别人的官方动态抽奖！')
    return await get_others_lot_dyn.get_official_lot_dyn()


@router.get(RouterPaths.GET_OTHERS_BIG_LOT, name=RouterNames.GET_OTHERS_BIG_LOT)
async def api_get_others_big_lot():
    myfastapi_logger.error('GetOthersLotDyn 开始获取别人的大奖！')
    return await get_others_lot_dyn.get_unignore_Big_lot_dyn()


@router.get(RouterPaths.GET_OTHERS_BIG_RESERVE, name=RouterNames.GET_OTHERS_BIG_RESERVE)
async def api_get_others_big_reserve() -> list[reserveInfo]:
    myfastapi_logger.error('GetOthersLotDyn 开始获取重要的预约抽奖！')
    result = await get_others_lot_dyn.get_unignore_reserve_lot_space()
    reserveInfos = []
    for i in result:  # 对df的每一行数据访问
        reserve_info = reserveInfo(
            reserve_url=f'https://space.bilibili.com/{str(i.upmid)}/dynamic',
            etime=i.etime,
            lottery_prize_info=i.text,
            jump_url=i.jumpUrl,
            reserve_sid=i.sid,
            available=True
        )
        reserveInfos.append(reserve_info)
    return reserveInfos


@router.get(RouterPaths.ZHIHU_GET_OTHERS_LOT_PINS, name=RouterNames.ZHIHU_GET_OTHERS_LOT_PINS, description='获取知乎抽奖内容，返回url列表，直接访问即可')
async def zhuhu_avaliable_api():
    myfastapi_logger.info('开始获取zhihu抽奖内容')
    resp = await zhihu_lotScrapy.api_get_all_pins()
    await a_pushme(f'获取到知乎抽奖{len(resp)}条', '\n'.join(resp)
                   )
    return resp


@router.get(RouterPaths.TOUTIAO_GET_OTHERS_LOT_IDS, name=RouterNames.TOUTIAO_GET_OTHERS_LOT_IDS)
async def toutiao_get_others_lot_ids():
    myfastapi_logger.info('开始获取toutiao抽奖内容')
    result = await toutiaoSpaceFeedLotService.main()
    result = result if result else []
    await a_pushme(f'获取到头条抽奖{len(result)}条', '\n'.join(result)
                   )
    return result

# endregion
