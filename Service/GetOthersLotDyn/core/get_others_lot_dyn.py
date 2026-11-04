import asyncio
import datetime
import time
from typing import Sequence

from log.base_log import get_others_lot_logger as get_others_lot_log
from CONFIG import settings
from Service.GetOthersLotDyn.core.robot import (
    GetOthersLotDynRobot,
    get_others_lot_dyn_robot,
)
from Service.GetOthersLotDyn.core.user_pool_service import (
    LotUserPoolSummary,
    UserPoolCullSummary,
    UserPoolSupplementSummary,
    lot_user_pool_service,
)
from Service.GetOthersLotDyn.filter.lottery_filter import is_need_lot, solve_return_lot
from Service.GetOthersLotDyn.Sql.models import TLotmaininfo
from Service.GetOthersLotDyn.Sql.sql_helper import (
    SqlHelper,
    TargetUserItem,
    get_other_lot_redis_manager,
)
from Service.GrpcModule.GrpcSrc.SQLObject.DynDetailSqlHelperMysqlVer import (
    grpc_sql_helper,
)
from Service.GrpcModule.GrpcSrc.SQLObject.models import Lotdata
from Service.opus新版官方抽奖.预约抽奖.db.models import TUpReserveRelationInfo
from Service.opus新版官方抽奖.预约抽奖.db.sqlHelper import (
    bili_reserve_sqlhelper as mysq,
)
from Utils.推送.PushMe import a_pushme


class GetOthersLotDyn:
    """
    获取更新的抽奖，如果时间在1天之内，那么直接读取文件获取结果，将结果返回回去
    """

    def __init__(self):
        self.is_getting_dyn_flag_lock = asyncio.Lock()
        self.is_getting_dyn_flag = False
        self.robot: GetOthersLotDynRobot | None = None
        self.get_dyn_ts = 0

    async def get_get_dyn_ts(self):
        get_dyn_ts = await get_other_lot_redis_manager.get_get_dyn_ts()
        if not get_dyn_ts:
            latest_round: TLotmaininfo | None = await SqlHelper.getLatestFinishedRound()
            if latest_round and latest_round.updated_at:
                return int(latest_round.updated_at.timestamp())
        return get_dyn_ts

    # region 主函数 （包括获取普通新抽奖，推送官方抽奖，推送大奖，推送预约抽奖）
    async def get_new_dyn(self) -> list[dict]:
        """
        主函数，获取一般最新的抽奖

        用户池管理（剔除 + 补充）与总结推送统一由 robot.main() 在采集结束后执行，
        这里不再重复调用，避免同一轮被处理两次。
        """
        while 1:
            async with self.is_getting_dyn_flag_lock:
                if self.is_getting_dyn_flag:
                    await asyncio.sleep(30)
                    continue
                else:
                    self.is_getting_dyn_flag = True
                    break
        self.get_dyn_ts = await self.get_get_dyn_ts()
        get_others_lot_log.info(
            f"上次获取第三方抽奖动态时间：{datetime.datetime.fromtimestamp(self.get_dyn_ts)}"
        )
        if (
            int(time.time()) - self.get_dyn_ts
            >= settings.get_others_lot.get_dyn_interval
        ):
            # 复用全局单例机器人（与其它爬虫保持一致，不再每次新建）
            self.robot = get_others_lot_dyn_robot
            await self.robot.main()
            await get_other_lot_redis_manager.set_get_dyn_ts(int(time.time()))
        self.is_getting_dyn_flag = False
        return await self.solve_return_lot()

    async def _get_users_from_latest_lot_dyn_comment(
        self, pick_count: int = 5, exclude_uids: set[int] | None = None
    ) -> list[TargetUserItem]:
        """委托给 LotUserPoolService：从近期高互动抽奖动态评论区挖掘参与者"""
        return await lot_user_pool_service._get_users_from_latest_lot_dyn_comment(
            pick_count=pick_count, exclude_uids=exclude_uids
        )

    async def _supplement_users(self) -> UserPoolSupplementSummary:
        """委托给 LotUserPoolService 的补充阶段"""
        return await lot_user_pool_service.supplement_users()

    async def _cull_users(self) -> UserPoolCullSummary:
        """委托给 LotUserPoolService 的剔除阶段"""
        return await lot_user_pool_service.cull_users()

    async def _manage_user_list(self) -> LotUserPoolSummary:
        """委托给 LotUserPoolService 的用户池管理（剔除 + 补充）"""
        return await lot_user_pool_service.manage_user_list()

    async def _push_round_summary(self, summary: LotUserPoolSummary) -> None:
        """委托给 LotUserPoolService 的总结推送"""
        await lot_user_pool_service.push_round_summary(summary)

    async def get_official_lot_dyn(self) -> list[str]:
        """
        返回官方抽奖信息，结尾是tab=1
        SVM 必抽判断通过独立批量查询 t_lot_extra_info 子表获取，与主表查询解耦
        :return:
        """
        recent_official_lot_data: Sequence[
            Lotdata
        ] = await grpc_sql_helper.query_official_lottery_by_timelimit(
            time_limit=30 * 24 * 3600, order_by_ts_desc=False
        )
        # 批量查询 extra_info，一次 SQL 得到所有结果
        extra_map = await grpc_sql_helper.get_extra_info_map(
            [x.lottery_id for x in recent_official_lot_data]
        )
        ret_list = []
        for x in recent_official_lot_data:
            if (
                x.lottery_id in extra_map
                and extra_map[x.lottery_id].is_grand_prize == 1
            ):
                # 忽略两天以内的
                if x.lottery_time - int(time.time()) < 2 * 3600 * 24:
                    continue
                ret_list.append(f"https://t.bilibili.com/{x.business_id}?tab=1")
        if ret_list:
            await a_pushme(
                f"必抽的官方抽奖【{len(ret_list)}】条", "\n".join(ret_list), "text"
            )
        return ret_list

    async def get_unignore_Big_lot_dyn(
        self, time_limit: int | None = None
    ) -> list[str]:
        """
        获取必抽的大奖，SVM 判断结果从 t_lot_extra_info 子表读取
        :return:
        """
        all_lot = await SqlHelper.getAllLotDynByTimeLimit()
        all_lot = [x for x in all_lot if is_need_lot(x, self.get_dyn_ts)]
        if not all_lot:
            return []
        dyn_ids = [int(x.dynId) for x in all_lot]
        grand_prize_flags = await SqlHelper.get_extra_info_by_ref_ids(dyn_ids, "common")
        ret_list = []
        for x in all_lot:
            if grand_prize_flags.get(int(x.dynId), 0) == 1:
                ret_list.append(x.dynamicUrl)
        if ret_list:
            await a_pushme(
                f"必抽的大奖【{len(ret_list)}】条", "\n".join(ret_list), "text"
            )
        return ret_list

    async def get_unignore_reserve_lot_space(self) -> list[TUpReserveRelationInfo]:
        all_lots = await mysq.get_all_available_reserve_lotterys()
        recent_lots: list[TUpReserveRelationInfo] = [
            x
            for x in all_lots
            if x.etime and x.etime - int(time.time()) < 2 * 3600 * 24
        ]
        ret_list = []
        ret_info_list = []
        for x in recent_lots:
            if x.is_grand_prize == 1:
                ret_info_list.append(
                    " ".join([f"https://space.bilibili.com/{x.upmid}/dynamic", x.text])
                )
                ret_list.append(x)
        if ret_info_list:
            await a_pushme(
                f"必抽的预约抽奖【{len(ret_info_list)}】条",
                "\n".join(ret_info_list),
                "text",
            )
        return ret_list

    # endregion

    # region 获取抽奖csv里的数据
    async def solve_return_lot(self, time_limit: int | None = None) -> list[dict]:
        """
        解析并过滤抽奖，直接从数据库读取，按插入时间过滤，按动态发布时间排序
        委托给 lottery_filter.solve_return_lot 独立函数
        :return:
        """
        return await solve_return_lot(
            time_limit=time_limit or settings.get_others_lot.dyn_time_limit,
            get_dyn_ts=self.get_dyn_ts,
        )

    # endregion


get_others_lot_dyn = GetOthersLotDyn()

if __name__ == "__main__":

    async def _test_main():
        await get_others_lot_dyn.get_new_dyn()

    async def _test_supplement():
        summary = await get_others_lot_dyn._supplement_users()
        print(summary)

    async def _test_cull():
        summary = await get_others_lot_dyn._cull_users()
        print(summary)

    # asyncio.run(_test_supplement())
    # asyncio.run(_test_cull())
    asyncio.run(_test_supplement())
