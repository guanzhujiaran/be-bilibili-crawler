import ast
import json
from enum import StrEnum
from typing import List

from CONFIG import CONFIG
from Models.lottery_database.bili.LotteryDataModels import BiliLotStatisticLotTypeEnum, \
    BiliLotStatisticRankTypeEnum, BiliUserInfoSimple, BiliLotStatisticRankDateTypeEnum
from Utils.通用.Common import asyncio_gather
from Utils.redisTool.RedisManager import RedisManagerBase


class LotteryDataStatisticRedis(RedisManagerBase):
    class RedisMap(StrEnum):
        lot_type_rank = 'LotteryDataStatisticRedis:{date}:{lot_type}:{rank_type}_prize'  # 转发抽奖类
        lot_sync_ts = 'LotteryDataStatisticRedis:{lot_type}:sync_ts'
        bili_user_uid_face_name = 'LotteryDataStatisticRedis:user_info'

        @classmethod
        def get_lot_sync_ts(
                cls,
                lot_type: BiliLotStatisticLotTypeEnum):
            return cls.lot_sync_ts.format(lot_type=lot_type)

        @classmethod
        def get_lot_type_rank_name(
                cls,
                date: BiliLotStatisticRankDateTypeEnum,
                lot_type: BiliLotStatisticLotTypeEnum,
                rank_type: BiliLotStatisticRankTypeEnum):
            return cls.lot_type_rank.format(date=date, lot_type=lot_type, rank_type=rank_type)

    def __init__(self):
        super().__init__(db=CONFIG.database.lotDataRedisObj.db,
                         host=CONFIG.database.lotDataRedisObj.host,
                         port=CONFIG.database.lotDataRedisObj.port, )
   
    async def set_sync_ts(self, lot_type: BiliLotStatisticLotTypeEnum, ts: int):
        return await self._set(self.RedisMap.get_lot_sync_ts(lot_type=lot_type), ts)

    async def get_sync_ts(self, lot_type: BiliLotStatisticLotTypeEnum):
        if res := await self._get(self.RedisMap.get_lot_sync_ts(lot_type=lot_type)):
            return int(res)
        return 0



lottery_data_statistic_redis = LotteryDataStatisticRedis()
