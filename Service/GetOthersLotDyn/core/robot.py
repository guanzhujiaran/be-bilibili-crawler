from typing import Any, Set, AsyncGenerator
from pydantic import ConfigDict

from log.base_log import get_others_lot_logger as get_others_lot_log
from Models.get_other_lot_dyn.dyn_robot_model import RobotScrapyInfo
from Models.base.custom_pydantic import CustomBaseModelHashable
from Service.GetOthersLotDyn.core.bili_dynamic_item import BiliDynamicItem
from Service.GetOthersLotDyn.fetcher.space_dynamic_fetcher import BiliSpaceUserItem
from CONFIG import settings
from Service.GetOthersLotDyn.Sql.models import TLotmaininfo
from Service.GetOthersLotDyn.Sql.sql_helper import SqlHelper, get_other_lot_redis_manager
from Service.opus新版官方抽奖.Model.BaseLotModel import ProgressCounter
from Utils.通用.Common import sem_gen
from Utils.数据库.SqlalchemyTool import sqlalchemy_model_2_dict

from Service.BaseCrawler.CrawlerType import UnlimitedCrawler
from Service.BaseCrawler.config import GetOthersLotDynRobotConfig
from Service.BaseCrawler.model.base import WorkerStatus


class RobotTaskParams(CustomBaseModelHashable):
    """任务参数包装：直接持有领域对象引用，交由 UnlimitedCrawler 的 worker 池处理"""
    model_config = ConfigDict(arbitrary_types_allowed=True)
    obj: object

    def __hash__(self) -> int:
        return hash(id(self.obj))


class GetOthersLotDynRobot(UnlimitedCrawler[RobotTaskParams]):
    """
    获取其他人的抽奖动态
    """

    Config = GetOthersLotDynRobotConfig

    def __init__(self):
        self.isPreviousRoundFinished = False  # 上一轮抽奖是否结束
        self.nowRound: TLotmaininfo = TLotmaininfo()
        self.username = ''
        self.nonLotteryWords = ['分享视频', '分享动态']
        self.SpareTime = settings.get_others_lot.spare_time  # 多少时间以前的就不获取别人的动态了
        self.bili_space_user_items_set: Set[BiliSpaceUserItem] = set()
        self.bili_dynamic_items_set: Set[BiliDynamicItem] = set()
        self.scrapy_info = RobotScrapyInfo()
        self.space_succ_counter = ProgressCounter()
        self.dyn_succ_counter = ProgressCounter()
        self.goto_check_dynamic_item_set: Set[BiliDynamicItem] = set()
        # 当前阶段：1=第一阶段空间动态；2=第二阶段(发起抽奖用户)空间动态；3=判定是否为抽奖
        self._phase: int = 1
        self._current_phase_objs: list = []
        # 配置（logger / 超时 / 重试 / 插件等）统一由 GetOthersLotDynRobotConfig 控制；
        # max_sem 由 main() 中的 _set_concurrency 按阶段动态调整，故此处无需传入
        super().__init__()

    def _set_concurrency(self, n: int) -> None:
        """不同阶段并发要求不同，按需调整 worker 数量与信号量"""
        self.max_sem = n
        self.sem = sem_gen(n)

    # region UnlimitedCrawler 抽象方法实现

    async def is_stop(self) -> bool:
        return False

    async def key_params_gen(
        self, params: RobotTaskParams | None = None
    ) -> AsyncGenerator[RobotTaskParams, None]:
        for obj in self._current_phase_objs:
            yield RobotTaskParams(obj=obj)

    async def handle_fetch(self, params: RobotTaskParams) -> WorkerStatus:
        obj = params.obj
        if self._phase in (1, 2):
            is_pub = self._phase == 2
            uid = obj.uid
            self.space_succ_counter.running_params.add(uid)
            try:
                await obj.get_user_space_dynamic_id(
                    isPubLotUser=is_pub,
                    SpareTime=self.SpareTime,
                    succ_counter=self.space_succ_counter,
                )
            finally:
                self.space_succ_counter.running_params.discard(uid)
            self.space_succ_counter.succ_count += 1
        elif self._phase == 3:
            self.dyn_succ_counter.running_params.add(self.__hash__())
            try:
                await obj.judge_lottery(lotRound_id=self.nowRound.lotRound_id)
            finally:
                self.dyn_succ_counter.running_params.discard(self.__hash__())
            self.dyn_succ_counter.succ_count += 1
        return WorkerStatus.complete

    # endregion

    async def __init(self):
        async def init_round():
            latest_round = await SqlHelper.getLatestRound()
            if not latest_round:
                latest_round = TLotmaininfo(
                    lotRound_id=1,
                    allNum=0,
                    lotNum=0,
                    uselessNum=0,
                    isRoundFinished=False,
                )
                self.isPreviousRoundFinished = True
            elif latest_round.isRoundFinished:
                latest_round = TLotmaininfo(
                    lotRound_id=latest_round.lotRound_id + 1,
                    allNum=0,
                    lotNum=0,
                    uselessNum=0,
                    isRoundFinished=False,
                )
                self.isPreviousRoundFinished = True
            self.nowRound = latest_round
            get_others_lot_log.critical(
                f'当前抽奖获取轮次信息：{sqlalchemy_model_2_dict(latest_round)}')
            await SqlHelper.addLotMainInfo(latest_round)

        async def init_bili_space_user():
            if redis_data := await get_other_lot_redis_manager.get_target_uid_list():
                self.bili_space_user_items_set.update(
                    [BiliSpaceUserItem(
                        uid=x.uid, lot_round_id=self.nowRound.lotRound_id) for x in redis_data]
                )
            else:
                get_others_lot_log.critical('从Redis获取抽奖用户列表失败，使用内置默认用户列表')
                default_list = [
                    319857159,
                    14017844,
                    1234306704,
                    31497476,
                    2147319744,
                    410550169,
                    646686238,
                    71583520,
                    279262754,
                    275744172,
                    332793152,
                    1397970246,
                    3493092200024392,
                    386051299,
                    381282283,
                    20958956,
                    1869690859,
                    1183157743,
                    4586734,
                    1741486871,
                    266223923,
                    646327721,
                    1803790683,
                    8544035,
                    1123570168,
                    3494361237031878,
                    223712517,
                    480906586,
                    1040677577,
                    471565816,
                    343104186,
                    2204166,
                    290089137,
                    1855888816,
                    691536906,
                    6477408,
                    1586295950,
                    1369967146,
                    40809204,
                    1992326018,
                    649407876,
                    256316789,
                    143412922,
                    1278208248,
                    499023056,
                    565064296,
                    693445761,
                    7538278,
                    3546876406139528,
                    482187762
                ]
                self.bili_space_user_items_set.update(
                    [BiliSpaceUserItem(uid=x, lot_round_id=self.nowRound.lotRound_id) for x in default_list])

        await init_round()
        await init_bili_space_user()
        get_others_lot_log.info('机器人初始化完成')

    async def main(self):
        await self.__init()
        # ---- 第一阶段：获取抽奖号的空间动态 ----
        self._phase = 1
        self._current_phase_objs = list(self.bili_space_user_items_set)
        self.space_succ_counter = ProgressCounter()
        self.space_succ_counter.total_num = len(self._current_phase_objs)
        self.space_succ_counter.is_running = True
        self._set_concurrency(settings.get_others_lot.space_dyn_concurrency)
        await self.run(init_params=None)
        self.space_succ_counter.is_running = False

        pub_lot_uid_set: Set[BiliSpaceUserItem] = set()
        for x in self.bili_space_user_items_set:
            pub_lot_uid_set.update(x.pub_lot_users)
        get_others_lot_log.critical(f'第一阶段完成，开始获取{len(pub_lot_uid_set)}个发起抽奖用户的空间动态')

        # ---- 第二阶段：获取发起抽奖用户的空间动态 ----
        self._phase = 2
        self._current_phase_objs = list(pub_lot_uid_set)
        self.space_succ_counter = ProgressCounter()
        self.space_succ_counter.total_num = len(self._current_phase_objs)
        self.space_succ_counter.is_running = True
        self._set_concurrency(settings.get_others_lot.space_dyn_concurrency)
        await self.run(init_params=None)
        self.space_succ_counter.is_running = False

        total_lot_uid_set: Set[BiliSpaceUserItem] = set()
        total_lot_uid_set.update(self.bili_space_user_items_set)
        total_lot_uid_set.update(pub_lot_uid_set)
        get_others_lot_log.critical(f'第二阶段完成，共获取了{len(pub_lot_uid_set)}个发起抽奖用户的空间动态')
        self.goto_check_dynamic_item_set = set()
        for x in total_lot_uid_set:
            self.goto_check_dynamic_item_set.update(x.dynamic_infos)
            for y in x.pub_lot_users:
                self.goto_check_dynamic_item_set.update(y.dynamic_infos)

        get_others_lot_log.critical(
            f'共{len(self.goto_check_dynamic_item_set)}条动态待判断是否为抽奖')
        # ---- 第三阶段：判断每条动态是否为抽奖 ----
        self._phase = 3
        self._current_phase_objs = list(self.goto_check_dynamic_item_set)
        self.dyn_succ_counter = ProgressCounter()
        self.dyn_succ_counter.total_num = len(self._current_phase_objs)
        self.dyn_succ_counter.is_running = True
        self._set_concurrency(settings.get_others_lot.judge_dyn_concurrency)
        await self.run(init_params=None)
        self.dyn_succ_counter.is_running = False
        await self._after_scrapy()
        self.nowRound.isRoundFinished = 1
        await SqlHelper.addLotMainInfo(self.nowRound)
        # 抽奖获取结束 尝试将这一轮获取到的非图片抽奖添加进数据库

    async def _after_scrapy(self):
        all_dyn_this_round = await SqlHelper.getAllLotDynInfoByRoundNum(self.nowRound.lotRound_id)
        all_t_lot_dyn_info = []
        all_useless_dyn_info = []
        for x in all_dyn_this_round:
            if x.isLot == 1:
                all_t_lot_dyn_info.append(x)
            else:
                all_useless_dyn_info.append(x)

        self.nowRound.allNum = len(all_dyn_this_round)
        self.nowRound.lotNum = len(all_t_lot_dyn_info)
        self.nowRound.uselessNum = len(all_useless_dyn_info)
        self.scrapy_info.all_lot_dyn_info_list = all_t_lot_dyn_info
        self.scrapy_info.all_useless_info_list = all_useless_dyn_info


# 全局单例：与其它爬虫保持一致，模块加载时构造一次，由调度器 / GetOthersLotDyn 复用。
# main() 内部会重新初始化轮次与用户列表等关键状态，故可安全跨轮次复用。
get_others_lot_dyn_robot = GetOthersLotDynRobot()
