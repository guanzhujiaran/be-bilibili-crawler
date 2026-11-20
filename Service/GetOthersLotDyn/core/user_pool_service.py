"""第三方抽奖（GetOthersLotDyn）的用户池管理服务。

职责：
- 剔除：按「用户自己空间里转发的原动态命中抽奖数」淘汰低产出用户；
- 裁剪：把超过 ``max_user_list_size`` 的列表按窗口内命中抽奖数降序裁剪到容量上限；
- 补充：优先从近期高互动抽奖动态的转发列表挖掘参与者（转发用户才是真实参与者），
  转发列表不可用时回退评论区，补到（不超过）容量上限；
- 推送：把本轮采集与用户池变动情况推送到 PushMe。

容量上限是硬约束：``total == 0`` 豁免剔除只决定「谁被剔除」，不能让列表超过上限。

从 ``GetOthersLotDyn`` 中拆出来，是为了让定时调度路径
（``GetOthersLotDynRobot.main()``）也能直接调用：``robot.py`` 与
``get_others_lot_dyn.py`` 互相 import 会形成循环依赖，项目规范也不允许
函数内动态 import。
"""

import asyncio
import base64
import datetime
import gzip
from dataclasses import dataclass, field

from log.base_log import get_others_lot_logger as get_others_lot_log
from CONFIG import settings
from Service.GetOthersLotDyn.Sql.models import TLotmaininfo
from Service.GetOthersLotDyn.Sql.sql_helper import (
    LotUserFetchStat,
    SqlHelper,
    TargetUserItem,
    get_other_lot_redis_manager,
)
from Service.GetOthersLotDyn.parser.dynamic_detail_parser import (
    comment_type_to_dynamic_type,
)
from Service.GrpcModule.Grpc.Bapi.BiliApi import get_reply_main
from Service.GrpcModule.Grpc.Bapi.BapiUtils import gen_trace_id
from Service.GrpcModule.Grpc.GrpcProto.bilibili.app.dynamic.v2.dynamic_pb2 import (
    RepostListReq,
    RepostListRsp,
    repost_general,
)
from Service.GrpcModule.Models.CustomRequestErrorModel import Request352Error
from Utils.GrpcUtils.GrpcMsgTools import raw_resp_content_2_dict
from Utils.GrpcUtils.metadata.makeMetaData import make_metadata
from Utils.推送.PushMe import a_pushme
from Utils.代理.mdoel.RequestConf import RequestConf
from Utils.代理.mdoel.ProxyTier import ProxyTier
from Utils.代理.redisProxyRequest.RedisRequestProxy import request_with_proxy_internal

# RepostList 的 gRPC 地址（与 BiliGrpc.base_uri 一致）。
# 这里直接发原生 gRPC 帧、走 RequestConf 的代理通道，不引入 grpc_api 的 metadata 池。
_REPOST_LIST_URL = (
    "https://grpc.biliapi.net/bilibili.app.dynamic.v2.Dynamic/RepostList"
)
# 转发列表来源的整体硬预算：接口抖动 / 代理池恢复都不能把补充阶段（甚至整轮）拖住
_REPOST_FETCH_TIMEOUT_SECONDS = 60.0
# 生成 metadata 的超时：metadata 涉及多次外部请求，必须有超时兜底
_REPOST_METADATA_TIMEOUT_SECONDS = 20.0
# 单条动态的转发列表请求重试次数（每次重试由代理层重新选一条代理）
_REPOST_REQUEST_ATTEMPTS = 2


@dataclass(slots=True)
class UserPoolCullSummary:
    """剔除 + 超容量裁剪阶段摘要"""

    before_count: int
    after_count: int = 0
    removed: list[int] = field(default_factory=list)
    active_in_window: int = 0
    # 列表超过容量上限时被裁掉的 uid（按原列表顺序）
    trimmed: list[int] = field(default_factory=list)


@dataclass(slots=True)
class UserPoolSupplementSummary:
    """补充阶段摘要"""

    before_count: int
    after_count: int = 0
    added: list[int] = field(default_factory=list)
    round_id: int | None = None
    round_all: int = 0
    round_lot: int = 0
    round_useless: int = 0
    # 补充后仍超容量时的防御性截断 uid（正常应为空）
    trimmed: list[int] = field(default_factory=list)


@dataclass(slots=True)
class LotUserPoolSummary:
    """用户池管理总摘要"""

    before_count: int
    after_count: int
    added: list[int]
    removed: list[int]
    active_in_window: int
    round_id: int | None
    round_all: int
    round_lot: int
    round_useless: int
    low_active_uids: list[int] = field(default_factory=list)
    # 剔除阶段裁剪 + 补充阶段防御性截断的合计
    trimmed: list[int] = field(default_factory=list)


class LotUserPoolService:
    """用户池的剔除 / 补充 / 推送"""

    async def manage_user_list(
        self,
        low_active_uids: list[int] | None = None,
        exclude_from_cull: set[int] | None = None,
    ) -> LotUserPoolSummary:
        """管理用户列表：剔除 -> 超容量裁剪 -> 补充，聚合各阶段摘要用于推送。

        :param low_active_uids: 本轮抓到 0 条动态的用户（由 robot 阶段一统计），
            仅用于轮次总结展示；是否剔除仍由 ``cull_users`` 的窗口统计决定。
        :param exclude_from_cull: 本轮**因代理池不可用**而 0 产出的用户，
            剔除阶段必须豁免：低产出是环境问题，不是用户质量问题。
        """
        cull = await self.cull_users(exclude_uids=exclude_from_cull)
        supp = await self.supplement_users(exclude_uids=exclude_from_cull)
        return LotUserPoolSummary(
            # 进入管理前的列表长度：剔除阶段的 before_count
            before_count=cull.before_count,
            # 补充之后的最终长度：补充阶段的 after_count
            after_count=supp.after_count,
            added=supp.added,
            removed=cull.removed,
            # 容量是硬约束：剔除阶段的裁剪 + 补充阶段的防御性截断都要计入
            trimmed=cull.trimmed + supp.trimmed,
            active_in_window=cull.active_in_window,
            round_id=supp.round_id,
            round_all=supp.round_all,
            round_lot=supp.round_lot,
            round_useless=supp.round_useless,
            low_active_uids=low_active_uids or [],
        )

    async def cull_users(
        self, exclude_uids: set[int] | None = None
    ) -> UserPoolCullSummary:
        """剔除阶段：剔除低产出用户，并把列表裁剪到容量上限。

        - 剔除口径一：按最近 N 天内「转发的原动态命中抽奖数」淘汰；窗口内抓到过动态
          但命中不足的用户视为低产出；
        - 剔除口径二：窗口内一条都抓不到（``total == 0``）的用户按
          ``zero_output_rounds`` 连续计数淘汰。首轮 ``total == 0`` 仍豁免（新补用户 /
          本轮抓取失败），连续 ``zero_output_cull_rounds`` 轮才剔除，避免误杀；
        - 豁免：``exclude_uids`` 里的用户（本轮因代理池不可用而未取到数）两个口径都
          不参与剔除；
        - 超容量裁剪：豁免剔除不能突破容量上限，写回 Redis 前把列表裁剪到
          ``max_user_list_size``，位置在剔除之后、补充之前。
        """
        uid_list = await get_other_lot_redis_manager.get_target_uid_list()
        excluded = exclude_uids or set()
        summary = UserPoolCullSummary(
            before_count=len(uid_list),
            after_count=0,
            removed=[],
            active_in_window=0,
            trimmed=[],
        )
        get_others_lot_log.info(f"[剔除阶段] 当前列表: {summary.before_count}个用户")

        if not uid_list:
            get_others_lot_log.warning("[剔除阶段] 列表为空，跳过")
            return summary

        uids = [item.uid for item in uid_list]
        stats = await SqlHelper.getUserLotHitStats(
            uids, days=settings.get_others_lot.remove_check_days
        )
        stat_map = {stat.uid: stat for stat in stats}
        threshold = settings.get_others_lot.min_valid_lot_threshold
        zero_output_cull_rounds = max(
            1, settings.get_others_lot.zero_output_cull_rounds
        )
        removed_items: list[TargetUserItem] = []
        low_lot_removed: list[int] = []
        zero_output_removed: list[int] = []
        for item in uid_list:
            if item.uid in excluded:
                continue
            stat = stat_map.get(item.uid)
            if stat is not None and stat.total > 0:
                # 口径一：抓到了动态，但命中抽奖数不足
                if stat.lot <= threshold:
                    removed_items.append(item)
                    low_lot_removed.append(item.uid)
            elif item.zero_output_rounds >= zero_output_cull_rounds:
                # 口径二：连续多轮 total == 0（stat 缺失等价于 total == 0）
                removed_items.append(item)
                zero_output_removed.append(item.uid)

        if excluded:
            get_others_lot_log.info(
                f"[剔除阶段] 豁免{len(excluded)}个本轮因代理池不可用未取到数的用户"
            )
        if low_lot_removed:
            get_others_lot_log.info(
                f"[剔除阶段] 按{settings.get_others_lot.remove_check_days}天内"
                f"命中抽奖数<={threshold}剔除: {low_lot_removed}"
            )
        if zero_output_removed:
            get_others_lot_log.info(
                f"[剔除阶段] 连续{zero_output_cull_rounds}轮无产出(total=0)剔除: "
                f"{zero_output_removed}"
            )
        if removed_items:
            removed_uids = {item.uid for item in removed_items}
            summary.removed = [item.uid for item in removed_items]
            uid_list = [item for item in uid_list if item.uid not in removed_uids]

        max_size = settings.get_others_lot.max_user_list_size
        uid_list, summary.trimmed = self._trim_to_capacity(uid_list, stats, max_size)
        if summary.trimmed:
            get_others_lot_log.info(
                f"[剔除阶段] 超过容量上限{max_size}，按窗口内命中抽奖数降序"
                f"超容量裁剪: {len(summary.trimmed)}个 {summary.trimmed}"
            )

        await get_other_lot_redis_manager.set_target_uid_list(uid_list)
        summary.after_count = len(uid_list)
        # 活跃数按最终保留下来的列表统计，避免裁剪掉的用户仍被计入
        summary.active_in_window = sum(
            1
            for item in uid_list
            if (stat := stat_map.get(item.uid)) is not None
            and stat.lot > threshold
        )
        get_others_lot_log.info(
            f"[剔除阶段] 完成: {summary.before_count} -> {summary.after_count}个用户 "
            f"(活跃: {summary.active_in_window}, 裁剪: {len(summary.trimmed)})"
        )
        return summary

    @staticmethod
    def _trim_to_capacity(
        uid_list: list[TargetUserItem],
        stats: list[LotUserFetchStat],
        max_size: int,
    ) -> tuple[list[TargetUserItem], list[int]]:
        """把列表裁剪到容量上限，返回 ``(保留列表, 被裁掉的 uid 列表)``。

        排序规则（明确且稳定，保证幂等）：
        1. 主键：窗口内命中抽奖数 ``stat.lot`` 降序；统计缺失（含 ``total == 0`` 的
           新补充 / 抓取失败用户）视为 ``lot = 0``，排在后面；
        2. 同分：按 Redis 原列表顺序保留（``sorted`` 是稳定排序，先入池者优先），
           保留结果也按原列表顺序输出，不改动既有顺序；
        3. ``trimmed`` 按原列表顺序输出，便于日志 / 推送对照。

        幂等：``len(uid_list) <= max_size`` 时原样返回且 ``trimmed`` 为空，
        因此连续跑两轮时第二轮不会再触发裁剪。
        """
        if len(uid_list) <= max_size:
            return uid_list, []
        lot_map = {stat.uid: stat.lot for stat in stats}
        ranked = sorted(
            enumerate(uid_list), key=lambda pair: -lot_map.get(pair[1].uid, 0)
        )
        keep_indexes = {index for index, _ in ranked[:max_size]}
        kept = [
            item for index, item in enumerate(uid_list) if index in keep_indexes
        ]
        trimmed = [
            item.uid
            for index, item in enumerate(uid_list)
            if index not in keep_indexes
        ]
        return kept, trimmed

    async def supplement_users(
        self, exclude_uids: set[int] | None = None
    ) -> UserPoolSupplementSummary:
        """补充阶段：从转发列表 / 评论区挖掘参与者 + 充实用户信息，补到（但不超过）列表容量。

        :param exclude_uids: 本轮因代理池不可用未取到数的用户：
            既不会被新来源重复补充，也不累计 ``zero_output_rounds``（环境问题不算低产出）。
        """
        uid_list = await get_other_lot_redis_manager.get_target_uid_list()
        summary = UserPoolSupplementSummary(
            before_count=len(uid_list),
            after_count=0,
            added=[],
            round_id=None,
            round_all=0,
            round_lot=0,
            round_useless=0,
            trimmed=[],
        )
        get_others_lot_log.info(f"[补充阶段] 当前列表: {summary.before_count}个用户")

        latest_round: TLotmaininfo | None = await SqlHelper.getLatestFinishedRound()
        if latest_round:
            summary.round_id = latest_round.lotRound_id
            summary.round_all = latest_round.allNum or 0
            summary.round_lot = latest_round.lotNum or 0
            summary.round_useless = latest_round.uselessNum or 0

        max_size = settings.get_others_lot.max_user_list_size
        need_count = max_size - len(uid_list)
        if need_count > 0:
            pick_count = min(
                need_count, settings.get_others_lot.supplement_max_per_round
            )
            existing = {item.uid for item in uid_list}
            new_users = await self._collect_supplement_users(
                pick_count=pick_count, exclude_uids=existing
            )
            # 容量硬约束：即使来源返回多于缺口，也只补到上限
            for user in new_users[:need_count]:
                uid_list.append(user)
                summary.added.append(user.uid)

        # 兜底：触发语义不变（只有列表完全为空时才使用内置默认名单），
        # 但数量必须受容量上限约束，避免默认名单一次性突破上限。
        if not uid_list:
            defaults = settings.get_others_lot.default_user_uids
            get_others_lot_log.warning(
                f"[补充阶段] 评论区未收集到用户，使用默认用户列表 ({len(defaults)}个)"
            )
            uid_list = [TargetUserItem(uid=uid) for uid in defaults[:max_size]]
            summary.added = [item.uid for item in uid_list]

        # 容量是硬约束：任何路径都不得让写回 Redis 的列表超过上限
        if len(uid_list) > max_size:
            summary.trimmed = [item.uid for item in uid_list[max_size:]]
            uid_list = uid_list[:max_size]
            get_others_lot_log.warning(
                f"[补充阶段] 补充后仍超过容量上限{max_size}，防御性截断: "
                f"{len(summary.trimmed)}个 {summary.trimmed}"
            )

        await self._enrich_user_info(
            uid_list,
            latest_round,
            exclude_uids=exclude_uids,
            new_uids=set(summary.added),
        )

        await get_other_lot_redis_manager.set_target_uid_list(uid_list)
        summary.after_count = len(uid_list)
        get_others_lot_log.info(
            f"[补充阶段] 完成: {summary.before_count} -> {summary.after_count}个用户 "
            f"(新增 {len(summary.added)}个)"
        )
        return summary

    async def _enrich_user_info(
        self,
        uid_list: list[TargetUserItem],
        latest_round: TLotmaininfo | None,
        exclude_uids: set[int] | None = None,
        new_uids: set[int] | None = None,
    ) -> None:
        """补齐用户名 / 最近抽奖发布时间 / 本轮产出统计 / 连续零产出轮数。

        ``zero_output_rounds`` 只在拿到已结束轮次时累计：
        - 本轮因代理池不可用未取到数的用户（``exclude_uids``）不计入，环境问题不算低产出；
        - 刚入池的新用户（``new_uids``）不计入，它们还没参与过任何一轮采集，
          首轮 total=0 必须豁免。
        """
        excluded = exclude_uids or set()
        fresh = new_uids or set()
        now_ts = int(datetime.datetime.now().timestamp())
        uids = [item.uid for item in uid_list]

        latest_dyns = await SqlHelper.getLatestLotDynInfoByUidList(uids)
        latest_dyn_map: dict[int, tuple[int, datetime.datetime | None, str]] = {}
        for d in latest_dyns:
            if d.pubTime:
                latest_dyn_map[int(d.up_uid)] = (
                    int(d.pubTime.timestamp()),
                    d.pubTime,
                    d.authorName or "",
                )

        user_info_map = await SqlHelper.getLotUserInfoByUidList(uids)

        round_stats: list[LotUserFetchStat] = []
        if latest_round:
            round_stats = await SqlHelper.getUserLotHitStatsByRound(
                uids, latest_round.lotRound_id
            )
        round_stat_map = {stat.uid: stat for stat in round_stats}

        no_uname_uids: list[int] = []
        for item in uid_list:
            uid = item.uid
            if not item.uname:
                dyn_info = latest_dyn_map.get(uid)
                if dyn_info:
                    item.uname = dyn_info[2] or ""
                if not item.uname and user_info_map.get(uid):
                    item.uname = user_info_map[uid].uname or ""

            dyn_info = latest_dyn_map.get(uid)
            if dyn_info:
                item.last_dyn_pub_ts = dyn_info[0]
                item.last_dyn_pub_datetime = dyn_info[1]

            if not item.uname:
                no_uname_uids.append(uid)

            stat = round_stat_map.get(uid)
            item.last_round_lot_count = stat.lot if stat else 0
            item.last_round_total_count = stat.total if stat else 0
            if not item.added_ts:
                item.added_ts = now_ts
            if latest_round is not None and uid not in excluded and uid not in fresh:
                if item.last_round_total_count == 0:
                    item.zero_output_rounds += 1
                else:
                    item.zero_output_rounds = 0

        if no_uname_uids:
            get_others_lot_log.warning(
                f"[补充阶段] {len(no_uname_uids)}个用户未能获取到用户名: {no_uname_uids}"
            )

    async def _collect_supplement_users(
        self, pick_count: int, exclude_uids: set[int] | None = None
    ) -> list[TargetUserItem]:
        """按「转发列表优先 + 评论区兜底」收集新用户，合并去重后截断到 ``pick_count``。

        转发列表来源失败 / 无结果时不会抛错，直接由评论区把剩余名额补齐，
        因此轮次不会被转发列表接口拖住。
        """
        seen: set[int] = set(exclude_uids or ())
        result: list[TargetUserItem] = []
        sources = [self._get_users_from_latest_lot_dyn_comment]
        if settings.get_others_lot.repost_user_fetch:
            # 转发用户才是真实参与者：优先转发列表，评论区只做兜底
            sources.insert(0, self._get_users_from_latest_lot_dyn_repost)

        for source in sources:
            remaining = pick_count - len(result)
            if remaining <= 0:
                break
            users = await source(pick_count=remaining, exclude_uids=seen)
            for user in users:
                if user.uid in seen:
                    continue
                seen.add(user.uid)
                result.append(user)
                if len(result) >= pick_count:
                    break
        return result[:pick_count]

    async def _get_users_from_latest_lot_dyn_repost(
        self, pick_count: int = 5, exclude_uids: set[int] | None = None
    ) -> list[TargetUserItem]:
        """从近期高互动抽奖动态的转发列表挖掘参与者（转发用户才是真实参与者）。

        转发列表是 gRPC 接口（``Dynamic/RepostList``），走公共代理池 FAST 档；
        整体有硬超时，任何失败只记 warning 并返回空列表，由调用方回退评论区。
        """
        if not settings.get_others_lot.repost_user_fetch:
            return []
        try:
            return await asyncio.wait_for(
                self._fetch_users_from_repost_lists(pick_count, exclude_uids),
                timeout=_REPOST_FETCH_TIMEOUT_SECONDS,
            )
        except TimeoutError:
            get_others_lot_log.warning(
                f"[补充阶段] 转发列表超过{_REPOST_FETCH_TIMEOUT_SECONDS:.0f}s未完成，"
                f"回退评论区"
            )
        except Exception as e:
            get_others_lot_log.warning(
                f"[补充阶段] 转发列表来源不可用，回退评论区：{type(e).__name__} {e}"
            )
        return []

    async def _fetch_users_from_repost_lists(
        self, pick_count: int, exclude_uids: set[int] | None = None
    ) -> list[TargetUserItem]:
        """逐条高互动动态拉取转发列表首页，凑够 ``pick_count`` 即停。"""
        hot_dyn_days = settings.get_others_lot.hot_lot_dyn_days
        hot_dyn_count = settings.get_others_lot.hot_lot_dyn_count
        hot_dyns = await SqlHelper.getRecentHotLotDyns(
            days=hot_dyn_days, top_n=hot_dyn_count
        )
        if not hot_dyns:
            get_others_lot_log.warning(
                "[补充阶段] 数据库中未找到近期高互动抽奖动态，跳过转发列表来源"
            )
            return []

        # 每轮单独构建一份 metadata（不复用 grpc_api 的 metadata 池）：
        # 这条来源是可失败 / 可超时的旁路，不能因为取消而污染共享池的计数。
        md, _ticket, _basic = await asyncio.wait_for(
            make_metadata(""),
            timeout=_REPOST_METADATA_TIMEOUT_SECONDS,
        )
        headers = self._build_grpc_headers(md)
        if not headers.get("x-bili-ticket"):
            raise ValueError("转发列表请求缺少 x-bili-ticket")
        # 转发列表是 gRPC 通道：走公共代理池 FAST 档（不落到固定自建出口），
        # 返回原始响应由调用方按 protobuf 解析
        request_conf = RequestConf(
            pool_only=True,
            tier=ProxyTier.FAST,
            is_return_raw_response=True,
            risk_scope="grpc:dynamic",
        )

        seen: set[int] = set(exclude_uids or ())
        result: list[TargetUserItem] = []
        for dyn in hot_dyns:
            if len(result) >= pick_count:
                break
            resp_dict = await self._request_repost_list_with_retry(
                int(dyn.dynId), headers, request_conf
            )
            dyn_added = 0
            for item in resp_dict.get("list") or []:
                user = self._extract_repost_user(item)
                if user is None or user.uid in seen:
                    continue
                seen.add(user.uid)
                result.append(user)
                dyn_added += 1
                if len(result) >= pick_count:
                    break
            if dyn_added > 0:
                get_others_lot_log.debug(
                    f"[补充阶段] 动态 dynamic_id={dyn.dynId} "
                    f"({dyn.commentCount}评{dyn.repostCount}转) "
                    f"转发列表收集到{dyn_added}个新用户"
                )

        if not result:
            get_others_lot_log.warning("[补充阶段] 转发列表未收集到新用户，回退评论区")
            return []
        get_others_lot_log.info(
            f"[补充阶段] 从{hot_dyn_days}天内高互动抽奖动态的转发列表收集到"
            f"{len(result)}个用户: {[f'{u.uid}({u.uname})' for u in result]}"
        )
        return result

    async def _request_repost_list_with_retry(
        self, dynamic_id: int, headers: dict[str, str], request_conf: RequestConf
    ) -> dict:
        """带重试预算的转发列表请求：失败只记 warning，返回空 dict 由上层继续下一条。"""
        last_error: Exception | None = None
        for attempt in range(1, _REPOST_REQUEST_ATTEMPTS + 1):
            try:
                return await self._request_repost_list(
                    dynamic_id, headers, request_conf
                )
            except Exception as e:
                last_error = e
                get_others_lot_log.debug(
                    f"[补充阶段] 转发列表请求失败（dynamic_id={dynamic_id}，"
                    f"第{attempt}/{_REPOST_REQUEST_ATTEMPTS}次）："
                    f"{type(e).__name__} {e}"
                )
        get_others_lot_log.warning(
            f"[补充阶段] 转发列表请求放弃（dynamic_id={dynamic_id}，"
            f"已重试{_REPOST_REQUEST_ATTEMPTS}次）：{type(last_error).__name__} "
            f"{last_error}"
        )
        return {}

    async def _request_repost_list(
        self, dynamic_id: int, headers: dict[str, str], request_conf: RequestConf
    ) -> dict:
        """构造并发送一条 RepostList gRPC 请求，返回解析后的响应 dict。"""
        resp = await request_with_proxy_internal.request_with_proxy(
            method="POST",
            url=_REPOST_LIST_URL,
            data=self._encode_grpc_frame(self._build_repost_list_req(dynamic_id)),
            headers=headers,
            request_conf=request_conf,
        )
        bili_status = str(resp.headers.get("bili-status-code") or "")
        grpc_message = str(resp.headers.get("grpc-message") or "")
        if bili_status == "-352" or "-352" in grpc_message:
            raise Request352Error(
                f"转发列表 -352 风控：{grpc_message or bili_status}", -352
            )
        grpc_status = str(resp.headers.get("grpc-status") or "")
        if grpc_status and grpc_status != "0":
            raise RuntimeError(
                f"转发列表 grpc-status={grpc_status} {grpc_message}".strip()
            )
        if not resp.content or len(resp.content) <= 5:
            return {}
        return raw_resp_content_2_dict(
            raw_resp=resp, protobuf_msg=RepostListRsp(), is_gzip=True
        )

    @staticmethod
    def _build_repost_list_req(dynamic_id: int) -> RepostListReq:
        """构造转发列表请求：dynamic_id 已能唯一定位动态，不再传 rid / dyn_type，
        避免评论业务类型与动态类型两套编码混用导致取不到数据。"""
        return RepostListReq(
            dynamic_id=str(int(dynamic_id)),
            repost_type=repost_general,
        )

    @staticmethod
    def _encode_grpc_frame(message) -> bytes:
        """gRPC 帧：压缩标志 + 4 字节长度 + body。

        metadata 里声明了 ``grpc-encoding: gzip``，body 必须真的 gzip，
        否则服务端解不开（见 test/grpc_test.py 的历史教训）。
        """
        compressed = gzip.compress(message.SerializeToString(), compresslevel=6)
        return b"\x01" + len(compressed).to_bytes(4, "big") + compressed

    @staticmethod
    def _build_grpc_headers(md: tuple) -> dict[str, str]:
        """gRPC metadata -> 请求头字典（二进制头做 base64 去 padding）。"""
        headers: dict[str, str] = {}
        for key, value in md:
            if isinstance(value, bytes):
                headers[key] = base64.b64encode(value).decode("utf-8").strip("=")
            elif key == "x-bili-trace-id":
                headers[key] = gen_trace_id()
            else:
                headers[key] = str(value)
        headers["content-type"] = "application/grpc"
        return headers

    @classmethod
    def _extract_repost_user(cls, item: dict) -> TargetUserItem | None:
        """从转发列表的 DynamicItem 中取转发者（module_author）。"""
        author = None
        for module in item.get("modules") or []:
            author = module.get("moduleAuthor") or module.get("module_author")
            if author:
                break
        if not author:
            return None
        uid = author.get("mid")
        uname = (author.get("author") or {}).get("name") or ""
        if not cls._is_valid_user(uid, uname):
            return None
        return TargetUserItem(uid=int(uid), uname=str(uname).strip())

    @staticmethod
    def _is_valid_user(uid: int | str | None, uname: str | None) -> bool:
        """新补用户的基本校验：uid 为正整数且位数合理，uname 非空。"""
        try:
            uid_int = int(uid)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            return False
        if uid_int <= 0 or len(str(uid_int)) > 19:
            return False
        return bool(str(uname or "").strip())

    async def _get_users_from_latest_lot_dyn_comment(
        self, pick_count: int = 5, exclude_uids: set[int] | None = None
    ) -> list[TargetUserItem]:
        """从近期高互动抽奖动态的评论区挖掘参与者。

        评论接口使用 type + rid 形式，其中 type 与动态类型不是同一套编码，
        映射见下方 ``comment_type``；游客态同样可以取到评论。
        """
        hot_dyn_days = settings.get_others_lot.hot_lot_dyn_days
        hot_dyn_count = settings.get_others_lot.hot_lot_dyn_count

        hot_dyns = await SqlHelper.getRecentHotLotDyns(
            days=hot_dyn_days, top_n=hot_dyn_count
        )
        if not hot_dyns:
            get_others_lot_log.warning(
                "数据库中未找到近期高互动抽奖动态记录，无法从评论中获取用户"
            )
            return []

        result: list[TargetUserItem] = []
        seen: set[int] = set(exclude_uids) if exclude_uids else set()
        # rawJsonStr 已拆到 t_lotdyninfo_raw：一次主键 IN 批量取，避免逐行 N+1
        raw_json_map = await SqlHelper.get_raw_json_by_dyn_ids(
            [dyn.dynId for dyn in hot_dyns]
        )

        for dyn in hot_dyns:
            dyn_id = str(dyn.dynId)
            rid = None
            _type = None
            raw_json = raw_json_map.get(int(dyn.dynId))
            if raw_json:
                basic = raw_json.get("basic") or {}
                rid = basic.get("comment_id_str")
                _type = comment_type_to_dynamic_type(basic.get("comment_type"))
            if not rid or not _type:
                get_others_lot_log.debug(
                    f"动态 dynamic_id={dyn_id} 的 rawJsonStr 中缺少 rid/type，跳过"
                )
                continue

            # 评论接口用游标翻页：首页 next=0，后续页取响应里的 data.cursor.next，
            # 不能简单用 1、2、3 递增（那样只会拿到 1 条且很快就 cursor=0 结束）。
            next_cursor: int | str = 0
            page = 0
            dyn_added = 0
            while len(result) < pick_count and next_cursor is not None:
                try:
                    reply = await get_reply_main(
                        dyn_id,
                        rid,
                        next_cursor,
                        str(_type),
                        mode=2,
                        request_conf=RequestConf(
                            is_use_available_proxy=True,
                            pool_only=True,
                            tier=ProxyTier.FAST,
                        ),
                    )
                    if not reply or reply.get("code") != 0:
                        break
                    data = reply.get("data") or {}
                    replies = data.get("replies", [])
                    if not replies:
                        break
                    page += 1
                    for c in replies:
                        mid = c.get("mid")
                        if not mid:
                            continue
                        uid = int(mid)
                        if uid in seen:
                            continue
                        uname = (c.get("member") or {}).get("uname", "")
                        # 新补用户基本校验：uid 有效 + uname 非空，避免脏数据占坑
                        if not self._is_valid_user(uid, uname):
                            continue
                        result.append(
                            TargetUserItem(uid=uid, uname=str(uname).strip())
                        )
                        seen.add(uid)
                        dyn_added += 1
                        if len(result) >= pick_count:
                            break
                    cursor = data.get("cursor") or {}
                    next_cursor = cursor.get("next")
                    if not next_cursor:
                        break
                except Exception as e:
                    get_others_lot_log.debug(
                        f"获取动态 dynamic_id={dyn_id} 第{page + 1}页评论失败：{e}"
                    )
                    break

            if dyn_added > 0:
                get_others_lot_log.debug(
                    f"从动态 dynamic_id={dyn_id} ({dyn.commentCount}评{dyn.repostCount}转) "
                    f"{page}页评论中收集到{dyn_added}个新用户"
                )

            if len(result) >= pick_count:
                break

        if not result:
            get_others_lot_log.warning("所有近期高互动抽奖动态均未收集到评论用户")
            return []

        get_others_lot_log.info(
            f"从{hot_dyn_days}天内高互动抽奖动态的评论中收集到{len(result)}个用户: "
            f"{[f'{u.uid}({u.uname})' for u in result]}"
        )
        return result

    async def push_round_summary(self, summary: "LotUserPoolSummary") -> None:
        """推送本轮数据采集与用户池管理的总结到 PushMe"""
        days = settings.get_others_lot.remove_check_days
        lines = [
            f"轮次ID: {summary.round_id}",
            f"检查动态: {summary.round_all}条 (有效抽奖 {summary.round_lot}条, 无效 {summary.round_useless}条)",
            f"用户列表: {summary.before_count} -> {summary.after_count}个",
            # 剔除 / 超容量裁剪 / 新补充三行恒定输出：池子收敛后（无剔除、无补充）
            # 的轮次也要能看到三个数字，便于确认「稳定在容量上限、没有反弹」。
            f"剔除(低产出/连续无产出): {len(summary.removed)}个 {summary.removed}",
            f"超容量裁剪(上限{settings.get_others_lot.max_user_list_size}): "
            f"{len(summary.trimmed)}个 {summary.trimmed}",
            f"新补充: {len(summary.added)}个 {summary.added}",
        ]
        if summary.low_active_uids:
            lines.append(
                f"本轮无新增动态: {len(summary.low_active_uids)}个 "
                f"{summary.low_active_uids}"
            )
        lines.append(f"{days}天活跃用户: {summary.active_in_window}个")

        title = f"第三方抽奖采集完成 - 轮次{summary.round_id}"
        content = "\n".join(lines)
        get_others_lot_log.info(f"推送总结: {title}\n{content}")
        await a_pushme(title, content, "text")


lot_user_pool_service = LotUserPoolService()
