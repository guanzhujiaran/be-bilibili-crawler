#!/usr/bin/env python3
"""
手动执行脚本：对所有已入库的抽奖数据使用 LLM 进行大奖判断，并将结果写入子表。

使用方式:
    cd FastapiApp
    uv run python -m scripts.judge_grand_prize                            # 判断全部类型 (.env 默认配置)
    uv run python -m scripts.judge_grand_prize --type common              # 仅判断普通抽奖
    uv run python -m scripts.judge_grand_prize --type reserve             # 仅判断预约抽奖
    uv run python -m scripts.judge_grand_prize --type official            # 仅判断官方/充电抽奖
    uv run python -m scripts.judge_grand_prize --force-update             # 强制重新判断所有

指定大模型 (有 GPU 的机器):
    uv run python -m scripts.judge_grand_prize \\
        --llm-base-url https://api.openai.com/v1 \\
        --llm-token sk-xxx \\
        --llm-model gpt-4o

指定目标数据库:
    uv run python -m scripts.judge_grand_prize \\
        --db-host 192.168.1.200 --db-port 10000 --db-user root

可选参数:
    --type all|common|reserve|official  抽奖类型 (默认: all=全部)
    --batch-size 200                    每批处理数量 (默认: 200)
    --concurrency 1                     并发处理数量，0/负数按 1 处理 (默认: 1)
    --dry-run                           仅打印将要处理的数量，不实际写入
    --force-update                      强制重新判断所有记录（即使已有flag）
    --limit N                           限制最大处理数量，0=不限制 (默认: 0)
    --llm-base-url                      大模型 API 地址 (覆盖 .env)
    --llm-token                         大模型 API token (覆盖 .env)
    --llm-model                         模型名称 (覆盖 .env)
    --llm-headers                       自定义 HTTP 头 (JSON 字符串)，用于创建 ChatOpenAI 实例
    --db-host                           MySQL 主机 (覆盖 .env)
    --db-port                           MySQL 端口 (覆盖 .env)
    --db-user                           MySQL 用户名 (覆盖 .env)
    --db-password                       MySQL 密码 (覆盖 .env)

注意:
    - 所有抽奖类型的大奖判断结果均写入独立子表 t_lot_extra_info
    - 采用“处理完一个立即保存一个”的策略，避免批量写入中途失败导致结果丢失
    - 普通抽奖使用 (ref_id=dynId, lot_type='common')
    - 预约抽奖使用 (ref_id=ids, lot_type='reserve')
    - 官方/充电抽奖使用 (business_id, 通过 Grpc SQLHelper)
    - 基于 Qwen3.5-0.8B + vLLM 推理，替代原有 SVM 模型
    - --concurrency 控制 LLM 判断的并发数，提高吞吐；默认 1 为串行
"""

import argparse
import asyncio
import sys
import time
from pathlib import Path
from langchain_openai import ChatOpenAI
from tqdm.auto import tqdm
# 确保项目根目录在 sys.path 中
_project_root = Path(__file__).resolve().parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))





def _make_chat_openai_client(headers: dict | None) -> ChatOpenAI | None:
    """根据当前 settings.llm_apis 配置 + 自定义 headers 构造一个 ChatOpenAI 实例。

    仅当传入非空 headers 且存在可用的云端 LLM 配置时才会真正创建实例；
    否则返回 None（由 extract 走默认云端 LLM 流程）。

    参数:
        headers: 注入到 ChatOpenAI 请求的自定义 HTTP 头（如鉴权/路由头）。
    """
    if not headers:
        return None
    from CONFIG import settings
    from langchain_openai import ChatOpenAI
    from pydantic import SecretStr

    usable = [c for c in settings.llm_apis if c.base_url and c.model_name]
    if not usable:
        return None
    cfg = usable[0]
    return ChatOpenAI(
        model=cfg.model_name,
        base_url=cfg.base_url,
        api_key=SecretStr(cfg.token) if cfg.token else SecretStr("not-needed"),
        default_headers=headers,
    )


def _resolve_chat_openai_client(
    headers: dict | None,
    llm_args: ChatOpenAI | None,
) -> ChatOpenAI | None:
    """统一解析最终用于提取的 ChatOpenAI 客户端。

    优先级: 直接传入的 llm_args（已建好的 ChatOpenAI 实例） > 用 headers 新建的实例 > None。
    """
    if isinstance(llm_args, ChatOpenAI):
        return llm_args
    return _make_chat_openai_client(headers)


def _format_stats(stats: dict, title: str, total_elapsed: float) -> None:
    """统一格式化输出统计信息"""
    print("-" * 60)
    print(f"处理完成! [{title}] 总耗时: {total_elapsed:.1f}s")
    print(f"  总记录数: {stats['total_records']}")
    print(f"  已处理:   {stats['processed']}")
    print(f"  大奖:     {stats['grand_prize']}")
    print(f"  非大奖:   {stats['not_grand_prize']}")
    print(f"  跳过:     {stats['skipped']}")
    print(f"  错误:     {stats['errors']}")


async def _run_workers_concurrent(
    workers: list, concurrency: int, pbar: tqdm | None = None
) -> dict:
    """并发执行一组 worker 协程并汇总统计。

    workers: 一组 awaitable，每个 worker 应返回 'grand' / 'not_grand' / 'error' 之一。
    concurrency: 最大并发数（<=1 时退化为串行）。
    pbar: 可选的 tqdm 进度条，每个 worker 完成后实时 +1。
    每个 worker 内部应“处理完一个立即保存一个”，避免批量保存在中途失败时丢失结果。
    """
    sem = asyncio.Semaphore(max(1, concurrency))

    async def _wrap(w):
        async with sem:
            try:
                return await w
            finally:
                if pbar is not None:
                    pbar.update(1)

    counts = {
        "processed": 0,
        "grand_prize": 0,
        "not_grand_prize": 0,
        "errors": 0,
    }
    results = await asyncio.gather(
        *(_wrap(w) for w in workers), return_exceptions=True
    )
    for r in results:
        if isinstance(r, Exception):
            counts["errors"] += 1
            continue
        if r == "grand":
            counts["grand_prize"] += 1
            counts["processed"] += 1
        elif r == "not_grand":
            counts["not_grand_prize"] += 1
            counts["processed"] += 1
        else:
            counts["errors"] += 1
    return counts


async def judge_common_lottery(
    batch_size: int = 200,
    dry_run: bool = False,
    force_update: bool = False,
    limit: int = 0,
    headers: dict | None = None,
    llm_args: ChatOpenAI | None = None,
    concurrency: int = 1,
) -> dict:
    """
    对所有普通抽奖动态 (TLotdyninfo) 执行 LLM 大奖判断并写入 t_lot_extra_info。

    返回统计信息。
    """
    from Service.GetOthersLotDyn.Sql.sql_helper import SqlHelper
    from Service.GetOthersLotDyn.parser.prize_extractor import extract_prize_info_for_biliopusdb

    chat_openai_client = _resolve_chat_openai_client(headers, llm_args)

    stats = {
        "total_records": 0,
        "processed": 0,
        "grand_prize": 0,
        "not_grand_prize": 0,
        "skipped": 0,
        "errors": 0,
    }

    print("=" * 60)
    print("开始对普通抽奖动态执行 LLM 大奖判断")
    print(f"  每批数量: {batch_size}")
    print(f"  Dry-Run: {dry_run}")
    print(f"  强制更新: {force_update}")
    print(f"  最大数量: {'无限制' if limit == 0 else limit}")
    print(f"  并发数:   {concurrency}")
    print("=" * 60)

    if force_update:
        dyn_ids = await SqlHelper.get_all_common_lot_dyn_ids(limit=limit)
    else:
        dyn_ids = await SqlHelper.get_ref_ids_without_extra_info(
            lot_type="common", limit=limit
        )

    stats["total_records"] = len(dyn_ids)

    if not dyn_ids:
        print("没有找到需要判断的记录。")
        return stats

    print(f"共找到 {len(dyn_ids)} 条需要判断的记录")

    if dry_run:
        print(f"[Dry-Run] 将处理 {len(dyn_ids)} 条记录，不实际写入数据库。")
        return stats

    async def _judge_one(dyn_id: int, content: str) -> str:
        try:
            result = await extract_prize_info_for_biliopusdb(
                dyn_content=content,
                chat_openai_client=chat_openai_client)
            is_grand = int(result.result.is_grand_prize)
            # 处理完一个立即保存一个，防止中途失败丢失结果
            await SqlHelper.save_extra_info(
                ref_id=dyn_id,
                lot_type="common",
                is_grand_prize=is_grand,
            )
            return "grand" if is_grand == 1 else "not_grand"
        except Exception as e:
            tqdm.write(f"  dynId={dyn_id} 失败: {e}")
            return "error"

    total_batches = (len(dyn_ids) + batch_size - 1) // batch_size
    start_time = time.time()

    pbar = tqdm(total=len(dyn_ids), desc="普通抽奖动态", unit="条")
    for batch_idx in range(0, len(dyn_ids), batch_size):
        batch = dyn_ids[batch_idx : batch_idx + batch_size]
        batch_num = batch_idx // batch_size + 1

        content_map = await SqlHelper.get_dyn_info_batch(batch)

        if not content_map:
            tqdm.write(f"  [批次 {batch_num}/{total_batches}] 无有效内容，跳过")
            pbar.update(len(batch))
            continue

        batch_items = [(dyn_id, content_map[dyn_id]) for dyn_id in batch if dyn_id in content_map]
        if not batch_items:
            tqdm.write(f"  [批次 {batch_num}/{total_batches}] 内容均为空，跳过")
            pbar.update(len(batch))
            continue

        # 本批中无有效内容的记录也计入进度
        skipped_in_batch = len(batch) - len(batch_items)
        if skipped_in_batch > 0:
            pbar.update(skipped_in_batch)

        workers = [_judge_one(dyn_id, content) for dyn_id, content in batch_items]
        counts = await _run_workers_concurrent(workers, concurrency, pbar=pbar)
        for k in ("processed", "grand_prize", "not_grand_prize", "errors"):
            stats[k] += counts[k]
    pbar.close()

    total_elapsed = time.time() - start_time
    _format_stats(stats, "普通抽奖动态", total_elapsed)

    return stats


async def judge_reserve_lottery(
    batch_size: int = 200,
    dry_run: bool = False,
    force_update: bool = False,
    limit: int = 0,
    headers: dict | None = None,
    llm_args: ChatOpenAI | None = None,
    concurrency: int = 1,
) -> dict:
    """
    对所有预约抽奖 (t_up_reserve_relation_info) 执行 LLM 大奖判断，
    结果写入 t_lot_extra_info (lot_type='reserve')。

    返回统计信息。
    """
    from Service.GetOthersLotDyn.Sql.sql_helper import SqlHelper
    from Service.GetOthersLotDyn.parser.prize_extractor import extract_prize_info_for_biliopusdb
    from Service.opus新版官方抽奖.预约抽奖.db.sqlHelper import bili_reserve_sqlhelper

    chat_openai_client = _resolve_chat_openai_client(headers, llm_args)

    stats = {
        "total_records": 0,
        "processed": 0,
        "grand_prize": 0,
        "not_grand_prize": 0,
        "skipped": 0,
        "errors": 0,
    }

    print("=" * 60)
    print("开始对预约抽奖执行 LLM 大奖判断")
    print("  结果写入: t_lot_extra_info (lot_type=reserve)")
    print(f"  每批数量: {batch_size}")
    print(f"  Dry-Run: {dry_run}")
    print(f"  强制更新: {force_update}")
    print(f"  最大数量: {'无限制' if limit == 0 else limit}")
    print(f"  并发数:   {concurrency}")
    print("=" * 60)

    all_records = await bili_reserve_sqlhelper.get_all_reserve_lottery()

    if not all_records:
        print("没有找到预约抽奖记录。")
        return stats

    all_records = [r for r in all_records if r.text]

    if not force_update:
        all_ids = [r.ids for r in all_records]
        existing_flags = await SqlHelper.get_extra_info_by_ref_ids(
            all_ids, lot_type="reserve"
        )
        target_records = [r for r in all_records if r.ids not in existing_flags]
        stats["skipped"] = len(all_records) - len(target_records)
    else:
        target_records = all_records

    if limit > 0:
        target_records = target_records[:limit]

    stats["total_records"] = len(target_records)

    print(f"共找到 {len(target_records)} 条需要判断的记录 "
          f"(总记录数: {len(all_records)}, 已跳过: {stats['skipped']})")

    if not target_records:
        print("没有需要判断的记录。")
        return stats

    if dry_run:
        print(f"[Dry-Run] 将处理 {len(target_records)} 条记录，不实际写入数据库。")
        return stats

    async def _judge_one(record) -> str:
        try:
            result = await extract_prize_info_for_biliopusdb(
                dyn_content=record.text,
                chat_openai_client=chat_openai_client)
            is_grand = int(result.result.is_grand_prize)
            # 处理完一个立即保存一个，防止中途失败丢失结果
            await SqlHelper.save_extra_info(
                ref_id=record.ids,
                lot_type="reserve",
                is_grand_prize=is_grand,
            )
            return "grand" if is_grand == 1 else "not_grand"
        except Exception as e:
            tqdm.write(f"  ids={record.ids} 失败: {e}")
            return "error"

    start_time = time.time()

    pbar = tqdm(total=len(target_records), desc="预约抽奖", unit="条")
    for batch_idx in range(0, len(target_records), batch_size):
        batch = target_records[batch_idx : batch_idx + batch_size]

        workers = [_judge_one(record) for record in batch]
        counts = await _run_workers_concurrent(workers, concurrency, pbar=pbar)
        for k in ("processed", "grand_prize", "not_grand_prize", "errors"):
            stats[k] += counts[k]
    pbar.close()

    total_elapsed = time.time() - start_time
    _format_stats(stats, "预约抽奖", total_elapsed)

    return stats


async def judge_official_lottery(
    batch_size: int = 200,
    dry_run: bool = False,
    force_update: bool = False,
    limit: int = 0,
    headers: dict | None = None,
    llm_args: ChatOpenAI | None = None,
    concurrency: int = 1,
) -> dict:
    """
    对所有官方/充电抽奖 (lotdata 表) 执行 LLM 大奖判断，
    结果写入 t_lot_extra_info (通过 lottery_id 关联)。

    返回统计信息。
    """
    from Service.GetOthersLotDyn.parser.prize_extractor import extract_prize_info_for_dyndetail
    from Service.GrpcModule.GrpcSrc.SQLObject.DynDetailSqlHelperMysqlVer import (
        grpc_sql_helper,
    )

    chat_openai_client = _resolve_chat_openai_client(headers, llm_args)

    stats = {
        "total_records": 0,
        "processed": 0,
        "grand_prize": 0,
        "not_grand_prize": 0,
        "skipped": 0,
        "errors": 0,
    }

    print("=" * 60)
    print("开始对官方/充电抽奖 (lotdata) 执行 LLM 大奖判断")
    print("  结果写入: t_lot_extra_info (via lottery_id)")
    print(f"  每批数量: {batch_size}")
    print(f"  Dry-Run: {dry_run}")
    print(f"  强制更新: {force_update}")
    print(f"  最大数量: {'无限制' if limit == 0 else limit}")
    print(f"  并发数:   {concurrency}")
    print("=" * 60)

    all_records = await grpc_sql_helper.query_all_lottery_data()

    if not all_records:
        print("没有找到 lotdata 记录。")
        return stats

    record_map: dict[int, tuple] = {}
    skipped_no_bid = 0
    skipped_no_text = 0
    for r in all_records:
        if not r.business_id:
            skipped_no_bid += 1
            continue
        prize_cmts = [r.first_prize_cmt, r.second_prize_cmt, r.third_prize_cmt]
        lottery_text = " ".join(filter(lambda a: a, prize_cmts)).strip()
        if not lottery_text:
            skipped_no_text += 1
            continue
        record_map[r.lottery_id] = (lottery_text, r)

    print(f"总 lotdata 记录数: {len(all_records)}")
    print(f"  无 business_id 跳过: {skipped_no_bid}")
    print(f"  无奖品描述跳过:     {skipped_no_text}")
    print(f"  有效记录数:          {len(record_map)}")

    if not record_map:
        print("没有有效的 lotdata 记录可供判断。")
        return stats

    target_ids = list(record_map.keys())
    if not force_update:
        existing_ids = await grpc_sql_helper.batch_check_existing_extra_info(
            target_ids
        )
        target_ids = [bid for bid in target_ids if bid not in existing_ids]
        stats["skipped"] = len(record_map) - len(target_ids)

    if limit > 0:
        target_ids = target_ids[:limit]

    stats["total_records"] = len(target_ids)

    print(f"共找到 {len(target_ids)} 条需要判断的记录 "
          f"(有效总数: {len(record_map)}, 已跳过: {stats['skipped']})")

    if not target_ids:
        print("没有需要判断的记录。")
        return stats

    if dry_run:
        print(f"[Dry-Run] 将处理 {len(target_ids)} 条记录，不实际写入数据库。")
        return stats

    async def _judge_one(bid: int) -> str:
        try:
            lottery_text = record_map[bid][0]
            result = await extract_prize_info_for_dyndetail(
                dyn_content=lottery_text,
                chat_openai_client=chat_openai_client)
            is_grand = int(result.result.is_grand_prize)
            # 处理完一个立即保存一个，防止批量写入中途失败丢失结果
            await grpc_sql_helper.save_extra_info(
                lottery_id=bid, is_grand_prize=is_grand)
            return "grand" if is_grand == 1 else "not_grand"
        except Exception as e:
            tqdm.write(f"  lottery_id={bid} 失败: {e}")
            return "error"

    start_time = time.time()

    pbar = tqdm(total=len(target_ids), desc="官方/充电抽奖", unit="条")
    for batch_idx in range(0, len(target_ids), batch_size):
        batch_ids = target_ids[batch_idx : batch_idx + batch_size]

        workers = [_judge_one(bid) for bid in batch_ids]
        counts = await _run_workers_concurrent(workers, concurrency, pbar=pbar)
        for k in ("processed", "grand_prize", "not_grand_prize", "errors"):
            stats[k] += counts[k]
    pbar.close()

    total_elapsed = time.time() - start_time
    _format_stats(stats, "官方/充电抽奖", total_elapsed)

    return stats


async def main():
    parser = argparse.ArgumentParser(
        description="对所有已入库的抽奖数据使用 LLM 进行大奖判断并写入子表"
    )
    parser.add_argument(
        "--type",
        type=str,
        default="all",
        choices=["all", "common", "reserve", "official"],
        help="抽奖类型: all=全部, common=普通抽奖动态, reserve=预约抽奖, official=官方/充电抽奖 (默认: all)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=200,
        help="每批处理数量 (默认: 200)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="仅打印将要处理的数量，不实际写入数据库",
    )
    parser.add_argument(
        "--force-update",
        action="store_true",
        help="强制重新判断所有记录（即使已有 flag 记录）",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="限制最大处理数量，0=不限制 (默认: 0)"
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=1,
        help="并发处理数量，0/负数按 1 处理 (默认: 1)"
    )

    from scripts._cli_utils import (
        add_llm_args,
        add_db_args,
        apply_cli_overrides,
        build_mysql_uri,
    )
    add_llm_args(parser)
    add_db_args(parser)

    args = parser.parse_args()
    apply_cli_overrides(args)

    # 脚本禁止回退策略，必须确保已配置可用的云端 LLM，否则直接失败退出。
    from CONFIG import settings
    usable = [
        c for c in settings.llm_apis
        if c.base_url and c.model_name
    ]
    if not usable:
        print("[错误] 未配置任何可用的云端 LLM（需要 base_url 与 model_name 均非空），"
              "脚本禁止回退策略，无法继续。")
        print("请通过 --llm-base-url/--llm-token/--llm-model 完整指定，"
              "或在 .env 中配置 llm_apis。")
        sys.exit(1)

    # ---- 根据 CLI 的 --db-* 覆盖，显式构造指向同一 MySQL 实例的 SqlHelper ----
    # 各 SqlHelper 单例在模块导入时已用当时的 CONFIG.database.MYSQL URI 固化了
    # 连接池；apply_cli_overrides 虽更新了 settings/CONFIG，但已创建的单例不会自动
    # 刷新。因此这里按最终连接参数 new 实例，并覆盖各源模块的全局单例
    # （含其它模块内部直接引用处），确保所有调用点都连到正确的 MySQL 实例。
    from Service.GetOthersLotDyn.Sql import sql_helper as _gol_sql_mod
    from Service.GetOthersLotDyn.Sql.sql_helper import __SqlHelper as _GetOtherLotSqlHelper
    from Service.opus新版官方抽奖.预约抽奖.db import sqlHelper as _res_sql_mod
    from Service.opus新版官方抽奖.预约抽奖.db.sqlHelper import _SqlHelper as _ReserveSqlHelper
    import Service.GrpcModule.GrpcSrc.SQLObject.DynDetailSqlHelperMysqlVer as _dyn_sql_mod
    from Service.GrpcModule.GrpcSrc.SQLObject.DynDetailSqlHelperMysqlVer import (
        SQLHelper as _DynDetailSqlHelper,
    )

    sql_helper = _GetOtherLotSqlHelper(build_mysql_uri("biliopusdb", args))
    reserve_sql_helper = _ReserveSqlHelper(build_mysql_uri("bili_reserve", args))
    grpc_sql_helper = _DynDetailSqlHelper(build_mysql_uri("dyndetail", args))

    # 覆盖源模块的全局单例：确保被其它模块内部直接引用（如预约模块调用
    # SqlHelper.save_extra_info）时也能连到正确的 MySQL 实例。
    _gol_sql_mod.SqlHelper = sql_helper
    _res_sql_mod.bili_reserve_sqlhelper = reserve_sql_helper
    _dyn_sql_mod.grpc_sql_helper = grpc_sql_helper

    # ---- 自定义 HTTP headers → 创建 ChatOpenAI 实例 ----
    # --llm-headers 传入 JSON 字符串，用于构造注入自定义请求头的 ChatOpenAI 实例，
    # 后续以 llm_args（ChatOpenAI 实例）形式透传给各 judge 函数。
    llm_headers: dict | None = None
    llm_client: ChatOpenAI | None = None
    if getattr(args, "llm_headers", None):
        import json as _json
        try:
            llm_headers = _json.loads(args.llm_headers)
        except Exception as e:
            print(f"[错误] --llm-headers 不是合法 JSON: {e}")
            sys.exit(1)
        llm_client = _make_chat_openai_client(llm_headers)
        print(f"[CLI] 使用 --llm-headers 创建 ChatOpenAI 实例: {llm_headers}")

    if args.type in ("all", "common"):
        await judge_common_lottery(
            batch_size=args.batch_size,
            dry_run=args.dry_run,
            force_update=args.force_update,
            limit=args.limit,
            llm_args=llm_client,
            concurrency=args.concurrency,
        )
    if args.type in ("all", "reserve"):
        await judge_reserve_lottery(
            batch_size=args.batch_size,
            dry_run=args.dry_run,
            force_update=args.force_update,
            limit=args.limit,
            llm_args=llm_client,
            concurrency=args.concurrency,
        )
    if args.type in ("all", "official"):
        await judge_official_lottery(
            batch_size=args.batch_size,
            dry_run=args.dry_run,
            force_update=args.force_update,
            limit=args.limit,
            llm_args=llm_client,
            concurrency=args.concurrency,
        )


if __name__ == "__main__":
    asyncio.run(main())