"""把 t_lotdyninfo.rawJsonStr 迁移到垂直分表 t_lotdyninfo_raw。

背景：``rawJsonStr`` 单行约 8KB，占 ``t_lotdyninfo`` 数据量 95% 以上；但业务只按
dynId 点查（爬取去重缓存 / 从高互动动态的评论里挖用户），从不参与列表与统计。
拆表后主表只留元数据，汇总聚合不再被 6GB 大字段挤出 buffer pool。

流程（可重复执行、可分批中断后续跑）：

1. 建分表（等价于 alembic 迁移 ``a1b2c3d4e5f6``，``checkfirst`` 幂等）；
2. 按 dynId keyset 分批把 ``rawJsonStr`` 复制到分表（``INSERT IGNORE`` 幂等），
   JSON 在服务端直接复制，不经过 Python 往返；
3. 回读校验：主表所有 non-null 行必须都能在分表找到，并抽样比对 CRC32；
4. 校验通过后分批把主表 ``rawJsonStr`` 置 NULL（显式 ``updated_at = updated_at``，
   避免 ``ON UPDATE CURRENT_TIMESTAMP`` 把收录时间刷成今天）；
5. 可选 ``--optimize``：复用表空间维护逻辑回收主表空洞（耗时且需要接近表大小的
   临时磁盘，务必低峰执行）。

用法（在 be-bilibili-crawler 目录下）：

    # 先看规模，不动数据
    python -m scripts.database.split_lotdyninfo_raw --dry-run

    # 迁移 + 校验 + 置空
    python -m scripts.database.split_lotdyninfo_raw

    # 只迁移不置空（先观察一轮，确认读路径正常）
    python -m scripts.database.split_lotdyninfo_raw --keep-main-raw

    # 先小批量验证 1000 行
    python -m scripts.database.split_lotdyninfo_raw --limit 1000 --keep-main-raw

    # 迁移完成后回收主表空间
    python -m scripts.database.split_lotdyninfo_raw --optimize

建议顺序：先跑 alembic 迁移（或直接跑本脚本）→ 发布后端代码 → 再执行置空，
这样回滚窗口内主表仍有原始 JSON 可用。
"""

import argparse
import asyncio
import json

from sqlalchemy import bindparam, func, select, text, update

from Service.GetOthersLotDyn.Sql.models import TLotdyninfo, TLotdyninfoRaw
from Service.GetOthersLotDyn.Sql.sql_helper import SqlHelper

_DEFAULT_BATCH_SIZE = 500
# 抽样校验的步长：每 N 个 dynId 抽一行比对 CRC32（避免全表 CAST 大字段）
_VERIFY_STEP = 500
_VERIFY_LIMIT = 500


async def _ensure_table() -> None:
    async with SqlHelper.async_session() as session:
        conn = await session.connection()
        await conn.run_sync(TLotdyninfoRaw.__table__.create, checkfirst=True)
        await session.commit()


async def _counts() -> dict:
    async with SqlHelper.async_session() as session:
        main_total = (
            await session.execute(select(func.count()).select_from(TLotdyninfo))
        ).scalar() or 0
        main_with_raw = (
            await session.execute(
                select(func.count())
                .select_from(TLotdyninfo)
                .where(TLotdyninfo.rawJsonStr.isnot(None))
            )
        ).scalar() or 0
        raw_total = (
            await session.execute(select(func.count()).select_from(TLotdyninfoRaw))
        ).scalar() or 0
    return {
        "main_total": int(main_total),
        "main_with_raw": int(main_with_raw),
        "raw_total": int(raw_total),
    }


async def _migrate(batch_size: int, limit: int = 0) -> tuple[int, int | None]:
    """按 dynId keyset 分批复制，返回 (本次复制的行数, 最后处理的 dynId)。"""
    migrated = 0
    last_pk: int | None = None
    while True:
        current_batch = batch_size
        if limit:
            current_batch = min(batch_size, limit - migrated)
            if current_batch <= 0:
                break
        async with SqlHelper.async_session() as session:
            stmt = select(TLotdyninfo.dynId).where(
                TLotdyninfo.rawJsonStr.isnot(None)
            )
            if last_pk is not None:
                stmt = stmt.where(TLotdyninfo.dynId > last_pk)
            stmt = stmt.order_by(TLotdyninfo.dynId).limit(current_batch)
            ids = (await session.execute(stmt)).scalars().all()
            if not ids:
                break
            # 服务端复制 JSON：避免把 8KB 文本拉到 Python 再写回
            await session.execute(
                text(
                    "INSERT IGNORE INTO t_lotdyninfo_raw "
                    "(dynId, rawJsonStr, created_at) "
                    "SELECT dynId, rawJsonStr, created_at FROM t_lotdyninfo "
                    "WHERE dynId IN :ids"
                ).bindparams(bindparam("ids", expanding=True)),
                {"ids": list(ids)},
            )
            await session.commit()
        migrated += len(ids)
        last_pk = int(ids[-1])
        print(f"  已复制 {migrated} 行（last dynId={last_pk}）", flush=True)
    return migrated, last_pk


async def _verify(max_dyn_id: int | None = None) -> tuple[int, int]:
    """返回 (分表缺失行数, 抽样 CRC32 不一致行数)。

    ``max_dyn_id`` 用于「--limit 小批量验证」：只校验已处理的主键区间，
    避免把还没迁移的行误判为缺失。
    """
    async with SqlHelper.async_session() as session:
        conditions = [TLotdyninfo.rawJsonStr.isnot(None)]
        if max_dyn_id is not None:
            conditions.append(TLotdyninfo.dynId <= max_dyn_id)
        missing = (
            await session.execute(
                select(func.count())
                .select_from(TLotdyninfo)
                .outerjoin(
                    TLotdyninfoRaw, TLotdyninfoRaw.dynId == TLotdyninfo.dynId
                )
                .where(
                    *conditions,
                    TLotdyninfoRaw.dynId.is_(None),
                )
            )
        ).scalar() or 0
        sample_sql = (
            "SELECT COUNT(*) FROM ("
            "  SELECT t.dynId, "
            "         CRC32(CAST(t.rawJsonStr AS CHAR)) AS main_crc "
            "  FROM t_lotdyninfo t "
            "  WHERE t.rawJsonStr IS NOT NULL AND t.dynId % :step = 0 "
            f"  {'AND t.dynId <= :max_id ' if max_dyn_id is not None else ''}"
            "  ORDER BY t.dynId LIMIT :limit"
            ") s "
            "JOIN t_lotdyninfo_raw r ON r.dynId = s.dynId "
            "WHERE r.rawJsonStr IS NULL "
            "   OR CRC32(CAST(r.rawJsonStr AS CHAR)) <> s.main_crc"
        )
        params = {"step": _VERIFY_STEP, "limit": _VERIFY_LIMIT}
        if max_dyn_id is not None:
            params["max_id"] = max_dyn_id
        mismatch = (
            await session.execute(text(sample_sql), params)
        ).scalar() or 0
    return int(missing), int(mismatch)


async def _null_main(batch_size: int, limit: int = 0) -> int:
    """分批把主表 rawJsonStr 置 NULL，返回置空行数。"""
    cleared = 0
    last_pk: int | None = None
    while True:
        current_batch = batch_size
        if limit:
            current_batch = min(batch_size, limit - cleared)
            if current_batch <= 0:
                break
        async with SqlHelper.async_session() as session:
            stmt = select(TLotdyninfo.dynId).where(
                TLotdyninfo.rawJsonStr.isnot(None)
            )
            if last_pk is not None:
                stmt = stmt.where(TLotdyninfo.dynId > last_pk)
            stmt = stmt.order_by(TLotdyninfo.dynId).limit(current_batch)
            ids = (await session.execute(stmt)).scalars().all()
            if not ids:
                break
            await session.execute(
                update(TLotdyninfo)
                .where(TLotdyninfo.dynId.in_(list(ids)))
                # updated_at 显式赋回自身，阻止 ON UPDATE 刷新收录时间
                .values(rawJsonStr=None, updated_at=TLotdyninfo.updated_at)
            )
            await session.commit()
        cleared += len(ids)
        last_pk = int(ids[-1])
        print(f"  已置空 {cleared} 行（last dynId={last_pk}）", flush=True)
    return cleared


async def _optimize() -> dict:
    """复用表空间维护逻辑回收主表空洞。"""
    from Models.database_archive.tablespace_maintenance_models import (
        TablespaceMaintenanceConf,
    )
    from Service.DataArchive.tablespace_maintenance import TablespaceMaintenance

    result = await TablespaceMaintenance(
        TablespaceMaintenanceConf(tables=["t_lotdyninfo"])
    ).run_all()
    return result.model_dump(mode="json")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="把 t_lotdyninfo.rawJsonStr 迁移到垂直分表 t_lotdyninfo_raw"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=_DEFAULT_BATCH_SIZE,
        help=f"每批处理行数（默认 {_DEFAULT_BATCH_SIZE}）",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="最多处理多少行（0=不限），用于先小批量验证",
    )
    parser.add_argument(
        "--keep-main-raw",
        action="store_true",
        help="只复制到分表，不清空主表 rawJsonStr（观察一轮后再手动清理）",
    )
    parser.add_argument(
        "--optimize",
        action="store_true",
        help="置空完成后执行表空间维护，回收主表 .ibd 空洞（低峰执行）",
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="只统计规模，不建表、不迁移、不清空"
    )
    parser.add_argument(
        "--skip-verify",
        action="store_true",
        help="跳过回读校验直接置空（不推荐，仅在确认数据可丢时使用）",
    )
    return parser.parse_args()


async def _main() -> None:
    args = _parse_args()
    if args.batch_size <= 0:
        raise SystemExit("--batch-size 必须大于 0")
    if args.limit < 0:
        raise SystemExit("--limit 不能为负数")

    before = await _counts()
    print(f"迁移前：{json.dumps(before, ensure_ascii=False)}")
    if args.dry_run:
        return

    await _ensure_table()
    copied, last_pk = await _migrate(args.batch_size, args.limit)
    print(f"复制完成：本次 {copied} 行")

    if not args.skip_verify:
        missing, mismatch = await _verify(last_pk)
        print(f"校验结果：分表缺失 {missing} 行，抽样 CRC32 不一致 {mismatch} 行")
        if missing or mismatch:
            raise SystemExit("校验未通过，已中止（主表 rawJsonStr 保持原样）")

    if args.keep_main_raw:
        print("已按 --keep-main-raw 跳过主表置空")
    else:
        cleared = await _null_main(args.batch_size, args.limit)
        print(f"主表置空完成：{cleared} 行")

    if args.optimize:
        print("开始表空间维护（ALTER TABLE ... FORCE），请确认磁盘余量充足 ...")
        print(json.dumps(await _optimize(), ensure_ascii=False, indent=2))

    after = await _counts()
    print(f"迁移后：{json.dumps(after, ensure_ascii=False)}")


if __name__ == "__main__":
    asyncio.run(_main())
