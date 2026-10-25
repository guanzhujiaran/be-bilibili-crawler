"""
从生产库同步抽奖数据到本地开发库。

设计要点（详见 docs/be-bilibili-crawler-生产库抽奖数据同步脚本计划书.md）：
  - 源（生产库）默认读 .env.fastapi.prod 的 MYSQL_*；目标（本地开发库）走全局 settings
    （CONFIG.py 的 env_file 为 (.env.fastapi.prod, .env.fastapi.dev)，dev 覆盖 prod）；
  - 表清单以 ORM 模型为单一事实来源，写入顺序按外键依赖拓扑排序；
  - 同步列 = ORM 列 ∩ 源库实际列 ∩ 目标库实际列，并排除 Computed 列；
  - 主键 keyset 分页读取 + INSERT ... ON DUPLICATE KEY UPDATE 批量 upsert，可反复执行；
  - 只读源库、只写目标库；不同步 alembic_version / Redis / Milvus。

用法:
  # 预览：校验源/目标连通性并统计两边行数，不写库
  uv run python -m scripts.database.sync_lottery_from_prod.sync_lottery --dry-run

  # 全量同步（生产 mysql 主机在容器网络内，本地执行需覆盖源地址）
  uv run python -m scripts.database.sync_lottery_from_prod.sync_lottery \
      --source-db-host <生产库地址> --source-db-port 3306 \
      --source-db-user root --source-db-password <密码>

  # 只同步 dyndetail 的 lotdata 一张表
  uv run python -m scripts.database.sync_lottery_from_prod.sync_lottery --database dyndetail --table lotdata

  # 精确镜像：先按逆拓扑序清空目标表，再全量写入
  uv run python -m scripts.database.sync_lottery_from_prod.sync_lottery --truncate
"""

import argparse
import asyncio
import os
import sys
import time
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

# 确保项目根目录在 sys.path 中，否则导入 CONFIG / Service 会失败
_project_root = Path(__file__).resolve().parents[3]
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from dotenv import dotenv_values
from log.base_log import myfastapi_logger
from sqlalchemy import Column, Table, bindparam, delete, func, inspect, select, tuple_
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.sql import Executable

from CONFIG import settings
from Service.GetOthersLotDyn.Sql.models import (
    TLotdyninfo,
    TLotExtraInfo,
    TLotmaininfo,
    TLotuserinfo,
    TLotuserspaceresp,
    TRiddynid,
)
from Service.GrpcModule.GrpcSrc.SQLObject.models import (
    ArticlePubRecord,
    BiliAtariInfo,
    BiliUserInfo,
    Bilidyndetail,
    Lotdata,
    LotExtraInfo,
)
from Service.opus新版官方抽奖.活动抽奖.话题抽奖.db.models import (
    TActivityLottery,
    TActivityMatchLottery,
    TActivityMatchTask,
    TCapsule,
    TClickAreaCard,
    TEraJika,
    TEraLottery,
    TEraTask,
    TEraVideo,
    TFunctionalCard,
    TTopDetails,
    TTopic,
    TTopicCreator,
    TTopicItem,
    TTrafficCard,
)
from Service.opus新版官方抽奖.预约抽奖.db.models import (
    TReserveRoundInfo,
    TUpReserveRelationInfo,
)
from Utils.数据库.SqlalchemyTool import sqlalchemy_session_factory
from scripts._cli_utils import add_db_args, add_llm_args, apply_cli_overrides

# 源库连接的环境变量覆盖前缀（优先级低于 --source-db-*，高于 .env.fastapi.prod 文件）
_SOURCE_ENV_PREFIX = "SYNC_SOURCE_MYSQL_"
# 每写入这么多批次打印一次进度
_PROGRESS_BATCH_INTERVAL = 25


@dataclass(frozen=True)
class LotteryDatabase:
    """一个抽奖库的同步清单（models 顺序仅作偏好顺序，实际按外键拓扑排序）。"""

    name: str
    models: tuple[type[DeclarativeBase], ...]


# 抽奖数据表清单：与 create_database.py 的 4 个抽奖库一致。
# 不含 bilidb.t_spaceseries：B 站播放列表缓存，不属于抽奖数据（约 400 万行，同步只会拖慢脚本）。
LOTTERY_DATABASES: tuple[LotteryDatabase, ...] = (
    LotteryDatabase(
        name="dyndetail",
        models=(
            Lotdata,
            BiliUserInfo,
            BiliAtariInfo,
            LotExtraInfo,
            ArticlePubRecord,
            Bilidyndetail,
        ),
    ),
    LotteryDatabase(
        name="biliopusdb",
        models=(
            TLotmaininfo,
            TLotuserinfo,
            TLotdyninfo,
            TRiddynid,
            TLotuserspaceresp,
            TLotExtraInfo,
        ),
    ),
    LotteryDatabase(
        name="bili_reserve",
        models=(TReserveRoundInfo, TUpReserveRelationInfo),
    ),
    LotteryDatabase(
        name="bilidb",
        models=(
            TTrafficCard,
            TClickAreaCard,
            TTopicCreator,
            TTopicItem,
            TFunctionalCard,
            TTopDetails,
            TActivityLottery,
            TActivityMatchLottery,
            TActivityMatchTask,
            TEraJika,
            TEraLottery,
            TEraTask,
            TEraVideo,
            TCapsule,
            TTopic,
        ),
    ),
)


@dataclass(frozen=True)
class MySqlConnConfig:
    """MySQL 连接配置（不含库名，库名按同步清单逐库拼接）。"""

    host: str
    port: int
    user: str
    password: str

    def display(self) -> str:
        return f"{self.user}@{self.host}:{self.port}"


@dataclass(frozen=True)
class TableSyncTask:
    db_name: str
    model: type[DeclarativeBase]

    @property
    def table(self) -> Table:
        return self.model.__table__


@dataclass
class TableSyncResult:
    db_name: str
    table_name: str
    status: str = "ok"  # ok / skipped / failed
    source_rows: int = 0
    target_rows_before: int = 0
    target_rows_after: int = 0
    written_rows: int = 0
    sync_columns: int = 0
    detail: str = ""
    elapsed_seconds: float = 0.0


@dataclass(frozen=True)
class SyncOptions:
    batch_size: int
    limit: int
    dry_run: bool
    truncate: bool


# ---------------------------------------------------------------------------
# 连接配置解析
# ---------------------------------------------------------------------------


def _resolve_source_config(args: argparse.Namespace) -> MySqlConnConfig:
    """解析源（生产库）连接：--source-db-* > SYNC_SOURCE_MYSQL_* 环境变量 > env 文件。"""
    env_path = (
        Path(args.source_env) if args.source_env else _project_root / ".env.fastapi.prod"
    )
    if not env_path.is_file():
        raise SystemExit(
            f"源环境文件不存在: {env_path}"
            "（可用 --source-env 指定其它文件，或用 --source-db-* 直接覆盖连接信息）"
        )
    file_values = dotenv_values(env_path)

    def pick(cli_value: str | None, env_key: str, file_key: str) -> str:
        for candidate in (cli_value, os.environ.get(env_key), file_values.get(file_key)):
            if candidate:
                return candidate
        raise SystemExit(
            f"缺少源库配置项 {file_key}"
            f"（--source-db-* / 环境变量 {env_key} / {env_path.name} 均未提供）"
        )

    host = pick(args.source_db_host, f"{_SOURCE_ENV_PREFIX}HOST", "MYSQL_HOST")
    port_text = pick(args.source_db_port, f"{_SOURCE_ENV_PREFIX}PORT", "MYSQL_PORT")
    user = pick(args.source_db_user, f"{_SOURCE_ENV_PREFIX}USER", "MYSQL_USER")
    password = pick(
        args.source_db_password, f"{_SOURCE_ENV_PREFIX}PASSWORD", "MYSQL_PASSWORD"
    )
    try:
        port = int(port_text)
    except ValueError as exc:
        raise SystemExit(f"源库端口不是数字: {port_text!r}") from exc
    return MySqlConnConfig(host=host, port=port, user=user, password=password)


def _resolve_target_config(args: argparse.Namespace) -> MySqlConnConfig:
    """解析目标（本地开发库）连接：CLI --db-* 经 apply_cli_overrides 写入全局 settings。"""
    apply_cli_overrides(args)
    return MySqlConnConfig(
        host=settings.MYSQL_HOST,
        port=int(settings.MYSQL_PORT),
        user=settings.MYSQL_USER,
        password=settings.MYSQL_PASSWORD,
    )


def _build_db_url(conn: MySqlConnConfig, db_name: str) -> str:
    """按连接配置拼某个库的异步 MySQL URL（密码特殊字符由 URL.create 自动转义）。"""
    url = URL.create(
        "mysql+aiomysql",
        username=conn.user,
        password=conn.password,
        host=conn.host,
        port=conn.port,
        database=db_name,
        query={"charset": "utf8mb4", "autocommit": "true"},
    )
    return url.render_as_string(hide_password=False)


def _engine_for(conn: MySqlConnConfig, db_name: str) -> AsyncEngine:
    """按 (url, is_crawler) 复用进程内连接池，源/目标各库各一个 Engine。"""
    _session, engine = sqlalchemy_session_factory(
        _build_db_url(conn, db_name), is_crawler=False
    )
    return engine


# ---------------------------------------------------------------------------
# 过滤条件
# ---------------------------------------------------------------------------


def _parse_filters(args: argparse.Namespace) -> tuple[set[str], set[str]]:
    """解析 --database / --table（均可重复或逗号分隔），返回 (库名集合, 表名集合)。"""

    def flatten(values: list[str] | None) -> set[str]:
        if not values:
            return set()
        return {
            item.strip() for value in values for item in value.split(",") if item.strip()
        }

    databases = flatten(args.database)
    tables = flatten(args.table)

    valid_databases = {db.name for db in LOTTERY_DATABASES}
    invalid_databases = databases - valid_databases
    if invalid_databases:
        raise SystemExit(
            f"未知数据库: {sorted(invalid_databases)}，可选: {sorted(valid_databases)}"
        )

    valid_tables = {
        model.__tablename__ for db in LOTTERY_DATABASES for model in db.models
    }
    invalid_tables = tables - valid_tables
    if invalid_tables:
        raise SystemExit(
            f"未知表: {sorted(invalid_tables)}，可选: {sorted(valid_tables)}"
        )
    return databases, tables


def _build_tasks(databases: set[str], tables: set[str]) -> list[LotteryDatabase]:
    """按过滤条件裁剪同步清单；保持 LOTTERY_DATABASES 内的拓扑友好顺序。"""
    selected: list[LotteryDatabase] = []
    for db in LOTTERY_DATABASES:
        if databases and db.name not in databases:
            continue
        models = tuple(
            model for model in db.models if not tables or model.__tablename__ in tables
        )
        if models:
            selected.append(LotteryDatabase(name=db.name, models=models))
    if not selected:
        raise SystemExit("过滤条件下没有可同步的表")
    return selected


# ---------------------------------------------------------------------------
# 表结构工具
# ---------------------------------------------------------------------------


def _topological_order(tables: Sequence[Table]) -> list[Table]:
    """按表间外键依赖做拓扑排序（Kahn），保证子表后写、父表先写。"""
    name_to_table = {table.name: table for table in tables}
    pending_parents: dict[str, set[str]] = {}
    for table in tables:
        parents: set[str] = set()
        for constraint in table.foreign_key_constraints:
            for foreign_key in constraint.elements:
                parent_name = foreign_key.column.table.name
                if parent_name in name_to_table and parent_name != table.name:
                    parents.add(parent_name)
        pending_parents[table.name] = parents

    ordered: list[Table] = []
    while pending_parents:
        ready = sorted(
            name
            for name, parents in pending_parents.items()
            if not parents & set(pending_parents)
        )
        if not ready:
            myfastapi_logger.warning(
                f"检测到外键循环依赖，剩余表按原顺序写入: {sorted(pending_parents)}"
            )
            ordered.extend(name_to_table[name] for name in pending_parents)
            break
        for name in ready:
            ordered.append(name_to_table[name])
            del pending_parents[name]
    return ordered


async def _fetch_table_columns(engine: AsyncEngine, table_name: str) -> set[str] | None:
    """读取某库实际存在的列名；表不存在返回 None。"""

    def _query(sync_conn) -> set[str] | None:
        inspector = inspect(sync_conn)
        if not inspector.has_table(table_name):
            return None
        return {column["name"] for column in inspector.get_columns(table_name)}

    async with engine.connect() as conn:
        return await conn.run_sync(_query)


def _resolve_sync_columns(
    table: Table, source_columns: set[str], target_columns: set[str]
) -> list[Column]:
    """同步列 = ORM 列 ∩ 源库列 ∩ 目标库列，排除 Computed 列（如 bilidyndetail.rid_int）。"""
    return [
        column
        for column in table.columns
        if column.computed is None
        and column.name in source_columns
        and column.name in target_columns
    ]


def _build_upsert(
    table: Table, columns: Sequence[Column], rows: Sequence[dict[str, object]]
) -> Executable:
    """构造 INSERT ... ON DUPLICATE KEY UPDATE；无可用更新列时退化为 INSERT IGNORE。"""
    stmt = mysql_insert(table).values(list(rows))
    updatable = {
        column.key: stmt.inserted[column.key]
        for column in columns
        if not column.primary_key
    }
    if not updatable:
        return stmt.prefix_with("IGNORE")
    return stmt.on_duplicate_key_update(**updatable)


def _keyset_predicate(
    pk_columns: Sequence[Column], last_key: Sequence[object]
) -> Executable:
    """主键元组 keyset 条件：WHERE (pk...) > (:last...)，单列主键自动退化为主键比较。"""
    params = [
        bindparam(f"_pk_{index}", value=value, type_=column.type)
        for index, (column, value) in enumerate(zip(pk_columns, last_key))
    ]
    return tuple_(*pk_columns) > tuple_(*params)


# ---------------------------------------------------------------------------
# 同步执行
# ---------------------------------------------------------------------------


async def _count_rows(engine: AsyncEngine, table: Table) -> int:
    async with engine.connect() as conn:
        result = await conn.execute(select(func.count()).select_from(table))
        return result.scalar() or 0


async def _execute_with_retry(engine: AsyncEngine, stmt: Executable) -> None:
    """单批写入带重试（死锁 / 瞬时错误），仍失败则抛给上层按表记录失败。"""
    max_tries = 3
    for attempt in range(1, max_tries + 1):
        try:
            async with engine.begin() as conn:
                await conn.execute(stmt)
            return
        except Exception as exc:  # noqa: BLE001
            if attempt >= max_tries:
                raise
            wait_seconds = 0.5 * attempt
            myfastapi_logger.warning(
                f"写入失败，第 {attempt}/{max_tries} 次重试（等待 {wait_seconds:.1f}s）: {exc}"
            )
            await asyncio.sleep(wait_seconds)


async def _iter_source_batches(
    src_engine: AsyncEngine,
    table: Table,
    columns: Sequence[Column],
    pk_columns: Sequence[Column],
    options: SyncOptions,
):
    """按主键 keyset 分页从源库读数据，逐批 yield 行字典。"""
    last_key: tuple[object, ...] | None = None
    read_rows = 0
    async with src_engine.connect() as conn:
        while True:
            stmt = select(*columns).order_by(*pk_columns).limit(options.batch_size)
            if last_key is not None:
                stmt = stmt.where(_keyset_predicate(pk_columns, last_key))
            result = await conn.execute(stmt)
            rows = result.fetchall()
            # 及时结束只读事务，尽快释放源库的 MVCC 读快照
            await conn.commit()
            if not rows:
                return
            yield [dict(row._mapping) for row in rows]
            read_rows += len(rows)
            if len(rows) < options.batch_size:
                return
            if options.limit and read_rows >= options.limit:
                return
            last_key = tuple(
                rows[-1]._mapping[column.key] for column in pk_columns
            )


async def _truncate_table(tgt_engine: AsyncEngine, table: Table) -> None:
    """清空目标表（DELETE 而非 TRUNCATE，避免外键约束与隐式提交问题）。"""
    async with tgt_engine.begin() as conn:
        await conn.execute(delete(table))


async def _sync_table(
    task: TableSyncTask,
    src_engine: AsyncEngine,
    tgt_engine: AsyncEngine,
    options: SyncOptions,
) -> TableSyncResult:
    """同步单张表：列交集 -> keyset 分页读 -> 批量 upsert 写。"""
    table = task.table
    result = TableSyncResult(db_name=task.db_name, table_name=table.name)
    started = time.perf_counter()

    source_columns = await _fetch_table_columns(src_engine, table.name)
    if source_columns is None:
        result.status = "skipped"
        result.detail = "源库不存在该表"
        return result
    target_columns = await _fetch_table_columns(tgt_engine, table.name)
    if target_columns is None:
        result.status = "skipped"
        result.detail = "目标库不存在该表（目标库是否未执行迁移？）"
        return result

    columns = _resolve_sync_columns(table, source_columns, target_columns)
    pk_columns = [
        column
        for column in table.primary_key.columns
        if column.name in {item.name for item in columns}
    ]
    if not columns or not pk_columns:
        result.status = "skipped"
        result.detail = "源库与目标库没有可同步的列（或主键列缺失）"
        return result
    result.sync_columns = len(columns)

    missing_in_target = sorted(source_columns - target_columns)
    if missing_in_target:
        result.detail = f"目标库缺少列，未同步: {missing_in_target}"

    if options.dry_run:
        result.source_rows = await _count_rows(src_engine, table)
        result.target_rows_before = await _count_rows(tgt_engine, table)
        result.elapsed_seconds = time.perf_counter() - started
        return result

    read_rows = 0
    batch_index = 0
    async for batch in _iter_source_batches(
        src_engine, table, columns, pk_columns, options
    ):
        batch_index += 1
        await _execute_with_retry(tgt_engine, _build_upsert(table, columns, batch))
        read_rows += len(batch)
        if batch_index % _PROGRESS_BATCH_INTERVAL == 0:
            myfastapi_logger.info(
                f"[{task.db_name}.{table.name}] 进度: 已写入 {read_rows} 行"
            )

    result.source_rows = read_rows
    result.written_rows = read_rows
    result.elapsed_seconds = time.perf_counter() - started
    return result


async def _sync_database(
    db: LotteryDatabase,
    source: MySqlConnConfig,
    target: MySqlConnConfig,
    options: SyncOptions,
) -> list[TableSyncResult]:
    """同步单个库：可选清空目标表 -> 按拓扑序逐表 upsert。"""
    src_engine = _engine_for(source, db.name)
    tgt_engine = _engine_for(target, db.name)
    ordered_tables = _topological_order([model.__table__ for model in db.models])

    if options.truncate:
        for table in reversed(ordered_tables):
            if await _fetch_table_columns(tgt_engine, table.name) is None:
                continue
            await _truncate_table(tgt_engine, table)
            myfastapi_logger.info(f"[{db.name}] 已清空目标表 {table.name}")

    results: list[TableSyncResult] = []
    for table in ordered_tables:
        task = TableSyncTask(db_name=db.name, model=_model_for_table(db, table))
        myfastapi_logger.info(f"[{db.name}] 开始同步表 {table.name}")
        try:
            results.append(await _sync_table(task, src_engine, tgt_engine, options))
        except Exception as exc:  # noqa: BLE001
            myfastapi_logger.exception(f"[{db.name}] 同步表 {table.name} 失败: {exc}")
            failed = TableSyncResult(
                db_name=db.name, table_name=table.name, status="failed", detail=str(exc)
            )
            results.append(failed)
    return results


def _model_for_table(db: LotteryDatabase, table: Table) -> type[DeclarativeBase]:
    for model in db.models:
        if model.__table__.name == table.name:
            return model
    raise ValueError(f"库 {db.name} 中找不到表 {table.name} 对应的 ORM 模型")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="从生产库同步抽奖数据到本地开发库（源默认 .env.fastapi.prod，目标为全局 settings）",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""示例:
  # 预览（不写库）
  python -m scripts.database.sync_lottery_from_prod.sync_lottery --dry-run

  # 全量同步（生产 mysql 在容器网络内，本地跑需覆盖源地址）
  python -m scripts.database.sync_lottery_from_prod.sync_lottery \\
      --source-db-host <生产库地址> --source-db-port 3306 \\
      --source-db-user root --source-db-password <密码>

  # 只同步 dyndetail 的 lotdata
  python -m scripts.database.sync_lottery_from_prod.sync_lottery --database dyndetail --table lotdata

  # 精确镜像（先清空目标表再写入）
  python -m scripts.database.sync_lottery_from_prod.sync_lottery --truncate
""",
    )
    # 目标（本地开发库）连接：复用项目通用 CLI 参数（含 apply_cli_overrides 依赖的 llm 参数）
    add_llm_args(parser)
    add_db_args(parser)

    source_group = parser.add_argument_group(
        "源数据库配置（生产库，默认读取 .env.fastapi.prod）"
    )
    source_group.add_argument(
        "--source-env",
        type=str,
        default=None,
        help="源环境 env 文件路径（默认: 项目根目录 .env.fastapi.prod）",
    )
    source_group.add_argument("--source-db-host", type=str, default=None, help="源 MySQL 主机")
    source_group.add_argument("--source-db-port", type=str, default=None, help="源 MySQL 端口")
    source_group.add_argument("--source-db-user", type=str, default=None, help="源 MySQL 用户名")
    source_group.add_argument(
        "--source-db-password", type=str, default=None, help="源 MySQL 密码"
    )

    parser.add_argument(
        "--database",
        action="append",
        default=None,
        help="仅同步指定库（可重复或逗号分隔）：dyndetail / biliopusdb / bili_reserve / bilidb",
    )
    parser.add_argument(
        "--table",
        action="append",
        default=None,
        help="仅同步指定表（可重复或逗号分隔，如 lotdata,t_lotdyninfo）",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="每批读取/写入的行数（默认: 500；含 LONGTEXT 的表可调小）",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="每张表最多同步的行数，0 表示不限制（默认: 0；仅用于小批量试跑）",
    )
    parser.add_argument(
        "--truncate",
        action="store_true",
        help="先按逆拓扑序清空目标表再写入（精确镜像，会删除目标库既有数据）",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="仅校验连通性并统计源/目标行数，不写任何数据",
    )
    return parser


def _print_plan(
    source: MySqlConnConfig,
    target: MySqlConnConfig,
    selected: Sequence[LotteryDatabase],
    options: SyncOptions,
) -> None:
    if options.dry_run:
        mode = "DRY-RUN（只统计，不写入）"
    elif options.truncate:
        mode = "精确镜像（先清空目标表再写入）"
    else:
        mode = "upsert（增量刷新，可反复执行）"
    print("=" * 72)
    print(f"源（生产库）: {source.display()}")
    print(f"目标（开发库）: {target.display()}")
    print(
        f"模式: {mode} | 批次大小: {options.batch_size}"
        + (f" | 每表上限: {options.limit} 行" if options.limit else "")
    )
    print("同步范围:")
    for db in selected:
        tables = "、".join(model.__tablename__ for model in db.models)
        print(f"  [{db.name}] {tables}")
    print("=" * 72)


def _print_summary(
    results: Sequence[TableSyncResult], elapsed: float, dry_run: bool
) -> None:
    print()
    print("=" * 72)
    print("同步汇总")
    print("=" * 72)
    for item in results:
        if item.status == "skipped":
            line = f"[{item.db_name}] {item.table_name}: 跳过（{item.detail}）"
        elif item.status == "failed":
            line = f"[{item.db_name}] {item.table_name}: 失败（{item.detail[:120]}）"
        elif dry_run:
            line = (
                f"[{item.db_name}] {item.table_name}: "
                f"源 {item.source_rows} 行, 目标 {item.target_rows_before} 行"
            )
        else:
            line = (
                f"[{item.db_name}] {item.table_name}: "
                f"读取 {item.source_rows} 行, 写入 {item.written_rows} 行"
                f"（{item.sync_columns} 列, {item.elapsed_seconds:.1f}s）"
            )
        if item.detail and item.status == "ok":
            line += f" | {item.detail}"
        print(line)
    total_read = sum(item.source_rows for item in results)
    total_written = sum(item.written_rows for item in results)
    failed = [item for item in results if item.status == "failed"]
    print("-" * 72)
    print(
        f"合计: 读取 {total_read} 行, 写入 {total_written} 行, "
        f"耗时 {elapsed:.1f}s, 失败 {len(failed)} 张表"
    )
    print("=" * 72)


async def _async_main(
    args: argparse.Namespace, source: MySqlConnConfig, target: MySqlConnConfig
) -> int:
    databases, tables = _parse_filters(args)
    selected = _build_tasks(databases, tables)
    options = SyncOptions(
        batch_size=args.batch_size,
        limit=args.limit,
        dry_run=args.dry_run,
        truncate=args.truncate,
    )

    if source.host == target.host and source.port == target.port:
        myfastapi_logger.warning(
            f"源与目标指向同一实例（{source.display()}），本次等同于库内自同步，仅用于验证脚本"
        )

    _print_plan(source, target, selected, options)

    started = time.perf_counter()
    results: list[TableSyncResult] = []
    for db in selected:
        results.extend(await _sync_database(db, source, target, options))
    elapsed = time.perf_counter() - started

    _print_summary(results, elapsed, options.dry_run)
    return 1 if any(item.status == "failed" for item in results) else 0


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    if args.batch_size <= 0:
        raise SystemExit("--batch-size 必须为正数")
    if args.limit < 0:
        raise SystemExit("--limit 不能为负数")

    # 先解析源配置（纯文件/环境变量，不建连接），再做目标库 CLI 覆盖
    source = _resolve_source_config(args)
    target = _resolve_target_config(args)

    exit_code = asyncio.run(_async_main(args, source, target))
    if exit_code:
        sys.exit(exit_code)


if __name__ == "__main__":
    main()
