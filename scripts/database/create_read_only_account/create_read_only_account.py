"""创建「查询服务」专用的只读 MySQL 账号（一次性运维动作，可重复执行）。

背景
----
拆分出 crawler-query（``SERVICE_ROLE=query``）之后，查询进程理论上不写库：
写入路由没挂载、预约刷新已按角色关闭、lifespan 不跑 alembic。
但「理论上不写」和「权限上不能写」是两回事——用只读账号，任何漏网的写
（比如以后新增查询接口时又写了个 ``_background_save_xxx``）都会立刻报
``1142 SELECT command denied``，而不是悄悄把生产库写脏。

为什么不放进 init 目录（``docker_vol/mysql_data/init`` /
``/docker-entrypoint-initdb.d``）：
  1. 本项目的 mysql 容器只挂了 ``conf.d`` 与 ``data``，没有 init 机制，不该为此引入；
  2. MySQL 的 init 脚本**只在数据目录为空时执行一次**，生产库早已初始化，
     放进去永远不会执行——看起来配好了实际没跑，这是最容易踩的坑；
  3. 密码会以明文进 git；
  4. 建账号是一次性动作，不是每次部署都要执行的步骤。

授权范围
--------
只授查询服务真正读的 4 个库（见 ``CONFIG.DatabaseConfig.MYSQL`` 的 URI 定义，
注意 ``get_other_lot_URI`` 对应的真实库名是 ``biliopusdb``）：

    bilidb / bili_reserve / biliopusdb / dyndetail

``proxy_db`` / ``samsclub`` 是爬虫侧专用，不授权。只给 ``SELECT``，不给
``SHOW VIEW``——除非下面第 2 步查出来确有视图（见脚本运行时的提示）。

用法
----
本地开发库（端口按根 .env 的 MYSQL_PORT）：

    uv run python -m scripts.database.create_read_only_account.create_read_only_account \\
        --db-host 127.0.0.1 --db-port 10000 \\
        --admin-user root --admin-password '<root 密码>' \\
        --readonly-user bili_ro --readonly-password '<只读密码>'

生产库（管理员账号需有 CREATE USER / GRANT 权限）：

    uv run python -m scripts.database.create_read_only_account.create_read_only_account \\
        --db-host <生产地址> --db-port 3306 \\
        --admin-user root --admin-password '<管理员密码>' \\
        --readonly-user bili_ro --readonly-password '<只读密码>' \\
        --allow-host '<开发服务器出口IP>'

只看要执行什么（不连库）：

    ... --dry-run

回收账号：

    ... --revoke
"""

import argparse
import asyncio
import os
import re
import sys
from pathlib import Path

import aiomysql

_project_root = Path(__file__).resolve().parents[3]
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

# 查询服务实际读取的库（与 Utils/FastAPI/QueryFastapiLifespan.py 的探测清单一致）
READONLY_DATABASES = ("bilidb", "bili_reserve", "biliopusdb", "dyndetail")

# 用户名只允许字母数字下划线，避免把 ``'`` 拼进 DDL（CREATE USER 不支持绑定参数）
_USERNAME_RE = re.compile(r"^[A-Za-z0-9_]{1,32}$")


def _sql_quote(value: str) -> str:
    """把字符串转成 MySQL 单引号字面量（DDL 无法用占位符绑定）。"""
    return "'" + value.replace("\\", "\\\\").replace("'", "''") + "'"


def _build_statements(
    readonly_user: str,
    readonly_password: str,
    allow_host: str,
    max_user_connections: int,
    revoke: bool,
) -> list[str]:
    account = f"{_sql_quote(readonly_user)}@{_sql_quote(allow_host)}"

    if revoke:
        return [
            f"DROP USER IF EXISTS {account}",
        ]

    statements = [
        # 已存在时不改密码，所以后面再补一条 ALTER USER，保证幂等且密码一致
        f"CREATE USER IF NOT EXISTS {account} IDENTIFIED BY {_sql_quote(readonly_password)}",
        f"ALTER USER {account} IDENTIFIED BY {_sql_quote(readonly_password)}",
    ]
    # 连接数配额：默认不设置（0 = 不限制）；确需收窄时显式传 --max-user-connections
    if max_user_connections > 0:
        statements.append(
            f"ALTER USER {account} WITH MAX_USER_CONNECTIONS {max_user_connections}"
        )
    statements += [
        f"GRANT SELECT ON {db}.* TO {account}" for db in READONLY_DATABASES
    ]
    return statements


async def _run(args) -> int:
    statements = _build_statements(
        readonly_user=args.readonly_user,
        readonly_password=args.readonly_password,
        allow_host=args.allow_host,
        max_user_connections=args.max_user_connections,
        revoke=args.revoke,
    )

    print(f"目标实例: {args.db_host}:{args.db_port}（管理员: {args.admin_user}）")
    print(f"数据库范围: {', '.join(READONLY_DATABASES)}")
    print("待执行语句（逐条执行；整段粘贴到不支持多语句的客户端会报 1064）:")
    for st in statements:
        # 遮蔽密码，避免贴日志时泄露
        shown = re.sub(r"IDENTIFIED BY '[^']*'", "IDENTIFIED BY '******'", st)
        print("  ", shown + ";")

    if args.dry_run:
        print("\n--dry-run：未连接数据库，未执行任何语句")
        return 0

    conn = await aiomysql.connect(
        host=args.db_host,
        port=args.db_port,
        user=args.admin_user,
        password=args.admin_password,
        autocommit=True,
        connect_timeout=10,
    )
    try:
        async with conn.cursor() as cur:
            for st in statements:
                await cur.execute(st)
            # 不执行 FLUSH PRIVILEGES：GRANT/REVOKE 会立即生效，
            # 且 MySQL 8.4 起该语句已废弃（会打 deprecation warning）

            if args.revoke:
                print("\n账号已回收")
                return 0

            account = f"{_sql_quote(args.readonly_user)}@{_sql_quote(args.allow_host)}"
            await cur.execute(f"SHOW GRANTS FOR {account}")
            rows = await cur.fetchall()
            print("\n生效的授权:")
            for (grant,) in rows:
                print("  ", grant)

            # 存在视图就必须补 SHOW VIEW，否则查询视图会 1142
            await cur.execute(
                "SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.VIEWS "
                "WHERE TABLE_SCHEMA IN ("
                + ", ".join(_sql_quote(db) for db in READONLY_DATABASES)
                + ")"
            )
            views = await cur.fetchall()
            if views:
                print("\n注意: 以下视图需要额外授权 SHOW VIEW")
                for schema, name in views:
                    print(f"   {schema}.{name}")
                print(
                    "  "
                    f"GRANT SHOW VIEW ON <db>.* TO "
                    f"{_sql_quote(args.readonly_user)}@{_sql_quote(args.allow_host)};"
                )
    finally:
        conn.close()

    print("\n完成。用下面的命令自检（应只看到 4 个业务库 + information_schema）:")
    print(
        f"  mysql -h{args.db_host} -P{args.db_port} "
        f"-u{args.readonly_user} -p -e 'SHOW DATABASES;'"
    )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="创建/回收查询服务专用的只读 MySQL 账号"
    )
    parser.add_argument("--db-host", required=True, help="MySQL 地址")
    parser.add_argument("--db-port", type=int, default=3306, help="MySQL 端口")
    parser.add_argument(
        "--admin-user", default="root", help="管理员账号（需 CREATE USER / GRANT 权限）"
    )
    parser.add_argument(
        "--admin-password",
        default="",
        help="管理员密码；不传则读环境变量 MYSQL_ADMIN_PASSWORD",
    )
    parser.add_argument("--readonly-user", default="bili_ro", help="只读账号名")
    parser.add_argument(
        "--readonly-password",
        default="",
        help="只读账号密码；不传则读环境变量 MYSQL_READONLY_PASSWORD",
    )
    parser.add_argument(
        "--allow-host",
        default="%",
        help="只读账号的来源主机，默认 %%（建议用出口 IP 收窄）",
    )
    parser.add_argument(
        "--max-user-connections",
        type=int,
        default=0,
        help="该账号的最大并发连接数；默认 0 表示不设置（不限制）",
    )
    parser.add_argument("--revoke", action="store_true", help="回收账号（DROP USER）")
    parser.add_argument(
        "--dry-run", action="store_true", help="只打印将执行的语句，不连库"
    )
    args = parser.parse_args()

    if not _USERNAME_RE.match(args.readonly_user):
        print(
            f"非法用户名: {args.readonly_user!r}（只允许字母/数字/下划线）", file=sys.stderr
        )
        return 2

    args.admin_password = args.admin_password or os.environ.get(
        "MYSQL_ADMIN_PASSWORD", ""
    )
    args.readonly_password = args.readonly_password or os.environ.get(
        "MYSQL_READONLY_PASSWORD", ""
    )
    if not args.dry_run and not args.admin_password:
        print(
            "缺少管理员密码：用 --admin-password 或环境变量 MYSQL_ADMIN_PASSWORD",
            file=sys.stderr,
        )
        return 2
    if not args.revoke and not args.dry_run and not args.readonly_password:
        print(
            "缺少只读密码：用 --readonly-password 或环境变量 MYSQL_READONLY_PASSWORD",
            file=sys.stderr,
        )
        return 2

    return asyncio.run(_run(args))


if __name__ == "__main__":
    raise SystemExit(main())
