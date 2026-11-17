"""按「失败原因 + 时间窗口」清理 proxy_tab 僵尸代理（默认 dry-run）。

用法（在 be-bilibili-crawler 目录下）：

    # 只统计各原因分组的候选行数，不删除（默认）
    .venv/bin/python -m scripts.代理.clean_unusable_proxy --dry-run

    # 确认真实删除（分批 + 单轮上限）
    .venv/bin/python -m scripts.代理.clean_unusable_proxy --execute

    # 临时覆盖批大小 / 单轮上限（不改 .env）
    .venv/bin/python -m scripts.代理.clean_unusable_proxy --execute --batch-size 2000 --max-rows 10000

口径见 ``SQLHelper.clear_unusable_proxy``：
network / unknown 按阈值 + 窗口内无成功判定；response 只降权；risk_control 永不删除。
"""

import argparse
import asyncio

from CONFIG import CONFIG
from Utils.代理.数据库操作.async_proxy_op_alchemy_mysql_ver import SQLHelper


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="按失败原因清理 proxy_tab 僵尸代理")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--dry-run",
        action="store_true",
        help="只输出各原因分组的候选行数，不删除（默认行为）",
    )
    mode.add_argument(
        "--execute",
        action="store_true",
        help="真实删除（分批 + 批间 sleep + 单轮上限）",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=None,
        help="单批删除行数（默认取 PROXY_HEALTH__CLEAN_BATCH_SIZE）",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=None,
        help="单轮最多删除行数（默认取 PROXY_HEALTH__CLEAN_MAX_ROWS_PER_RUN）",
    )
    return parser.parse_args()


async def main() -> None:
    args = _parse_args()
    cfg = CONFIG.proxy_health
    if args.batch_size is not None:
        cfg.clean_batch_size = max(1, args.batch_size)
    if args.max_rows is not None:
        cfg.clean_max_rows_per_run = max(1, args.max_rows)
    dry_run = not args.execute
    result = await SQLHelper.clear_unusable_proxy(dry_run=dry_run)
    label = "候选" if dry_run else "删除"
    print(
        "代理池清理{}：{}".format(
            "（dry-run，未删除任何行）" if dry_run else "",
            " / ".join(f"{kind} {label} {count} 行" for kind, count in result.items()),
        )
    )


if __name__ == "__main__":
    asyncio.run(main())
