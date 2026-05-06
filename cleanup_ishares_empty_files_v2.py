#!/usr/bin/env python3
# -*- coding: utf-8 -*-
r"""
cleanup_ishares_empty_files_v2.py

用途：
  清理 iShares / BlackRock 非交易日、节假日下载到的“空 holdings CSV”。

空文件样本特征：
  Fund Holdings as of,"-"
  Shares Outstanding,"-"
  Stock,"-"
  Bond,"-"
  Cash,"-"
  Other,"-"
  Ticker,Name,Sector,Asset Class,...
  然后没有真实持仓行，直接进入 BlackRock 免责声明。

相比 v1：
  不再只靠文件大小。
  先用内容判断；可选再叠加大小阈值保护。

CMD 示例：
  cd /d F:\globle

  只预览，不删除：
  python cleanup_ishares_empty_files_v2.py --root F:\zhenghe

  真正删除：
  python cleanup_ishares_empty_files_v2.py --root F:\zhenghe --delete

  只清理指定日期范围：
  python cleanup_ishares_empty_files_v2.py --root F:\zhenghe --start 2021-01-04 --end 2026-04-24 --delete

  额外要求小于 12KB 才删除，更保守：
  python cleanup_ishares_empty_files_v2.py --root F:\zhenghe --max-bytes 12288 --delete
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import re
from pathlib import Path


HOLDINGS_HEADER_PREFIX = "Ticker,Name,Sector,Asset Class,Market Value,Weight (%)"
DISCLAIMER_PREFIX = '"The content contained herein is owned or licensed by BlackRock'
DISCLAIMER_PREFIX_2 = "The content contained herein is owned or licensed by BlackRock"


def parse_date(value: str | None) -> dt.date | None:
    if not value:
        return None
    for fmt in ("%Y-%m-%d", "%Y%m%d"):
        try:
            return dt.datetime.strptime(value, fmt).date()
        except ValueError:
            pass
    raise ValueError(f"日期格式错误: {value}，请用 YYYY-MM-DD 或 YYYYMMDD")


def parse_date_from_dir_name(name: str) -> dt.date | None:
    try:
        return dt.datetime.strptime(name, "%Y-%m-%d").date()
    except ValueError:
        return None


def read_text(path: Path) -> str:
    raw = path.read_bytes()
    for enc in ("utf-8-sig", "utf-8", "latin-1", "gb18030"):
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="replace")


def normalize_line(line: str) -> str:
    # 去掉 BOM、NBSP、普通空白
    return line.replace("\ufeff", "").replace("\xa0", "").strip()


def is_ishares_empty_template(path: Path) -> tuple[bool, str]:
    """
    返回 (是否空模板, 原因)
    """
    try:
        text = read_text(path)
    except Exception as exc:
        return False, f"read_error: {exc!r}"

    lines = [normalize_line(x) for x in text.splitlines()]
    non_empty = [x for x in lines if x]

    if not non_empty:
        return True, "empty_file"

    joined_head = "\n".join(non_empty[:12])

    # 关键特征：日期和份额等关键元数据都是 "-"
    dash_markers = [
        'Fund Holdings as of,"-"',
        'Shares Outstanding,"-"',
        'Stock,"-"',
        'Bond,"-"',
        'Cash,"-"',
        'Other,"-"',
    ]

    marker_count = sum(1 for m in dash_markers if m in joined_head)

    if marker_count < 3:
        return False, f"not_empty_template: dash_marker_count={marker_count}"

    # 找 holdings 表头
    header_idx = None
    for i, line in enumerate(non_empty):
        if line.startswith(HOLDINGS_HEADER_PREFIX):
            header_idx = i
            break

    if header_idx is None:
        return False, "no_holdings_header"

    # 表头后面第一个非空行
    after_header = non_empty[header_idx + 1 :]

    if not after_header:
        return True, "header_only_no_rows"

    first_after = after_header[0]

    # 空模板：表头后直接免责声明
    if first_after.startswith(DISCLAIMER_PREFIX) or first_after.startswith(DISCLAIMER_PREFIX_2):
        return True, "header_then_blackrock_disclaimer"

    # 有些文件中间可能有一行特殊空白，前面 non_empty 已经去掉；
    # 如果表头后没有像 CSV 持仓行的内容，也认为是空模板。
    # 真实行通常至少有多个逗号，并且第一列不会是免责声明。
    possible_data_rows = []
    for line in after_header:
        if line.startswith(DISCLAIMER_PREFIX) or line.startswith(DISCLAIMER_PREFIX_2):
            break
        if line.count(",") >= 8:
            possible_data_rows.append(line)

    if len(possible_data_rows) == 0:
        return True, "no_data_rows_between_header_and_disclaimer"

    return False, f"has_possible_data_rows={len(possible_data_rows)}"


def main() -> int:
    parser = argparse.ArgumentParser(description="Content-aware cleanup for empty iShares holdings CSV files.")
    parser.add_argument("--root", default=r"F:\zhenghe", help="项目根目录，例如 F:\\zhenghe")
    parser.add_argument("--vendor", default="ishares", help="vendor 名称，默认 ishares")
    parser.add_argument("--start", default=None, help="开始日期 YYYY-MM-DD，可选")
    parser.add_argument("--end", default=None, help="结束日期 YYYY-MM-DD，可选")
    parser.add_argument("--delete", action="store_true", help="真正删除；不加则只 dry-run")
    parser.add_argument("--max-bytes", type=int, default=None, help="可选：只有文件 <= max-bytes 才删除，更保守")
    parser.add_argument("--print-every", type=int, default=1000, help="每扫描多少个文件打印一次进度")

    args = parser.parse_args()

    root = Path(args.root)
    raw_root = root / "data" / "vendors" / args.vendor / "raw"
    history_dir = root / "data" / "vendors" / args.vendor / "history"
    history_dir.mkdir(parents=True, exist_ok=True)

    start = parse_date(args.start)
    end = parse_date(args.end)

    if not raw_root.exists():
        print(f"[ERROR] raw 目录不存在: {raw_root}")
        return 2

    rows = []
    scanned = 0
    matched = 0
    deleted = 0

    date_dirs = sorted([p for p in raw_root.iterdir() if p.is_dir()], key=lambda p: p.name)

    for date_dir in date_dirs:
        date_obj = parse_date_from_dir_name(date_dir.name)
        if date_obj is None:
            continue
        if start and date_obj < start:
            continue
        if end and date_obj > end:
            continue

        for file_path in date_dir.glob("*.csv"):
            scanned += 1

            if scanned % args.print_every == 0:
                print(f"[scan] scanned={scanned}, matched={matched}, deleted={deleted}")

            size = file_path.stat().st_size

            if args.max_bytes is not None and size > args.max_bytes:
                continue

            is_empty, reason = is_ishares_empty_template(file_path)

            if is_empty:
                matched += 1
                action = "dry_run"

                if args.delete:
                    file_path.unlink()
                    deleted += 1
                    action = "deleted"

                rows.append(
                    {
                        "date": date_dir.name,
                        "file": str(file_path),
                        "bytes": size,
                        "reason": reason,
                        "action": action,
                    }
                )

    ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = history_dir / f"cleanup_empty_template_{args.vendor}_{ts}.csv"

    with log_path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=["date", "file", "bytes", "reason", "action"])
        writer.writeheader()
        writer.writerows(rows)

    print("========================================")
    print("iShares empty template cleanup v2")
    print("========================================")
    print(f"raw_root    : {raw_root}")
    print(f"date range  : {start or '-'} -> {end or '-'}")
    print(f"mode        : {'DELETE' if args.delete else 'DRY-RUN'}")
    print(f"max_bytes   : {args.max_bytes if args.max_bytes is not None else 'content-only'}")
    print(f"scanned     : {scanned}")
    print(f"matched     : {matched}")
    print(f"deleted     : {deleted}")
    print(f"log         : {log_path}")
    print("========================================")

    if not args.delete:
        print("[NOTE] 当前只是 dry-run，没有删除。确认日志没问题后，加 --delete 再跑。")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
