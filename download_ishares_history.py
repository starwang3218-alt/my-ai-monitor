#!/usr/bin/env python3
# -*- coding: utf-8 -*-
r"""
download_ishares_history.py

用途：
  根据 iShares / BlackRock holdings ajax key URL，批量回填历史持仓 CSV。

输入文件支持：
  1) TXT:
     https://www.ishares.com/us/products/...ajax?fileType=csv&fileName=IVV_holdings&dataType=fund&asOfDate=20260424    IVV

  2) CSV:
     由 find_ishares_holding_urls.py 输出，包含 found_url,ticker,status 等列。

下载逻辑：
  - 读取每个 ticker 的 key URL
  - 把 URL 里的 asOfDate=YYYYMMDD 替换成目标日期
  - 请求下载
  - HTTP 200 且内容不像 HTML -> 保存
  - 404 / 400 等无数据日期 -> 跳过并记录日志
  - 已存在文件默认跳过，支持断点续跑

输出目录：
  <root>/data/vendors/ishares/raw/YYYY-MM-DD/<ticker>_holdings_YYYYMMDD.csv

示例 CMD：
  cd /d F:\globle

  先测试 4 个 ETF、1 天：
  python download_ishares_history.py --root F:\zhenghe --url-file F:\globle\ishares_holding_urls.txt --start 2026-04-24 --end 2026-04-24 --tickers IVV IWF IJH IJR --workers 2 --print-every 1

  全量历史回填：
  python download_ishares_history.py --root F:\zhenghe --url-file F:\globle\ishares_holding_urls.txt --start 2025-01-01 --end 2026-04-24 --workers 3 --min-sleep 0.5 --max-sleep 2.0 --print-every 100
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import html
import os
import random
import re
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse
from urllib.request import HTTPCookieProcessor, Request, build_opener


USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0 Safari/537.36"
)


@dataclass
class UrlKey:
    ticker: str
    key_url: str
    product_id: str
    file_name: str


@dataclass
class DownloadResult:
    date: str
    ticker: str
    status: str
    http_status: Optional[int]
    url: str
    path: str
    bytes: int
    message: str


def parse_date(value: str) -> dt.date:
    v = value.strip().lower()
    if v == "today":
        return dt.date.today()
    if v == "yesterday":
        return dt.date.today() - dt.timedelta(days=1)

    for fmt in ("%Y-%m-%d", "%Y%m%d"):
        try:
            return dt.datetime.strptime(value, fmt).date()
        except ValueError:
            pass

    raise ValueError(f"日期格式错误: {value}，请用 YYYY-MM-DD 或 YYYYMMDD")


def iter_dates(start: dt.date, end: dt.date, weekdays_only: bool = False):
    if end < start:
        raise ValueError("end 不能早于 start")

    current = start
    while current <= end:
        if not weekdays_only or current.weekday() < 5:
            yield current
        current += dt.timedelta(days=1)


def read_text_with_fallback(path: Path) -> str:
    for enc in ("utf-8-sig", "utf-8", "gbk", "gb18030"):
        try:
            return path.read_text(encoding=enc)
        except UnicodeDecodeError:
            continue
    raise UnicodeDecodeError("unknown", b"", 0, 1, f"无法识别编码: {path}")


def clean_url(raw: str) -> str:
    s = html.unescape(raw.strip())
    s = s.replace("\\/", "/")
    s = s.replace("\\u0026", "&")
    s = s.replace("&amp;", "&")
    return s


def extract_product_id(url: str) -> str:
    m = re.search(r"/products/(\d+)", url)
    return m.group(1) if m else ""


def extract_file_name(url: str) -> str:
    q = parse_qs(urlparse(url).query, keep_blank_values=True)
    return q.get("fileName", [""])[0]


def extract_ticker_from_url(url: str) -> str:
    file_name = extract_file_name(url)
    if file_name:
        return file_name.replace("_holdings", "").replace("_HOLDINGS", "").upper()

    m = re.search(r"fileName=([A-Za-z0-9]+)_holdings", url, re.IGNORECASE)
    if m:
        return m.group(1).upper()

    return ""


def replace_asof_date(url: str, yyyymmdd: str) -> str:
    u = clean_url(url)
    parsed = urlparse(u)
    q = parse_qs(parsed.query, keep_blank_values=True)

    q["asOfDate"] = [yyyymmdd]

    ordered = []
    for key in ("fileType", "fileName", "dataType", "asOfDate"):
        if key in q:
            for val in q[key]:
                ordered.append((key, val))

    for key, values in q.items():
        if key in {"fileType", "fileName", "dataType", "asOfDate"}:
            continue
        for val in values:
            ordered.append((key, val))

    new_query = urlencode(ordered, doseq=True)
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment))


def load_txt_url_keys(path: Path) -> list[UrlKey]:
    text = read_text_with_fallback(path)
    keys: list[UrlKey] = []
    seen: set[str] = set()

    for line_no, raw_line in enumerate(text.splitlines(), start=1):
        line = raw_line.strip()
        if not line:
            continue

        parts = re.split(r"\s+", line)
        url = ""
        ticker = ""

        for part in parts:
            if part.lower().startswith(("http://", "https://")) and ".ajax?" in part:
                url = clean_url(part)
                break

        if not url:
            continue

        # 最后一列通常是 ticker
        if len(parts) >= 2:
            candidate = parts[-1].strip().upper()
            if re.fullmatch(r"[A-Z0-9]{1,12}", candidate):
                ticker = candidate

        if not ticker:
            ticker = extract_ticker_from_url(url)

        if not ticker:
            print(f"[WARN] 第 {line_no} 行无法识别 ticker，跳过: {raw_line}")
            continue

        if "asOfDate=" not in url:
            print(f"[WARN] 第 {line_no} 行 URL 没有 asOfDate，仍会尝试补日期: {url}")

        key_id = f"{ticker}|{url}"
        if key_id in seen:
            continue

        seen.add(key_id)
        keys.append(
            UrlKey(
                ticker=ticker,
                key_url=url,
                product_id=extract_product_id(url),
                file_name=extract_file_name(url),
            )
        )

    return keys


def load_csv_url_keys(path: Path) -> list[UrlKey]:
    keys: list[UrlKey] = []
    seen: set[str] = set()

    for enc in ("utf-8-sig", "utf-8", "gbk", "gb18030"):
        try:
            f = path.open("r", newline="", encoding=enc)
            break
        except UnicodeDecodeError:
            continue
    else:
        raise UnicodeDecodeError("unknown", b"", 0, 1, f"无法识别编码: {path}")

    with f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []

        if "found_url" not in fieldnames and "url" not in fieldnames:
            raise ValueError("CSV 需要包含 found_url 或 url 列")

        for row in reader:
            url = clean_url(row.get("found_url") or row.get("url") or "")
            if not url:
                continue

            status = (row.get("status") or "").lower()
            if status and status not in ("found", "constructed", "downloaded", "ok"):
                continue

            ticker = (row.get("ticker") or "").strip().upper()
            if not ticker:
                ticker = extract_ticker_from_url(url)

            if not ticker:
                continue

            key_id = f"{ticker}|{url}"
            if key_id in seen:
                continue

            seen.add(key_id)
            keys.append(
                UrlKey(
                    ticker=ticker,
                    key_url=url,
                    product_id=row.get("product_id") or extract_product_id(url),
                    file_name=extract_file_name(url),
                )
            )

    return keys


def load_url_keys(path: Path) -> list[UrlKey]:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        keys = load_csv_url_keys(path)
    else:
        keys = load_txt_url_keys(path)

    if not keys:
        raise ValueError(f"没有从文件中解析到 iShares URL key: {path}")

    # 如果同一个 ticker 有多个 URL，保留第一个
    deduped: list[UrlKey] = []
    seen_tickers: set[str] = set()
    for k in keys:
        if k.ticker in seen_tickers:
            continue
        seen_tickers.add(k.ticker)
        deduped.append(k)

    return deduped


def looks_like_html(data: bytes) -> bool:
    prefix = data[:1024].lstrip().lower()
    return (
        prefix.startswith(b"<!doctype html")
        or prefix.startswith(b"<html")
        or b"<html" in prefix[:256]
        or b"access denied" in prefix
        or b"request rejected" in prefix
    )


def looks_like_csv(data: bytes) -> bool:
    head = data[:2048].decode("utf-8", errors="ignore").lower()

    csv_markers = [
        "ticker",
        "name",
        "sedol",
        "isin",
        "market value",
        "weight",
        "shares",
        "holding",
    ]

    return "," in head and any(m in head for m in csv_markers)


def download_one(
    *,
    root: Path,
    vendor: str,
    key: UrlKey,
    date_obj: dt.date,
    timeout: int,
    retries: int,
    overwrite: bool,
    dry_run: bool,
    min_sleep: float,
    max_sleep: float,
) -> DownloadResult:
    yyyymmdd = date_obj.strftime("%Y%m%d")
    date_dash = date_obj.strftime("%Y-%m-%d")
    url = replace_asof_date(key.key_url, yyyymmdd)

    out_dir = root / "data" / "vendors" / vendor / "raw" / date_dash
    out_file = out_dir / f"{key.ticker.lower()}_holdings_{yyyymmdd}.csv"

    if out_file.exists() and out_file.stat().st_size > 0 and not overwrite:
        return DownloadResult(
            date=date_dash,
            ticker=key.ticker,
            status="exists",
            http_status=None,
            url=url,
            path=str(out_file),
            bytes=out_file.stat().st_size,
            message="skip existing file",
        )

    if dry_run:
        return DownloadResult(
            date=date_dash,
            ticker=key.ticker,
            status="dry_run",
            http_status=None,
            url=url,
            path=str(out_file),
            bytes=0,
            message="dry run only",
        )

    if max_sleep > 0:
        time.sleep(random.uniform(min_sleep, max_sleep))

    opener = build_opener(HTTPCookieProcessor())
    last_message = ""

    for attempt in range(retries + 1):
        try:
            req = Request(
                url,
                headers={
                    "User-Agent": USER_AGENT,
                    "Accept": "text/csv,application/csv,text/plain,*/*",
                    "Accept-Language": "en-US,en;q=0.9",
                    "Referer": key.key_url.split(".ajax?")[0] if ".ajax?" in key.key_url else "https://www.ishares.com/",
                    "Connection": "close",
                },
                method="GET",
            )

            with opener.open(req, timeout=timeout) as resp:
                http_status = getattr(resp, "status", None) or resp.getcode()
                data = resp.read()

            if http_status == 200:
                if not data:
                    return DownloadResult(date_dash, key.ticker, "empty", http_status, url, str(out_file), 0, "200 but empty body")

                if looks_like_html(data):
                    return DownloadResult(date_dash, key.ticker, "bad_content", http_status, url, str(out_file), len(data), "200 but body looks like html/access denied")

                if not looks_like_csv(data):
                    # 不直接丢弃，先标记为 suspicious 但仍保存，便于人工检查
                    out_dir.mkdir(parents=True, exist_ok=True)
                    fd, tmp_name = tempfile.mkstemp(prefix=out_file.name + ".", suffix=".tmp", dir=str(out_dir))
                    try:
                        with os.fdopen(fd, "wb") as f:
                            f.write(data)
                        os.replace(tmp_name, out_file)
                    finally:
                        if os.path.exists(tmp_name):
                            os.remove(tmp_name)

                    return DownloadResult(date_dash, key.ticker, "suspicious_saved", http_status, url, str(out_file), len(data), "saved but csv markers not obvious")

                out_dir.mkdir(parents=True, exist_ok=True)
                fd, tmp_name = tempfile.mkstemp(prefix=out_file.name + ".", suffix=".tmp", dir=str(out_dir))

                try:
                    with os.fdopen(fd, "wb") as f:
                        f.write(data)
                    os.replace(tmp_name, out_file)
                finally:
                    if os.path.exists(tmp_name):
                        os.remove(tmp_name)

                return DownloadResult(date_dash, key.ticker, "downloaded", http_status, url, str(out_file), len(data), "ok")

            return DownloadResult(
                date_dash,
                key.ticker,
                "http_other",
                http_status,
                url,
                str(out_file),
                0,
                f"unexpected http status: {http_status}",
            )

        except HTTPError as exc:
            # iShares 有时无数据可能是 400/404，均视为无文件日期
            if exc.code in (400, 404):
                return DownloadResult(date_dash, key.ticker, "not_found", exc.code, url, str(out_file), 0, f"HTTP {exc.code}")

            last_message = f"HTTPError {exc.code}: {exc.reason}"

            if exc.code in (403, 429, 500, 502, 503, 504) and attempt < retries:
                time.sleep(2.0 * (attempt + 1))
                continue

            return DownloadResult(date_dash, key.ticker, "http_error", exc.code, url, str(out_file), 0, last_message)

        except (URLError, TimeoutError, ConnectionError) as exc:
            last_message = repr(exc)
            if attempt < retries:
                time.sleep(2.0 * (attempt + 1))
                continue

            return DownloadResult(date_dash, key.ticker, "network_error", None, url, str(out_file), 0, last_message)

        except Exception as exc:
            return DownloadResult(date_dash, key.ticker, "error", None, url, str(out_file), 0, repr(exc))

    return DownloadResult(date_dash, key.ticker, "failed", None, url, str(out_file), 0, last_message or "failed after retries")


def write_log(log_path: Path, rows: list[DownloadResult]) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)

    fields = [
        "date",
        "ticker",
        "status",
        "http_status",
        "url",
        "path",
        "bytes",
        "message",
        "logged_at",
    ]

    with log_path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        now = dt.datetime.now().isoformat(timespec="seconds")

        for r in rows:
            writer.writerow(
                {
                    "date": r.date,
                    "ticker": r.ticker,
                    "status": r.status,
                    "http_status": r.http_status if r.http_status is not None else "",
                    "url": r.url,
                    "path": r.path,
                    "bytes": r.bytes,
                    "message": r.message,
                    "logged_at": now,
                }
            )


def main() -> int:
    parser = argparse.ArgumentParser(description="Download iShares historical holdings CSV files.")
    parser.add_argument("--root", default=r"F:\zhenghe", help="项目根目录，例如 F:\\zhenghe")
    parser.add_argument("--vendor", default="ishares", help="vendor 名称，默认 ishares")
    parser.add_argument("--url-file", required=True, help="包含 iShares key URL + ticker 的 txt，或 URL finder 输出 CSV")
    parser.add_argument("--start", default="2025-01-01", help="开始日期 YYYY-MM-DD / YYYYMMDD")
    parser.add_argument("--end", default="yesterday", help="结束日期 YYYY-MM-DD / YYYYMMDD / today / yesterday")
    parser.add_argument("--workers", type=int, default=3, help="并发下载线程数，建议 1-3")
    parser.add_argument("--timeout", type=int, default=35, help="单请求超时秒数")
    parser.add_argument("--retries", type=int, default=2, help="网络错误 / 429 / 5xx 重试次数")
    parser.add_argument("--overwrite", action="store_true", help="覆盖已存在文件")
    parser.add_argument("--dry-run", action="store_true", help="只打印计划，不下载")
    parser.add_argument("--weekdays-only", action="store_true", help="只尝试周一到周五，减少周末无数据请求")
    parser.add_argument("--tickers", nargs="*", default=None, help="只下载指定 ticker，用于测试，例如 --tickers IVV IWF")
    parser.add_argument("--min-sleep", type=float, default=0.5, help="每个请求前最小随机等待秒数")
    parser.add_argument("--max-sleep", type=float, default=2.0, help="每个请求前最大随机等待秒数")
    parser.add_argument("--print-every", type=int, default=100, help="每处理多少个任务打印一次进度")

    args = parser.parse_args()

    root = Path(args.root)
    vendor = args.vendor.strip().lower()
    url_file = Path(args.url_file)

    start = parse_date(args.start)
    end = parse_date(args.end)

    keys = load_url_keys(url_file)

    if args.tickers:
        requested = {x.upper() for x in args.tickers}
        keys = [k for k in keys if k.ticker.upper() in requested]
        missing = sorted(requested - {k.ticker.upper() for k in keys})
        if missing:
            print(f"[WARN] url-file 中没找到这些 ticker: {missing}")

    if not keys:
        print("[ERROR] URL key 列表为空")
        return 2

    dates = list(iter_dates(start, end, weekdays_only=args.weekdays_only))
    total_tasks = len(keys) * len(dates)

    print("========================================")
    print("iShares holdings history downloader")
    print("========================================")
    print(f"root        : {root}")
    print(f"vendor      : {vendor}")
    print(f"url_file    : {url_file}")
    print(f"tickers     : {len(keys)}")
    print(f"date range  : {start} -> {end}")
    print(f"dates       : {len(dates)}")
    print(f"tasks       : {total_tasks}")
    print(f"workers     : {args.workers}")
    print(f"dry_run     : {args.dry_run}")
    print(f"weekdays_only: {args.weekdays_only}")
    print("========================================")

    if total_tasks == 0:
        print("[ERROR] 没有任务")
        return 2

    results: list[DownloadResult] = []
    counters: dict[str, int] = {}

    processed = 0

    def submit_tasks(executor: ThreadPoolExecutor):
        for d in dates:
            for key in keys:
                yield executor.submit(
                    download_one,
                    root=root,
                    vendor=vendor,
                    key=key,
                    date_obj=d,
                    timeout=args.timeout,
                    retries=args.retries,
                    overwrite=args.overwrite,
                    dry_run=args.dry_run,
                    min_sleep=args.min_sleep,
                    max_sleep=args.max_sleep,
                )

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = list(submit_tasks(executor))

        for fut in as_completed(futures):
            r = fut.result()
            results.append(r)
            counters[r.status] = counters.get(r.status, 0) + 1
            processed += 1

            if processed % args.print_every == 0 or processed == total_tasks:
                downloaded = counters.get("downloaded", 0)
                exists = counters.get("exists", 0)
                not_found = counters.get("not_found", 0)
                suspicious = counters.get("suspicious_saved", 0)
                errors = sum(
                    v
                    for k, v in counters.items()
                    if k not in ("downloaded", "exists", "not_found", "dry_run", "suspicious_saved")
                )
                print(
                    f"[{processed}/{total_tasks}] "
                    f"downloaded={downloaded}, exists={exists}, "
                    f"404/400={not_found}, suspicious={suspicious}, errors={errors}"
                )

    start_s = start.strftime("%Y%m%d")
    end_s = end.strftime("%Y%m%d")
    log_path = (
        root
        / "data"
        / "vendors"
        / vendor
        / "history"
        / f"download_log_{vendor}_{start_s}_{end_s}.csv"
    )
    write_log(log_path, results)

    print("\n========== SUMMARY ==========")
    for k in sorted(counters):
        print(f"{k:18s}: {counters[k]}")
    print(f"log: {log_path}")
    print("=============================")

    hard_errors = sum(
        v
        for k, v in counters.items()
        if k not in ("downloaded", "exists", "not_found", "dry_run", "suspicious_saved")
    )

    return 1 if hard_errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
