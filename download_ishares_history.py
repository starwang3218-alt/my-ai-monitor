#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
iShares / BlackRock holdings historical downloader for GitHub Actions.

Input link format, one per line:
    https://www.blackrock.com/...&asOfDate=20260520&component=holdings    IVV

Output layout:
    <root>/data/vendors/ishares/raw/YYYYMMDD/TICKER.csv

Notes:
- File name comes from the ETF ticker/name in the link file.
- Folder name comes from the strict internal fund-level As Of date.
- Empty templates / invalid files are deleted by default and written to manifest only.
- Date mismatches are quarantined by default to avoid polluting the historical raw tree.
"""

from __future__ import annotations

import argparse
import csv
import html
import random
import re
import shutil
import sys
import time
import traceback
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

BAD_HTML_MARKERS = ("<html", "<!doctype html", "access denied", "captcha", "akamai")
VALID_HEADER_HINTS = ("TICKER", "CUSIP", "ISIN", "ASSET CLASS")


@dataclass(frozen=True)
class Job:
    ticker: str
    url_template: str


@dataclass
class DownloadResult:
    requested_date: str
    ticker: str
    status: str
    http_status: str = ""
    bytes: int = 0
    real_asof_date: str = ""
    output_path: str = ""
    header_cut_lines: int = 0
    note: str = ""


def parse_date(value: str) -> date:
    s = str(value or "").strip()
    if re.fullmatch(r"\d{8}", s):
        return datetime.strptime(s, "%Y%m%d").date()
    if re.fullmatch(r"\d{4}-\d{1,2}-\d{1,2}", s):
        return datetime.strptime(s, "%Y-%m-%d").date()
    raise argparse.ArgumentTypeError(f"Invalid date: {value!r}; use YYYY-MM-DD or YYYYMMDD")


def parse_asof_text(value: str) -> str:
    s = str(value or "").strip().replace('"', "").replace(",", " ")
    s = re.sub(r"\s+", " ", s)
    if not s:
        return ""

    formats = [
        "%Y-%m-%d", "%Y/%m/%d", "%Y.%m.%d",
        "%m/%d/%Y", "%m-%d-%Y", "%m.%d.%Y",
        "%m/%d/%y", "%m-%d-%y", "%m.%d.%y",
        "%b %d %Y", "%B %d %Y",
        "%d %b %Y", "%d %B %Y",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return ""


def yyyymmdd(d: date | str) -> str:
    if isinstance(d, date):
        return d.strftime("%Y%m%d")
    parsed = parse_asof_text(d)
    return parsed.replace("-", "") if parsed else ""


def iter_dates(start: date, end: date, weekdays_only: bool) -> Iterable[date]:
    cur = start
    while cur <= end:
        if not weekdays_only or cur.weekday() < 5:
            yield cur
        cur += timedelta(days=1)


def safe_ticker(value: str) -> str:
    ticker = str(value or "").strip().upper()
    ticker = ticker.replace("/", "-").replace("\\", "-")
    ticker = re.sub(r"[^A-Z0-9._\-]+", "_", ticker)
    ticker = ticker.strip("._-")
    return ticker or "UNKNOWN"


def parse_jobs_file(path: Path) -> List[Job]:
    jobs: List[Job] = []
    seen: set[Tuple[str, str]] = set()
    for line_no, raw in enumerate(path.read_text(encoding="utf-8-sig", errors="ignore").splitlines(), 1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue

        parts = [p.strip() for p in re.split(r"\s+|,", line) if p.strip()]
        url = next((p for p in parts if p.startswith(("http://", "https://"))), "")
        ticker = next((p for p in reversed(parts) if not p.startswith(("http://", "https://"))), "")
        if not url or not ticker:
            print(f"WARN: skip bad line {line_no}: {line[:160]}", file=sys.stderr)
            continue

        job = Job(ticker=safe_ticker(ticker), url_template=url)
        key = (job.ticker, job.url_template)
        if key in seen:
            continue
        seen.add(key)
        jobs.append(job)

    return jobs


def set_asof_date(url_template: str, target: date) -> str:
    ds = target.strftime("%Y%m%d")
    if re.search(r"([?&]asOfDate=)\d{8}", url_template):
        return re.sub(r"([?&]asOfDate=)\d{8}", rf"\g<1>{ds}", url_template)
    sep = "&" if "?" in url_template else "?"
    return f"{url_template}{sep}asOfDate={ds}"


def decode_bytes(data: bytes) -> str:
    for enc in ("utf-8-sig", "utf-8", "latin1"):
        try:
            return data.decode(enc, errors="ignore")
        except Exception:
            pass
    return data.decode("latin1", errors="ignore")


def find_header_index(lines: List[str]) -> int:
    for i, line in enumerate(lines[:80]):
        upper = line.upper()
        if (
            ("TICKER" in upper and ("CUSIP" in upper or "ISIN" in upper or "ASSET CLASS" in upper))
            or ("ASSET CLASS" in upper and ("CUSIP" in upper or "ISIN" in upper))
        ):
            return i
    return -1


def sniff_as_of_date_from_lines(lines: List[str]) -> str:
    """Strict fund-level As Of detector.

    Only scan metadata before the holdings table, so holdings-row columns such as
    Effective Date / Accrual Date / Maturity Date cannot be mistaken for fund As Of.
    """
    if not lines:
        return ""

    header_idx = find_header_index(lines)
    scan_lines = lines[:header_idx] if header_idx >= 0 else lines[:15]
    text_block = " ".join(scan_lines)
    text_block = html.unescape(text_block).replace('"', " ").replace(",", " ")
    text_block = re.sub(r"\s+", " ", text_block)

    patterns = [
        r"#\s*As\s+of\s+(\d{4}[-/.]\d{1,2}[-/.]\d{1,2})",
        r"#\s*As\s+of\s+([A-Za-z]{3,9}\s+\d{1,2}\s+\d{4})",
        r"(?:Fund\s+Holdings\s+as\s+of|Holdings\s+as\s+of|As\s+of)[^\dA-Za-z]*([A-Za-z]{3,9}\s+\d{1,2}\s+\d{4})",
        r"(?:Fund\s+Holdings\s+as\s+of|Holdings\s+as\s+of|As\s+of)[^\d]*(\d{1,2}[-/.]\d{1,2}[-/.]\d{2,4})",
        r"(?:Fund\s+Holdings\s+as\s+of|Holdings\s+as\s+of|As\s+of)[^\d]*(\d{4}[-/.]\d{1,2}[-/.]\d{1,2})",
    ]

    for pat in patterns:
        m = re.search(pat, text_block, re.IGNORECASE)
        if not m:
            continue
        parsed = parse_asof_text(m.group(1))
        if parsed:
            return parsed
    return ""


def validate_and_clean(data: bytes, min_bytes: int = 1000) -> Tuple[bool, str, str, int, str]:
    """Return: ok, status, cleaned_text, header_cut_lines, real_asof_date."""
    size = len(data or b"")
    if size < min_bytes:
        return False, "TOO_SMALL", "", 0, ""

    text = decode_bytes(data)
    low_head = text[:2000].lower()
    if any(marker in low_head for marker in BAD_HTML_MARKERS):
        return False, "BAD_HTML_OR_BLOCK_PAGE", "", 0, ""

    lines = text.splitlines(keepends=True)
    if not lines:
        return False, "EMPTY_READ", "", 0, ""

    head15 = "".join(lines[:15])
    if 'Fund Holdings as of,"-"' in head15 or "Fund Holdings as of,-" in head15:
        return False, "EMPTY_TEMPLATE_DASH_DATE", "", 0, ""

    header_idx = find_header_index(lines)
    if header_idx < 0:
        return False, "NO_HOLDINGS_HEADER", "", 0, ""
    if header_idx + 1 >= len(lines):
        return False, "NO_DATA_AFTER_HEADER", "", header_idx, ""

    # Validate at least one data row with a reasonable number of CSV columns.
    data_lines = [ln for ln in lines[header_idx + 1: header_idx + 20] if ln.strip()]
    valid_data_row = False
    for ln in data_lines:
        try:
            row = next(csv.reader([ln]))
        except Exception:
            row = ln.split(",")
        non_empty = [x for x in row if str(x).strip()]
        if len(row) >= 5 and len(non_empty) >= 2:
            valid_data_row = True
            break
    if not valid_data_row:
        return False, "NO_VALID_DATA_ROW", "", header_idx, ""

    real_asof = sniff_as_of_date_from_lines(lines)
    clean_lines = lines[header_idx:]
    prefix = f"# As of {real_asof}\n" if real_asof else ""
    cleaned = prefix + "".join(clean_lines)
    return True, "VALID", cleaned, header_idx, real_asof


def fetch_url(url: str, timeout: int) -> Tuple[int, bytes, str]:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "text/csv,application/csv,text/plain,*/*",
            "Accept-Language": "en-US,en;q=0.9",
            "Cache-Control": "no-cache",
        },
        method="GET",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            status = int(getattr(resp, "status", 200) or 200)
            return status, resp.read(), ""
    except urllib.error.HTTPError as e:
        try:
            data = e.read()
        except Exception:
            data = b""
        return int(e.code), data, f"HTTPError {e.code}"
    except Exception as e:
        return 0, b"", f"{type(e).__name__}: {str(e)[:180]}"


def atomic_write_text(path: Path, text: str, overwrite: bool) -> bool:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and not overwrite:
        return False
    tmp = path.with_suffix(path.suffix + f".tmp_{random.randint(100000, 999999)}")
    tmp.write_text(text, encoding="utf-8-sig", newline="")
    if path.exists() and overwrite:
        path.unlink(missing_ok=True)
    shutil.move(str(tmp), str(path))
    return True


def save_bad_sample(raw_root: Path, requested: str, ticker: str, status: str, data: bytes, overwrite: bool) -> str:
    bad_dir = raw_root / "_bad" / requested
    bad_dir.mkdir(parents=True, exist_ok=True)
    safe_status = re.sub(r"[^A-Z0-9_\-]+", "_", status.upper())
    path = bad_dir / f"{ticker}_{safe_status}.bin"
    if path.exists() and not overwrite:
        return str(path)
    path.write_bytes(data)
    return str(path)


def download_one(
    job: Job,
    target_date: date,
    raw_root: Path,
    overwrite: bool,
    min_sleep: float,
    max_sleep: float,
    timeout: int,
    max_retries: int,
    min_bytes: int,
    keep_bad: bool,
    allow_date_mismatch: bool,
    allow_undated: bool,
) -> DownloadResult:
    requested = target_date.strftime("%Y-%m-%d")
    requested_dir = target_date.strftime("%Y%m%d")
    ticker = job.ticker

    expected_path = raw_root / requested_dir / f"{ticker}.csv"
    if expected_path.exists() and not overwrite:
        return DownloadResult(
            requested_date=requested,
            ticker=ticker,
            status="EXISTS",
            output_path=str(expected_path),
            note="target file already exists",
        )

    url = set_asof_date(job.url_template, target_date)
    last_status = ""
    last_http = ""
    last_bytes = 0
    last_note = ""
    last_data = b""

    for attempt in range(max_retries + 1):
        if max_sleep > 0:
            time.sleep(random.uniform(max(0.0, min_sleep), max(0.0, max_sleep)))

        http_status, data, err = fetch_url(url, timeout=timeout)
        last_http = str(http_status) if http_status else "ERR"
        last_bytes = len(data or b"")
        last_data = data or b""

        if http_status != 200:
            last_status = f"HTTP_{http_status}" if http_status else "REQUEST_ERROR"
            last_note = err
            if http_status in (0, 429, 500, 502, 503, 504) and attempt < max_retries:
                time.sleep(1.5 + attempt * 2.0 + random.uniform(0.0, 1.0))
                continue
            break

        ok, status, cleaned, cut_lines, real_asof = validate_and_clean(data, min_bytes=min_bytes)
        if not ok:
            last_status = status
            last_note = "invalid or empty holdings file deleted"
            if status in ("BAD_HTML_OR_BLOCK_PAGE",) and attempt < max_retries:
                time.sleep(2.0 + attempt * 2.0 + random.uniform(0.0, 2.0))
                continue
            break

        # Valid file: route by strict internal As Of date.
        if real_asof:
            real_dir = real_asof.replace("-", "")
            if real_dir != requested_dir and not allow_date_mismatch:
                out_path = raw_root / "_date_mismatch" / f"requested_{requested_dir}" / f"asof_{real_dir}" / f"{ticker}.csv"
                written = atomic_write_text(out_path, cleaned, overwrite=overwrite)
                return DownloadResult(
                    requested_date=requested,
                    ticker=ticker,
                    status="DATE_MISMATCH_QUARANTINED" if written else "DATE_MISMATCH_EXISTS",
                    http_status=last_http,
                    bytes=last_bytes,
                    real_asof_date=real_asof,
                    output_path=str(out_path),
                    header_cut_lines=cut_lines,
                    note=f"requested {requested_dir}, internal as-of {real_dir}",
                )

            final_dir = real_dir
            out_path = raw_root / final_dir / f"{ticker}.csv"
            written = atomic_write_text(out_path, cleaned, overwrite=overwrite)
            return DownloadResult(
                requested_date=requested,
                ticker=ticker,
                status="OK" if written else "EXISTS",
                http_status=last_http,
                bytes=last_bytes,
                real_asof_date=real_asof,
                output_path=str(out_path),
                header_cut_lines=cut_lines,
                note="saved by internal as-of date",
            )

        if not allow_undated:
            out_path = raw_root / "_undated" / f"requested_{requested_dir}" / f"{ticker}.csv"
            written = atomic_write_text(out_path, cleaned, overwrite=overwrite)
            return DownloadResult(
                requested_date=requested,
                ticker=ticker,
                status="UNDATED_QUARANTINED" if written else "UNDATED_EXISTS",
                http_status=last_http,
                bytes=last_bytes,
                output_path=str(out_path),
                header_cut_lines=cut_lines,
                note="valid holdings body but no strict fund-level as-of date found",
            )

        out_path = raw_root / requested_dir / f"{ticker}.csv"
        written = atomic_write_text(out_path, cleaned, overwrite=overwrite)
        return DownloadResult(
            requested_date=requested,
            ticker=ticker,
            status="OK_UNDATED" if written else "EXISTS",
            http_status=last_http,
            bytes=last_bytes,
            output_path=str(out_path),
            header_cut_lines=cut_lines,
            note="saved by requested date because --allow-undated was enabled",
        )

    output_path = ""
    if keep_bad and last_data:
        output_path = save_bad_sample(raw_root, requested_dir, ticker, last_status or "BAD", last_data, overwrite=overwrite)

    return DownloadResult(
        requested_date=requested,
        ticker=ticker,
        status=last_status or "FAILED",
        http_status=last_http,
        bytes=last_bytes,
        output_path=output_path,
        note=last_note,
    )


def append_manifest(path: Path, rows: List[DownloadResult]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    exists = path.exists()
    fieldnames = [
        "requested_date", "ticker", "status", "http_status", "bytes",
        "real_asof_date", "output_path", "header_cut_lines", "note",
    ]
    with path.open("a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not exists:
            writer.writeheader()
        for r in rows:
            writer.writerow({k: getattr(r, k) for k in fieldnames})


def summarize(rows: List[DownloadResult]) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for r in rows:
        out[r.status] = out.get(r.status, 0) + 1
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill iShares / BlackRock holdings history by date range")
    parser.add_argument("--root", required=True, help="Output package root. Raw files go under root/data/vendors/ishares/raw")
    parser.add_argument("--url-file", required=True, help="Link file: URL + ETF ticker per line")
    parser.add_argument("--start", required=True, type=parse_date, help="Start date, YYYY-MM-DD or YYYYMMDD")
    parser.add_argument("--end", required=True, type=parse_date, help="End date, YYYY-MM-DD or YYYYMMDD")
    parser.add_argument("--workers", type=int, default=6, help="Concurrent downloader workers")
    parser.add_argument("--min-sleep", type=float, default=0.5, help="Random sleep lower bound before each request")
    parser.add_argument("--max-sleep", type=float, default=2.0, help="Random sleep upper bound before each request")
    parser.add_argument("--print-every", type=int, default=25, help="Print progress every N completed tasks")
    parser.add_argument("--weekdays-only", action="store_true", help="Skip Saturdays and Sundays")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing raw files")
    parser.add_argument("--timeout", type=int, default=45, help="HTTP timeout seconds")
    parser.add_argument("--max-retries", type=int, default=2, help="Retries for transient HTTP/network failures")
    parser.add_argument("--min-bytes", type=int, default=1000, help="Minimum bytes for a valid holdings CSV candidate")
    parser.add_argument("--keep-bad", action="store_true", help="Keep invalid/empty/block-page samples under raw/_bad")
    parser.add_argument("--allow-date-mismatch", action="store_true", help="Save valid files even if internal As Of differs from requested date")
    parser.add_argument("--allow-undated", action="store_true", help="Save valid files with no strict As Of under the requested date")
    args = parser.parse_args()

    if args.end < args.start:
        raise SystemExit("--end must be >= --start")
    if args.workers < 1:
        raise SystemExit("--workers must be >= 1")

    root = Path(args.root).resolve()
    url_file = Path(args.url_file)
    if not url_file.is_absolute():
        url_file = Path.cwd() / url_file
    url_file = url_file.resolve()
    if not url_file.exists():
        raise SystemExit(f"URL file not found: {url_file}")

    raw_root = root / "data" / "vendors" / "ishares" / "raw"
    raw_root.mkdir(parents=True, exist_ok=True)
    manifest = root / "data" / "vendors" / "ishares" / "download_manifest.csv"

    jobs = parse_jobs_file(url_file)
    dates = list(iter_dates(args.start, args.end, weekdays_only=args.weekdays_only))
    total = len(jobs) * len(dates)

    print("=" * 100, flush=True)
    print("BACKFILL iSHARES HOLDINGS HISTORY", flush=True)
    print("=" * 100, flush=True)
    print(f"ROOT          : {root}", flush=True)
    print(f"RAW ROOT      : {raw_root}", flush=True)
    print(f"URL FILE      : {url_file}", flush=True)
    print(f"DATE RANGE    : {args.start} -> {args.end}", flush=True)
    print(f"WEEKDAYS ONLY : {args.weekdays_only}", flush=True)
    print(f"DATES         : {len(dates)}", flush=True)
    print(f"JOBS          : {len(jobs)}", flush=True)
    print(f"TOTAL TASKS   : {total}", flush=True)
    print(f"WORKERS       : {args.workers}", flush=True)
    print(f"MANIFEST      : {manifest}", flush=True)
    print("=" * 100, flush=True)

    if not jobs:
        raise SystemExit("No jobs parsed from URL file")
    if not dates:
        raise SystemExit("No dates to download")

    done = 0
    all_counts: Dict[str, int] = {}
    started = time.time()

    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        for d in dates:
            date_label = d.strftime("%Y-%m-%d")
            print(f"\n▶ DATE {date_label} | tasks={len(jobs)}", flush=True)
            futures = [
                ex.submit(
                    download_one,
                    job,
                    d,
                    raw_root,
                    args.overwrite,
                    args.min_sleep,
                    args.max_sleep,
                    args.timeout,
                    args.max_retries,
                    args.min_bytes,
                    args.keep_bad,
                    args.allow_date_mismatch,
                    args.allow_undated,
                )
                for job in jobs
            ]

            batch_rows: List[DownloadResult] = []
            for fut in as_completed(futures):
                try:
                    r = fut.result()
                except Exception as e:
                    r = DownloadResult(
                        requested_date=date_label,
                        ticker="UNKNOWN",
                        status="WORKER_EXCEPTION",
                        note=f"{type(e).__name__}: {str(e)[:180]} | {traceback.format_exc(limit=1).strip()}",
                    )
                batch_rows.append(r)
                done += 1
                all_counts[r.status] = all_counts.get(r.status, 0) + 1
                if args.print_every > 0 and (done % args.print_every == 0 or done == total):
                    elapsed = time.time() - started
                    rate = done / elapsed if elapsed > 0 else 0.0
                    print(
                        f"  progress {done}/{total} | {rate:.2f} tasks/s | "
                        f"{r.ticker} {r.requested_date} {r.status} {r.http_status}",
                        flush=True,
                    )

            append_manifest(manifest, batch_rows)
            day_counts = summarize(batch_rows)
            print("  DAY SUMMARY: " + ", ".join(f"{k}={v}" for k, v in sorted(day_counts.items())), flush=True)

    print("\n" + "=" * 100, flush=True)
    print("BACKFILL COMPLETE", flush=True)
    print("=" * 100, flush=True)
    for k, v in sorted(all_counts.items()):
        print(f"{k:28s} {v}", flush=True)
    print(f"MANIFEST: {manifest}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
