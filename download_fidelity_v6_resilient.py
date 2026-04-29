#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import os
import re
import sys
import time
import shutil  # 新增：用于物理移动文件
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Optional
from urllib.parse import parse_qs, urlencode, urlparse

import pandas as pd
import requests
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError, sync_playwright

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

# Confirmed Fidelity ETF CUSIPs. If a ticker is listed here, the script
# enters fundresearch/eproredirect directly and skips the quote summary page.
KNOWN_FIDELITY_CUSIPS = {
    "FBOT": "316092170",
}

@dataclass
class Job:
    url: str
    name: str
    original_url: str = ""

@dataclass
class DownloadResult:
    ok: bool
    page_url: str
    page_name: str
    saved_path: str = ""
    file_url: str = ""
    via: str = ""
    note: str = ""

# ==========================================
# 🌟 新增核心：X光时间嗅探器 (Date Sniffer)
# ==========================================
def sniff_as_of_date(filepath: Path) -> str:
    """用极低的资源消耗读取文件前 20 行，嗅探出文件真实的业务日期"""
    fallback_date = datetime.now().strftime("%Y-%m-%d")
    if not filepath.exists() or filepath.suffix.lower() != '.csv':
        return fallback_date
        
    try:
        df = pd.read_csv(filepath, header=None, nrows=20, encoding='utf-8-sig', on_bad_lines='skip')
    except Exception:
        return fallback_date

    patterns = [
        r'(?i)holdings[^\d]*(\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4})',
        r'(?i)as\s+of[^\d]*(\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4})',
        r'(?i)as\s+of[^\d]*(\d{4}[\/\-]\d{1,2}[\/\-]\d{1,2})',
        r'(\d{1,2}\-[A-Za-z]{3}\-\d{4})', 
        r'([A-Za-z]{3}\s+\d{1,2},?\s+\d{4})',  
        r'(\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4})', 
        r'(\d{4}[\/\-]\d{1,2}[\/\-]\d{1,2})'    
    ]
    
    sample_text = " ".join(df.fillna("").astype(str).values.flatten())
    for p in patterns:
        m = re.search(p, sample_text)
        if m:
            try:
                return pd.to_datetime(m.group(1)).strftime('%Y-%m-%d')
            except:
                continue
    return fallback_date

# --- 辅助函数 ---
def summary_url_for_ticker(ticker: str) -> str:
    return "https://digital.fidelity.com/prgw/digital/research/quote/dashboard/summary?" + urlencode({"symbol": ticker})

def eproredirect_url_for_cusip(cusip: str) -> str:
    """Build Fidelity prospectus/report redirect URL from a 9-character CUSIP."""
    cusip = cusip.strip().upper()
    return "https://fundresearch.fidelity.com/prospectus/eproredirect?" + urlencode({
        "clientId": "Fidelity",
        "applicationId": "MFL",
        "securityIdType": "CUSIP",
        "critical": "N",
        "securityId": cusip,
    })

def is_probable_cusip(text: str) -> bool:
    return bool(re.fullmatch(r"[A-Z0-9]{9}", text.strip().upper()))

def extract_query_param(url: str, key: str) -> str:
    try:
        vals = parse_qs(urlparse(url).query).get(key)
        return vals[0] if vals else ""
    except Exception:
        return ""

def extract_symbol_from_url(url: str) -> str:
    return extract_query_param(url, "symbol").upper()

def extract_cusip_from_url(url: str) -> str:
    sid = extract_query_param(url, "securityId").upper()
    return sid if is_probable_cusip(sid) else ""

def looks_like_report_entry_url(url: str) -> bool:
    url = (url or "").lower()
    return (
        "fundresearch.fidelity.com/prospectus/eproredirect" in url
        or "actionsxchangerepository.fidelity.com/showdocument" in url
    )

def direct_or_summary_job(ticker: str, cusip: str = "") -> Job:
    ticker = ticker.upper().strip()
    cusip = (cusip or KNOWN_FIDELITY_CUSIPS.get(ticker, "")).upper().strip()
    if cusip:
        url = eproredirect_url_for_cusip(cusip)
        return Job(url=url, name=ticker, original_url=url)
    url = summary_url_for_ticker(ticker)
    return Job(url=url, name=ticker, original_url=url)

def safe_name(text: str, max_len: int = 120) -> str:
    text = re.sub(r'[<>:"/\\|?*\x00-\x1f]+', "_", str(text))
    text = re.sub(r"\s+", " ", text).strip().rstrip(".")
    return text[:max_len].strip() or "file"

def save_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")

def append_jsonl(path: Path, item: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(item, ensure_ascii=False) + "\n")

def parse_jobs(input_file: Path) -> list[Job]:
    """Parse input lines.

    Supported forms:
      FBOT                         -> uses KNOWN_FIDELITY_CUSIPS when present, else summary page
      FBOT 316092170               -> direct eproredirect by CUSIP
      316092170                    -> direct eproredirect; output file name is CUSIP
      FBOT https://fundresearch... -> direct URL; output file name is FBOT
      https://fundresearch... FBOT -> direct URL; output file name is FBOT
      https://digital.fidelity...  -> summary URL fallback
    """
    jobs: list[Job] = []
    for raw in input_file.read_text(encoding="utf-8").splitlines():
        raw = raw.strip()
        if not raw or raw.startswith("#"):
            continue
        parts = [p.strip() for p in re.split(r"\t+|\s{2,}|,", raw) if p.strip()]
        if not parts:
            continue

        url_token = next((p for p in parts if p.startswith(("http://", "https://"))), "")
        cusip_token = next((p.upper() for p in parts if is_probable_cusip(p)), "")
        ticker_token = next((p.upper() for p in parts if not p.startswith(("http://", "https://")) and not is_probable_cusip(p)), "")

        if url_token:
            name = ticker_token or extract_symbol_from_url(url_token) or extract_cusip_from_url(url_token) or "FIDELITY_REPORT"
            jobs.append(Job(url=url_token, name=name, original_url=url_token))
        elif cusip_token and ticker_token:
            url = eproredirect_url_for_cusip(cusip_token)
            jobs.append(Job(url=url, name=ticker_token, original_url=url))
        elif cusip_token:
            url = eproredirect_url_for_cusip(cusip_token)
            jobs.append(Job(url=url, name=cusip_token, original_url=url))
        else:
            jobs.append(direct_or_summary_job(parts[0].upper()))
    return jobs

def build_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9,zh-CN;q=0.8",
    })
    return s

def read_excel_to_dataframe(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    last_error = None
    engines = [None]
    if suffix == ".xls": engines = [None, "xlrd"]
    elif suffix == ".xlsx": engines = [None, "openpyxl"]

    for engine in engines:
        try: return pd.read_excel(path, sheet_name=0, dtype=str, engine=engine)
        except Exception as e: last_error = e
    raise RuntimeError(f"读取 Excel 失败: {last_error}")

def convert_download_to_flat_csv(download_path: Path, ticker: str, output_root: Path) -> Path:
    final_csv = output_root / f"{safe_name(ticker)}.csv"
    suffix = download_path.suffix.lower()

    if suffix == ".csv":
        text = download_path.read_text(encoding="utf-8", errors="ignore")
        final_csv.write_text(text, encoding="utf-8-sig")
        download_path.unlink(missing_ok=True)
        return final_csv

    if suffix in {".xls", ".xlsx"}:
        df = read_excel_to_dataframe(download_path)
        df.to_csv(final_csv, index=False, encoding="utf-8-sig")
        download_path.unlink(missing_ok=True)
        return final_csv

    raise RuntimeError(f"不支持的下载文件格式: {download_path.name}")

def click_first(page, patterns: list[str], timeout_ms: int = 4000) -> bool:
    """Click the first matching link/button/text node.

    Fidelity sometimes renders the visible text inside a child span; if the
    child itself is clicked nothing happens, so fall back to its closest
    clickable ancestor.
    """
    locators = []
    for pat in patterns:
        regex = re.compile(pat, re.I)
        locators.extend([
            page.get_by_role("link", name=regex),
            page.get_by_role("button", name=regex),
            page.get_by_text(regex),
        ])
    for loc in locators:
        try:
            if loc.count() <= 0:
                continue
            target = loc.first
            try:
                target.click(timeout=timeout_ms)
            except Exception:
                # Click nearest <a>/<button> if the locator hit an inner span/img.
                target.evaluate("el => (el.closest('a,button') || el).click()")
            return True
        except Exception:
            try:
                loc.first.click(timeout=timeout_ms, force=True)
                return True
            except Exception:
                pass
    return False

def wait_and_choose_active_report_page(context, fallback_page, wait_ms: int = 3500):
    """Return the most likely Fidelity report/envelope page after a click."""
    fallback_page.wait_for_timeout(wait_ms)
    for pg in reversed(context.pages):
        try:
            url = pg.url or ""
            if "actionsxchangerepository.fidelity.com" in url or "fundresearch.fidelity.com/prospectus" in url:
                return pg
        except Exception:
            pass
    return context.pages[-1] if context.pages else fallback_page

def collect_excel_urls(page) -> list[str]:
    """Collect direct Excel download URLs from anchors or clickable image parents."""
    urls: list[str] = []
    try:
        handles = page.locator("a, img, input, button").element_handles()
        for h in handles:
            try:
                href = h.evaluate("""el => {
                    const a = el.closest && el.closest('a');
                    return (a && a.href) || el.href || el.src || el.getAttribute('data-href') || '';
                }""")
                if href and ("documentExcel" in href or re.search(r"\.(xls|xlsx|csv)(\?|$)", href, re.I)):
                    if href not in urls:
                        urls.append(href)
            except Exception:
                pass
    except Exception:
        pass
    return urls

def save_download_object(dl, tmp_dir: Path, stem: str) -> Path:
    suggested = dl.suggested_filename or f"{stem}.xls"
    suffix = Path(suggested).suffix or ".xls"
    save_path = tmp_dir / f"{stem}{suffix}"
    dl.save_as(str(save_path))
    return save_path

def browser_request_download(context, url: str, tmp_dir: Path, stem: str) -> Optional[Path]:
    """Download with Playwright's request context, sharing browser cookies."""
    try:
        resp = context.request.get(url, timeout=45000)
        if not resp.ok:
            return None
        body = resp.body()
        ctype = (resp.headers.get("content-type") or "").lower()
        cdisp = (resp.headers.get("content-disposition") or "").lower()
        # Do not save an error/login HTML page as a report.
        head = body[:80].lstrip().lower()
        if b"<html" in head or b"<!doctype" in head or "text/html" in ctype:
            return None
        suffix = ".xls"
        if "xlsx" in ctype or ".xlsx" in cdisp or body[:2] == b"PK":
            suffix = ".xlsx"
        elif "csv" in ctype or ".csv" in cdisp:
            suffix = ".csv"
        path = tmp_dir / f"{stem}{suffix}"
        path.write_bytes(body)
        return path
    except Exception:
        return None

def find_excel_candidates(page):
    selectors = [
        'a[href*="xls"]', 'a[href*="xlsx"]', 'a[href*="csv"]',
        'a[title*="Excel" i]', 'a[aria-label*="Excel" i]',
        'a:has(img[alt*="Excel" i])', 'a:has(img[src*="excel" i])',
        'a:has(img[src*="xls" i])', 'img[alt*="Excel" i]',
        'img[src*="excel" i]', 'img[src*="xls" i]',
    ]
    hits = []
    for sel in selectors:
        try:
            loc = page.locator(sel)
            for i in range(min(loc.count(), 10)): hits.append(loc.nth(i))
        except Exception: pass

    try:
        imgs = page.locator("img")
        for i in range(min(imgs.count(), 50)):
            el = imgs.nth(i)
            alt = (el.get_attribute("alt") or "").lower()
            src = (el.get_attribute("src") or "").lower()
            title = (el.get_attribute("title") or "").lower()
            if "excel" in alt or "excel" in title or "excel" in src or ".xls" in src:
                hits.append(el)
    except Exception: pass
    return hits

def detect_headless_mode() -> bool:
    raw = os.environ.get("FIDELITY_HEADLESS", "").strip().lower()
    if raw in {"1", "true", "yes", "y"}: return True
    if raw in {"0", "false", "no", "n"}: return False
    return False

def goto_entry_loose(page, url: str, timeout_ms: int = 30000) -> None:
    """Navigate without waiting for Fidelity/ActionsXChange scripts to finish.

    Some Fidelity report redirects visibly land on the target page but never fire
    Playwright's domcontentloaded/load events within the requested timeout. For
    this workflow we only need the navigation to commit, then we poll for the
    Daily Holdings / Excel controls ourselves. If a timeout happens after the
    page URL has moved away from about:blank, keep going.
    """
    try:
        page.goto(url, wait_until="commit", timeout=timeout_ms)
        return
    except PlaywrightTimeoutError:
        current_url = page.url or ""
        if current_url and current_url != "about:blank":
            return
        raise
    except Exception as e:
        # Older Playwright builds may not support wait_until=commit. Fall back
        # to a short domcontentloaded wait, then continue if the URL changed.
        if "commit" not in str(e).lower():
            raise
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=min(timeout_ms, 15000))
            return
        except PlaywrightTimeoutError:
            current_url = page.url or ""
            if current_url and current_url != "about:blank":
                return
            raise

def page_has_daily_holdings(page) -> bool:
    """Return True only when the Daily Holdings Report control/text is present.

    Do not treat landing on actionsxchangerepository as ready. That URL can be
    visible long before the table of report links is painted.
    """
    daily_re = re.compile(r"Daily\s+Holdings", re.I)
    try:
        if page.get_by_role("link", name=daily_re).count() > 0:
            return True
    except Exception:
        pass
    try:
        if page.get_by_role("button", name=daily_re).count() > 0:
            return True
    except Exception:
        pass
    try:
        if page.get_by_text(daily_re).count() > 0:
            return True
    except Exception:
        pass
    try:
        body_text = page.locator("body").inner_text(timeout=1000)
        return bool(daily_re.search(body_text or ""))
    except Exception:
        return False


def wait_for_daily_holdings(page, timeout_ms: int = 90000, poll_ms: int = 1000) -> bool:
    """Poll until Daily Holdings Report is actually rendered.

    Fidelity/ActionsXChange may reach the final URL quickly while the report
    links load much later. This function waits for the real business signal: the
    Daily Holdings Report text/control. It returns False on timeout instead of
    throwing so the caller can retry in a later batch round.
    """
    deadline = time.time() + timeout_ms / 1000
    while time.time() < deadline:
        if page_has_daily_holdings(page):
            return True
        page.wait_for_timeout(poll_ms)
    return False


def wait_for_excel_ready(page, timeout_ms: int = 45000, poll_ms: int = 1000) -> bool:
    """Poll until an Excel/CSV download URL or icon is visible."""
    deadline = time.time() + timeout_ms / 1000
    while time.time() < deadline:
        try:
            if collect_excel_urls(page):
                return True
        except Exception:
            pass
        try:
            if find_excel_candidates(page):
                return True
        except Exception:
            pass
        page.wait_for_timeout(poll_ms)
    return False


def click_daily_holdings_when_ready(page, timeout_ms: int = 90000) -> bool:
    """Wait for Daily Holdings Report, then click it.

    A page that already exposes documentExcel.htm is considered ready; this can
    happen if ActionsXChange restores the document view after a redirect.
    """
    try:
        if collect_excel_urls(page):
            return True
    except Exception:
        pass
    if not wait_for_daily_holdings(page, timeout_ms=timeout_ms):
        return False
    return click_first(page, [r"Daily\s+Holdings\s+Report", r"Daily\s+Holdings"], timeout_ms=8000)


def wait_for_report_ready(page, timeout_ms: int = 15000) -> None:
    """Backward-compatible wrapper: wait for Daily Holdings, but don't fail."""
    wait_for_daily_holdings(page, timeout_ms=timeout_ms)

# --- 核心执行逻辑 ---
def run_one(
    task: Job,
    output_root: Path,
    overwrite: bool,
    debug: bool = False,
    daily_timeout_ms: int = 90000,
    excel_timeout_ms: int = 45000,
) -> DownloadResult:
    stem = safe_name(task.name)

    # 全局穿透缓存检查：任意日期文件夹存在同名 CSV 就跳过
    existing_files = list(output_root.rglob(f"{stem}.csv"))
    if existing_files and not overwrite:
        return DownloadResult(
            ok=True, page_url=task.url, page_name=task.name,
            saved_path=str(existing_files[0]), file_url=task.url,
            via="pre-cache", note=f"已存在于 {existing_files[0].parent.name}，跳过"
        )

    tmp_dir = output_root.parent / "_tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)

    launch_kwargs = {"headless": detect_headless_mode()}

    with sync_playwright() as p:
        browser = p.chromium.launch(**launch_kwargs)
        context = browser.new_context(
            user_agent=USER_AGENT,
            accept_downloads=True,
            viewport={"width": 1440, "height": 1200},
            locale="en-US",
        )
        page = context.new_page()
        output_download: Optional[Path] = None
        download_url = ""

        try:
            direct_entry = looks_like_report_entry_url(task.url)

            # Direct mode:
            #   https://fundresearch.fidelity.com/prospectus/eproredirect?...securityId=<CUSIP>
            # skips the Fidelity quote summary page and goes straight to the
            # ActionsXChange report envelope. Do NOT wait for domcontentloaded:
            # this redirect often paints the page but never completes that event.
            goto_entry_loose(page, task.url, timeout_ms=30000)

            if direct_entry:
                report_page = page
                # eproredirect can swap into a new page/tab in some browser builds.
                # Do not assume ActionsXChange is ready just because the URL changed;
                # Step 2 below waits for the actual Daily Holdings Report control.
                report_page = wait_and_choose_active_report_page(context, report_page, wait_ms=1500)

                if debug:
                    save_text(tmp_dir / f"{stem}_direct_report_before_daily.html", report_page.content())
            else:
                try:
                    page.wait_for_load_state("networkidle", timeout=15000)
                except Exception:
                    pass
                page.wait_for_timeout(2500)

                if debug:
                    save_text(tmp_dir / f"{stem}_summary.html", page.content())

                # Step 1 fallback: Fidelity summary -> Prospectus, holdings & reports
                if not click_first(page, [r"Prospectus,\s*holdings?\s*&\s*reports", r"Prospectus"], timeout_ms=7000):
                    raise RuntimeError("未找到 Prospectus, holdings & reports 按钮")

                report_page = wait_and_choose_active_report_page(context, page, wait_ms=3500)
                # The report envelope can keep network/load events open. Do not
                # block on load states; Step 2 waits for the actual report control.

                # Some Fidelity redirects take a second hop into actionsxchange.
                for _ in range(6):
                    if "actionsxchangerepository.fidelity.com" in (report_page.url or ""):
                        break
                    report_page.wait_for_timeout(1000)
                    report_page = wait_and_choose_active_report_page(context, report_page, wait_ms=400)

                if debug:
                    save_text(tmp_dir / f"{stem}_report_before_daily.html", report_page.content())

            # Step 2: wait until Daily Holdings Report is really rendered, then click it.
            clicked_daily = click_daily_holdings_when_ready(report_page, timeout_ms=daily_timeout_ms)
            if not clicked_daily:
                raise RuntimeError(f"等待 Daily Holdings Report 超时 {daily_timeout_ms // 1000}s")

            report_page = wait_and_choose_active_report_page(context, report_page, wait_ms=1500)

            # After clicking Daily Holdings, the Excel icon/link can also arrive late.
            # Poll for the download control instead of immediately declaring failure.
            wait_for_excel_ready(report_page, timeout_ms=excel_timeout_ms)

            if debug:
                save_text(tmp_dir / f"{stem}_report_after_daily.html", report_page.content())

            # Step 3A: most reliable path: extract documentExcel.htm and download with browser cookies.
            excel_urls = collect_excel_urls(report_page)
            if debug:
                save_text(tmp_dir / f"{stem}_excel_urls.txt", "\n".join(excel_urls))

            for url in excel_urls:
                download_url = url
                # Try browser-native download first.
                try:
                    with report_page.expect_download(timeout=15000) as dl_info:
                        report_page.evaluate("""url => {
                            const a = document.createElement('a');
                            a.href = url;
                            a.target = '_self';
                            document.body.appendChild(a);
                            a.click();
                        }""", url)
                    output_download = save_download_object(dl_info.value, tmp_dir, stem)
                    break
                except Exception:
                    pass
                # Fallback: fetch using the same browser context/cookies.
                output_download = browser_request_download(context, url, tmp_dir, stem)
                if output_download is not None:
                    break

            # Step 3B: fallback to clicking visible Excel icons/links.
            if output_download is None:
                candidates = find_excel_candidates(report_page)
                if not candidates:
                    note = "未找到 Excel 下载图标"
                    if not clicked_daily:
                        note = "未找到 Daily Holdings Report 按钮，也未找到 Excel 下载图标"
                    raise RuntimeError(note)

                for loc in candidates:
                    try:
                        href = loc.evaluate("el => (el.closest('a') && el.closest('a').href) || el.href || el.src || ''")
                        if href:
                            download_url = href
                    except Exception:
                        pass
                    try:
                        with report_page.expect_download(timeout=15000) as dl_info:
                            try:
                                loc.evaluate("el => (el.closest('a') || el).click()")
                            except Exception:
                                loc.click(timeout=5000, force=True)
                        output_download = save_download_object(dl_info.value, tmp_dir, stem)
                        break
                    except Exception:
                        if download_url:
                            output_download = browser_request_download(context, download_url, tmp_dir, stem)
                            if output_download is not None:
                                break

            if output_download is None:
                raise RuntimeError("打开了报告页面，但捕获 Excel 下载失败")

            # 转成平铺 CSV，然后按文件内真实业务日期分拣入库
            flat_csv = convert_download_to_flat_csv(output_download, task.name, output_root)
            real_date = sniff_as_of_date(flat_csv)
            final_folder = output_root / real_date
            final_folder.mkdir(parents=True, exist_ok=True)

            routed_csv = final_folder / flat_csv.name
            if routed_csv.exists():
                if overwrite:
                    routed_csv.unlink()
                else:
                    flat_csv.unlink(missing_ok=True)
                    return DownloadResult(
                        ok=True, page_url=task.url, page_name=task.name,
                        saved_path=str(routed_csv), file_url=download_url or task.url,
                        via="playwright-fidelity-smart", note=f"目标已存在 -> {real_date}/"
                    )

            shutil.move(str(flat_csv), str(routed_csv))

            return DownloadResult(
                ok=True, page_url=task.url, page_name=task.name,
                saved_path=str(routed_csv), file_url=download_url or task.url,
                via="playwright-fidelity-smart", note=f"入库成功 -> {real_date}/"
            )

        finally:
            try:
                context.close()
            finally:
                browser.close()

async def process_single_job(idx: int, total: int, job: Job, output_dir: Path, session: requests.Session, args: argparse.Namespace, sem: asyncio.Semaphore) -> DownloadResult:
    async with sem:
        try:
            result = await asyncio.to_thread(
                run_one,
                task=job,
                output_root=output_dir,
                overwrite=bool(getattr(args, "overwrite", False)),
                debug=bool(getattr(args, "debug", False)),
                daily_timeout_ms=int(getattr(args, "daily_timeout", 90)) * 1000,
                excel_timeout_ms=int(getattr(args, "excel_timeout", 45)) * 1000,
            )
            if result.ok:
                print(f"[{idx:03d}/{total}] ✅ 成功 | {job.name} | {result.note}", flush=True)
            else:
                print(f"[{idx:03d}/{total}] ❌ 失败 | {job.name} | {result.note}", flush=True)
            return result
        except Exception as e:
            print(f"[{idx:03d}/{total}] ❌ 失败 | {job.name} | 异常: {e}", flush=True)
            return DownloadResult(ok=False, page_url=job.url, page_name=job.name, via="error", note=f"异常: {e}")

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Fidelity Smart Routing Downloader with direct CUSIP/eproredirect support")
    group = p.add_mutually_exclusive_group(required=True)
    group.add_argument("-t", "--ticker", help="单只基金 ticker，例如 FBOT")
    group.add_argument("-i", "--input", help="输入文件：每行一个 ticker / ticker+CUSIP / eproredirect URL")
    group.add_argument("--url", help="直接传入 eproredirect / ActionsXChange / Fidelity summary URL")
    p.add_argument("--cusip", help="配合 --ticker 使用：直接通过 CUSIP 进入 eproredirect，例如 316092170")
    p.add_argument("--name", help="配合 --url 使用：指定输出文件名/基金 ticker")
    p.add_argument("-o", "--output", default="fidelity_raw", help="输出根目录")
    p.add_argument("--overwrite", action="store_true", help="覆盖已存在文件")
    p.add_argument("--debug", action="store_true", help="保存中间 HTML 和候选下载链接，便于排错")
    p.add_argument("--daily-timeout", type=int, default=90, help="等待 Daily Holdings Report 出现的最长秒数，默认 90")
    p.add_argument("--excel-timeout", type=int, default=45, help="点击 Daily Holdings 后等待 Excel 下载按钮/链接的最长秒数，默认 45")
    p.add_argument("--max-rounds", type=int, default=3, help="批量失败任务的最大轮次数，默认 3")
    p.add_argument("--round-delay", type=int, default=60, help="两轮之间暂停秒数，默认 60")
    return p

async def _standalone_async(args: argparse.Namespace) -> int:
    output_root = Path(args.output).expanduser().resolve()
    output_root.mkdir(parents=True, exist_ok=True)

    jobs = []
    if args.url:
        name = (args.name or extract_symbol_from_url(args.url) or extract_cusip_from_url(args.url) or "FIDELITY_REPORT").upper()
        jobs = [Job(url=args.url, name=name, original_url=args.url)]
    elif args.ticker:
        jobs = [direct_or_summary_job(args.ticker.upper(), getattr(args, "cusip", "") or "")]
    else:
        jobs = parse_jobs(Path(args.input).expanduser().resolve())

    print(f"🚀 Fidelity 爬虫启动 | 检测到 {len(jobs)} 个任务", flush=True)
    print(
        f"⏱️ 等待设置 | Daily Holdings 最长 {args.daily_timeout}s | "
        f"Excel 最长 {args.excel_timeout}s | 最多 {args.max_rounds} 轮",
        flush=True,
    )

    session = build_session()
    sem = asyncio.Semaphore(1) # Fidelity 建议单线程慢慢跑防封
    ns = SimpleNamespace(
        overwrite=args.overwrite,
        debug=getattr(args, "debug", False),
        daily_timeout=getattr(args, "daily_timeout", 90),
        excel_timeout=getattr(args, "excel_timeout", 45),
    )

    pending = list(jobs)
    final_results: list[DownloadResult] = []

    for round_no in range(1, max(1, args.max_rounds) + 1):
        if not pending:
            break
        print(f"\n🔁 第 {round_no}/{args.max_rounds} 轮 | 待下载 {len(pending)} 个", flush=True)

        round_results: list[DownloadResult] = []
        for idx, job in enumerate(pending, start=1):
            round_results.append(await process_single_job(idx, len(pending), job, output_root, session, ns, sem))

        final_results.extend([r for r in round_results if r.ok])
        failed_results = [r for r in round_results if not r.ok]

        if not failed_results:
            pending = []
            break

        failed_keys = {(r.page_name, r.page_url) for r in failed_results}
        pending = [j for j in pending if (j.name, j.url) in failed_keys]

        if pending and round_no < args.max_rounds:
            print(f"⏳ 本轮失败 {len(pending)} 个，{args.round_delay}s 后进入下一轮", flush=True)
            await asyncio.sleep(max(0, args.round_delay))

    success = len(final_results)
    failed = len(pending)
    print(f"\n✨ 完成：成功 {success} / 失败 {failed}", flush=True)
    if pending:
        print("失败清单：" + ", ".join(j.name for j in pending), flush=True)
    return 0 if failed == 0 else 1

if __name__ == "__main__":
    sys.exit(asyncio.run(_standalone_async(build_parser().parse_args())))