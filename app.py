# -------------------------ABOUT --------------------------

# pyinstaller --onefile app.py
# Tool: NCOP DB Import Manager Tool
# Developer: dyoliya
# Created: 2026-03-03

# © 2026 dyoliya. All rights reserved.

# ---------------------------------------------------------

import os
import threading
from tkinter import messagebox
from zoneinfo import ZoneInfo
import re
import pandas as pd
import shutil
import sys
import customtkinter as ctk
import sqlite3
from openpyxl import load_workbook, Workbook
from openpyxl.cell import WriteOnlyCell
import json
import uuid

from customtkinter import filedialog

from ncop_transform import clean_and_prepare_df, sanitize_colname
from ncop_sqlite import (
    connect_sqlite,
    ensure_table_and_columns,
    get_existing_columns,
    sync_rows_by_ncop_id,
)
from ncop_deal_id_matcher import populate_ncop_deal_ids_in_df
from ncop_budb import (
    load_budb_lookup_maps,
    populate_budb_ids_in_df,
    validate_budb_file,
    connect_sqlite_readonly,
)
from ncop_existing_deals_report import generate_ncop_associated_deals_report

from datetime import datetime
from datetime import date
from fractions import Fraction
from pathlib import Path
from typing import Optional, Tuple

import contextlib
import queue
import time
import tempfile
import webbrowser
from datetime import timedelta
from urllib.parse import urlparse
from google.oauth2.service_account import Credentials
from google.auth.transport.requests import AuthorizedSession
from openpyxl.utils import get_column_letter
from PIL import Image, ImageDraw
from pipedrive_lookup import load_pipedrive_lookup_maps
from pipedrive_api import (
    cache_status as pipedrive_cache_status,
    ensure_fresh_cache as ensure_fresh_pipedrive_cache,
    load_settings as load_pipedrive_settings,
    save_settings as save_pipedrive_settings,
)
from ncop_action_log import compare_databases
from ncop_build_state import (
    read_state, save_state, prepare_build, finish_build,
    fail_build, mark_writeback_started,
)
from ncop_run_lock import exclusive_run

CENTRAL_TZ = ZoneInfo("America/Chicago")

def get_app_base_dir() -> Path:
    """
    When running as PyInstaller onefile exe:
      - sys.executable = path to the exe
    When running as .py:
      - __file__ = this script path
    """
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent

def get_daily_db_path(tool_prefix: str = "ncop") -> str:
    """
    Daily-versioned + cumulative, but detection is based ONLY on database/ folder.

    - Active DB lives in database/: YYYY-MM-DD-<tool_prefix>.db
    - If today's DB doesn't exist:
        - Find latest YYYY-MM-DD-<tool_prefix>.db in database/ ONLY
        - Copy it to today's DB (so today includes history)
        - Move all other .db files in database/ to database/previous_versions/
    - Does NOT look at previous_versions/ when choosing the base DB.
    """
    base_dir = get_app_base_dir()
    db_dir = base_dir / "database"
    prev_dir = db_dir / "previous_versions"
    db_dir.mkdir(parents=True, exist_ok=True)
    prev_dir.mkdir(parents=True, exist_ok=True)

    today_str = datetime.now(CENTRAL_TZ).strftime("%Y-%m-%d")
    today_name = f"{today_str}-{tool_prefix}.db"
    today_path = db_dir / today_name

    # Case 1: today's DB already exists
    if today_path.exists():
        for p in db_dir.glob("*.db"):
            if p.is_file() and p.name != today_name:
                p.rename(prev_dir / p.name)
        return str(today_path), None, "existing"

    # Find latest dated DB in database/
    pat = re.compile(rf"^(\d{{4}}-\d{{2}}-\d{{2}})-{re.escape(tool_prefix)}\.db$")
    candidates = []
    for p in db_dir.glob("*.db"):
        m = pat.match(p.name)
        if m:
            candidates.append((m.group(1), p))

    latest_path = sorted(candidates, key=lambda x: x[0])[-1][1] if candidates else None

    # Case 2: copy forward from latest base DB
    if latest_path and latest_path.exists():
        shutil.copy2(latest_path, today_path)
        copied_from = latest_path.name

        # archive other DBs
        for p in db_dir.glob("*.db"):
            if p.is_file() and p.name != today_name:
                p.rename(prev_dir / p.name)

        return str(today_path), copied_from, "copied"

    # Case 3: truly new (no base DB)
    # (No need to create the file here; connect_sqlite will create it when connecting.)
    for p in db_dir.glob("*.db"):
        if p.is_file() and p.name != today_name:
            p.rename(prev_dir / p.name)

    return str(today_path), None, "new"

def is_excel_hash_overflow(v) -> bool:
    if v is None:
        return False
    s = str(v).strip()
    return bool(re.fullmatch(r"#+", s))  # "#######"

def table_exists(conn, table_name: str) -> bool:
    cur = conn.cursor()
    cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,)
    )
    return cur.fetchone() is not None


def get_expected_columns(conn, table_name: str) -> set[str]:
    # existing DB columns
    cols = set(get_existing_columns(conn, table_name))
    # ignore system columns you add automatically
    ignore = {"ncop_id", "date_uploaded", "serial_number"}
    return {c for c in cols if c not in ignore}


def validate_strict_schema(
    incoming_cols: list[str],
    expected_cols: set[str],
    original_cols: list[str] | None = None,
    sanitized_cols: list[str] | None = None,
):
    system_cols = {"ncop_id", "date_uploaded", "serial_number"}

    incoming_set = set(incoming_cols) - system_cols
    expected_set = set(expected_cols) - system_cols

    # Missing columns are now allowed.
    # Meaning: if Excel lacks a DB column, we accept it.
    missing = sorted(expected_set - incoming_set)

    # Extra columns are still not allowed.
    extra = sorted(incoming_set - expected_set)

    if not extra:
        return

    mapping_info = ""
    if original_cols is not None and sanitized_cols is not None:
        pairs = list(zip(original_cols, sanitized_cols))
        mapping_info = "\n\n[DEBUG] Column mapping (original → sanitized):\n" + "\n".join(
            f"  - {o}  →  {s}" for o, s in pairs[:80]
        )
        if len(pairs) > 80:
            mapping_info += "\n  ... (truncated)"

    msg_lines = ["Schema mismatch: incoming file has unexpected columns."]

    if missing:
        msg_lines.append("\nAllowed missing columns detected. These are in DB but not in Excel:")
        msg_lines += [f"  - {c}" for c in missing]

    if extra:
        msg_lines.append("\nUnexpected columns present in Excel but not in DB:")
        msg_lines += [f"  - {c}" for c in extra]

    raise ValueError("\n".join(msg_lines) + mapping_info)


NCOP_SHEET_TAB = "CONSOLIDATED LIST"

def extract_google_sheet_id(url: str) -> str:
    parsed = urlparse(url.strip())

    if parsed.scheme != "https" or parsed.hostname != "docs.google.com":
        raise ValueError(
            "Please paste a Google Sheets URL starting with "
            "https://docs.google.com/spreadsheets/d/"
        )

    match = re.match(
        r"^/spreadsheets/d/([A-Za-z0-9_-]+)(?:/|$)",
        parsed.path,
    )

    if not match:
        raise ValueError(
            "Invalid Google Sheets URL. Copy the normal spreadsheet "
            "URL from your browser."
        )

    return match.group(1)


def google_sheets_session():
    credentials_path = (
        get_app_base_dir()
        / "config"
        / "google_service_account.json"
    )

    credentials = Credentials.from_service_account_file(
        str(credentials_path),
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive.readonly",
        ],
    )

    return AuthorizedSession(credentials, refresh_timeout=60)


def google_pending_path(url: str) -> Path:
    spreadsheet_id = extract_google_sheet_id(url)

    folder = get_app_base_dir() / "google_sheet_writeback"
    folder.mkdir(parents=True, exist_ok=True)

    return folder / f"{spreadsheet_id}_pending.json"


def google_api_request(session, method, endpoint, **kwargs):
    """Retry transient Sheets/Drive errors, including quota limits."""
    kwargs.setdefault("timeout", (15, 90))

    for attempt in range(7):
        response = session.request(method, endpoint, **kwargs)

        retryable = response.status_code in {429, 500, 502, 503, 504}

        if response.status_code == 403:
            try:
                reasons = response.json().get("error", {}).get("errors", [])
                retryable = any(
                    item.get("reason") in {
                        "rateLimitExceeded",
                        "userRateLimitExceeded",
                    }
                    for item in reasons
                )
            except ValueError:
                pass

        if not retryable or attempt == 6:
            response.raise_for_status()
            return response

        retry_after = response.headers.get("Retry-After", "")
        wait = float(retry_after) if retry_after.isdigit() else 2 ** attempt
        time.sleep(min(60, max(1, wait)))


def get_google_sheet_version(url: str) -> str:
    """Check whether the Google spreadsheet changed."""
    spreadsheet_id = extract_google_sheet_id(url)

    endpoint = (
        f"https://www.googleapis.com/drive/v3/files/{spreadsheet_id}"
    )

    with google_sheets_session() as session:
        response = google_api_request(
            session,
            "GET",
            endpoint,
            params={
                "fields": "modifiedTime",
                "supportsAllDrives": "true",
            },
            timeout=(15, 60),
        )

    return response.json()["modifiedTime"]


def get_google_writeback_columns(snapshot: list) -> dict:
    if not snapshot:
        raise ValueError("The Google Sheets tab is empty.")

    headers = [sanitize_colname(h) for h in snapshot[0]]
    columns = {}

    for field in ("ncop_id", "ncop_serial_number", "date_added_to_db"):
        matches = [
            index
            for index, header in enumerate(headers)
            if header == field
        ]

        if len(matches) != 1:
            raise ValueError(
                f"Google Sheets must contain exactly one '{field}' "
                "column in row 1."
            )

        columns[field] = matches[0]

    return columns


def google_cell_text(cell, column_name="", *, is_header=False):
    effective = cell.get("effectiveValue", {})

    # Headers must retain their names, not their hyperlink targets.
    if is_header:
        return str(
            effective.get("stringValue", cell.get("formattedValue", ""))
        )

    # Read actual Google date values independently of display formatting.
    name = sanitize_colname(column_name)
    if name.replace("_", "").endswith(("firstseen", "lastseen")):
        numeric = effective.get("numberValue")
        kind = (
            cell.get("effectiveFormat", {})
            .get("numberFormat", {})
            .get("type")
        )

        if numeric is not None and kind in {"DATE", "DATE_TIME"}:
            return (
                datetime(1899, 12, 30) + timedelta(days=numeric)
            ).strftime("%Y-%m-%d")

    # Keep error text so the existing transformation can audit it,
    # then convert it to NULL.
    if "errorValue" in effective:
        return cell.get("formattedValue") or "#N/A"

    links = [cell.get("hyperlink")]

    # Links attached to all or part of the displayed text.
    links.extend(
        run.get("format", {}).get("link", {}).get("uri")
        for run in cell.get("textFormatRuns", [])
    )

    # Google file/resource smart chips.
    links.extend(
        run.get("chip", {}).get("richLinkProperties", {}).get("uri")
        for run in cell.get("chipRuns", [])
    )

    # Remove blanks and duplicate URLs, preserving their order.
    links = list(dict.fromkeys(link for link in links if link))

    # Whole-cell formatting is a fallback; rich-text links take precedence.
    if not links:
        for format_key in ("effectiveFormat", "userEnteredFormat"):
            link = (
                cell.get(format_key, {})
                .get("textFormat", {})
                .get("link", {})
                .get("uri")
            )
            if link:
                links.append(link)
                break

    if links:
        return " | ".join(links)

    if "stringValue" in effective:
        return effective["stringValue"]

    name = sanitize_colname(column_name)
    numeric = effective.get("numberValue")
    if numeric is not None:
        if name in {"ncop_id", "contact_group_id"} or re.fullmatch(r"(?:cleaned_)?phone_?[1-5]", name):
            return str(int(numeric)) if float(numeric).is_integer() else str(numeric)
        if name in {"ownership_portion", "ownerhsip_portion"}:
            kind = cell.get("effectiveFormat", {}).get("numberFormat", {}).get("type")
            if kind in {"DATE", "DATE_TIME"}:
                parsed = datetime(1899, 12, 30) + timedelta(days=numeric)
                return f"{parsed.month}/{parsed.day}"
            if 0 < numeric < 1 and kind != "PERCENT":
                fraction = Fraction(numeric).limit_denominator(256)
                return f"{fraction.numerator}/{fraction.denominator}"

    if "formattedValue" in cell:
        return cell["formattedValue"]

    if "numberValue" in effective:
        return str(effective["numberValue"])

    if "boolValue" in effective:
        return "TRUE" if effective["boolValue"] else "FALSE"

    return ""


def capture_ncop_sheet(session, sheet_url, backup_root):
    match = re.fullmatch(
        r"https://docs\.google\.com/spreadsheets/d/([A-Za-z0-9_-]+)(?:[/?#].*)?",
        str(sheet_url).strip(),
    )
    if not match:
        raise ValueError("Enter a valid Google Sheets spreadsheet URL.")

    spreadsheet_id = match.group(1)

    fields = (
        "spreadsheetId,sheets(properties(sheetId,title),"
        "data(startRow,startColumn,rowData(values("
        "formattedValue,effectiveValue,hyperlink,textFormatRuns,chipRuns,"
        "userEnteredFormat(textFormat(link)),"
        "effectiveFormat(numberFormat,textFormat(link))))))"
    )

    def fetch():
        response = google_api_request(
            session,
            "GET",
            f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}",
            params={
                "ranges": [f"'{NCOP_SHEET_TAB}'"],
                "fields": fields,
            },
            timeout=(15, 90),
        )
        return response.json()

    source_version = get_google_sheet_version(sheet_url)
    payload = fetch()

    # Detect changes between two reads before accepting the snapshot.
    if fetch() != payload or get_google_sheet_version(sheet_url) != source_version:
        raise ValueError(
            "Sheet values changed during capture. "
            "Wait until editing/calculation finishes, then retry."
        )

    sheets = [
        sheet for sheet in payload.get("sheets", [])
        if sheet.get("properties", {}).get("title") == NCOP_SHEET_TAB
    ]
    if len(sheets) != 1:
        raise ValueError(f"Required tab not found: {NCOP_SHEET_TAB}")

    sheet = sheets[0]
    cells = {}

    for block in sheet.get("data", []):
        for r, row in enumerate(
            block.get("rowData", []),
            block.get("startRow", 0),
        ):
            for c, cell in enumerate(
                row.get("values", []),
                block.get("startColumn", 0),
            ):
                value = google_cell_text(cell, cells.get((0, c), "") if r else "", is_header=(r == 0),)
                if value != "":
                    cells[(r, c)] = value

    if not cells:
        raise ValueError("The Sheet contains no values.")

    width = max(c for _, c in cells) + 1
    height = max(r for r, _ in cells) + 1

    headers = [cells.get((0, c), "") for c in range(width)]

    if any(not header.strip() for header in headers):
        raise ValueError("Every used column must have a header in row 1.")

    normalized = [sanitize_colname(header) for header in headers]
    invalid_headers = [
        header for header in normalized
        if re.fullmatch(r"range_?id_?\d+", header)
    ]
    if invalid_headers:
        raise ValueError(
            "Unexpected range-ID column header(s): "
            + ", ".join(invalid_headers)
            + ". Correct these headers in row 1 before building."
        )

    if len(normalized) != len(set(normalized)):
        raise ValueError("Sheet headers normalize to duplicate column names.")

    required = {
        "ncop_id",
        "ncop_serial_number",
        "date_added_to_db",
        "fixed_combined_county",
    }
    missing = sorted(required - set(normalized))

    if missing:
        raise ValueError(
            "Missing required Sheet headers: " + ", ".join(missing)
        )

    rows = [
        [cells.get((r, c), "") for c in range(width)]
        for r in range(1, height)
    ]

    df_raw = pd.DataFrame(rows, columns=headers, dtype=str)

    nonempty = df_raw.apply(
        lambda col: col.str.strip().ne("")
    ).any(axis=1)

    # Preserve physical Sheet row positions for later writeback.
    df_raw = df_raw.loc[nonempty].copy()

    if df_raw.empty:
        raise ValueError("The Sheet has no data rows; capture stopped.")

    capture_id = datetime.now(ZoneInfo("Asia/Manila")).strftime(
        "%Y-%m-%d_%H%M%S_%f"
    )

    folder = Path(backup_root) / "google_sheet_backups" / spreadsheet_id
    folder.mkdir(parents=True, exist_ok=True)

    backup_path = folder / f"{capture_id}_PHT_google_sheet.xlsx"
    snapshot_path = folder / f"{capture_id}_PHT_snapshot.json"

    metadata = {
        "capture_id": capture_id,
        "captured_at": datetime.now(ZoneInfo("Asia/Manila")).isoformat(),
        "spreadsheet_id": spreadsheet_id,
        "sheet_id": sheet["properties"]["sheetId"],
        "tab": NCOP_SHEET_TAB,
        "source_row_count": len(rows),
        "nonempty_row_count": len(df_raw),
        "xlsx_path": str(backup_path),
        "snapshot_path": str(snapshot_path),
        "google_modified_time": source_version,
    }

    # Preserve evaluated cell data and hyperlink metadata.
    # Original formulas are not requested from Google.
    snapshot_path.write_text(
        json.dumps(
            {"metadata": metadata, "snapshot": payload},
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    workbook = Workbook(write_only=True)
    worksheet = workbook.create_sheet(NCOP_SHEET_TAB)

    try:
        for values in [headers, *rows]:
            output = []

            for value in values:
                if len(value) > 32767:
                    raise ValueError(
                        "A cell exceeds Excel's 32,767-character limit."
                    )

                cell = WriteOnlyCell(worksheet, value=value)

                # Preserve literal text without creating Excel formulas.
                cell.data_type = "s"
                output.append(cell)

            worksheet.append(output)

        workbook.save(metadata["xlsx_path"])

    finally:
        workbook.close()

    return df_raw, metadata


def read_ncop_source(url):
    with google_sheets_session() as session:
        return capture_ncop_sheet(
            session, url, get_app_base_dir() / "input",
        )


def prepare_ncop_input(df_raw):
    result = clean_and_prepare_df(df_raw)
    df, originals, sanitized, phones, dates, audit = result
    system = {"ncop_id", "serial_number", "date_uploaded"}
    derived = {"budb_id", "contact_group_id", "deal_id", "prod_id", "po_id", "source_url"}
    business = [c for c in df.columns if c not in system | derived]
    has_business = (
        df[business].notna().any(axis=1)
        if business else pd.Series(False, index=df.index)
    )
    ids = df["ncop_id"].fillna("").astype(str).str.strip()
    new_marker = ids.str.casefold().isin({"", "nan", "none", "null", "nat", "*0000"})
    old_identity = pd.Series(False, index=df.index)
    for c in ("serial_number", "date_uploaded"):
        if c in df:
            old_identity |= df[c].notna()
    # Skip template/ignored-only rows with no identity or lead data.
    # Serial/date-only rows remain new entries, matching the agreed identity rule.
    # Numeric and malformed nonblank IDs are retained for sync validation.
    keep = ~(new_marker & ~has_business & ~old_identity)
    df = df.loc[keep].copy()
    if df.empty:
        raise ValueError("No importable NCOP lead rows; synchronization stopped.")
    return df, originals, sanitized, phones, dates, audit


def validate_ncop_baseline(path):
    path = Path(path).resolve()
    conn = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)
    try:
        schema = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='ncop'"
        ).fetchone()
        if not schema:
            raise ValueError("The baseline has no ncop table.")
        columns = {row[1]: row for row in conn.execute('PRAGMA table_info("ncop")')}
        required = {"ncop_id", "serial_number", "date_uploaded", "prod_id", "po_id", "source_url"}
        missing = required - set(columns)
        if missing:
            raise ValueError("Baseline columns missing: " + ", ".join(sorted(missing)))
        if columns["ncop_id"][5] != 1 or "AUTOINCREMENT" not in schema[0].upper():
            raise ValueError("Baseline ncop_id must be INTEGER PRIMARY KEY AUTOINCREMENT.")
        if not conn.execute('SELECT 1 FROM ncop LIMIT 1').fetchone():
            raise ValueError("Baseline ncop table is empty.")
    finally:
        conn.close()


def commit_and_write_back_google(
    conn,
    db_path: str,
    url: str,
    headers: list,
    source_row_count: int,
    expected_modified_time: str,
    rows: list,
    sheet_name: str = "CONSOLIDATED LIST",
    snapshot_path: str = "",
) -> Path:
    spreadsheet_id = extract_google_sheet_id(url)
    pending_path = google_pending_path(url)

    if pending_path.exists():
        raise RuntimeError(
            f"An unresolved Google writeback exists:\n{pending_path}"
        )

    columns = get_google_writeback_columns([headers])

    # Abort if the source changed since the download.
    if get_google_sheet_version(url) != expected_modified_time:
        raise RuntimeError(
            "The Google Sheet changed during processing. "
            "The build will restore its pre-build snapshot. "
            "Run again when nobody is editing the sheet."
        )

    if not rows:
        raise ValueError("No rows are available for writeback.")

    rows = sorted(rows, key=lambda row: row["sheet_row"])

    sheet_row_numbers = [
        int(row["sheet_row"])
        for row in rows
    ]

    if (
        len(set(sheet_row_numbers)) != len(sheet_row_numbers)
        or any(
            not 2 <= r <= source_row_count + 1
            for r in sheet_row_numbers
        )
    ):
        raise ValueError(
            "Invalid or duplicate Google Sheets writeback row numbers."
        )

    sheet_range = "'" + sheet_name.replace("'", "''") + "'"

    payloads = []

    # Write back at most 2,000 processed rows per API request.
    for offset in range(0, len(rows), 2000):
        batch = rows[offset:offset + 2000]
        data = []

        for field, column_index in columns.items():
            letter = get_column_letter(column_index + 1)

            first = last = None
            values = []

            def append_range():
                if first is not None:
                    data.append({
                        "range": (
                            f"{sheet_range}!"
                            f"{letter}{first}:{letter}{last}"
                        ),
                        "majorDimension": "ROWS",
                        "values": values,
                    })

            for record in batch:
                row_num = int(record["sheet_row"])

                if first is not None and row_num != last + 1:
                    append_range()
                    first = None
                    values = []

                if first is None:
                    first = row_num

                last = row_num
                values.append([record[field]])

            append_range()

        payloads.append({
            "valueInputOption": "RAW",
            "data": data,
        })

    # Save the complete recovery mapping before committing SQLite.
    state = read_state(get_app_base_dir())
    if not state or state["status"] != "building":
        raise RuntimeError("No active build state; writeback stopped.")
    if not snapshot_path or not Path(snapshot_path).is_file():
        raise RuntimeError("Source snapshot is missing; writeback stopped.")
    recovery = {
        "build_id": state["build_id"],
        "snapshot_path": str(Path(snapshot_path).resolve()),
        "db_path": str(Path(db_path).resolve()),
        "spreadsheet_id": spreadsheet_id,
        "sheet_name": sheet_name,
        "expected_modified_time": expected_modified_time,
        "rows": rows,
        "payloads": payloads,
    }

    with pending_path.open("x", encoding="utf-8") as file:
        json.dump(
            recovery,
            file,
            ensure_ascii=False,
            indent=2,
        )
        file.flush()
        os.fsync(file.fileno())

    try:
        conn.commit()

        endpoint = (
            "https://sheets.googleapis.com/v4/spreadsheets/"
            f"{spreadsheet_id}/values:batchUpdate"
        )

        with google_sheets_session() as session:
            # Persist this before sending any write request.
            mark_writeback_started(get_app_base_dir())

            version = expected_modified_time
            for payload in payloads:
                if get_google_sheet_version(url) != version:
                    raise RuntimeError("Sheet changed between writeback batches; recovery required.")
                google_api_request(
                    session,
                    "POST",
                    endpoint,
                    json=payload,
                    timeout=(15, 90),
                )
                version = get_google_sheet_version(url)

    except Exception as exc:
        raise RuntimeError(
            "Google writeback did not finish with a confirmed success. "
            "SQLite may already contain the assigned IDs.\n\n"
            f"Recovery mapping: {pending_path}\n\n"
            "Resolve this pending writeback before importing again."
        ) from exc

    stamp = datetime.now(
        ZoneInfo("Asia/Manila")
    ).strftime("%Y-%m-%d_%H%M%S_%f")

    completed_path = pending_path.with_name(
        f"{spreadsheet_id}_{stamp}_completed.json"
    )

    pending_path.rename(completed_path)

    return completed_path


def read_input_file(path: str, sheet_name: str | None = None) -> pd.DataFrame:
    ext = os.path.splitext(path)[1].lower()

    if ext == ".csv":
        return pd.read_csv(path, dtype=str, keep_default_na=False)

    if ext not in {".xlsx", ".xlsm", ".xls"}:
        raise ValueError("Unsupported file type. Please select a .csv or .xlsx file.")

    # --- Read via pandas first (fast) ---
    xls = pd.ExcelFile(path)
    sheet = sheet_name or xls.sheet_names[0]
    df = pd.read_excel(
        xls,
        sheet_name=sheet,
        dtype=str,
        keep_default_na=False,  # ✅ do not convert stuff into NaN
        na_filter=False,        # ✅ don’t treat blanks/tokens as NaN
    ).fillna("")
    
    PHONE_COLS = {
        "Phone1", "Phone2", "Phone3", "Phone4",
        "CLEANED PHONE1", "CLEANED PHONE2", "CLEANED PHONE3", "CLEANED PHONE4",
        "Phone 1", "Phone 2", "Phone 3", "Phone 4", "Phone 5",
    }

    HEIRSHIP_COL = "Heirship Report Link"
    # accept either spelling, case-insensitive
    OWNER_COL_CANDIDATES = {"OWNERHSIP PORTION", "OWNERSHIP PORTION"}

    def _norm_header(x) -> str:
        return str(x).strip().casefold() if x is not None else ""

    # Decide if we even need openpyxl
    needs_wb = (
        any(c in df.columns for c in PHONE_COLS)
        or (HEIRSHIP_COL in df.columns)
        or any(c in df.columns for c in OWNER_COL_CANDIDATES)
    )

    if not needs_wb:
        return df

    wb = None
    try:
        wb = load_workbook(path, data_only=True)
        ws = wb[sheet]

        # Build header -> column index map (case-insensitive)
        header_to_col = {}
        for i, cell in enumerate(ws[1], start=1):
            header_to_col[_norm_header(cell.value)] = i

        n = len(df)

        # --- 1) Fix "#######" for phone columns only ---
        for col in PHONE_COLS:
            if col not in df.columns:
                continue

            excel_col_idx = header_to_col.get(_norm_header(col))
            if not excel_col_idx:
                continue

            for row_i in range(n):
                v = df.at[row_i, col]

                # pandas might give "", NaN, or "#######" depending on how it read the file
                is_blank_or_nan = (v == "") or pd.isna(v)
                is_hash = is_excel_hash_overflow(v)

                if is_blank_or_nan or is_hash:
                    xl_cell = ws.cell(row=2 + row_i, column=excel_col_idx)
                    raw = xl_cell.value

                    # store raw cell value as text
                    if raw is None:
                        df.at[row_i, col] = ""
                    elif isinstance(raw, (int, float)):
                        # avoid "9723612930.0"
                        df.at[row_i, col] = str(int(raw)) if float(raw).is_integer() else str(raw)
                    else:
                        df.at[row_i, col] = str(raw)

        # --- 2) Extract real hyperlink targets for Heirship Report Link ---
        if HEIRSHIP_COL in df.columns:
            excel_col_idx = header_to_col.get(_norm_header(HEIRSHIP_COL))
            if excel_col_idx:
                real_links = []
                for r in range(2, 2 + n):
                    cell = ws.cell(row=r, column=excel_col_idx)
                    if cell.hyperlink and cell.hyperlink.target:
                        real_links.append(cell.hyperlink.target)
                    else:
                        val = cell.value
                        real_links.append("" if val is None else str(val))
                df[HEIRSHIP_COL] = real_links

        # --- 3) Fix Ownership Portion fractions being misread as dates (1/4, 1/2) ---
        # --- 3) Fix Ownership Portion fractions being misread as dates (1/4, 1/2, 1/3) ---
        owner_col_in_df = None
        for candidate in OWNER_COL_CANDIDATES:
            if candidate in df.columns:
                owner_col_in_df = candidate
                break

        def ownership_cell_to_text(cell) -> str:
            v = cell.value
            if v is None:
                return ""

            # If Excel stored it as a DATE, convert back to M/D (Jan 3 => "1/3")
            if getattr(cell, "is_date", False) and isinstance(v, (datetime, date)):
                return f"{v.month}/{v.day}"

            # Already text (best case)
            if isinstance(v, str):
                return v.strip()

            # Numeric: try to express as fraction if it looks like a portion
            if isinstance(v, (int, float)):
                # Keep percent as percent if it was entered that way
                if "%" in str(getattr(cell, "number_format", "")):
                    return f"{v * 100:.0f}%"

                # If it's between 0 and 1, it's likely a portion; turn into a nice fraction
                if 0 < float(v) < 1:
                    frac = Fraction(v).limit_denominator(256)
                    return f"{frac.numerator}/{frac.denominator}"

                return str(v)

            return str(v)

        if owner_col_in_df:
            excel_col_idx = header_to_col.get(_norm_header(owner_col_in_df))
            if excel_col_idx:
                fixed_vals = []
                for r in range(2, 2 + n):
                    cell = ws.cell(row=r, column=excel_col_idx)
                    fixed_vals.append(ownership_cell_to_text(cell))
                df[owner_col_in_df] = fixed_vals

        return df  # ✅ IMPORTANT: return the DataFrame
    
    finally:
        try:
            if wb:
                wb.close()
        except Exception:
            pass


def split_pipe_ids(value):
    if value is None or pd.isna(value):
        return []

    s = str(value).strip()
    if not s or s.lower() in {"nan", "none", "null"}:
        return []

    return [x.strip() for x in s.split("|") if x.strip()]


def add_county_state_pair(items, county, state):
    county = "" if county is None or pd.isna(county) else str(county).strip().upper()
    state = "" if state is None or pd.isna(state) else str(state).strip().upper()

    if not county or not state:
        return

    # PO can be: "CAMPBELL, CONVERSE" or "CAMPBELL & CONVERSE"
    counties = re.split(r"\s*(?:,|&)\s*", county)

    for c in counties:
        c = c.strip().upper()
        if c:
            items.append(f"{c}, {state}")


def parse_existing_combined_county(value):
    items = []

    if value is None or pd.isna(value):
        return items

    s = str(value).strip()
    if not s or s.lower() in {"nan", "none", "null"}:
        return items

    for part in s.split("|"):
        part = part.strip().upper()
        if part:
            items.append(part)

    return items


def unique_pipe_join(items):
    seen = set()
    output = []

    for item in items:
        item = str(item).strip().upper()
        if not item:
            continue

        if item not in seen:
            seen.add(item)
            output.append(item)

    return " | ".join(output)


def load_budb_county_lookup(budb_path):
    if not budb_path:
        return {}

    conn = connect_sqlite_readonly(budb_path)
    try:
        query = """
            SELECT
                [id] AS budb_id,
                [County] AS county,
                [State] AS state
            FROM [bottoms_up]
        """
        df = pd.read_sql_query(query, conn)

        lookup = {}
        for row in df.itertuples(index=False):
            budb_id = "" if pd.isna(row.budb_id) else str(row.budb_id).strip()
            if not budb_id:
                continue

            items = []
            add_county_state_pair(items, row.county, row.state)
            lookup[budb_id] = items

        return lookup

    finally:
        conn.close()


def populate_combined_counties_from_all_sources(df, budb_county_lookup):
    combined_values = []
    for row in df.itertuples(index=False):
        items = list(parse_existing_combined_county(getattr(row, "combined_county", "")))
        for budb_id in split_pipe_ids(getattr(row, "budb_id", "")):
            items.extend(budb_county_lookup.get(budb_id, []))
        combined_values.append(unique_pipe_join(items))
    df = df.copy()
    df["combined_counties_from_all_sources"] = combined_values
    return df

class ActivityWriter:
    """Convert printed output into complete activity-log lines."""

    def __init__(self, log):
        self.log = log
        self.buffer = ""

    def write(self, text):
        self.buffer += text

        while "\n" in self.buffer:
            line, self.buffer = self.buffer.split("\n", 1)
            if line.strip():
                self.log(line)

        return len(text)

    def flush(self):
        if self.buffer.strip():
            self.log(self.buffer)
        self.buffer = ""


class NCOPImporterApp(ctk.CTk):
    def __init__(self):
        super().__init__()

        self.busy = False
        self.references_ready = False
        self.ui_events = queue.Queue()
        self.reference_labels = {}

        # --- Signature UI colors (your theme) ---
        self.PANEL_BG = "#273946"
        self.APP_BG = "#fff6de"
        self.ACCENT = "#CB1F47"
        self.ACCENT_HOVER = "#ffab4c"
        self.TEXT_DARK = "#273946"

        self.title("NCOP DB Builder [v2.0.0]")
        default_width, default_height = 760, 500
        width = min(default_width, self.winfo_screenwidth() - 40)
        height = min(default_height, self.winfo_screenheight() - 80)
        self.geometry(f"{width}x{height}")
        self.resizable(True, True)
        self.minsize(760, 560)

        ctk.set_appearance_mode("dark")
        ctk.set_default_color_theme("dark-blue")
        self.configure(fg_color=self.APP_BG)

        self.main_frame = ctk.CTkFrame(self, fg_color=self.APP_BG, corner_radius=12)
        self.main_frame.pack(fill="both", expand=True, padx=10, pady=10)
        # Content follows the height of the left panels.
        self.main_frame.grid_rowconfigure(1, weight=0)

        # No expandable gap between the panels and controls.
        self.main_frame.grid_rowconfigure(2, weight=0)

        # Controls keep their natural height.
        self.main_frame.grid_rowconfigure(3, weight=0)

        # Extra window height stays below the controls.
        self.main_frame.grid_rowconfigure(4, weight=1)
        self.main_frame.grid_columnconfigure(
            0, weight=1, uniform="main_columns"
        )
        self.main_frame.grid_columnconfigure(
            1, weight=1, uniform="main_columns"
        )

        self.title_label = ctk.CTkLabel(
            self.main_frame,
            text="NCOP DB Builder Tool",
            font=ctk.CTkFont(size=20, weight="bold"),
            text_color=self.TEXT_DARK
        )
        self.title_label.grid(row=0, column=0, columnspan=2, pady=(12, 6))

        # Left column: settings and Google Sheets input only.
        left_column = ctk.CTkFrame(
            self.main_frame,
            width=350,
            fg_color="transparent",
        )
        left_column.grid(
            row=1, column=0,
            sticky="nsew",
            padx=(0, 8),
        )
        # The packed settings panels determine this column's height.
        left_column.pack_propagate(True)

        pipedrive_tab = self._create_locked_tab_section(
            left_column,
            title="Pipedrive",
            height=260,
        )
        self._setup_pipedrive_tab(pipedrive_tab)
        budb_tab = self._create_locked_tab_section(
            left_column,
            title="Bottoms Up",
            height=110,
        )
        self._setup_budb_tab(budb_tab)
        input_tab = self._create_locked_tab_section(
            left_column,
            title="Import",
            height=180,
        )
        self._setup_import_tab(input_tab)

        # Right column: references, log, progress, actions.
        self.log_container = ctk.CTkFrame(
            self.main_frame,
            fg_color="transparent",
        )
        self.log_container.grid(
            row=1, column=1,
            sticky="nsew",
            padx=(8, 0),
            pady=(6, 6),
        )

        # Follow the height established by the left settings panels.
        self.log_container.grid_propagate(False)
        self.log_container.grid_columnconfigure(0, weight=1)

        # The log fills the space remaining below References.
        self.log_container.grid_rowconfigure(2, weight=1)

        self.status_icons = {
            state: self._make_status_icon(state)
            for state in ("Needed", "Checking...", "Ready", "Error")
        }

        references = ctk.CTkFrame(
            self.log_container,
            fg_color=self.APP_BG,
            corner_radius=0,
        )
        references.grid(
            row=0, column=0,
            sticky="ew",
            pady=(0, 12),
        )
        references.grid_columnconfigure(0, weight=1)

        ctk.CTkLabel(
            references,
            text="References",
            height=22,
            font=("Segoe UI", 12, "bold"),
            text_color=self.TEXT_DARK,
            anchor="w",
        ).grid(
            row=0, column=0,
            sticky="w",
            pady=(0, 5),
        )

        for row, name in enumerate(
            ("Pipedrive", "BUDB", "Google Sheets", "Local storage"),
            start=1,
        ):
            label = ctk.CTkLabel(
                references,
                text=f"{name}   ·   Needed",
                image=self.status_icons["Needed"],
                compound="left",
                anchor="w",
                padx=5,
                height=22,
                font=("Segoe UI", 11),
                text_color="#98651d",
            )
            label.grid(
                row=row, column=0,
                sticky="w",
                pady=1,
            )
            self.reference_labels[name] = label

        ctk.CTkLabel(
            self.log_container,
            text="Activity Log",
            height=22,
            font=("Segoe UI", 12, "bold"),
            text_color=self.TEXT_DARK,
            anchor="w",
        ).grid(
            row=1, column=0,
            sticky="w",
            pady=(0, 5),
        )

        self.log_box = ctk.CTkTextbox(
            self.log_container,
            height=150,
            fg_color=self.PANEL_BG,
            text_color=self.APP_BG,
            font=("Consolas", 11),
            wrap="word",
            corner_radius=10,
            scrollbar_button_color="#516571",
            scrollbar_button_hover_color="#71828b",
        )
        self.log_box.grid(
            row=2, column=0,
            sticky="nsew",
        )
        self.log_box.configure(state="disabled")

        # Bottom left: progress bar, vertically centered with the buttons.
        progress_container = ctk.CTkFrame(
            self.main_frame,
            fg_color="transparent",
            height=34,
        )
        progress_container.grid(
            row=3,
            column=0,
            sticky="nsew",
            padx=(0, 8),
            pady=(4, 0),
        )
        progress_container.grid_columnconfigure(0, weight=1)
        progress_container.grid_rowconfigure(0, weight=1)

        self.progress = ctk.CTkProgressBar(
            progress_container,
            height=6,
            fg_color="#ded9ca",
            progress_color=self.ACCENT,
        )
        self.progress.grid(
            row=0,
            column=0,
            sticky="ew",
        )
        self.progress.set(0)

        # Bottom right: all three actions in one horizontal row.
        actions = ctk.CTkFrame(
            self.main_frame,
            fg_color="transparent",
        )
        actions.grid(
            row=3,
            column=1,
            sticky="ew",
            padx=(8, 0),
            pady=(4, 0),
        )

        actions.grid_columnconfigure(
            (0, 1, 2),
            weight=1,
            uniform="actions",
        )

        self.check_btn = ctk.CTkButton(
            actions,
            text="Check references",
            command=self.check_references,
            width=1,
            height=34,
            corner_radius=8,
            font=("Segoe UI", 11),
            fg_color=self.PANEL_BG,
            hover_color="#3e5564",
            text_color=self.APP_BG,
        )
        self.check_btn.grid(
            row=0,
            column=0,
            sticky="ew",
            padx=(0, 4),
        )

        self.import_btn = ctk.CTkButton(
            actions,
            text="Build DB",
            command=self.start_import,
            width=1,
            height=34,
            corner_radius=8,
            font=("Segoe UI", 11, "bold"),
            fg_color="#26734b",
            hover_color="#1e5c3c",
            text_color="#ffffff",
            state="disabled",
        )
        self.import_btn.grid(
            row=0,
            column=1,
            sticky="ew",
            padx=4,
        )

        self.action_log_btn = ctk.CTkButton(
            actions,
            text="Update Action Log",
            command=self.start_action_log,
            width=1,
            height=34,
            corner_radius=8,
            font=("Segoe UI", 11),
            fg_color="#ffab4c",
            hover_color="#e69538",
            text_color="#273946",
            state="disabled",
        )
        self.action_log_btn.grid(
            row=0,
            column=2,
            sticky="ew",
            padx=(4, 0),
        )

        self._refresh_action_buttons()

                # github link (hihi)
        self.credit_label = ctk.CTkLabel(
        self,
        text="© dyoliya • GitHub",
        text_color="#484949",
        font=ctk.CTkFont(size=8, underline=False),
        cursor="hand2"
        )
        self.credit_label.place(relx=1.0, x=-10, y=1, anchor="ne")
        self.credit_label.bind("<Button-1>", lambda event: webbrowser.open("https://github.com/dyoliya"),)

        # Editing an input cancels its previous validation.
        for variable in (
            self.google_sheet_url,
            self.pipedrive_token_var,
            self.budb_path_var,
        ):
            variable.trace_add("write", self._invalidate_references)

        self.protocol("WM_DELETE_WINDOW", self._close_app)
        self.after(100, self._poll_ui)


    def _create_locked_tab_section(self, parent, title: str, height: int):
        # Keep the existing method signature, but let content set the height.
        panel = ctk.CTkFrame(
            parent,
            fg_color=self.PANEL_BG,
            corner_radius=10,
        )
        panel.pack(fill="x", padx=0, pady=(4, 6))

        ctk.CTkLabel(
            panel,
            text=title,
            font=("Segoe UI", 12, "bold"),
            height=22,
            text_color=self.ACCENT_HOVER,
            anchor="w",
        ).pack(fill="x", padx=12, pady=(10, 4))

        return panel


    def _setup_import_tab(self, tab):
        self.input_source = ctk.StringVar(value="Google Sheets")
        self.google_sheet_url = ctk.StringVar(value="")

        ctk.CTkLabel(
            tab,
            text="Google Sheets URL",
            height=20,
            font=("Segoe UI", 12),
            text_color=self.APP_BG,
            anchor="w",
        ).pack(
            fill="x",
            padx=10,
            pady=(6, 4),
        )

        self.google_url_entry = ctk.CTkEntry(
            tab,
            textvariable=self.google_sheet_url,
            placeholder_text="Paste Google Sheets URL here",
            height=30,
            font=("Segoe UI", 12),
        )
        self.google_url_entry.pack(
            fill="x",
            padx=10,
            pady=(0, 5),
        )

        ctk.CTkLabel(
            tab,
            text="Source tab: CONSOLIDATED LIST",
            height=20,
            font=("Segoe UI", 11),
            text_color="#c5cfd5",
            anchor="w",
        ).pack(
            fill="x",
            padx=10,
            pady=(0, 12),
        )


    def _browse_budb(self):
        if self.busy:
            return

        selected = filedialog.askopenfilename(
            parent=self,
            title="Select the latest Bottoms Up database",
            filetypes=[("SQLite database", "*.db")],
        )

        if not selected:
            return

        # Clear the previous selection if the new file is invalid.
        self.budb_path_var.set("")
        self.budb_filename_var.set("No database selected")
        self.budb_info_label.configure(
            text="Select the latest Bottoms Up .db file."
        )

        try:
            path = validate_budb_file(selected)
            modified = datetime.fromtimestamp(
                path.stat().st_mtime
            ).strftime("%Y-%m-%d %H:%M")
        except Exception as exc:
            self._set_reference("BUDB", "Error")
            self._log(f"[BUDB] Selection rejected: {exc}")
            messagebox.showerror(
                "Invalid Bottoms Up database",
                str(exc),
            )
            return

        self.budb_path_var.set(str(path))
        self.budb_filename_var.set(path.name)
        self.budb_info_label.configure(
            text=f"File modified: {modified} · Check references next."
        )

        self._log(f"[BUDB] Selected: {path}")
        self._log(f"[BUDB] File modified: {modified}")



    # ---------- log/progress helpers ----------
    def _log(self, text: str):
        stamp = datetime.now().strftime("%H:%M:%S")

        for line in str(text).splitlines():
            if line.strip():
                self.ui_events.put(
                    ("log", f"[{stamp}] {line}")
                )


    def _divider(self):
        self._log("- - - - - - - - - - - - - - - - - - - - - - - - - - -")

    def progress_callback(self, fraction, msg=None):
        value = max(0.0, min(1.0, float(fraction)))
        self.ui_events.put(("progress", value))

        if msg:
            self._log(msg)


    def _ui_error(self, title: str, msg: str):
        self._call_ui(lambda m=msg: messagebox.showerror(title, m))


    # ---------- UI actions ----------
    def start_import(self):
        if self.busy or not self.references_ready:
            return

        url = self.google_sheet_url.get().strip()
        budb_path = self.budb_path_var.get().strip()

        self._set_busy(True)
        self.references_ready = False

        def worker():
            writer = ActivityWriter(self._log)
            build = None
            base = get_app_base_dir()

            try:
                with contextlib.redirect_stdout(writer):
                    selected_budb = validate_budb_file(budb_path)
                    active = list((base / "database").glob("*.db"))
                    if len(active) != 1:
                        raise RuntimeError("Expected one initialized NCOP baseline.")
                    validate_ncop_baseline(active[0])

                    build = prepare_build(
                        base,
                        datetime.now(CENTRAL_TZ).strftime("%Y-%m-%d"),
                        google_pending_path(url),
                    )

                    self._log(
                        f"[DB] Building: {build['new_db']}"
                    )
                    self._log(
                        "[DB] Comparison baseline: "
                        f"{build['old_db'] or 'First build'}"
                    )

                    self._import_worker(
                        build["new_db"],
                        [url],
                        "ncop",
                        "Google Sheets",
                        budb_path=selected_budb,
                    )

                    finish_build(base)
                    self._log(
                        f"[DONE] Daily DB updated: "
                        f"{Path(build['new_db']).name} → table 'ncop'"
                    )
                    self._log(
                        "[BUILD COMPLETE] Click Update Action Log "
                        "to finish."
                    )

                    self._call_ui(lambda: messagebox.showinfo(
                            "Database built",
                            "NCOP update and writeback completed.\n\n"
                            "Click Update Action Log to finish.",
                        ),
                    )

            except Exception as exc:
                self._log(f"[BUILD ERROR] {exc}")
                recovery = ""

                if build is not None:
                    try:
                        recovery = fail_build(base)
                    except Exception as recovery_error:
                        recovery = (
                            "Automatic recovery failed: "
                            f"{recovery_error}. "
                            "Keep the database and backups."
                        )

                    self._log(f"[RECOVERY] {recovery}")

                self._ui_error(
                    "Build not completed",
                    f"{exc}\n\n{recovery}",
                )

            finally:
                writer.flush()
                self._divider()
                self.progress_callback(
                    0,
                    "Build attempt finished. Check the messages above "
                    "for completion or recovery instructions.",
                )
                self._call_ui(self._finish_job)

        threading.Thread(target=worker, daemon=True).start()


    def _load_pipedrive_settings_safe(self) -> dict:
        try:
            return load_pipedrive_settings(get_app_base_dir())
        except (OSError, ValueError):
            return {}

    def _setup_pipedrive_tab(self, tab):
        settings = self._load_pipedrive_settings_safe()

        self.pipedrive_token_var = ctk.StringVar(
            value=str(settings.get("api_token") or "")
        )

        ctk.CTkLabel(
            tab,
            text="API token:",
            text_color=self.APP_BG,
            anchor="w",
            font=("Segoe UI", 12),
        ).pack(fill="x", padx=10, pady=(4, 2))

        self.pipedrive_token_entry = ctk.CTkEntry(
            tab,
            textvariable=self.pipedrive_token_var,
            show="•",
            placeholder_text="Paste your Pipedrive API token",
            font=("Segoe UI", 12),
        )
        self.pipedrive_token_entry.pack(
            fill="x", padx=10, pady=(0, 6)
        )

        row = ctk.CTkFrame(tab, fg_color="transparent")
        row.pack(fill="x", padx=10, pady=(0, 8))
        row.grid_columnconfigure((0, 1), weight=1)

        ctk.CTkButton(
            row,
            text="Open API token page",
            width=150,
            fg_color=self.PANEL_BG,
            hover_color="#334957",
            command=self._open_pipedrive_token_page,
            font=("Segoe UI", 12),
        ).grid(
            row=0, column=0,
            sticky="ew",
            padx=(0, 6),
        )

        self.save_pipedrive_btn = ctk.CTkButton(
            row,
            text="Save API token",
            width=120,
            fg_color=self.ACCENT,
            hover_color=self.ACCENT_HOVER,
            command=self._save_pipedrive_token,
            font=("Segoe UI", 12),
        )
        self.save_pipedrive_btn.grid(
            row=0, column=1,
            sticky="ew",
        )

        self.pipedrive_status_label = ctk.CTkLabel(
            tab,
            text="",
            text_color=self.APP_BG,
            anchor="w",
            justify="left",
            wraplength=310,
            font=("Segoe UI", 11),
        )
        self.pipedrive_status_label.pack(
            fill="x", padx=10, pady=(0, 10)
        )

        self._refresh_pipedrive_status()


    def _open_pipedrive_token_page(self):
        settings = self._load_pipedrive_settings_safe()
        base_url = settings.get("base_url", "").rstrip("/")
        if not base_url:
            messagebox.showwarning("Missing config", "base_url is not set in config/pipedrive_settings.json.")
            return
        webbrowser.open(f"{base_url}/settings/api")


    def _refresh_pipedrive_status(self):
        status = pipedrive_cache_status(get_app_base_dir())

        if not status.get("exists"):
            description = (
                "No cached deals. Check references will download them."
            )
        else:
            age_seconds = status["age_seconds"]
            age_hours = age_seconds / 3600

            rows = status.get("rows")
            rows_text = (
                f"{rows:,} deals"
                if rows is not None
                else "unknown row count"
            )

            if 0 <= age_seconds < 8 * 60 * 60:
                description = (
                    f"Cached: {rows_text}, {age_hours:.1f}h old. "
                    "Check references will validate reuse."
                )
            else:
                description = (
                    f"Cached: {rows_text}. Refresh required "
                    "during Check references."
                )

        self.pipedrive_status_label.configure(
            text=f"{description}\nCache validity: 8 hours."
        )


    def _save_pipedrive_token(self):
        if self.busy:
            return

        token = self.pipedrive_token_var.get().strip()

        if not token:
            messagebox.showwarning(
                "Missing token",
                "Enter your Pipedrive API token first.",
            )
            return

        try:
            save_pipedrive_settings(
                get_app_base_dir(),
                api_token=token,
            )
        except (OSError, ValueError) as exc:
            messagebox.showerror(
                "Could not save token",
                str(exc),
            )
            return

        self._invalidate_references()
        self._refresh_pipedrive_status()

        self._log(
            "[PIPEDRIVE] API token saved locally. "
            "Click Check references to continue."
        )

        messagebox.showinfo(
            "API token saved",
            "Your token has been saved to:\n"
            "config/pipedrive_settings.json\n\n"
            "It will load automatically next time you open the app.",
        )


    def _make_status_icon(self, state):
        size, scale = 16, 4

        image = Image.new(
            "RGBA",
            (size * scale, size * scale),
        )
        draw = ImageDraw.Draw(image)

        colors = {
            "Needed": "#98651d",
            "Checking...": "#55758a",
            "Ready": "#26734b",
            "Error": self.ACCENT,
        }

        draw.ellipse(
            (2, 2, 62, 62),
            fill=colors[state],
        )

        def line(points):
            draw.line(
                [(x * scale, y * scale) for x, y in points],
                fill="white",
                width=2 * scale,
                joint="curve",
            )

        if state == "Ready":
            # Check mark.
            line([(4, 8), (7, 11), (12, 5)])

        elif state == "Error":
            # Cross.
            line([(5, 5), (11, 11)])
            line([(5, 11), (11, 5)])

        elif state == "Checking...":
            # Clock.
            line([(8, 4), (8, 8), (11, 9)])

        else:
            # Exclamation mark.
            line([(8, 4), (8, 8)])
            draw.ellipse(
                (28, 42, 36, 50),
                fill="white",
            )

        return ctk.CTkImage(
            light_image=image,
            dark_image=image,
            size=(size, size),
        )


    def _set_reference(self, key, state):
        colors = {
            "Needed": "#98651d",
            "Checking...": "#55758a",
            "Ready": "#26734b",
            "Error": self.ACCENT,
        }

        self.reference_labels[key].configure(
            text=f"{key}   ·   {state}",
            image=self.status_icons[state],
            text_color=colors[state],
        )

    def _invalidate_references(self, *_):
        self.references_ready = False
        self.import_btn.configure(state="disabled")

        for key in self.reference_labels:
            self._set_reference(key, "Needed")


    def _set_busy(self, busy):
        self.busy = busy
        state = "disabled" if busy else "normal"

        for widget in (
            self.check_btn,
            self.google_url_entry,
            self.pipedrive_token_entry,
            self.save_pipedrive_btn,
            self.browse_budb_btn,
        ):
            widget.configure(state=state)

        self.import_btn.configure(
            state=(
                "normal"
                if self.references_ready and not busy
                else "disabled"
            )
        )

        self._refresh_action_buttons()


    def _finish_job(self):
        self._invalidate_references()
        self._set_busy(False)
        self._refresh_pipedrive_status()


    def _close_app(self):
        if self.busy:
            messagebox.showinfo(
                "Work in progress",
                "Wait for the current task to finish.",
            )
        else:
            self.destroy()

    def check_references(self):
        if self.busy:
            return

        self._invalidate_references()
        url = self.google_sheet_url.get().strip()
        budb_path = self.budb_path_var.get().strip()

        try:
            extract_google_sheet_id(url)

            settings = self._load_pipedrive_settings_safe()

            saved_token = str(
                settings.get("api_token") or ""
            ).strip()

            entered_token = self.pipedrive_token_var.get().strip()

            if entered_token != saved_token:
                messagebox.showwarning(
                    "Save your API token",
                    "The API token has unsaved changes.\n\n"
                    "Click Save API token, then Check references.",
                )
                return
        except Exception as exc:
            messagebox.showerror("Check references", str(exc))
            return

        self._set_busy(True)
        self._log("[CHECK] Validating references...")

        threading.Thread(
            target=self._check_worker,
            args=(url, budb_path),
            daemon=True,
        ).start()


    def _check_worker(self, url, budb_path):
        base = get_app_base_dir()

        def check_sheet():
            if google_pending_path(url).exists():
                raise RuntimeError(
                    "Pending Google writeback needs recovery."
                )

            sheet_id = extract_google_sheet_id(url)

            # Check edit permission without writing to the sheet.
            with google_sheets_session() as session:
                info = google_api_request(
                    session,
                    "GET",
                    (
                        "https://www.googleapis.com/drive/v3/files/"
                        f"{sheet_id}"
                    ),
                    params={
                        "fields": "capabilities(canEdit)",
                        "supportsAllDrives": "true",
                    },
                    timeout=(15, 60),
                ).json()

            if not info.get("capabilities", {}).get("canEdit"):
                raise ValueError(
                    "Share the sheet with the service account as Editor."
                )

            version = get_google_sheet_version(url)
            frame, capture = read_ncop_source(url)
            prepare_ncop_input(frame)

            get_google_writeback_columns([list(frame.columns)])

            headers = [sanitize_colname(c) for c in frame.columns]
            if len(headers) != len(set(headers)):
                raise ValueError(
                    "Duplicate column names after header cleanup."
                )

            if frame.empty or not frame.fillna("").astype(str).apply(
                lambda col: col.str.strip().ne("")
            ).any().any():
                raise ValueError("The CONSOLIDATED LIST tab has no data rows.")

            if get_google_sheet_version(url) != version:
                raise RuntimeError(
                    "The sheet changed during checking. Check again."
                )

        def check_pipedrive():
            ensure_fresh_pipedrive_cache(base, log=self._log)

            # Validate using the importer's existing parser.
            load_pipedrive_lookup_maps(base)

        def check_budb():
            path = validate_budb_file(budb_path)
            self._log(f"[BUDB] Validated: {path}")

        def check_local_storage():
            state = read_state(base)
            if not state or state["status"] not in {"complete", "rolled_back"}:
                raise RuntimeError("Initialize the baseline, finish the action log, or resolve recovery first.")
            for name in (
                "database",
                "database/previous_versions",
                "audit",
                "existing_deals",
                "cache",
                "input/google_sheet_backups",
                "google_sheet_writeback",
                "config",
            ):
                folder = base / name

                self._log(
                    f"[STORAGE CHECK] {name}/ — "
                    "checking folder and write access..."
                )

                try:
                    folder.mkdir(parents=True, exist_ok=True)

                    with tempfile.TemporaryFile(dir=folder) as probe:
                        probe.write(b"check")
                        probe.flush()

                except OSError as exc:
                    raise RuntimeError(
                        f"Folder check failed: {folder}\n"
                        f"Cannot create or write to this folder: {exc}"
                    ) from exc

                self._log(
                    f"[STORAGE OK] {name}/ — folder exists and is writable."
                )

            database_folder = base / "database"

            self._log(
                "[STORAGE CHECK] Active database count — "
                "checking .db files directly in database/ "
                "(excluding subfolders)..."
            )

            db_files = sorted(
                (
                    path
                    for path in database_folder.iterdir()
                    if path.is_file() and path.suffix.lower() == ".db"
                ),
                key=lambda path: path.name.lower(),
            )

            if len(db_files) > 1:
                filenames = "\n".join(
                    f"  - {path.name}" for path in db_files
                )

                raise RuntimeError(
                    f"Found {len(db_files)} .db files in database/.\n"
                    "Only one active NCOP database is allowed.\n"
                    f"{filenames}\n"
                    "Resolve the extra files, then check references again."
                )

            if not db_files:
                raise RuntimeError("No active NCOP baseline. Run initialize_ncop_baseline.py first.")
            path = db_files[0]
            validate_ncop_baseline(path)

            self._log(
                f"[STORAGE OK] Active database count: 1 — {path.name}"
            )

            self._log(
                f"[STORAGE CHECK] Database readability — {path.name}..."
            )

            conn = None

            try:
                conn = sqlite3.connect(
                    path.resolve().as_uri() + "?mode=ro",
                    uri=True,
                )

                conn.execute(
                    "SELECT name FROM sqlite_master LIMIT 1"
                ).fetchone()

            except (sqlite3.Error, OSError) as exc:
                raise RuntimeError(
                    f"Database readability check failed: {path}\n"
                    f"Details: {exc}"
                ) from exc

            finally:
                if conn is not None:
                    conn.close()

            self._log(
                "[STORAGE OK] Database opened successfully; "
                "SQLite table metadata is readable."
            )

        failures = []

        for key, check in (
            ("Pipedrive", check_pipedrive),
            ("BUDB", check_budb),
            ("Google Sheets", check_sheet),
            ("Local storage", check_local_storage),
        ):
            self._call_ui(lambda k=key: self._set_reference(k, "Checking..."),
            )

            try:
                check()

            except Exception as exc:
                error_text = str(exc)
                failures.append((key, error_text))

                self._log(f"[ERROR] {key}: {error_text}")

                self._call_ui(lambda k=key: self._set_reference(k, "Error"),
                )

            else:
                self._log(f"[READY] {key}")

                self._call_ui(lambda k=key: self._set_reference(k, "Ready"),
                )

        def finish():
            self.references_ready = not failures
            self._set_busy(False)
            self._refresh_pipedrive_status()

            if failures:
                self._log(
                    "[CHECK SUMMARY] Reference validation failed."
                )

                for key, error_text in failures:
                    self._log(f"[FAILED] {key}: {error_text}")

                failed_names = ", ".join(
                    key for key, _ in failures
                )

                self._log(
                    f"[NOT READY] Failed checks: {failed_names}. "
                    "Build DB is disabled. Resolve the errors above, "
                    "then click Check references again."
                )

            else:
                self._log(
                    "[CHECK SUMMARY] All reference checks passed."
                )

        self._call_ui(finish)


    def _setup_budb_tab(self, tab):
        # Require an explicit selection each time the app opens.
        self.budb_path_var = ctk.StringVar(value="")
        self.budb_filename_var = ctk.StringVar(
            value="No database selected"
        )

        row = ctk.CTkFrame(
            tab,
            fg_color="transparent",
        )
        row.pack(
            fill="x",
            padx=10,
            pady=(2, 4),
        )
        row.grid_columnconfigure(0, weight=1)

        self.budb_file_entry = ctk.CTkEntry(
            row,
            textvariable=self.budb_filename_var,
            font=("Segoe UI", 11),
            height=28,
            state="disabled",
            text_color="#ffffff",
        )
        self.budb_file_entry.grid(
            row=0, column=0,
            sticky="ew",
            padx=(0, 6),
        )

        self.browse_budb_btn = ctk.CTkButton(
            row,
            text="Browse",
            command=self._browse_budb,
            width=70,
            height=28,
            font=("Segoe UI", 11),
            fg_color=self.ACCENT,
            hover_color=self.ACCENT_HOVER,
        )
        self.browse_budb_btn.grid(row=0, column=1)

        self.budb_info_label = ctk.CTkLabel(
            tab,
            text="Select the latest Bottoms Up .db file.",
            font=("Segoe UI", 10),
            text_color="#c5cfd5",
            anchor="w",
            height=18,
        )
        self.budb_info_label.pack(
            fill="x",
            padx=10,
            pady=(0, 6),
        )


    def _call_ui(self, callback, *args):
        self.ui_events.put(("call", (callback, args)))


    def _poll_ui(self):
        for _ in range(200):
            try:
                kind, value = self.ui_events.get_nowait()
            except queue.Empty:
                break

            if kind == "call":
                callback, args = value
                callback(*args)
            elif kind == "progress":
                self.progress.set(value)
            else:
                self.log_box.configure(state="normal")
                self.log_box.insert("end", value + "\n")
                self.log_box.see("end")
                self.log_box.configure(state="disabled")

        self.after(100, self._poll_ui)


    def _refresh_action_buttons(self):
        try:
            state = read_state(get_app_base_dir())
            status = state["status"] if state else None
            self._last_build_state_error = None

        except Exception as exc:
            status = "recovery_required"
            error_text = str(exc)

            # Avoid repeating the same message on every button refresh.
            if getattr(self, "_last_build_state_error", None) != error_text:
                self._log(f"[BUILD STATE] {error_text}")
                self._last_build_state_error = error_text

        can_build = status in (
            None,
            "complete",
            "rolled_back",
        )

        self.import_btn.configure(
            state=(
                "normal"
                if self.references_ready and not self.busy and can_build
                else "disabled"
            )
        )

        if status == "complete":
            text = "Action Log updated"
        elif status not in (
            None,
            "complete",
            "rolled_back",
            "ready_for_log",
        ):
            text = "Recovery required"
        else:
            text = "Update Action Log"

        self.action_log_btn.configure(
            text=text,
            state=(
                "normal"
                if status == "ready_for_log" and not self.busy
                else "disabled"
            ),
        )


    def start_action_log(self):
        if self.busy:
            return

        self._set_busy(True)

        def worker():
            writer = ActivityWriter(self._log)

            try:
                with contextlib.redirect_stdout(writer):
                    base = get_app_base_dir()
                    state = read_state(base)

                    if not state or state["status"] != "ready_for_log":
                        raise RuntimeError(
                            "A successful build is required first."
                        )

                    if Path(state["pending_path"]).exists():
                        raise RuntimeError(
                            "Resolve the pending Google writeback first."
                        )

                    self._log(
                        "[ACTION LOG] Comparing the recorded "
                        "before/after databases..."
                    )

                    compare_databases(
                        old_db_path=state["old_db"],
                        new_db_path=state["new_db"],
                        build_id=state["build_id"],
                    )

                    state["status"] = "complete"
                    save_state(base, state)

                    self._log(
                        "[DONE] NCOP database and action log are complete."
                    )

                    self._call_ui(lambda: messagebox.showinfo(
                            "Complete",
                            "The NCOP database and action log "
                            "are both updated.",
                        ),
                    )

            except Exception as exc:
                self._log(f"[ACTION LOG ERROR] {exc}")
                self._ui_error(
                    "Action log not completed",
                    str(exc),
                )

            finally:
                writer.flush()
                self._call_ui(self._finish_job)

        threading.Thread(target=worker, daemon=True).start()


    def _import_worker(self, db_path, input_paths, table, input_source="Google Sheets", *, budb_path):
        if input_source != "Google Sheets" or len(input_paths) != 1 or table != "ncop":
            raise ValueError("Build expects one authoritative NCOP Google Sheet.")
        url = input_paths[0]
        base = get_app_base_dir()
        conn = None
        try:
            if google_pending_path(url).exists():
                raise RuntimeError("Resolve the previous pending writeback first.")
            pipedrive_result = ensure_fresh_pipedrive_cache(
                base, log=self._log, progress=self.progress_callback,
            )
            if pipedrive_result.get("downloaded"):
                self._log("[PIPEDRIVE] Fresh deals downloaded from Pipedrive.")
            self._call_ui(self._refresh_pipedrive_status)
            selected = validate_budb_file(budb_path)
            budb_signature = (selected.stat().st_size, selected.stat().st_mtime_ns)
            maps, meta = load_budb_lookup_maps(base, budb_path=selected)
            source_name = f"Google Sheets [{NCOP_SHEET_TAB}]"
            self.progress_callback(0, f"[1/1] Reading: {source_name}")
            df_raw, capture = read_ncop_source(url)
            self._log(f"[BACKUP] {capture['xlsx_path']}")
            self.progress_callback(0, "[1/1] Cleaning/standardizing...")
            df, originals, sanitized, phones, dates, null_audit = prepare_ncop_input(df_raw)
            expected_indices = list(df.index)
            self.progress_callback(0, "[1/1] Matching Pipedrive deal IDs...")
            df, pd_meta, deal_audit = populate_ncop_deal_ids_in_df(
                df, base_dir=base, progress_callback=self.progress_callback,
            )
            self._log(
                f"[PIPEDRIVE] Deal ID matching complete. "
                f"Matched: {df['deal_id'].notna().sum():,}/{len(df):,} rows | "
                f"Source rows loaded: {pd_meta.get('rows_loaded', 0):,}"
            )

            self.progress_callback(0, "[1/1] Matching BUDB IDs...")
            df, group_audit = populate_budb_ids_in_df(
                df, maps, progress_callback=self.progress_callback,
            )
            self._log(
                "[BUDB] contact_group_id populated from BUDB; "
                "Google Sheet values ignored.\n"
                f"[BUDB] Rows with multiple groups: {len(group_audit):,}. "
                "Lowest numeric group retained."
            )
            self._log(
                f"[BUDB] BUDB ID matching complete. "
                f"Matched: {df['budb_id'].notna().sum():,}/{len(df):,} rows | "
                f"Source rows loaded: {meta.get('rows_loaded', 0):,}"
            )

            self.progress_callback(
                0, "[1/1] Combining counties from all sources...",
            )
            county_lookup = load_budb_county_lookup(selected)
            df = populate_combined_counties_from_all_sources(df, county_lookup)
            county_count = (
                df["combined_counties_from_all_sources"]
                .fillna("").astype(str).str.strip().ne("").sum()
            )
            self._log(
                "[COUNTY] combined_counties_from_all_sources populated for "
                f"{county_count:,}/{len(df):,} rows."
            )
            self._log(
                "[INFO] Input contains ncop_id. "
                "File values will be used when present."
            )
            self._log(f"[1/1] {source_name}: {len(df):,} rows")
            self._log(
                f"[MAP] Columns: {len(originals)} original → "
                f"{len(sanitized)} sanitized"
            )
            if (selected.stat().st_size, selected.stat().st_mtime_ns) != budb_signature:
                raise RuntimeError("Selected BUDB changed during processing; build stopped.")
            if list(df.index) != expected_indices:
                raise RuntimeError("Row positions changed; Google writeback stopped.")
            self.progress_callback(0, "[1/1] Preparing table...")
            conn = connect_sqlite(db_path)
            ensure_table_and_columns(conn, table, list(df.columns))
            self._log("[SCHEMA] Table and incoming columns prepared.")

            self.progress_callback(0, "[1/1] Syncing rows...")
            result = sync_rows_by_ncop_id(
                conn, table, df, progress_callback=self.progress_callback,
                tz=CENTRAL_TZ, commit=False,
            )
            # Local commit lets report generators read this DB. Until Google
            # writes start, fail_build restores the complete pre-build snapshot.
            conn.commit()
            build = read_state(base)
            run_id = build["build_id"]
            if not group_audit.empty:
                by_row = {int(r["sheet_row"]): str(r["ncop_id"]) for r in result["writeback_rows"]}
                group_audit["ncop_id"] = group_audit["sheet_row"].map(by_row)
                if group_audit["ncop_id"].isna().any():
                    raise RuntimeError("Could not attach saved IDs to group audit.")
            audit_folder = base / "audit"
            audit_folder.mkdir(parents=True, exist_ok=True)
            if not null_audit.empty or not group_audit.empty:
                audit_path = audit_folder / f"ncop_audit_{run_id}.xlsx"
                with pd.ExcelWriter(audit_path, engine="openpyxl") as writer:
                    for name, data in (("null_conversions", null_audit), ("budb_group_selection", group_audit)):
                        if not data.empty:
                            data = data.copy()
                            data.insert(0, "source_file", url)
                            data.to_excel(writer, sheet_name=name, index=False)
                self._log(f"[AUDIT] Saved NCOP audit: {audit_path}")
            else:
                self._log(
                    "[AUDIT] No suspicious NULL conversions "
                    "or multiple BUDB contact groups detected."
                )

            self.progress_callback(
                0, "[REPORT] Generating NCOP associated deals report...",
            )
            report = generate_ncop_associated_deals_report(db_path=db_path, base_dir=base, table=table)
            report_dir = base / "existing_deals"
            if deal_audit is not None and not deal_audit.empty:
                match_path = report_dir / f"ncop_deal_match_audit_{run_id}.xlsx"
                deal_audit.to_excel(match_path, sheet_name="deal_match_audit", index=False)
                self._log(f"[AUDIT] Saved deal match audit: {match_path}")
            self._log(
                f"[REPORT] Saved associated deals report: {report['output_path']}\n"
                f"[REPORT] Exported rows: {report['rows_exported']:,} | "
                f"NCOP rows with deal_id: {report['unique_ncop_rows']:,}"
            )
            self.progress_callback(
                0,
                "[GOOGLE] Writing IDs, serial numbers, "
                f"and upload dates back to {NCOP_SHEET_TAB}...",
            )
            completed = commit_and_write_back_google(
                conn=conn, db_path=db_path, url=url,
                headers=list(df_raw.columns),
                source_row_count=capture["source_row_count"],
                expected_modified_time=capture["google_modified_time"],
                rows=result["writeback_rows"],
                sheet_name=NCOP_SHEET_TAB,
                snapshot_path=capture["snapshot_path"],
            )
            self._log(
                "[GOOGLE] Writeback completed for "
                f"{len(result['writeback_rows']):,} rows.\n"
                f"[GOOGLE] Writeback record: {completed}"
            )
            self._log(
                f"[SYNC] Inserted: {result['inserted']:,} | Updated: {result['updated']:,} | "
                f"Unchanged: {result['unchanged']:,} | Deleted: {result['deleted']:,}"
            )
        except Exception:
            if conn is not None:
                conn.rollback()
            raise
        finally:
            if conn is not None:
                conn.close()



if __name__ == "__main__":
    with exclusive_run(get_app_base_dir()):
        app = NCOPImporterApp()
        app.mainloop()