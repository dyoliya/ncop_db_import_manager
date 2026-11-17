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
import pymysql
from openpyxl import load_workbook

from customtkinter import filedialog

from ncop_transform import clean_and_prepare_df
from ncop_sqlite import (
    connect_sqlite,
    ensure_table_and_columns,
    get_existing_columns,
    sync_rows_by_ncop_id,
)
from ncop_deal_id_matcher import populate_ncop_deal_ids_in_df
from ncop_prod import load_prod_lookup_maps, populate_prod_ids_in_df
from ncop_budb import load_budb_lookup_maps, populate_budb_ids_in_df
from ncop_po import load_pooling_lookup_maps, populate_po_ids_in_df
from ncop_existing_deals_report import generate_ncop_associated_deals_report

from datetime import datetime
from datetime import date
from fractions import Fraction
from pathlib import Path
from typing import Optional, Tuple

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

    conn = sqlite3.connect(str(budb_path))
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


def load_po_county_lookup(pooling_path):
    if not pooling_path:
        return {}

    conn = sqlite3.connect(str(pooling_path))
    try:
        query = """
            SELECT
                [po_id] AS po_id,
                [target_county] AS county,
                [target_state] AS state
            FROM [pooling_order]
        """
        df = pd.read_sql_query(query, conn)

        lookup = {}
        for row in df.itertuples(index=False):
            po_id = "" if pd.isna(row.po_id) else str(row.po_id).strip()
            if not po_id:
                continue

            items = []
            add_county_state_pair(items, row.county, row.state)
            lookup[po_id] = items

        return lookup

    finally:
        conn.close()


def load_prod_county_lookup(mysql_config, prod_ids):
    prod_ids = sorted({str(x).strip() for x in prod_ids if str(x).strip()})

    if not prod_ids:
        return {}

    lookup = {}

    conn = pymysql.connect(
        host=mysql_config["host"],
        user=mysql_config["user"],
        password=mysql_config["password"],
        database=mysql_config["database"],
        port=mysql_config.get("port", 3306),
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
    )

    try:
        with conn.cursor() as cursor:
            placeholders = ", ".join(["%s"] * len(prod_ids))

            query = f"""
                SELECT
                    contact_id,
                    country AS county,
                    state
                FROM contact_targets
                WHERE contact_id IN ({placeholders})
            """

            cursor.execute(query, prod_ids)
            rows = cursor.fetchall()

        for row in rows:
            prod_id = "" if row.get("contact_id") is None else str(row.get("contact_id")).strip()
            if not prod_id:
                continue

            lookup.setdefault(prod_id, [])
            add_county_state_pair(lookup[prod_id], row.get("county"), row.get("state"))

        return lookup

    finally:
        conn.close()


def populate_combined_counties_from_all_sources(
    df,
    budb_county_lookup,
    po_county_lookup,
    # prod_county_lookup,
):
    combined_values = []

    for row in df.itertuples(index=False):
        items = []

        # 1. NCOP existing combined_county
        items.extend(parse_existing_combined_county(getattr(row, "combined_county", "")))

        # 2. BUDB matched counties
        for budb_id in split_pipe_ids(getattr(row, "budb_id", "")):
            items.extend(budb_county_lookup.get(budb_id, []))

        # 3. PO matched counties
        for po_id in split_pipe_ids(getattr(row, "po_id", "")):
            items.extend(po_county_lookup.get(po_id, []))

        # 4. PROD matched counties
        # for prod_id in split_pipe_ids(getattr(row, "prod_id", "")):
        #     items.extend(prod_county_lookup.get(prod_id, []))

        combined_values.append(unique_pipe_join(items))

    df = df.copy()
    df["combined_counties_from_all_sources"] = combined_values
    return df


class NCOPImporterApp(ctk.CTk):
    def __init__(self):
        super().__init__()

        # --- Signature UI colors (your theme) ---
        self.PANEL_BG = "#273946"
        self.APP_BG = "#fff6de"
        self.ACCENT = "#CB1F47"
        self.ACCENT_HOVER = "#ffab4c"
        self.TEXT_DARK = "#273946"

        self.title("NCOP: DB Import Manager [v1.0.3]")
        self.geometry("430x720")
        self.resizable(False, True)
        self.minsize(430, 650)

        ctk.set_appearance_mode("dark")
        ctk.set_default_color_theme("dark-blue")
        self.configure(fg_color=self.APP_BG)

        self.main_frame = ctk.CTkFrame(self, fg_color=self.APP_BG, corner_radius=12)
        self.main_frame.pack(fill="both", expand=True, padx=10, pady=10)

        self.title_label = ctk.CTkLabel(
            self.main_frame,
            text="NCOP DB Import Manager Tool",
            font=ctk.CTkFont(size=20, weight="bold"),
            text_color=self.TEXT_DARK
        )
        self.title_label.pack(pady=(12, 6))

        input_tab = self._create_locked_tab_section(title="I m p o r t", height=200)
        self._setup_import_tab(input_tab)

        self.progress = ctk.CTkProgressBar(
            self.main_frame,
            width=390,
            fg_color=self.PANEL_BG,
            progress_color=self.ACCENT
        )
        self.progress.set(0)
        self.progress.pack(pady=10)

        self.log_container = ctk.CTkFrame(self.main_frame, fg_color="transparent")
        self.log_container.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        self.log_container.grid_rowconfigure(1, weight=1)
        self.log_container.grid_columnconfigure(0, weight=1)

        ctk.CTkLabel(
            self.log_container,
            text="Activity Log",
            font=ctk.CTkFont(size=12, weight="bold"),
            text_color=self.TEXT_DARK
        ).grid(row=0, column=0, sticky="w", padx=10, pady=(0, 4))

        self.log_box = ctk.CTkTextbox(self.log_container, fg_color="#ffffff", text_color=self.TEXT_DARK)
        self.log_box.grid(row=1, column=0, sticky="nsew", padx=0, pady=0)
        self.log_box.configure(state="disabled")

        self.input_paths = []

                # github link (hihi)
        self.credit_label = ctk.CTkLabel(
        self,
        text="© dyoliya • GitHub",
        text_color="#484949",
        font=ctk.CTkFont(size=8, underline=False),
        cursor="hand2"
        )
        self.credit_label.place(relx=1.0, x=-10, y=1, anchor="ne") 
        self.credit_label.bind("<Button-1>", lambda e: self.open_url("https://github.com/dyoliya"))


    def _create_locked_tab_section(self, title: str, height: int):
        tab_font = ctk.CTkFont(size=12, weight="bold")
        tv = ctk.CTkTabview(self.main_frame, width=390, height=height)
        tv.configure(
            fg_color=self.PANEL_BG,
            segmented_button_fg_color=self.APP_BG,
            segmented_button_selected_color=self.PANEL_BG,
            segmented_button_selected_hover_color=self.PANEL_BG,
            segmented_button_unselected_color=self.PANEL_BG,
            text_color=self.ACCENT_HOVER,
            text_color_disabled=self.ACCENT_HOVER
        )
        tv.pack(fill="x", padx=10, pady=(10, 8), anchor="w")
        tv.configure(anchor="w")

        tab = tv.add(title)

        try:
            tv._segmented_button.grid_configure(sticky="w")
            btn = tv._segmented_button._buttons_dict[title]
            btn.configure(width=140, height=35)
            tv._segmented_button.configure(state="disabled", font=tab_font)
            for b in tv._segmented_button._buttons_dict.values():
                b.configure(state="disabled")
        except Exception:
            pass

        return tab

    def _setup_import_tab(self, tab):
        LABEL_W = 130

        # Row 1: Browse
        row1 = ctk.CTkFrame(tab, fg_color="transparent")
        row1.pack(fill="x", padx=10, pady=(10, 6), anchor="w")

        ctk.CTkLabel(
            row1,
            text="Select Files:",
            width=LABEL_W,
            anchor="w",
            text_color=self.APP_BG
        ).pack(side="left")

        self.file_label = ctk.CTkLabel(row1, text="(none)", anchor="w", text_color=self.APP_BG)
        self.file_label.pack(side="left", padx=(0, 8), fill="x", expand=True)

        self.pick_file_btn = ctk.CTkButton(
            row1,
            text="Browse",
            width=80,
            fg_color=self.ACCENT,
            hover_color=self.ACCENT_HOVER,
            command=self.pick_input_file
        )
        self.pick_file_btn.pack(side="right")

        # ✅ Selected files panel (dark + scroll)
        panel = ctk.CTkFrame(tab, fg_color="#1e2b34", corner_radius=10)
        # panel.pack(fill="both", expand=False, padx=10, pady=(6, 8))
        panel.pack(fill="x", expand=False, padx=10, pady=(6, 8))

        panel_header = ctk.CTkFrame(panel, fg_color="transparent")
        panel_header.pack(fill="x", padx=10, pady=(8, 4))

        ctk.CTkLabel(
            panel_header,
            text="Selected files",
            font=ctk.CTkFont(size=12, weight="bold"),
            text_color="#ffffff"
        ).pack(side="left")

        self.clear_btn = ctk.CTkButton(
            panel_header,
            text="Clear",
            width=70,
            fg_color=self.PANEL_BG,
            hover_color="#334957",
            command=self.clear_selected_files
        )
        self.clear_btn.pack(side="right")

        # scrollable list WRAPPER (controls the height)
        list_wrap = ctk.CTkFrame(panel, fg_color="#1e2b34", height=80, corner_radius=10)
        list_wrap.pack(fill="x", expand=False, padx=10, pady=(0, 10))
        list_wrap.pack_propagate(False)  # force the wrapper height

        self.files_list = ctk.CTkScrollableFrame(
            list_wrap,
            fg_color="#1e2b34",
            scrollbar_fg_color="#273946",
            scrollbar_button_color=self.ACCENT,
            scrollbar_button_hover_color=self.ACCENT_HOVER,
        )
        self.files_list.pack(fill="both", expand=True)

        # Import button
        self.import_btn = ctk.CTkButton(
            tab,
            text="Import & Sync to DB",
            fg_color=self.ACCENT,
            hover_color=self.ACCENT_HOVER,
            command=self.start_import
        )
        self.import_btn.pack(pady=(6, 12), padx=10)   # 12px space below

    def clear_selected_files(self):
        self.input_paths = []
        self.file_label.configure(text="(none)")
        self._refresh_files_list()
        self._log("[INFO] Cleared selected files.\n")

    def _refresh_files_list(self):
        # remove existing rows
        for child in self.files_list.winfo_children():
            child.destroy()

        if not self.input_paths:
            ctk.CTkLabel(
                self.files_list,
                text="No files selected",
                text_color="#cbd5e1"
            ).pack(anchor="w", pady=6)
            return

        for p in self.input_paths:
            row = ctk.CTkFrame(self.files_list, fg_color="transparent")
            row.pack(fill="x", pady=3)

            ctk.CTkLabel(
                row,
                text=os.path.basename(p),
                text_color="#ffffff",
                anchor="w"
            ).pack(side="left", fill="x", expand=True)

            ctk.CTkLabel(
                row,
                text=os.path.dirname(p),
                text_color="#94a3b8",
                anchor="e"
            ).pack(side="right")

    # ---------- log/progress helpers ----------
    def _log(self, text: str):
        def _append():
            self.log_box.configure(state="normal")
            self.log_box.insert("end", text + "\n")
            self.log_box.see("end")
            self.log_box.configure(state="disabled")
        self.after(0, _append)

    def _divider(self):
        self._log("- - - - - - - - - - - - - - - - - - - - - - - - - - -")

    def progress_callback(self, fraction, msg=None):
        self.progress.set(max(0.0, min(1.0, float(fraction))))
        if msg:
            self._log(msg)
        self.update_idletasks()

    def _ui_error(self, title: str, msg: str):
        self.after(0, lambda m=msg: messagebox.showerror(title, m))

    # ---------- UI actions ----------
    def pick_input_file(self):
        paths = filedialog.askopenfilenames(
            title="Select input file(s)",
            filetypes=[("CSV or Excel", "*.csv *.xlsx *.xlsm *.xls"), ("All files", "*.*")]
        )
        if not paths:
            return

        self.input_paths = list(paths)
        self.file_label.configure(text=f"{len(self.input_paths)} file(s) selected")
        self._refresh_files_list()  # ✅ update list UI

        self._log("[INFO] Selected inputs:\n  - " + "\n  - ".join(self.input_paths))
        self._log("\n")

    def start_import(self):
        self._divider()

        if not self.input_paths:
            messagebox.showwarning("Missing input", "Please select one or more CSV/XLSX files.")
            return

        db_path, copied_from, mode = get_daily_db_path("ncop")
        self._log(f"[DB] Using daily DB: {db_path}\n")

        if mode == "copied":
            self._log(f"[DB] Copied base DB: {copied_from}\n")
        elif mode == "existing":
            self._log("[DB] Today's DB already exists. Continuing sync..\n")
        else:  # "new"
            self._log("[DB] No base DB found. Starting a NEW DB for today.\n")

        table = "ncop"

        self.import_btn.configure(state="disabled")
        self.pick_file_btn.configure(state="disabled")
        self.progress_callback(0, "Starting import...\n")

        threading.Thread(
            target=self._import_worker,
            args=(db_path, self.input_paths, table),
            daemon=True
        ).start()

    def _import_worker(self, db_path, input_paths, table):

        try:
            conn = connect_sqlite(db_path)

            mysql_config = {
                "host": "communitymineralsproduction-instance-1.cwonazo2qluf.us-east-2.rds.amazonaws.com",
                "user": "admin",
                "password": "cmpass2024",
                "database": "prod_community_minerals",
                "port": 3306,
            }

            prod_lookup_maps, prod_meta = load_prod_lookup_maps(
                mysql_config=mysql_config,
                base_dir=get_app_base_dir(),
            )

            budb_lookup_maps, budb_meta = load_budb_lookup_maps(
                base_dir=get_app_base_dir()
            )

            po_lookup_maps, po_meta = load_pooling_lookup_maps(
                base_dir=get_app_base_dir()
            )
            try:
                total_files = len(input_paths)
                audit_chunks = []  # collects audit_df from each input file

                for idx, input_path in enumerate(input_paths, start=1):
                    self.progress_callback(0, f"[{idx}/{total_files}] Reading: {os.path.basename(input_path)}\n")
                    df_raw = read_input_file(input_path)  # ✅ no sheet_name
                    
                    if df_raw.empty:
                        self._log(f"[SKIP] {os.path.basename(input_path)} has no rows.\n")
                        continue

                    self.progress_callback(0, f"[{idx}/{total_files}] Cleaning/standardizing...\n")
                    df, original_cols, sanitized_cols, phone_cols, date_cols, audit_df = clean_and_prepare_df(df_raw)

                    self.progress_callback(0, f"[{idx}/{total_files}] Matching Pipedrive deal IDs...\n")

                    df, pd_meta, deal_match_audit_df = populate_ncop_deal_ids_in_df(
                        df,
                        base_dir=get_app_base_dir(),
                        progress_callback=lambda frac: self.progress_callback(frac),
                    )

                    matched_count = df["deal_id"].notna().sum()
                    self._log(
                        f"[PIPEDRIVE] Deal ID matching complete. "
                        f"Matched: {matched_count:,}/{len(df):,} rows | "
                        f"Source rows loaded: {pd_meta.get('rows_loaded', 0):,}\n"
                    )

                    self.progress_callback(0, f"[{idx}/{total_files}] Matching PROD IDs...\n")

                    df = populate_prod_ids_in_df(
                        df,
                        prod_lookup_maps,
                        progress_callback=lambda frac: self.progress_callback(frac),
                    )

                    self._log(
                        f"[PROD] PROD ID matching complete. "
                        f"Matched: {df['prod_id'].notna().sum():,}/{len(df):,} rows | "
                        f"Source rows loaded: {prod_meta.get('rows_loaded', 0):}\n"
                    )

                    self.progress_callback(0, f"[{idx}/{total_files}] Matching BUDB IDs...\n")

                    df = populate_budb_ids_in_df(
                        df,
                        budb_lookup_maps,
                        progress_callback=lambda frac: self.progress_callback(frac),
                    )

                    self._log(
                        f"[BUDB] BUDB ID matching complete. "
                        f"Matched: {df['budb_id'].notna().sum():,}/{len(df):,} rows | "
                        f"Source rows loaded: {budb_meta.get('rows_loaded', 0):}\n"
                    )

                    self.progress_callback(0, f"[{idx}/{total_files}] Matching PO IDs.\n")

                    df = populate_po_ids_in_df(
                        df,
                        po_lookup_maps,
                        progress_callback=lambda frac: self.progress_callback(frac),
                    )

                    self._log(
                        f"[PO] PO ID matching complete. "
                        f"Matched: {df['po_id'].notna().sum():,}/{len(df):,} rows | "
                        f"Source rows loaded: {po_meta.get('rows_loaded', 0):}\n"
                    )

                    self.progress_callback(0, f"[{idx}/{total_files}] Combining counties from all sources...\n")

                    budb_county_lookup = load_budb_county_lookup(budb_meta.get("budb_path"))
                    po_county_lookup = load_po_county_lookup(po_meta.get("pooling_path"))

                    all_prod_ids = []
                    for value in df.get("prod_id", pd.Series(dtype=str)):
                        all_prod_ids.extend(split_pipe_ids(value))

                    # prod_county_lookup = load_prod_county_lookup(mysql_config, all_prod_ids)

                    df = populate_combined_counties_from_all_sources(
                        df=df,
                        budb_county_lookup=budb_county_lookup,
                        po_county_lookup=po_county_lookup,
                        # prod_county_lookup=prod_county_lookup,
                    )

                    self._log(
                        f"[COUNTY] combined_counties_from_all_sources populated for "
                        f"{df['combined_counties_from_all_sources'].astype(str).str.strip().ne('').sum():,}/{len(df):,} rows.\n"
                    )
                    if "ncop_id" in df.columns:
                        self._log("[INFO] Input contains ncop_id. File values will be used when present.\n")
                    else:
                        self._log("[INFO] Input has no ncop_id. DB will auto-generate IDs.\n")

                    if audit_df is not None and not audit_df.empty:
                        audit_df = audit_df.copy()
                        audit_df.insert(0, "source_file", os.path.basename(input_path))
                        audit_chunks.append(audit_df)

                    if not table_exists(conn, table):
                        self._log("[SCHEMA] Table not found. Creating schema from this first file...\n")
                        ensure_table_and_columns(conn, table, list(df.columns))
                    else:
                        # Run migration first so system columns like serial_number are added/backfilled
                        ensure_table_and_columns(conn, table, list(df.columns))

                        expected = get_expected_columns(conn, table)
                        validate_strict_schema(
                            incoming_cols=list(df.columns),
                            expected_cols=expected,
                            original_cols=original_cols,
                            sanitized_cols=sanitized_cols,
                        )
                        self._log("[SCHEMA] OK: incoming columns match existing DB schema.\n")

                    self._log(f"[{idx}/{total_files}] {os.path.basename(input_path)}: {len(df):,} rows\n")
                    self._log(f"[MAP] Columns: {len(original_cols)} original → {len(sanitized_cols)} sanitized\n")

                    # if phone_cols:
                    #     self._log(f"[TYPE] Phone columns: {', '.join(phone_cols[:12])}" + ("..." if len(phone_cols) > 12 else ""))
                    # if date_cols:
                    #     self._log(f"[DATE] Date columns: {', '.join(date_cols[:12])}" + ("..." if len(date_cols) > 12 else ""))

                    self.progress_callback(0, f"[{idx}/{total_files}] Preparing table...\n")

                    existing_cols = get_existing_columns(conn, table)
                    if "ncop_id" not in existing_cols:
                        self._log("[WARN] Existing table has no ncop_id PK. Inserts will still work, but PK will not be auto-added.\n")
                    if "date_uploaded" not in existing_cols:
                        self._log("[WARN] date_uploaded was missing and should have been added.\n")

                    # self.progress_callback(0, f"[{idx}/{total_files}] Inserting rows...\n")
                    # sync_rows_by_ncop_id(
                    #     conn,
                    #     table,
                    #     df,
                    #     progress_callback=lambda frac: self.progress_callback(frac),
                    #     tz=CENTRAL_TZ
                    # )

                    self.progress_callback(0, f"[{idx}/{total_files}] Syncing rows...\n")

                    result = sync_rows_by_ncop_id(
                        conn,
                        table,
                        df,
                        progress_callback=lambda frac: self.progress_callback(frac),
                        tz=CENTRAL_TZ
                    )

                    self._log(
                        f"[SYNC] Inserted: {result['inserted']:,} | "
                        f"Updated: {result['updated']:,} | "
                        f"Unchanged: {result['unchanged']:,} | "
                        f"Deleted: {result['deleted']:,}\n"
                    )
                # --- Save audit report (if any) ---
                if audit_chunks:
                    audit_all = pd.concat(audit_chunks, ignore_index=True)

                    audit_dir = get_app_base_dir() / "audit"
                    audit_dir.mkdir(parents=True, exist_ok=True)

                    ts = datetime.now(CENTRAL_TZ).strftime("%Y-%m-%d_%H%M%S")
                    audit_path = audit_dir / f"ncop_null_audit_{ts}.xlsx"

                    with pd.ExcelWriter(audit_path, engine="openpyxl") as writer:
                        audit_all.to_excel(writer, sheet_name="null_conversions", index=False)

                    self._log(f"[AUDIT] Saved NULL conversion audit: {audit_path}\n")
                else:
                    self._log("[AUDIT] No suspicious NULL conversions detected.\n")

                self.progress_callback(0, "[REPORT] Generating NCOP associated deals report...\n")

                report_result = generate_ncop_associated_deals_report(
                    db_path=db_path,
                    base_dir=get_app_base_dir(),
                    table=table,
                )

                if deal_match_audit_df is not None and not deal_match_audit_df.empty:
                    existing_deals_dir = get_app_base_dir() / "existing_deals"
                    existing_deals_dir.mkdir(parents=True, exist_ok=True)

                    date_prefix = Path(db_path).name[:10]
                    audit_path = existing_deals_dir / f"{date_prefix}-ncop-deal-match-audit.xlsx"

                    with pd.ExcelWriter(audit_path, engine="openpyxl") as writer:
                        deal_match_audit_df.to_excel(writer, sheet_name="deal_match_audit", index=False)

                    self._log(f"[AUDIT] Saved deal match audit: {audit_path}\n")
                    
                self._log(
                    f"[REPORT] Saved associated deals report: {report_result['output_path']}\n"
                    f"[REPORT] Exported rows: {report_result['rows_exported']:,} | "
                    f"NCOP rows with deal_id: {report_result['unique_ncop_rows']:,}\n"
                )

                self._log(f"[DONE] Daily DB updated: {os.path.basename(db_path)} → table '{table}'\n")
                self.after(0, lambda: messagebox.showinfo("Success", f"Imported into:\n{db_path}\n\nTable: {table}"))

            finally:
                conn.close()

        except Exception as e:
            err_msg = str(e)
            self._log(f"[ERROR] {err_msg}\n")
            self._ui_error("Error", err_msg)

        finally:
            self.after(0, lambda: self.import_btn.configure(state="normal"))
            self.after(0, lambda: self.pick_file_btn.configure(state="normal"))
            self._divider()
            self.progress_callback(0, "Waiting for action...")


if __name__ == "__main__":
    app = NCOPImporterApp()
    app.mainloop()