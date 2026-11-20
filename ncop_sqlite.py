import os
import pandas as pd
import re
import sqlite3
from datetime import datetime
from zoneinfo import ZoneInfo

CENTRAL_TZ = ZoneInfo("America/Chicago")


def sqlite_quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def connect_sqlite(db_path: str) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
    return sqlite3.connect(db_path)


def get_existing_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({sqlite_quote_ident(table)})")
    rows = cur.fetchall()
    return [r[1] for r in rows]


def rebuild_table_with_serial_after_date_uploaded(conn: sqlite3.Connection, table: str):
    cur = conn.cursor()

    cols = get_existing_columns(conn, table)

    # Only rebuild if serial_number exists and is not already after date_uploaded
    if "date_uploaded" not in cols or "serial_number" not in cols:
        return

    date_idx = cols.index("date_uploaded")
    serial_idx = cols.index("serial_number")

    if serial_idx == date_idx + 1:
        return  # already in correct position

    new_order = []
    for c in cols:
        if c == "serial_number":
            continue
        new_order.append(c)
        if c == "date_uploaded":
            new_order.append("serial_number")

    temp_table = f"{table}_rebuild_tmp"

    cur.execute(f"DROP TABLE IF EXISTS {sqlite_quote_ident(temp_table)}")

    create_cols = []
    for c in new_order:
        if c == "ncop_id":
            create_cols.append(f"{sqlite_quote_ident(c)} INTEGER PRIMARY KEY AUTOINCREMENT")
        else:
            create_cols.append(f"{sqlite_quote_ident(c)} TEXT")

    cur.execute(
        f"CREATE TABLE {sqlite_quote_ident(temp_table)} ({', '.join(create_cols)})"
    )

    quoted_cols = ", ".join(sqlite_quote_ident(c) for c in new_order)

    cur.execute(
        f"""
        INSERT INTO {sqlite_quote_ident(temp_table)} ({quoted_cols})
        SELECT {quoted_cols}
        FROM {sqlite_quote_ident(table)}
        """
    )

    cur.execute(f"DROP TABLE {sqlite_quote_ident(table)}")
    cur.execute(f"ALTER TABLE {sqlite_quote_ident(temp_table)} RENAME TO {sqlite_quote_ident(table)}")

    conn.commit()


def ensure_table_and_columns(conn: sqlite3.Connection, table: str, df_cols: list[str]):
    cur = conn.cursor()

    cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table,)
    )
    exists = cur.fetchone() is not None

    if not exists:
        user_cols = [
            c for c in df_cols
            if c not in {"ncop_id", "date_uploaded", "serial_number"}
        ]

        create_cols = [
            f"{sqlite_quote_ident('ncop_id')} INTEGER PRIMARY KEY AUTOINCREMENT",
            f"{sqlite_quote_ident('date_uploaded')} TEXT",
            f"{sqlite_quote_ident('serial_number')} TEXT",
        ] + [f"{sqlite_quote_ident(c)} TEXT" for c in user_cols]

        cur.execute(f"CREATE TABLE {sqlite_quote_ident(table)} ({', '.join(create_cols)})")
        conn.commit()
        return

    existing = set(get_existing_columns(conn, table))

    unwanted_column = "rangeid1873955911"
    if unwanted_column in existing:
        cur.execute(
            f"ALTER TABLE {sqlite_quote_ident(table)} "
            f"DROP COLUMN {sqlite_quote_ident(unwanted_column)}"
        )
        existing.remove(unwanted_column)

    required = {"ncop_id", "date_uploaded", "serial_number"} | set(df_cols)
    missing = [c for c in required if c not in existing]

    for c in missing:
        if c == "ncop_id":
            continue
        cur.execute(f"ALTER TABLE {sqlite_quote_ident(table)} ADD COLUMN {sqlite_quote_ident(c)} TEXT")

    conn.commit()

    # Backfill serial_number for old rows
    existing_after = set(get_existing_columns(conn, table))
    if "serial_number" in existing_after:
        cur.execute(
            f"""
            UPDATE {sqlite_quote_ident(table)}
            SET serial_number = 'NCOP-' || ncop_id
            WHERE (serial_number IS NULL OR TRIM(serial_number) = '')
              AND ncop_id IS NOT NULL
            """
        )
        conn.commit()
    
    # Preserve the existing table layout, indexes, and AUTOINCREMENT history.

def clean_sql_value(x):
    if x is None:
        return None

    # catches pandas NaN / NaT
    try:
        if pd.isna(x):
            return None
    except Exception:
        pass

    s = str(x).strip()

    if s == "" or s.casefold() in {"nan", "nat", "none", "null"}:
        return None

    return s

def sync_rows_by_ncop_id(
    conn: sqlite3.Connection,
    table: str,
    df,
    progress_callback=None,
    tz: ZoneInfo = CENTRAL_TZ,
    commit: bool = True,
):
    """
    Sync Excel data against existing DB using ncop_id.

    Rules:
    - numeric ncop_id found in DB -> update only changed fields
    - numeric ncop_id not found in DB -> insert using that ncop_id
    - '*0000' or blank -> assign new ncop_id and insert
    - DB ncop_id missing from Excel -> delete
    - date_uploaded is preserved for existing rows
    - date_uploaded is only set for new rows
    """
    cur = conn.cursor()
    df = df.copy()

    if "ncop_id" not in df.columns:
        raise ValueError("Input file must contain ncop_id for sync mode.")

    if df.empty:
        raise ValueError("Input has no data rows; synchronization stopped.")

    def normalize_ncop_id(x):
        if pd.isna(x):
            return "*0000"

        s = str(x).strip()
        if s.casefold() in {"", "nan", "none", "null", "nat", "*0000"}:
            return "*0000"

        if re.fullmatch(r"[0-9]+(?:\.0+)?", s):
            return str(int(s.split(".")[0]))

        return s

    df["ncop_id"] = df["ncop_id"].apply(normalize_ncop_id)

    invalid_mask = df["ncop_id"].notna() & ~df["ncop_id"].astype(str).str.fullmatch(r"\d+|\*0000")

    if invalid_mask.any():
        bad_rows = df.loc[invalid_mask, ["ncop_id"]].copy()
        bad_rows.insert(0, "excel_row", bad_rows.index + 2)
        print(bad_rows.head(50).to_string(index=False))
        raise ValueError("Invalid ncop_id values found. Allowed values are whole numbers, blanks, or '*0000'.")

    # Check duplicate numeric ncop_id only
    numeric_mask = df["ncop_id"].astype(str).str.fullmatch(r"\d+")
    duplicate_mask = df.loc[numeric_mask, "ncop_id"].duplicated(keep=False)

    if duplicate_mask.any():
        dup_rows = df.loc[numeric_mask].loc[duplicate_mask, ["ncop_id"]].copy()
        dup_rows.insert(0, "excel_row", dup_rows.index + 2)
        print(dup_rows.head(50).to_string(index=False))
        raise ValueError("Duplicate numeric ncop_id values found in the input file.")

    existing_cols = get_existing_columns(conn, table)

    protected_columns = {"prod_id", "po_id", "source_url"}
    preserved_insert_columns = [
        c for c in ("prod_id", "po_id", "source_url")
        if c in existing_cols
    ]
    compare_cols = [
        c for c in df.columns
        if c in existing_cols
        and c not in {"ncop_id", "date_uploaded", "serial_number"}
        and c not in protected_columns
    ]


    # Load existing DB rows
    select_cols = ["ncop_id", "date_uploaded"] + compare_cols

    cur.execute(
        f"""
        SELECT {', '.join(map(sqlite_quote_ident, select_cols))}
        FROM {sqlite_quote_ident(table)}
        """
    )

    existing_rows = cur.fetchall()
    existing_map = {
        str(row[0]): dict(zip(select_cols, row))
        for row in existing_rows
    }

    excel_existing_ids = set(
        df.loc[df["ncop_id"].astype(str).str.fullmatch(r"\d+"), "ncop_id"].astype(str)
    )

    db_existing_ids = set(existing_map.keys())

    # Delete rows from DB that are no longer in Excel
    ids_to_delete = sorted(db_existing_ids - excel_existing_ids, key=int)

    for ncop_id in ids_to_delete:
        cur.execute(
            f"DELETE FROM {sqlite_quote_ident(table)} WHERE ncop_id = ?",
            (ncop_id,)
        )

    # Respect IDs present before this run deleted any rows.
    current_max_id = max((int(v) for v in db_existing_ids), default=0)

    # Also respect IDs allocated in previous runs, even if later deleted.
    has_sequence = cur.execute(
        "SELECT 1 FROM sqlite_master WHERE name = 'sqlite_sequence'"
    ).fetchone()
    if has_sequence:
        sequence_row = cur.execute(
            "SELECT seq FROM sqlite_sequence WHERE name = ?",
            (table,),
        ).fetchone()
        if sequence_row:
            current_max_id = max(current_max_id, int(sequence_row[0] or 0))

    max_file_id = max(
        [int(v) for v in excel_existing_ids],
        default=0
    )

    next_id = max(current_max_id, max_file_id) + 1

    inserted = 0
    updated = 0
    unchanged = 0
    deleted = len(ids_to_delete)

    total_rows = len(df)
    writeback_rows = []

    for i, (row_index, row) in enumerate(df.iterrows(), start=1):
        ncop_id = str(row["ncop_id"]).strip()

        is_new_marker = ncop_id == "*0000"

        if is_new_marker:
            ncop_id = str(next_id)
            next_id += 1

        row_values = {
            c: clean_sql_value(row[c])
            for c in compare_cols
        }

        # INSERT: new marker or numeric ID not currently in DB
        if is_new_marker or ncop_id not in existing_map:
            now_central = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")

            serial_number = f"NCOP-{ncop_id}"

            insert_cols = (
                ["ncop_id", "date_uploaded", "serial_number"]
                + compare_cols
                + preserved_insert_columns
            )

            insert_vals = [
                ncop_id,
                now_central,
                serial_number,
            ] + [row_values[c] for c in compare_cols] + [None] * len(preserved_insert_columns)

            sql = (
                f"INSERT INTO {sqlite_quote_ident(table)} "
                f"({', '.join(map(sqlite_quote_ident, insert_cols))}) "
                f"VALUES ({', '.join(['?'] * len(insert_cols))})"
            )

            cur.execute(sql, insert_vals)
            inserted += 1

        else:
            existing = existing_map[ncop_id]

            changed_cols = []

            for c in compare_cols:
                old_val = clean_sql_value(existing.get(c))
                new_val = row_values[c]

                if old_val != new_val:
                    changed_cols.append(c)

            if changed_cols:
                set_clause = ", ".join(
                    f"{sqlite_quote_ident(c)} = ?"
                    for c in changed_cols
                )

                update_vals = [row_values[c] for c in changed_cols] + [ncop_id]

                sql = (
                    f"UPDATE {sqlite_quote_ident(table)} "
                    f"SET {set_clause} "
                    f"WHERE ncop_id = ?"
                )

                cur.execute(sql, update_vals)
                updated += 1
            else:
                unchanged += 1

        saved = cur.execute(
            f"SELECT ncop_id, serial_number, date_uploaded "
            f"FROM {sqlite_quote_ident(table)} WHERE ncop_id = ?",
            (ncop_id,),
        ).fetchone()

        writeback_rows.append({
            "sheet_row": int(row_index) + 2,
            "ncop_id": int(saved[0]),
            "ncop_serial_number": saved[1] or "",
            "date_added_to_db": saved[2] or "",
        })

        if progress_callback and i % 100 == 0:
            progress_callback(i / total_rows)

    if commit:
        conn.commit()

    if progress_callback:
        progress_callback(1.0)

    return {
        "inserted": inserted,
        "updated": updated,
        "unchanged": unchanged,
        "deleted": deleted,
        "writeback_rows": writeback_rows,
    }

def insert_rows(
    conn: sqlite3.Connection,
    table: str,
    df,
    progress_callback=None,
    batch_size: int = 1000,
    tz: ZoneInfo = CENTRAL_TZ,
):
    """
    Inserts df rows into SQLite table (append mode).

    Behavior for ncop_id:
      - numeric ncop_id -> use as provided
      - '*0000' or blank -> assign next sequential ncop_id
      - no ncop_id column -> exclude it from INSERT so SQLite auto-generates it

    Adds date_uploaded (Central time) per batch.
    Row-level progress: updates every 100 rows.
    """
    cur = conn.cursor()
    df = df.copy()

    # Get current max ncop_id from DB
    cur.execute(f"SELECT MAX(ncop_id) FROM {sqlite_quote_ident(table)}")
    row = cur.fetchone()
    current_max_id = int(row[0]) if row and row[0] is not None else 0

    if "ncop_id" in df.columns:

        def normalize_ncop_id(x):
            if x is None:
                return None

            s = str(x).strip()

            # special marker meaning "new record"
            if s == "*0000":
                return "*0000"

            # blank → treat like new
            if s == "":
                return "*0000"

            # valid integer
            if re.fullmatch(r"\d+", s):
                return s

            # Excel float artifact
            if re.fullmatch(r"\d+\.0", s):
                return s[:-2]

            return s

        df["ncop_id"] = df["ncop_id"].apply(normalize_ncop_id)

        # Find highest numeric ncop_id already present in this file
        numeric_ids_in_file = [
            int(v) for v in df["ncop_id"]
            if v is not None and re.fullmatch(r"\d+", str(v))
        ]

        max_file_id = max(numeric_ids_in_file, default=0)

        # Start new IDs after the highest value from either DB or file
        next_id = max(current_max_id, max_file_id) + 1

    else:
        next_id = current_max_id + 1

    # Assign sequential IDs for "*0000"
    if "ncop_id" in df.columns:
        for idx, val in df["ncop_id"].items():
            if val == "*0000":
                df.at[idx, "ncop_id"] = str(next_id)
                next_id += 1

    # Validate remaining ncop_id values
    if "ncop_id" in df.columns:
        invalid_mask = df["ncop_id"].notna() & ~df["ncop_id"].astype(str).str.fullmatch(r"\d+")

        if invalid_mask.any():
            bad_rows = df.loc[invalid_mask, ["ncop_id"]].copy()
            bad_rows.insert(0, "excel_row", bad_rows.index + 2)

            print("\n[DEBUG] Invalid ncop_id rows:")
            print(bad_rows.head(20).to_string(index=False))

            raise ValueError(
                "Invalid ncop_id values found. Allowed values are whole numbers, blanks, or '*0000'."
            )

    # Check for duplicate ncop_id values after assignment
    if "ncop_id" in df.columns:
        duplicate_mask = df["ncop_id"].duplicated(keep=False)

        if duplicate_mask.any():
            dup_rows = df.loc[duplicate_mask, ["ncop_id"]].copy()
            dup_rows.insert(0, "excel_row", dup_rows.index + 2)

            print("\n[DEBUG] Duplicate ncop_id rows:")
            print(dup_rows.head(50).to_string(index=False))

            raise ValueError(
                "Duplicate ncop_id values found in the input after assigning '*0000' rows."
            )

    # Decide whether to include ncop_id in the INSERT
    use_input_ncop_id = (
        "ncop_id" in df.columns and df["ncop_id"].notna().any()
    )

    if use_input_ncop_id:
        df_cols = list(df.columns)
    else:
        df_cols = [c for c in df.columns if c != "ncop_id"]

    insert_cols = ["date_uploaded"] + df_cols
    placeholders = ",".join(["?"] * len(insert_cols))

    sql = (
        f"INSERT INTO {sqlite_quote_ident(table)} "
        f"({', '.join(map(sqlite_quote_ident, insert_cols))}) "
        f"VALUES ({placeholders})"
    )

    total_rows = len(df)
    processed_rows = 0

    for start in range(0, total_rows, batch_size):
        end = min(start + batch_size, total_rows)
        batch = df.iloc[start:end]

        now_central = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")

        rows_to_insert = []
        for _, row in batch.iterrows():
            processed_rows += 1

            row_vals = [now_central] + [row[c] for c in df_cols]
            rows_to_insert.append(tuple(row_vals))

            if progress_callback and processed_rows % 100 == 0:
                progress_callback(processed_rows / total_rows)

        cur.executemany(sql, rows_to_insert)
        conn.commit()

    if progress_callback:
        progress_callback(1.0)