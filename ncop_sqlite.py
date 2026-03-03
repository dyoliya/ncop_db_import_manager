import os
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


def ensure_table_and_columns(conn: sqlite3.Connection, table: str, df_cols: list[str]):
    """
    Creates table if missing with:
      - ncop_id INTEGER PRIMARY KEY AUTOINCREMENT
      - date_uploaded TEXT
      - df columns TEXT

    If table exists, adds any missing columns (date_uploaded + df cols).
    """
    cur = conn.cursor()

    cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table,)
    )
    exists = cur.fetchone() is not None

    if not exists:
        create_cols = [
            f"{sqlite_quote_ident('ncop_id')} INTEGER PRIMARY KEY AUTOINCREMENT",
            f"{sqlite_quote_ident('date_uploaded')} TEXT",
        ] + [f"{sqlite_quote_ident(c)} TEXT" for c in df_cols]

        cur.execute(f"CREATE TABLE {sqlite_quote_ident(table)} ({', '.join(create_cols)})")
        conn.commit()
        return

    existing = set(get_existing_columns(conn, table))
    required = {"ncop_id", "date_uploaded"} | set(df_cols)
    missing = [c for c in required if c not in existing]

    for c in missing:
        if c == "ncop_id":
            # can't easily add PK to existing table without rebuild; skip
            continue
        cur.execute(f"ALTER TABLE {sqlite_quote_ident(table)} ADD COLUMN {sqlite_quote_ident(c)} TEXT")

    conn.commit()


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
    Adds date_uploaded (Central time) per batch.
    Row-level progress: updates every 100 rows.
    """
    cur = conn.cursor()
    df_cols = list(df.columns)

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

            # row is already cleaned: None means NULL
            row_vals = [now_central] + [row[c] for c in df_cols]
            rows_to_insert.append(tuple(row_vals))

            if progress_callback and processed_rows % 100 == 0:
                progress_callback(processed_rows / total_rows)

        cur.executemany(sql, rows_to_insert)
        conn.commit()

    if progress_callback:
        progress_callback(1.0)