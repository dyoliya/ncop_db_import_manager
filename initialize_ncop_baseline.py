"""Run once on a COPY of the latest verified NCOP DB with complete log history."""
import re
import sqlite3
import uuid
from pathlib import Path
from ncop_build_state import copy_database, save_state, read_state
from ncop_run_lock import exclusive_run


def initialize(base):
    base = Path(base).resolve()
    folder = base / "database"
    files = list(folder.glob("*.db"))
    if len(files) != 1 or not re.fullmatch(r"\d{4}-\d{2}-\d{2}-ncop\.db", files[0].name):
        raise RuntimeError("Place exactly one YYYY-MM-DD-ncop.db baseline directly in database/.")
    database = files[0]
    for file in (base / "config/build_state.json", folder / "build_state.json"):
        if file.exists():
            raise RuntimeError(f"Build tracking already exists: {file}. Do not reinitialize.")
    if list((base / "google_sheet_writeback").glob("*_pending.json")):
        raise RuntimeError("Unresolved Google writeback exists; do not initialize.")
    conn = sqlite3.connect(database.as_uri() + "?mode=ro", uri=True)
    try:
        tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if not {"ncop", "ncop_action_log"} <= tables:
            raise RuntimeError("Baseline must contain ncop and its ncop_action_log history.")
        if "ncop_build_state" in tables:
            raise RuntimeError("Database already has build tracking; restore its state instead.")
        if conn.execute("PRAGMA quick_check").fetchone()[0] != "ok":
            raise RuntimeError("SQLite quick_check failed.")
        columns = {r[1]: r for r in conn.execute('PRAGMA table_info("ncop")')}
        required = {"ncop_id", "serial_number", "date_uploaded", "prod_id", "po_id", "source_url"}
        if required - columns.keys():
            raise RuntimeError("Missing baseline columns: " + ", ".join(sorted(required - columns.keys())))
        sql = conn.execute("SELECT sql FROM sqlite_master WHERE name='ncop'").fetchone()[0]
        if columns["ncop_id"][5] != 1 or "AUTOINCREMENT" not in sql.upper():
            raise RuntimeError("ncop_id must be INTEGER PRIMARY KEY AUTOINCREMENT.")
        if not conn.execute("SELECT 1 FROM ncop LIMIT 1").fetchone():
            raise RuntimeError("NCOP baseline is empty.")
    finally:
        conn.close()
    run_id = "baseline-" + uuid.uuid4().hex
    backup = folder / "previous_versions" / run_id / database.name
    backup.parent.mkdir(parents=True)
    copy_database(database, backup)
    state = {
        "build_id": run_id, "status": "complete",
        "new_db": str(database), "original_db": str(database),
        "old_db": None, "pending_path": None, "writeback_started": False,
        "initialization": "Existing verified NCOP baseline with completed action-log history.",
    }
    save_state(base, state)
    if read_state(base) != state:
        raise RuntimeError("Initialized state could not be verified.")
    print(f"Baseline initialized: {database}")
    print(f"Pre-initialization snapshot: {backup}")


if __name__ == "__main__":
    root = Path(__file__).resolve().parent
    with exclusive_run(root):
        initialize(root)
