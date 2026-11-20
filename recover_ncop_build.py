"""Inspect by default; --apply resumes only an unchanged source/known partial writeback."""
import argparse
import json
import sqlite3
from datetime import datetime
from pathlib import Path
from ncop_build_state import read_state, save_state, fail_build, finish_build
from ncop_run_lock import exclusive_run


def cells_from_snapshot(document, convert):
    metadata = document["metadata"]
    sheet = next(s for s in document["snapshot"]["sheets"]
                 if s["properties"]["sheetId"] == metadata["sheet_id"])
    raw = {}
    for block in sheet.get("data", []):
        for r, row in enumerate(block.get("rowData", []), block.get("startRow", 0)):
            for c, cell in enumerate(row.get("values", []), block.get("startColumn", 0)):
                raw[r, c] = cell
    headers = {
        c: convert(cell, is_header=True)
        for (r, c), cell in raw.items()
        if r == 0
    }

    result = {}
    for (r, c), cell in raw.items():
        value = convert(
            cell,
            headers.get(c, ""),
            is_header=(r == 0),
        )
        if value != "":
            result[(r, c)] = value

    return result


def validate_replay(original, current, rows, columns, convert, require_final=False):
    for key in ("spreadsheet_id", "sheet_id", "tab", "source_row_count"):
        if original["metadata"][key] != current["metadata"][key]:
            raise RuntimeError(f"Source layout changed ({key}); do not replay automatically.")
    before = cells_from_snapshot(original, convert)
    now = cells_from_snapshot(current, convert)
    expected = {}
    for row in rows:
        for field, column in columns.items():
            expected[int(row["sheet_row"]) - 1, column] = str(row[field])
    for pos in before.keys() | now.keys() | expected.keys():
        old, actual = before.get(pos, ""), now.get(pos, "")
        allowed = {expected[pos]} if require_final and pos in expected else {old}
        if pos in expected:
            allowed.add(expected[pos])
        if actual not in allowed:
            raise RuntimeError(f"Sheet changed at row {pos[0]+1}, column {pos[1]+1}; manual reconciliation required.")


def recover(base, apply=False):
    # Imported here so inspection of the module itself has no GUI/API effects.
    import app
    base = Path(base).resolve()
    state = read_state(base)
    if not state:
        raise RuntimeError("No initialized build state.")
    print("Build:", state["build_id"], "Status:", state["status"])
    if state["status"] in {"complete", "rolled_back"}:
        print("No interrupted build to recover.")
        return
    if state["status"] == "ready_for_log":
        print("Open app.py and click Update Action Log; do not rebuild.")
        return
    if state["status"] not in {"building", "recovery_required"}:
        raise RuntimeError("Unrecognized status; preserve all files.")
    if not apply:
        print("Close the app. Keep the Sheet unchanged. Run with --apply to validate and recover.")
        return
    if not state["writeback_started"]:
        print(fail_build(base))
        return
    pending = Path(state["pending_path"])
    candidates = [pending] if pending.exists() else list(pending.parent.glob("*_completed.json"))
    matches = []
    for path in candidates:
        document = json.loads(path.read_text(encoding="utf-8"))
        if document.get("build_id") == state["build_id"]:
            matches.append((path, document))
    if len(matches) != 1:
        raise RuntimeError("Expected one journal for this build; preserve the DB and all journals.")
    journal_path, journal = matches[0]
    database = Path(state["new_db"]).resolve()
    if Path(journal["db_path"]).resolve() != database:
        raise RuntimeError("Journal database does not match this build.")
    conn = sqlite3.connect(database.as_uri() + "?mode=ro", uri=True)
    try:
        for row in journal["rows"]:
            saved = conn.execute("SELECT ncop_id, serial_number, date_uploaded FROM ncop WHERE ncop_id=?",
                                 (row["ncop_id"],)).fetchone()
            wanted = (str(row["ncop_id"]), str(row["ncop_serial_number"]), str(row["date_added_to_db"]))
            if saved is None or tuple("" if v is None else str(v) for v in saved) != wanted:
                raise RuntimeError("Saved SQLite identity no longer matches the journal.")
    finally:
        conn.close()
    original = json.loads(Path(journal["snapshot_path"]).read_text(encoding="utf-8"))
    url = "https://docs.google.com/spreadsheets/d/" + journal["spreadsheet_id"]
    frame, metadata = app.read_ncop_source(url)
    current = json.loads(Path(metadata["snapshot_path"]).read_text(encoding="utf-8"))
    columns = app.get_google_writeback_columns([list(frame.columns)])
    validate_replay(original, current, journal["rows"], columns, app.google_cell_text,
                    require_final=journal_path != pending)
    version = metadata["google_modified_time"]
    try:
        if journal_path == pending:
            endpoint = f"https://sheets.googleapis.com/v4/spreadsheets/{journal['spreadsheet_id']}/values:batchUpdate"
            with app.google_sheets_session() as session:
                for payload in journal["payloads"]:
                    if app.get_google_sheet_version(url) != version:
                        raise RuntimeError("Sheet changed during recovery; stopped.")
                    app.google_api_request(session, "POST", endpoint, json=payload, timeout=(15, 90))
                    version = app.get_google_sheet_version(url)
            stamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
            pending.rename(pending.with_name(f"{journal['spreadsheet_id']}_{stamp}_completed.json"))
        finish_build(base)
    except Exception:
        state["status"] = "recovery_required"
        save_state(base, state)
        raise
    print("Writeback recovered. Open app.py and click Update Action Log.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    root = Path(__file__).resolve().parent
    with exclusive_run(root):
        recover(root, args.apply)
