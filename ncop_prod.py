import re
import sqlite3
from pathlib import Path
from collections import defaultdict
from typing import Optional, Tuple, Set

import pandas as pd
import pymysql


# =========================================================
# NORMALIZATION
# =========================================================

owner_changer = {
    r"\bCO\b": "COMPANY",
    r"\bCORP\b": "CORPORATION",
    r"\bEST\b": "ESTATE",
    r"\bFAM\b": "FAMILY",
    r"\bFMLY\b": "FAMILY",
    r"\bINC\b": "INCORPORATED",
    r"\bIRR\b": "IRREVOCABLE",
    r"\bIRREV\b": "IRREVOCABLE",
    r"\bIRRV\b": "IRREVOCABLE",
    r"\bIRRVCABLE\b": "IRREVOCABLE",
    r"\bIRV\b": "IRREVOCABLE",
    r"\bLIV\b": "LIVING",
    r"\bLLC\b": "LIMITED LIABILITY COMPANY",
    r"\bLP\b": "LIMITED PARTNERSHIP",
    r"\bLTD\b": "LIMITED",
    r"\bLVG\b": "LIVING",
    r"\bREV\b": "REVOCABLE",
    r"\bREVC\b": "REVOCABLE",
    r"\bREVOC\b": "REVOCABLE",
    r"\bRLT\b": "REVOCABLE LIVING TRUST",
    r"\bTR\b": "TRUST",
    r"\bTRST\b": "TRUST",
    r"\bTRT\b": "TRUST",
    r"\bTRTEE\b": "TRUSTEE",
    r"\bTST\b": "TRUST",
    r"\bTSTE\b": "TRUSTEE",
    r"\bTSTEE\b": "TRUSTEE",
    r"\bTSTEES\b": "TRUSTEES",
    r"\bTTEE\b": "TRUSTEE",
}

address_replacements = {
    r"\bSTATE HWY\b": "HWY",
    r"\bNORTHEAST\b": "NE",
    r"\bNORTHWEST\b": "NW",
    r"\bSOUTHEAST\b": "SE",
    r"\bSOUTHWEST\b": "SW",
    r"\bAPARTMENT\b": "APT",
    r"\bAVENUE\b": "AVE",
    r"\bBOULEVARD\b": "BLVD",
    r"\bCIRCLE\b": "CR",
    r"\bCOURT\b": "CT",
    r"\bDRIVE\b": "DR",
    r"\bEAST\b": "E",
    r"\bHIGHWAY\b": "HWY",
    r"\bLANE\b": "LN",
    r"\bNORTH\b": "N",
    r"\bPARKWAY\b": "PKWY",
    r"\bROAD\b": "RD",
    r"\bSOUTH\b": "S",
    r"\bSTREET\b": "ST",
    r"\bSUITE\b": "STE",
    r"\bTRAIL\b": "TRL",
    r"\bWEST\b": "W",
    r"\bP\.?\s*O\.?\b": "PO",
    r"\bFREEWAY\b": "FWY",
}

OWNER_REPLACEMENTS = [
    (re.compile(pattern, flags=re.IGNORECASE), repl)
    for pattern, repl in owner_changer.items()
]
ADDRESS_REPLACEMENTS = [
    (re.compile(pattern, flags=re.IGNORECASE), repl)
    for pattern, repl in address_replacements.items()
]

_BASIC_CACHE = {}
_OWNER_CACHE = {}
_ADDRESS_CACHE = {}
_CITYSTATE_CACHE = {}
_NAME_NO_MIDDLE_CACHE = {}
_NAME_WITH_MIDDLE_CACHE = {}
_INPUT_NAME_NO_MIDDLE_CACHE = {}
_MATCH_KEY_CACHE = {}


def normalize_basic_text(value) -> str:
    if value is None:
        return ""

    raw = str(value).strip()
    if raw.lower() in {"", "nan", "none", "null"}:
        return ""

    cached = _BASIC_CACHE.get(raw)
    if cached is not None:
        return cached

    s = raw.upper()
    s = re.sub(r"[^A-Z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()

    _BASIC_CACHE[raw] = s
    return s


def apply_replacements(text: str, compiled_replacements) -> str:
    if not text:
        return ""

    for pattern, repl in compiled_replacements:
        text = pattern.sub(repl, text)

    text = re.sub(r"[^A-Z0-9\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def normalize_owner_text(value) -> str:
    if value is None:
        return ""

    raw = str(value).strip()
    if raw.lower() in {"", "nan", "none", "null"}:
        return ""

    cached = _OWNER_CACHE.get(raw)
    if cached is not None:
        return cached

    s = normalize_basic_text(raw)
    if not s:
        return ""

    s = apply_replacements(s, OWNER_REPLACEMENTS)
    _OWNER_CACHE[raw] = s
    return s


def normalize_address_text(value) -> str:
    if value is None:
        return ""

    raw = str(value).strip()
    if raw.lower() in {"", "nan", "none", "null"}:
        return ""

    cached = _ADDRESS_CACHE.get(raw)
    if cached is not None:
        return cached

    s = normalize_basic_text(raw)
    if not s:
        return ""

    s = apply_replacements(s, ADDRESS_REPLACEMENTS)
    _ADDRESS_CACHE[raw] = s
    return s


def normalize_city_state_text(value) -> str:
    if value is None:
        return ""

    raw = str(value).strip()
    if raw.lower() in {"", "nan", "none", "null"}:
        return ""

    cached = _CITYSTATE_CACHE.get(raw)
    if cached is not None:
        return cached

    s = normalize_basic_text(raw)
    _CITYSTATE_CACHE[raw] = s
    return s


def normalize_name_with_middle(first_name, middle_name, last_name) -> str:
    raw_key = (
        "" if first_name is None else str(first_name).strip(),
        "" if middle_name is None else str(middle_name).strip(),
        "" if last_name is None else str(last_name).strip(),
    )

    cached = _NAME_WITH_MIDDLE_CACHE.get(raw_key)
    if cached is not None:
        return cached

    parts = [
        normalize_basic_text(raw_key[0]),
        normalize_basic_text(raw_key[1]),
        normalize_basic_text(raw_key[2]),
    ]
    result = " ".join([p for p in parts if p]).strip()
    _NAME_WITH_MIDDLE_CACHE[raw_key] = result
    return result


def normalize_name_no_middle(first_name, last_name) -> str:
    raw_key = (
        "" if first_name is None else str(first_name).strip(),
        "" if last_name is None else str(last_name).strip(),
    )

    cached = _NAME_NO_MIDDLE_CACHE.get(raw_key)
    if cached is not None:
        return cached

    parts = [
        normalize_basic_text(raw_key[0]),
        normalize_basic_text(raw_key[1]),
    ]
    result = " ".join([p for p in parts if p]).strip()
    _NAME_NO_MIDDLE_CACHE[raw_key] = result
    return result


def normalize_input_first_last(first_name, last_name) -> str:
    raw_key = (
        "" if first_name is None else str(first_name).strip(),
        "" if last_name is None else str(last_name).strip(),
    )

    cached = _INPUT_NAME_NO_MIDDLE_CACHE.get(raw_key)
    if cached is not None:
        return cached

    parts = [
        normalize_basic_text(raw_key[0]),
        normalize_basic_text(raw_key[1]),
    ]
    result = " ".join([p for p in parts if p]).strip()
    _INPUT_NAME_NO_MIDDLE_CACHE[raw_key] = result
    return result


def normalize_input_owner_as_full_name(lessor_owner) -> str:
    return normalize_owner_text(lessor_owner)


def build_prod_match_key(owner_like_name, address, city, state) -> Optional[Tuple[str, str, str, str]]:
    raw_key = (
        "" if owner_like_name is None else str(owner_like_name).strip(),
        "" if address is None else str(address).strip(),
        "" if city is None else str(city).strip(),
        "" if state is None else str(state).strip(),
    )

    cached = _MATCH_KEY_CACHE.get(raw_key)
    if cached is not None:
        return cached

    owner_n = normalize_input_owner_as_full_name(raw_key[0])
    address_n = normalize_address_text(raw_key[1])
    city_n = normalize_city_state_text(raw_key[2])
    state_n = normalize_city_state_text(raw_key[3])

    if not address_n:
        _MATCH_KEY_CACHE[raw_key] = None
        return None

    result = (owner_n, address_n, city_n, state_n)
    _MATCH_KEY_CACHE[raw_key] = result
    return result


def serialize_key(key: Tuple[str, str, str, str]) -> str:
    return "||".join(key)


def deserialize_key(key_str: str) -> Tuple[str, str, str, str]:
    parts = key_str.split("||")
    if len(parts) != 4:
        raise ValueError(f"Invalid cached key format: {key_str}")
    return tuple(parts)


# =========================================================
# MYSQL LOAD
# =========================================================

def load_prod_dataframe(mysql_config: dict) -> pd.DataFrame:
    query = """
    SELECT
        c.id AS id,
        c.first_name AS first_name,
        c.middle_name AS middle_name,
        c.last_name AS last_name,
        csa.address AS address,
        csa.city AS city,
        csa.state AS state
    FROM
        contacts c
    INNER JOIN
        contact_skip_traced_addresses csa
    ON
        c.id = csa.contact_id
    WHERE c.deleted_at IS NULL
      AND csa.deleted_at IS NULL
    """

    conn = pymysql.connect(
        host=mysql_config["host"],
        user=mysql_config["user"],
        password=mysql_config["password"],
        database=mysql_config["database"],
        port=mysql_config.get("port", 3306),
        charset="utf8mb4",
        cursorclass=pymysql.cursors.Cursor,   # ✅ use normal cursor
    )

    try:
        with conn.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]

        df = pd.DataFrame(rows, columns=columns)

        print("[DEBUG] PROD rows loaded from MySQL:", len(df))
        print(df.head(5))

        return df

    finally:
        conn.close()


# =========================================================
# CACHE HELPERS
# =========================================================

def get_cache_path(base_dir: str | Path) -> Path:
    base_dir = Path(base_dir)
    cache_dir = base_dir / "cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir / "prod_lookup_cache.db"


def connect_cache_db(cache_path: str | Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(cache_path))
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


def ensure_cache_tables(conn: sqlite3.Connection):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS cache_meta (
            meta_key TEXT PRIMARY KEY,
            meta_value TEXT
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS lookup_cache (
            combo_name TEXT NOT NULL,
            match_key TEXT NOT NULL,
            prod_id TEXT NOT NULL
        )
    """)

    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_lookup_cache_combo_key
        ON lookup_cache (combo_name, match_key)
    """)

    conn.commit()


def read_cache_meta(conn: sqlite3.Connection) -> dict:
    cur = conn.cursor()
    cur.execute("SELECT meta_key, meta_value FROM cache_meta")
    rows = cur.fetchall()
    return {k: v for k, v in rows}


def write_cache_meta(conn: sqlite3.Connection, meta: dict):
    conn.execute("DELETE FROM cache_meta")
    conn.executemany(
        "INSERT INTO cache_meta (meta_key, meta_value) VALUES (?, ?)",
        [(k, str(v)) for k, v in meta.items()]
    )
    conn.commit()


def clear_lookup_cache(conn: sqlite3.Connection):
    conn.execute("DELETE FROM lookup_cache")
    conn.commit()


def save_lookup_maps_to_cache(conn: sqlite3.Connection, lookup_maps: dict, metadata: dict):
    clear_lookup_cache(conn)

    rows_to_insert = []
    for combo_name, combo_map in lookup_maps.items():
        for key_tuple, prod_ids in combo_map.items():
            key_str = serialize_key(key_tuple)
            for prod_id in prod_ids:
                rows_to_insert.append((combo_name, key_str, str(prod_id)))

    conn.executemany(
        "INSERT INTO lookup_cache (combo_name, match_key, prod_id) VALUES (?, ?, ?)",
        rows_to_insert
    )

    write_cache_meta(conn, metadata)
    conn.commit()


def load_lookup_maps_from_cache(conn: sqlite3.Connection) -> tuple[dict, dict]:
    combo_full_name = defaultdict(set)
    combo_no_middle = defaultdict(set)

    combo_map_lookup = {
        "combo_full_name": combo_full_name,
        "combo_no_middle": combo_no_middle,
    }

    cur = conn.cursor()
    cur.execute("""
        SELECT combo_name, match_key, prod_id
        FROM lookup_cache
    """)
    rows = cur.fetchall()

    for combo_name, match_key, prod_id in rows:
        target_map = combo_map_lookup.get(combo_name)
        if target_map is None:
            continue
        target_map[deserialize_key(match_key)].add(str(prod_id))

    metadata = read_cache_meta(conn)

    lookup_maps = {
        "combo_full_name": combo_full_name,
        "combo_no_middle": combo_no_middle,
    }

    return lookup_maps, metadata


# =========================================================
# LOOKUP MAP BUILD
# =========================================================

def build_lookup_maps_from_prod_df(prod_df: pd.DataFrame) -> dict:
    combo_full_name = defaultdict(set)
    combo_no_middle = defaultdict(set)

    for row in prod_df.itertuples(index=False):
        prod_id = "" if pd.isna(row.id) else str(row.id).strip()
        if not prod_id:
            continue

        full_name = normalize_name_with_middle(row.first_name, row.middle_name, row.last_name)
        key1 = build_prod_match_key(full_name, row.address, row.city, row.state)
        if key1:
            combo_full_name[key1].add(prod_id)

        no_middle = normalize_name_no_middle(row.first_name, row.last_name)
        key2 = build_prod_match_key(no_middle, row.address, row.city, row.state)
        if key2:
            combo_no_middle[key2].add(prod_id)

    return {
        "combo_full_name": combo_full_name,
        "combo_no_middle": combo_no_middle,
    }


def load_prod_lookup_maps(mysql_config: dict, base_dir: str | Path, force_rebuild: bool = False):
    """
    PROD cache strategy:
    - if cache exists and force_rebuild=False, load from local cache DB
    - otherwise query MySQL, rebuild lookup maps, and save cache
    """
    base_dir = Path(base_dir)
    cache_path = get_cache_path(base_dir)
    cache_conn = connect_cache_db(cache_path)

    try:
        ensure_cache_tables(cache_conn)

        meta = read_cache_meta(cache_conn)
        has_cached_rows = False
        cur = cache_conn.cursor()
        cur.execute("SELECT COUNT(*) FROM lookup_cache")
        cache_row_count = cur.fetchone()[0]
        has_cached_rows = cache_row_count > 0

        if has_cached_rows and not force_rebuild:
            lookup_maps, metadata = load_lookup_maps_from_cache(cache_conn)
            metadata["cache_path"] = str(cache_path)
            metadata["cache_mode"] = "loaded_from_cache"
            return lookup_maps, metadata

        prod_df = load_prod_dataframe(mysql_config)
        lookup_maps = build_lookup_maps_from_prod_df(prod_df)

        metadata = {
            "rows_loaded": len(prod_df),
            "source": "mysql",
            "cache_mode": "rebuilt_from_mysql",
            "cache_path": str(cache_path),
        }

        save_lookup_maps_to_cache(cache_conn, lookup_maps, metadata)
        return lookup_maps, metadata

    finally:
        cache_conn.close()


# =========================================================
# MATCHING
# =========================================================

def match_prod_ids_for_ncop_row(row, lookup_maps: dict) -> Optional[str]:
    ncop_combos = [
        ("owner_name", "address", "city", "state"),
        ("owner_name", "address2", "city2", "state2"),
        ("owner_name", "add_address1", "add_address1_city", "add_address1_state"),
    ]

    all_matches: Set[str] = set()

    for owner_col, address_col, city_col, state_col in ncop_combos:
        key = build_prod_match_key(
            getattr(row, owner_col, ""),
            getattr(row, address_col, ""),
            getattr(row, city_col, ""),
            getattr(row, state_col, ""),
        )

        if not key:
            continue

        for combo_name in (
            "combo_full_name",
            "combo_no_middle",
        ):
            combo_map = lookup_maps.get(combo_name, {})
            all_matches.update(combo_map.get(key, set()))

        if all_matches:
            break

    if not all_matches:
        return None

    def sort_key(x: str):
        return (0, int(x)) if x.isdigit() else (1, x)

    return " | ".join(sorted(all_matches, key=sort_key))


def populate_prod_ids_in_df(df: pd.DataFrame, lookup_maps: dict, progress_callback=None) -> pd.DataFrame:
    df = df.copy()
    total = len(df)
    prod_ids = []

    for idx, row in enumerate(df.itertuples(index=False), start=1):
        prod_ids.append(match_prod_ids_for_ncop_row(row, lookup_maps))
        if progress_callback and total:
            progress_callback(idx / total)

    df["prod_id"] = prod_ids
    return df