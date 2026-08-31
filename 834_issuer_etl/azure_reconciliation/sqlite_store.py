"""SQLite persistence for Azure vs XML reconciliation outputs."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd

from azure_reconciliation.safe_export import ExportErrors, safe_write_sqlite
from utils.logger import get_logger

logger = get_logger(__name__)

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS xml_snapshot (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    issuer TEXT, enrollment_id TEXT, enrollee_id TEXT, insurance_type TEXT,
    canonical_status TEXT, benefit_effective_date TEXT, benefit_end_date TEXT,
    member_maint_effective_date TEXT, coverage_year TEXT, snapshot_month TEXT,
    event_count INTEGER, last_event_year TEXT, last_event_month TEXT,
    source_files TEXT, created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS azure_snapshot (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    issuer TEXT, enrollment_id TEXT, enrollee_id TEXT, insurance_type TEXT,
    canonical_status TEXT, benefit_effective_date TEXT, benefit_end_date TEXT,
    coverage_year TEXT, snapshot_month TEXT, raw_json TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS comparison_detail (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    partition TEXT, match_type TEXT, status_match INTEGER,
    join_key TEXT, xml_status TEXT, azure_status TEXT,
    detail_json TEXT, created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS comparison_summary (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    partition TEXT, total_keys INTEGER, matched_keys INTEGER,
    status_differences INTEGER, xml_not_in_azure INTEGER,
    azure_not_in_xml INTEGER, match_rate_pct REAL,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS issuer_month_summary (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    partition TEXT, total_keys INTEGER, matched_keys INTEGER,
    status_differences INTEGER, xml_not_in_azure INTEGER,
    azure_not_in_xml INTEGER, created_at TEXT DEFAULT (datetime('now'))
);
"""


class ReconciliationStore:
    def __init__(self, db_path: Path, export_errors: ExportErrors | None = None) -> None:
        self.db_path = db_path
        self.export_errors = export_errors or ExportErrors()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init()

    def _conn(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_path)

    def _init(self) -> None:
        with self._conn() as conn:
            conn.executescript(SCHEMA_SQL)

    def replace_table(self, table: str, df: pd.DataFrame) -> int:
        if df.empty:
            logger.warning("Skipping empty write to %s", table)
            return 0
        with self._conn() as conn:
            ok = safe_write_sqlite(
                conn, table, df, if_exists="replace", export_errors=self.export_errors,
            )
        if ok:
            logger.info("Wrote %d row(s) to %s", len(df), table)
            return len(df)
        return 0

    def append_table(self, table: str, df: pd.DataFrame) -> int:
        if df.empty:
            return 0
        with self._conn() as conn:
            ok = safe_write_sqlite(
                conn, table, df, if_exists="append", export_errors=self.export_errors,
            )
        return len(df) if ok else 0
