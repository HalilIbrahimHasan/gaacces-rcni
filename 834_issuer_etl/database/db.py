"""SQLite database manager."""

from __future__ import annotations

import sqlite3
from pathlib import Path

from config.config import settings

SCHEMA_PATH = Path(__file__).parent / "schema.sql"


class Database:
    def __init__(self, db_path: Path | None = None) -> None:
        self.db_path = db_path or settings.database_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row

    _MIGRATION_COLUMNS = [
        ("days_between_effective_and_end", "INTEGER"),
        ("days_between_transaction_and_end", "INTEGER"),
        ("reporting_month", "TEXT"),
        ("transaction_classification", "TEXT"),
        ("revenue_at_risk", "REAL"),
        ("withheld_user_fee", "REAL"),
        ("non_refundable_user_fee", "REAL"),
        ("refund_eligible_user_fee", "REAL"),
        ("enrollment_action_code", "TEXT"),
        ("enrollee_event_type_code", "TEXT"),
        ("enrollee_event_reason_code", "TEXT"),
        ("exchg_assigned_enrollee_id", "TEXT"),
        ("request_submit_timestamp", "TEXT"),
        ("issuer_subscriber_identifier", "TEXT"),
        ("issuer_indiv_identifier", "TEXT"),
        ("last_premium_paid_date", "TEXT"),
        ("qtyn", "TEXT"),
        ("qtyy", "TEXT"),
        ("qtyt", "TEXT"),
    ]

    def init_schema(self) -> None:
        sql = SCHEMA_PATH.read_text(encoding="utf-8")
        self.conn.executescript(sql)
        self._migrate_columns()
        self.conn.commit()

    def _migrate_columns(self) -> None:
        """Add new columns to existing databases without dropping data."""
        existing = {
            row[1]
            for row in self.conn.execute("PRAGMA table_info(stg_834_records)")
        }
        for col, col_type in self._MIGRATION_COLUMNS:
            if col not in existing:
                self.conn.execute(
                    f"ALTER TABLE stg_834_records ADD COLUMN {col} {col_type}"
                )

    def execute(self, sql: str, params: tuple | list = ()) -> sqlite3.Cursor:
        return self.conn.execute(sql, params)

    def executemany(self, sql: str, params: list) -> None:
        self.conn.executemany(sql, params)

    def commit(self) -> None:
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()

    def table_count(self, table: str, where: str = "", params: tuple = ()) -> int:
        sql = f"SELECT COUNT(*) FROM {table}"
        if where:
            sql += f" WHERE {where}"
        return self.execute(sql, params).fetchone()[0]
