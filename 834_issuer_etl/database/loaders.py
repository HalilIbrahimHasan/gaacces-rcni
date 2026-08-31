"""Load file inventory and staging records into SQLite."""

from __future__ import annotations

from datetime import datetime, timezone

from connectors.base_connector import SourceFile
from database.db import Database
from utils.hashing import sha256_file
from utils.logger import get_logger

logger = get_logger(__name__)

STG_COLUMNS = [
    "file_id", "issuer", "year", "month", "policy_id", "member_id", "subscriber_id",
    "member_first_name", "member_last_name", "relationship", "subscriber_flag",
    "action_code", "action_code_description", "maintenance_type_code",
    "additional_maint_reason_code", "coverage_status", "benefit_effective_date",
    "benefit_end_date", "member_maint_effective_date", "total_premium_amount",
    "individual_responsibility_amount", "aptc_amount", "user_fee_amount",
    "insurance_type_code", "health_coverage_policy_no", "household_or_employee_case_id",
    "rating_area", "source_exchg_id",
    "enrollment_action_code", "enrollee_event_type_code", "enrollee_event_reason_code",
    "exchg_assigned_enrollee_id", "request_submit_timestamp",
    "issuer_subscriber_identifier", "issuer_indiv_identifier", "last_premium_paid_date",
    "qtyn", "qtyy", "qtyt",
    "raw_xml_path", "raw_payload", "created_at",
]


def file_hash(path) -> str:
    return sha256_file(path)


class DataLoader:
    def __init__(self, db: Database) -> None:
        self.db = db

    def register_file(self, source: SourceFile) -> tuple[int, bool]:
        fhash = file_hash(source.file_path)
        existing = self.db.execute(
            "SELECT file_id FROM raw_file_inventory WHERE file_hash = ?",
            (fhash,),
        ).fetchone()
        if existing:
            logger.warning("Duplicate file skipped: %s", source.file_name)
            return existing["file_id"], True

        cur = self.db.execute(
            """INSERT INTO raw_file_inventory
               (issuer, year, month, file_name, file_path, file_hash, file_size,
                file_type, source_type, processed_status, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?)""",
            (
                source.issuer, source.year, source.month, source.file_name,
                str(source.file_path), fhash, source.file_size,
                source.file_path.suffix.lstrip("."), source.source_type,
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        self.db.commit()
        return cur.lastrowid, False

    def load_records(self, file_id: int, records: list[dict]) -> int:
        if not records:
            return 0
        placeholders = ", ".join("?" * len(STG_COLUMNS))
        sql = f"INSERT INTO stg_834_records ({', '.join(STG_COLUMNS)}) VALUES ({placeholders})"
        rows = [tuple(r.get(c) for c in STG_COLUMNS) for r in records]
        self.db.executemany(sql, rows)
        self.db.commit()
        logger.info("Inserted %d staging record(s) for file_id=%d", len(rows), file_id)
        return len(rows)

    def mark_file_status(self, file_id: int, status: str, error: str | None = None) -> None:
        self.db.execute(
            "UPDATE raw_file_inventory SET processed_status=?, error_message=? WHERE file_id=?",
            (status, error, file_id),
        )
        self.db.commit()

    def log_parse_error(self, source: SourceFile, error: str) -> None:
        self.db.execute(
            """INSERT INTO parse_errors (issuer, year, month, file_name, file_path, error_message)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (
                source.issuer, source.year, source.month,
                source.file_name, str(source.file_path), error,
            ),
        )
        self.db.commit()
