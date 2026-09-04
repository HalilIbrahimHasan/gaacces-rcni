"""Azure SQL writer for RCNI raw tables.

Reuses SQLAlchemy + pyodbc fast_executemany via inbound_automation.azure_common.
Does not connect on import. Does not execute DDL.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine

from rcni.raw_schema import RAW_INSERT_COLUMNS, STAGE_INSERT_COLUMNS, SQL_SOURCE_COLUMNS
from rcni.raw_store import quality_natural_key

SCHEMA = "dbo"

_STAGE_COL_SQL = ", ".join(f"[{c}]" for c in STAGE_INSERT_COLUMNS)
_STAGE_VAL_SQL = ", ".join(f":{c}" for c in STAGE_INSERT_COLUMNS)
STAGE_INSERT_SQL = text(
    f"INSERT INTO [{SCHEMA}].[rcni_stage] ({_STAGE_COL_SQL}) VALUES ({_STAGE_VAL_SQL})"
)

_QUALITY_COLUMNS = (
    "load_run_id",
    "source_file",
    "source_path",
    "file_hash",
    "issuer_id",
    "coverage_year",
    "row_number_in_file",
    "physical_line_number",
    "column_name",
    "invalid_value",
    "issue_code",
    "issue_message",
    "expected_column_count",
    "observed_column_count",
    "raw_record",
)
_QUALITY_COL_SQL = ", ".join(f"[{c}]" for c in _QUALITY_COLUMNS)
_QUALITY_VAL_SQL = ", ".join(f":{c}" for c in _QUALITY_COLUMNS)
QUALITY_INSERT_SQL = text(
    f"INSERT INTO [{SCHEMA}].[rcni_data_quality_issue] "
    f"({_QUALITY_COL_SQL}) VALUES ({_QUALITY_VAL_SQL})"
)

EXISTING_QUALITY_SQL = text(
    f"SELECT file_hash, row_number_in_file, issue_code, column_name "
    f"FROM [{SCHEMA}].[rcni_data_quality_issue] WHERE file_hash = :file_hash"
)

_PROMOTE_SELECT = ", ".join(
    ["load_run_id", "file_hash", "issuer_id", "coverage_year",
     "processing_year", "processing_month", "processing_day", "file_timestamp",
     "source_file", "source_path", "row_number_in_file", "quality_status",
     ":loaded_at AS loaded_at"]
    + list(SQL_SOURCE_COLUMNS)
)
_PROMOTE_INSERT = ", ".join(
    ["load_run_id", "file_hash", "issuer_id", "coverage_year",
     "processing_year", "processing_month", "processing_day", "file_timestamp",
     "source_file", "source_path", "row_number_in_file", "quality_status",
     "loaded_at"]
    + list(SQL_SOURCE_COLUMNS)
)
PROMOTE_SQL = text(
    f"INSERT INTO [{SCHEMA}].[rcni_raw] ({_PROMOTE_INSERT}) "
    f"SELECT {_PROMOTE_SELECT} FROM [{SCHEMA}].[rcni_stage] "
    f"WHERE load_run_id = :load_run_id AND file_hash = :file_hash"
)

DELETE_STAGE_SQL = text(
    f"DELETE FROM [{SCHEMA}].[rcni_stage] "
    f"WHERE load_run_id = :load_run_id AND file_hash = :file_hash"
)

DELETE_STAGE_BY_HASH_SQL = text(
    f"DELETE FROM [{SCHEMA}].[rcni_stage] WHERE file_hash = :file_hash"
)

COUNT_STAGE_SQL = text(
    f"SELECT COUNT(1) FROM [{SCHEMA}].[rcni_stage] "
    f"WHERE load_run_id = :load_run_id AND file_hash = :file_hash"
)

COUNT_RAW_SQL = text(
    f"SELECT COUNT(1) FROM [{SCHEMA}].[rcni_raw] "
    f"WHERE load_run_id = :load_run_id AND file_hash = :file_hash"
)

FIND_HASH_SQL = text(
    f"SELECT TOP 1 file_id, file_hash, processing_status, file_disposition, load_run_id "
    f"FROM [{SCHEMA}].[rcni_file_log] "
    f"WHERE file_hash = :file_hash AND processing_status = 'SUCCESS'"
)

FIND_LOGICAL_SQL = text(
    f"SELECT file_id, file_hash, processing_status, file_disposition, issuer_id, "
    f"document_type, coverage_year, file_timestamp "
    f"FROM [{SCHEMA}].[rcni_file_log] "
    f"WHERE issuer_id = :issuer_id "
    f"AND document_type = :document_type "
    f"AND coverage_year = :coverage_year "
    f"AND file_timestamp = :file_timestamp "
    f"AND processing_status = 'SUCCESS'"
)

FILE_LOG_INSERT_SQL = text(f"""
    INSERT INTO [{SCHEMA}].[rcni_file_log] (
        source_file, source_path, issuer_id, document_type, coverage_year,
        processing_year, processing_month, processing_day, file_timestamp,
        compression_type, file_size_bytes, file_hash,
        rows_read, rows_parsed, rows_loaded, rows_flagged, rows_rejected,
        processing_status, file_disposition, error_message, load_run_id,
        first_seen_at, started_at, completed_at, loaded_at
    )
    OUTPUT INSERTED.file_id
    VALUES (
        :source_file, :source_path, :issuer_id, :document_type, :coverage_year,
        :processing_year, :processing_month, :processing_day, :file_timestamp,
        :compression_type, :file_size_bytes, :file_hash,
        :rows_read, :rows_parsed, :rows_loaded, :rows_flagged, :rows_rejected,
        :processing_status, :file_disposition, :error_message, :load_run_id,
        :first_seen_at, :started_at, :completed_at, :loaded_at
    )
""")

FILE_LOG_UPDATE_SQL = text(f"""
    UPDATE [{SCHEMA}].[rcni_file_log]
    SET processing_status = COALESCE(:processing_status, processing_status),
        file_disposition = COALESCE(:file_disposition, file_disposition),
        rows_read = COALESCE(:rows_read, rows_read),
        rows_parsed = COALESCE(:rows_parsed, rows_parsed),
        rows_loaded = COALESCE(:rows_loaded, rows_loaded),
        rows_flagged = COALESCE(:rows_flagged, rows_flagged),
        rows_rejected = COALESCE(:rows_rejected, rows_rejected),
        error_message = :error_message,
        load_run_id = COALESCE(:load_run_id, load_run_id),
        started_at = COALESCE(:started_at, started_at),
        completed_at = :completed_at,
        loaded_at = COALESCE(:loaded_at, loaded_at)
    WHERE file_id = :file_id
""")

RUN_LOG_INSERT_SQL = text(f"""
    INSERT INTO [{SCHEMA}].[rcni_run_log] (
        load_run_id, started_at, completed_at, run_mode,
        issuer_scope, year_scope, month_scope,
        files_discovered, files_attempted, files_successful, files_failed, files_skipped,
        rows_parsed, rows_loaded, rows_flagged, status, error_message
    ) VALUES (
        :load_run_id, :started_at, NULL, :run_mode,
        :issuer_scope, :year_scope, :month_scope,
        0, 0, 0, 0, 0, 0, 0, 0, 'RUNNING', NULL
    )
""")

RUN_LOG_UPDATE_SQL = text(f"""
    UPDATE [{SCHEMA}].[rcni_run_log]
    SET completed_at = :completed_at,
        files_discovered = :files_discovered,
        files_attempted = :files_attempted,
        files_successful = :files_successful,
        files_failed = :files_failed,
        files_skipped = :files_skipped,
        rows_parsed = :rows_parsed,
        rows_loaded = :rows_loaded,
        rows_flagged = :rows_flagged,
        status = :status,
        error_message = :error_message
    WHERE load_run_id = :load_run_id
""")

_ = RAW_INSERT_COLUMNS  # documented; promote uses SELECT from stage


class AzureRcniTxn:
    """Narrow SQL transaction used for promote (raw INSERT SELECT only)."""

    def __init__(self, conn: Connection) -> None:
        self.conn = conn

    def promote_stage_to_raw(
        self,
        load_run_id: UUID | str,
        file_hash: str,
        loaded_at: datetime,
    ) -> int:
        self.conn.execute(
            PROMOTE_SQL,
            {
                "load_run_id": load_run_id,
                "file_hash": file_hash,
                "loaded_at": loaded_at,
            },
        )
        counted = self.conn.execute(
            COUNT_RAW_SQL,
            {"load_run_id": load_run_id, "file_hash": file_hash},
        ).fetchone()
        return int(counted[0]) if counted else 0


class AzureRcniStore:
    """SQLAlchemy store. Construct only after an explicit connect_rcni_engine()."""

    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def insert_run_log_start(
        self,
        *,
        load_run_id: UUID | str,
        started_at: datetime,
        run_mode: str,
        issuer_scope: str | None,
        year_scope: str | None,
        month_scope: str | None,
    ) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                RUN_LOG_INSERT_SQL,
                {
                    "load_run_id": load_run_id,
                    "started_at": started_at,
                    "run_mode": run_mode,
                    "issuer_scope": issuer_scope,
                    "year_scope": year_scope,
                    "month_scope": month_scope,
                },
            )

    def update_run_log_finish(self, load_run_id: UUID | str, **fields: Any) -> None:
        params = {
            "load_run_id": load_run_id,
            "completed_at": fields.get("completed_at"),
            "files_discovered": fields.get("files_discovered", 0),
            "files_attempted": fields.get("files_attempted", 0),
            "files_successful": fields.get("files_successful", 0),
            "files_failed": fields.get("files_failed", 0),
            "files_skipped": fields.get("files_skipped", 0),
            "rows_parsed": fields.get("rows_parsed", 0),
            "rows_loaded": fields.get("rows_loaded", 0),
            "rows_flagged": fields.get("rows_flagged", 0),
            "status": fields.get("status", "SUCCESS"),
            "error_message": fields.get("error_message"),
        }
        with self.engine.begin() as conn:
            conn.execute(RUN_LOG_UPDATE_SQL, params)

    def find_loaded_hash(self, file_hash: str) -> dict[str, Any] | None:
        with self.engine.connect() as conn:
            row = conn.execute(FIND_HASH_SQL, {"file_hash": file_hash}).mappings().fetchone()
        return dict(row) if row else None

    def find_logical_identity(
        self,
        *,
        issuer_id: str,
        document_type: str,
        coverage_year: int | None,
        file_timestamp: datetime | None,
    ) -> list[dict[str, Any]]:
        with self.engine.connect() as conn:
            rows = conn.execute(
                FIND_LOGICAL_SQL,
                {
                    "issuer_id": issuer_id,
                    "document_type": document_type,
                    "coverage_year": coverage_year,
                    "file_timestamp": file_timestamp,
                },
            ).mappings().fetchall()
        return [dict(r) for r in rows]

    def insert_file_log(self, row: dict[str, Any]) -> int:
        params = {
            "source_file": row["source_file"],
            "source_path": row["source_path"],
            "issuer_id": row["issuer_id"],
            "document_type": row.get("document_type", "INDV_MONTHLYDISCREPANCY"),
            "coverage_year": row.get("coverage_year"),
            "processing_year": row.get("processing_year"),
            "processing_month": row.get("processing_month"),
            "processing_day": row.get("processing_day"),
            "file_timestamp": row.get("file_timestamp"),
            "compression_type": row.get("compression_type", "none"),
            "file_size_bytes": row.get("file_size_bytes"),
            "file_hash": row["file_hash"],
            "rows_read": row.get("rows_read", 0),
            "rows_parsed": row.get("rows_parsed", 0),
            "rows_loaded": row.get("rows_loaded", 0),
            "rows_flagged": row.get("rows_flagged", 0),
            "rows_rejected": row.get("rows_rejected", 0),
            "processing_status": row["processing_status"],
            "file_disposition": row.get("file_disposition", "NEW"),
            "error_message": row.get("error_message"),
            "load_run_id": row.get("load_run_id"),
            "first_seen_at": row.get("first_seen_at"),
            "started_at": row.get("started_at"),
            "completed_at": row.get("completed_at"),
            "loaded_at": row.get("loaded_at"),
        }
        with self.engine.begin() as conn:
            file_id = conn.execute(FILE_LOG_INSERT_SQL, params).scalar_one()
        return int(file_id)

    def update_file_log(self, file_id: int, **fields: Any) -> None:
        params = {
            "file_id": file_id,
            "processing_status": fields.get("processing_status"),
            "file_disposition": fields.get("file_disposition"),
            "rows_read": fields.get("rows_read"),
            "rows_parsed": fields.get("rows_parsed"),
            "rows_loaded": fields.get("rows_loaded"),
            "rows_flagged": fields.get("rows_flagged"),
            "rows_rejected": fields.get("rows_rejected"),
            "error_message": fields.get("error_message"),
            "load_run_id": fields.get("load_run_id"),
            "started_at": fields.get("started_at"),
            "completed_at": fields.get("completed_at"),
            "loaded_at": fields.get("loaded_at"),
        }
        with self.engine.begin() as conn:
            conn.execute(FILE_LOG_UPDATE_SQL, params)

    def existing_quality_keys(self, file_hash: str) -> set[tuple[str, int, str, str]]:
        with self.engine.connect() as conn:
            rows = conn.execute(EXISTING_QUALITY_SQL, {"file_hash": file_hash}).mappings().fetchall()
        keys: set[tuple[str, int, str, str]] = set()
        for row in rows:
            keys.add(
                quality_natural_key(
                    {
                        "file_hash": row["file_hash"],
                        "row_number_in_file": row["row_number_in_file"],
                        "issue_code": row["issue_code"],
                        "column_name": row["column_name"],
                    }
                )
            )
        return keys

    def insert_quality_batch(self, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0
        file_hash = str(rows[0]["file_hash"])
        existing = self.existing_quality_keys(file_hash)
        fresh = []
        for row in rows:
            key = quality_natural_key(row)
            if key in existing:
                continue
            fresh.append(row)
            existing.add(key)
        if not fresh:
            return 0
        with self.engine.begin() as conn:
            conn.execute(QUALITY_INSERT_SQL, fresh)
        return len(fresh)

    def insert_stage_batch(self, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        with self.engine.begin() as conn:
            conn.execute(STAGE_INSERT_SQL, rows)

    def count_stage(self, load_run_id: UUID | str, file_hash: str) -> int:
        with self.engine.connect() as conn:
            row = conn.execute(
                COUNT_STAGE_SQL,
                {"load_run_id": load_run_id, "file_hash": file_hash},
            ).fetchone()
        return int(row[0]) if row else 0

    def count_raw(self, load_run_id: UUID | str, file_hash: str) -> int:
        with self.engine.connect() as conn:
            row = conn.execute(
                COUNT_RAW_SQL,
                {"load_run_id": load_run_id, "file_hash": file_hash},
            ).fetchone()
        return int(row[0]) if row else 0

    def promote_stage_to_raw(
        self,
        load_run_id: UUID | str,
        file_hash: str,
        loaded_at: datetime,
    ) -> int:
        with self.engine.begin() as conn:
            return AzureRcniTxn(conn).promote_stage_to_raw(load_run_id, file_hash, loaded_at)

    def delete_stage(self, load_run_id: UUID | str, file_hash: str) -> int:
        with self.engine.begin() as conn:
            result = conn.execute(
                DELETE_STAGE_SQL,
                {"load_run_id": load_run_id, "file_hash": file_hash},
            )
        return int(result.rowcount or 0)

    def delete_stage_by_file_hash(self, file_hash: str) -> int:
        with self.engine.begin() as conn:
            result = conn.execute(DELETE_STAGE_BY_HASH_SQL, {"file_hash": file_hash})
        return int(result.rowcount or 0)

    @contextmanager
    def promote_transaction(self) -> Iterator[AzureRcniTxn]:
        with self.engine.begin() as conn:
            yield AzureRcniTxn(conn)
