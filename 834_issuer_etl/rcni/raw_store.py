"""In-memory RCNI store used by unit tests. Mirrors Azure table grain.

Azure SQL is never contacted from this module.
"""

from __future__ import annotations

import copy
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from rcni.constants import FILE_STATUS_SUCCESS


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def quality_natural_key(row: dict[str, Any]) -> tuple[str, int, str, str]:
    """Match dbo.rcni_data_quality_issue UX_rcni_dq_natural."""
    row_key = row.get("row_number_in_file")
    if row_key is None:
        row_key = -1
    col_key = row.get("column_name") or ""
    return (str(row["file_hash"]), int(row_key), str(row["issue_code"]), str(col_key))


class CountMismatchError(RuntimeError):
    """Staged + malformed did not equal source records."""


class MemoryRcniStore:
    """Dict-backed stand-in for dbo.rcni_* tables."""

    def __init__(self) -> None:
        self.run_log: dict[str, dict[str, Any]] = {}
        self.file_log: list[dict[str, Any]] = []
        self.stage: list[dict[str, Any]] = []
        self.raw: list[dict[str, Any]] = []
        self.quality: list[dict[str, Any]] = []
        self._next_file_id = 1
        self._next_raw_id = 1
        self._next_stage_id = 1
        self._next_quality_id = 1
        self._raw_snapshot: list[dict[str, Any]] | None = None
        self._raw_id_snapshot: int | None = None

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
        key = str(load_run_id)
        self.run_log[key] = {
            "load_run_id": load_run_id,
            "started_at": started_at,
            "completed_at": None,
            "run_mode": run_mode,
            "issuer_scope": issuer_scope,
            "year_scope": year_scope,
            "month_scope": month_scope,
            "files_discovered": 0,
            "files_attempted": 0,
            "files_successful": 0,
            "files_failed": 0,
            "files_skipped": 0,
            "rows_parsed": 0,
            "rows_loaded": 0,
            "rows_flagged": 0,
            "status": "RUNNING",
            "error_message": None,
        }

    def update_run_log_finish(self, load_run_id: UUID | str, **fields: Any) -> None:
        self.run_log[str(load_run_id)].update(fields)

    def find_loaded_hash(self, file_hash: str) -> dict[str, Any] | None:
        for row in self.file_log:
            if row["file_hash"] == file_hash and row["processing_status"] == FILE_STATUS_SUCCESS:
                return row
        return None

    def find_logical_identity(
        self,
        *,
        issuer_id: str,
        document_type: str,
        coverage_year: int | None,
        file_timestamp: datetime | None,
    ) -> list[dict[str, Any]]:
        matches = []
        for row in self.file_log:
            if (
                row["issuer_id"] == issuer_id
                and row["document_type"] == document_type
                and row["coverage_year"] == coverage_year
                and row["file_timestamp"] == file_timestamp
                and row["processing_status"] == FILE_STATUS_SUCCESS
            ):
                matches.append(row)
        return matches

    def insert_file_log(self, row: dict[str, Any]) -> int:
        record = dict(row)
        record["file_id"] = self._next_file_id
        self._next_file_id += 1
        record.setdefault("file_disposition", "NEW")
        if "first_seen_at" not in record or record["first_seen_at"] is None:
            record["first_seen_at"] = _utcnow()
        self.file_log.append(record)
        return record["file_id"]

    def update_file_log(self, file_id: int, **fields: Any) -> None:
        for row in self.file_log:
            if row["file_id"] == file_id:
                row.update(fields)
                return
        raise KeyError(f"file_id {file_id} not found")

    def existing_quality_keys(self, file_hash: str) -> set[tuple[str, int, str, str]]:
        return {
            quality_natural_key(row)
            for row in self.quality
            if row["file_hash"] == file_hash
        }

    def insert_quality_batch(self, rows: list[dict[str, Any]]) -> int:
        """Persist quality issues in an independent audit write. Skip duplicates."""
        inserted = 0
        existing = {quality_natural_key(row) for row in self.quality}
        for row in rows:
            key = quality_natural_key(row)
            if key in existing:
                continue
            record = dict(row)
            record["quality_issue_id"] = self._next_quality_id
            self._next_quality_id += 1
            record.setdefault("created_at", _utcnow())
            self.quality.append(record)
            existing.add(key)
            inserted += 1
        return inserted

    def insert_stage_batch(self, rows: list[dict[str, Any]]) -> None:
        seen = {
            (str(row["load_run_id"]), row["file_hash"], row["row_number_in_file"])
            for row in self.stage
        }
        for row in rows:
            key = (str(row["load_run_id"]), row["file_hash"], row["row_number_in_file"])
            if key in seen:
                raise ValueError(f"Duplicate stage key {key}")
            record = dict(row)
            record["stage_id"] = self._next_stage_id
            self._next_stage_id += 1
            self.stage.append(record)
            seen.add(key)

    def count_stage(self, load_run_id: UUID | str, file_hash: str) -> int:
        return sum(
            1
            for row in self.stage
            if str(row["load_run_id"]) == str(load_run_id) and row["file_hash"] == file_hash
        )

    def count_raw(self, load_run_id: UUID | str, file_hash: str) -> int:
        return sum(
            1
            for row in self.raw
            if str(row["load_run_id"]) == str(load_run_id) and row["file_hash"] == file_hash
        )

    def promote_stage_to_raw(
        self,
        load_run_id: UUID | str,
        file_hash: str,
        loaded_at: datetime,
    ) -> int:
        promoted = 0
        for row in self.stage:
            if str(row["load_run_id"]) != str(load_run_id) or row["file_hash"] != file_hash:
                continue
            raw_row = {k: v for k, v in row.items() if k != "stage_id"}
            raw_row["rcni_raw_id"] = self._next_raw_id
            self._next_raw_id += 1
            raw_row["loaded_at"] = loaded_at
            self.raw.append(raw_row)
            promoted += 1
        return promoted

    def delete_stage(self, load_run_id: UUID | str, file_hash: str) -> int:
        keep: list[dict[str, Any]] = []
        deleted = 0
        for row in self.stage:
            if str(row["load_run_id"]) == str(load_run_id) and row["file_hash"] == file_hash:
                deleted += 1
            else:
                keep.append(row)
        self.stage = keep
        return deleted

    def delete_stage_by_file_hash(self, file_hash: str) -> int:
        """Remove stale stage rows for a file without truncating the table."""
        keep: list[dict[str, Any]] = []
        deleted = 0
        for row in self.stage:
            if row["file_hash"] == file_hash:
                deleted += 1
            else:
                keep.append(row)
        self.stage = keep
        return deleted

    @contextmanager
    def promote_transaction(self) -> Iterator[MemoryRcniStore]:
        """Rollback rcni_raw only. Stage and quality stay durable."""
        self._raw_snapshot = copy.deepcopy(self.raw)
        self._raw_id_snapshot = self._next_raw_id
        try:
            yield self
        except Exception:
            self.raw = self._raw_snapshot
            self._next_raw_id = self._raw_id_snapshot or self._next_raw_id
            self._raw_snapshot = None
            self._raw_id_snapshot = None
            raise
        self._raw_snapshot = None
        self._raw_id_snapshot = None
