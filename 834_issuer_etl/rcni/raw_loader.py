"""Per-file RCNI raw load: stream → staged batches → quality audit → promote.

Does not connect to Azure by itself. Pass a MemoryRcniStore (tests) or
AzureRcniStore (after explicit connect). Never truncates dbo.rcni_raw.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import UUID

from rcni.constants import (
    DOCUMENT_TYPE_RCNI,
    DQ_COUNT_MISMATCH,
    FILE_DISPOSITION_DUPLICATE,
    FILE_DISPOSITION_NEW,
    FILE_DISPOSITION_POSSIBLE_REPLACEMENT,
    FILE_STATUS_FAILED,
    FILE_STATUS_LOADING,
    FILE_STATUS_SKIPPED_DUPLICATE,
    FILE_STATUS_SUCCESS,
    FILE_STATUS_VALIDATING,
)
from rcni.filename import parse_rcni_filename
from rcni.raw_parse import (
    FileLineage,
    HeaderDecision,
    ParseCounters,
    QualityIssue,
    stream_rcni_file,
)
from rcni.raw_store import CountMismatchError
from utils.hashing import sha256_file
from utils.logger import get_logger

logger = get_logger(__name__)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


@dataclass
class BatchTiming:
    batch_number: int
    rows_in_batch: int
    batch_duration_ms: float
    rows_per_second: float


@dataclass
class FileLoadMetrics:
    batch_size: int
    batch_timings: list[BatchTiming] = field(default_factory=list)
    total_stage_duration_ms: float = 0.0
    quality_insert_ms: float = 0.0
    promote_duration_ms: float = 0.0
    total_file_duration_ms: float = 0.0

    @property
    def batch_count(self) -> int:
        return len(self.batch_timings)

    @property
    def rows_per_sec(self) -> float:
        if self.total_stage_duration_ms <= 0:
            return 0.0
        rows = sum(b.rows_in_batch for b in self.batch_timings)
        return rows / (self.total_stage_duration_ms / 1000.0)


@dataclass
class FileLoadResult:
    file_id: int | None
    processing_status: str
    file_disposition: str
    file_hash: str
    source_file: str
    source_path: str
    rows_read: int = 0
    rows_parsed: int = 0
    rows_loaded: int = 0
    rows_flagged: int = 0
    rows_rejected: int = 0
    error_message: str | None = None
    metrics: FileLoadMetrics = field(default_factory=lambda: FileLoadMetrics(batch_size=0))


def _flush_stage(store, batch: list[dict[str, Any]], metrics: FileLoadMetrics) -> None:
    if not batch:
        return
    n = len(batch)
    t0 = time.perf_counter()
    store.insert_stage_batch(batch)
    duration_ms = (time.perf_counter() - t0) * 1000.0
    metrics.total_stage_duration_ms += duration_ms
    rps = (n / (duration_ms / 1000.0)) if duration_ms > 0 else 0.0
    metrics.batch_timings.append(
        BatchTiming(
            batch_number=len(metrics.batch_timings) + 1,
            rows_in_batch=n,
            batch_duration_ms=round(duration_ms, 3),
            rows_per_second=round(rps, 2),
        )
    )
    batch.clear()


def _flush_quality(store, batch: list[dict[str, Any]], metrics: FileLoadMetrics) -> None:
    if not batch:
        return
    t0 = time.perf_counter()
    store.insert_quality_batch(batch)
    metrics.quality_insert_ms += (time.perf_counter() - t0) * 1000.0
    batch.clear()


def build_lineage(
    path: str | Path,
    *,
    load_run_id: UUID | str,
    file_hash: str,
    source_path: str | None = None,
    processing_year: int | None = None,
    processing_month: int | None = None,
    processing_day: int | None = None,
) -> FileLineage:
    file_path = Path(path)
    meta = parse_rcni_filename(file_path.name)
    coverage_year = int(meta.plan_year) if meta.plan_year.isdigit() else None
    compression = "gzip" if file_path.name.lower().endswith(".gz") else "none"
    issuer_id = meta.issuer_id or "UNKNOWN"
    return FileLineage(
        load_run_id=load_run_id,
        file_hash=file_hash,
        issuer_id=issuer_id,
        coverage_year=coverage_year,
        processing_year=processing_year,
        processing_month=processing_month,
        processing_day=processing_day,
        file_timestamp=meta.parsed_timestamp,
        source_file=file_path.name,
        source_path=source_path or str(file_path),
        document_type=meta.document_type or DOCUMENT_TYPE_RCNI,
        compression_type=compression,
    )


def process_local_file(
    store,
    path: str | Path,
    *,
    load_run_id: UUID | str,
    batch_size: int = 3000,
    source_path: str | None = None,
    processing_year: int | None = None,
    processing_month: int | None = None,
    processing_day: int | None = None,
) -> FileLoadResult:
    """
    Load one local RCNI file through staging. Original bytes are not modified.

    Staging batches commit independently. Quality issues commit independently.
    Promote is a separate transaction: INSERT rcni_raw SELECT FROM rcni_stage.
    A promote rollback never removes already-persisted quality rows.
    """
    file_path = Path(path)
    t_file = time.perf_counter()
    metrics = FileLoadMetrics(batch_size=batch_size)
    file_hash = sha256_file(file_path)
    lineage = build_lineage(
        file_path,
        load_run_id=load_run_id,
        file_hash=file_hash,
        source_path=source_path,
        processing_year=processing_year,
        processing_month=processing_month,
        processing_day=processing_day,
    )
    started_at = _utcnow()
    file_id = store.insert_file_log(
        {
            "source_file": lineage.source_file,
            "source_path": lineage.source_path,
            "issuer_id": lineage.issuer_id,
            "document_type": lineage.document_type,
            "coverage_year": lineage.coverage_year,
            "processing_year": lineage.processing_year,
            "processing_month": lineage.processing_month,
            "processing_day": lineage.processing_day,
            "file_timestamp": lineage.file_timestamp,
            "compression_type": lineage.compression_type,
            "file_size_bytes": file_path.stat().st_size,
            "file_hash": file_hash,
            "processing_status": FILE_STATUS_VALIDATING,
            "file_disposition": FILE_DISPOSITION_NEW,
            "load_run_id": load_run_id,
            "first_seen_at": started_at,
            "started_at": started_at,
        }
    )
    result = FileLoadResult(
        file_id=file_id,
        processing_status=FILE_STATUS_VALIDATING,
        file_disposition=FILE_DISPOSITION_NEW,
        file_hash=file_hash,
        source_file=lineage.source_file,
        source_path=lineage.source_path,
        metrics=metrics,
    )

    existing = store.find_loaded_hash(file_hash)
    if existing:
        store.delete_stage_by_file_hash(file_hash)
        store.update_file_log(
            file_id,
            processing_status=FILE_STATUS_SKIPPED_DUPLICATE,
            file_disposition=FILE_DISPOSITION_DUPLICATE,
            load_run_id=load_run_id,
            completed_at=_utcnow(),
            error_message=(
                f"SHA-256 already loaded as {existing.get('processing_status')} "
                f"(file_id={existing.get('file_id')})"
            ),
        )
        result.processing_status = FILE_STATUS_SKIPPED_DUPLICATE
        result.file_disposition = FILE_DISPOSITION_DUPLICATE
        result.error_message = "SKIPPED_DUPLICATE"
        metrics.total_file_duration_ms = (time.perf_counter() - t_file) * 1000.0
        return result

    logical_matches = store.find_logical_identity(
        issuer_id=lineage.issuer_id,
        document_type=lineage.document_type,
        coverage_year=lineage.coverage_year,
        file_timestamp=lineage.file_timestamp,
    )
    is_replacement = any(m["file_hash"] != file_hash for m in logical_matches)
    disposition = (
        FILE_DISPOSITION_POSSIBLE_REPLACEMENT if is_replacement else FILE_DISPOSITION_NEW
    )
    result.file_disposition = disposition

    meta = parse_rcni_filename(file_path.name)
    if not meta.parse_ok:
        store.update_file_log(
            file_id,
            processing_status=FILE_STATUS_FAILED,
            file_disposition=disposition,
            load_run_id=load_run_id,
            completed_at=_utcnow(),
            error_message=meta.parse_error,
        )
        result.processing_status = FILE_STATUS_FAILED
        result.error_message = meta.parse_error
        metrics.total_file_duration_ms = (time.perf_counter() - t_file) * 1000.0
        return result

    store.delete_stage_by_file_hash(file_hash)
    store.update_file_log(
        file_id,
        processing_status=FILE_STATUS_LOADING,
        file_disposition=disposition,
        load_run_id=load_run_id,
        started_at=started_at,
    )

    stream = stream_rcni_file(file_path, lineage)
    header = next(stream)
    if not isinstance(header, HeaderDecision):
        raise TypeError("Parser must yield HeaderDecision first")

    if not header.mapping_safe:
        if header.issues:
            store.insert_quality_batch([issue.as_dict() for issue in header.issues])
        store.update_file_log(
            file_id,
            processing_status=FILE_STATUS_FAILED,
            file_disposition=disposition,
            load_run_id=load_run_id,
            completed_at=_utcnow(),
            rows_flagged=len(header.issues),
            error_message=header.drift_reason or "SCHEMA_DRIFT",
        )
        result.processing_status = FILE_STATUS_FAILED
        result.rows_flagged = len(header.issues)
        result.error_message = header.drift_reason
        metrics.total_file_duration_ms = (time.perf_counter() - t_file) * 1000.0
        return result

    loaded_at = _utcnow()
    counters: ParseCounters | None = None
    try:
        stage_batch: list[dict[str, Any]] = []
        quality_batch: list[dict[str, Any]] = [issue.as_dict() for issue in header.issues]
        for event in stream:
            if isinstance(event, ParseCounters):
                counters = event
                break
            if isinstance(event, QualityIssue):
                quality_batch.append(event.as_dict())
                if len(quality_batch) >= batch_size:
                    _flush_quality(store, quality_batch, metrics)
            else:
                stage_batch.append(event)
                if len(stage_batch) >= batch_size:
                    _flush_stage(store, stage_batch, metrics)
        _flush_stage(store, stage_batch, metrics)
        _flush_quality(store, quality_batch, metrics)

        if counters is None:
            raise RuntimeError("Parser did not yield ParseCounters")

        staged_count = store.count_stage(load_run_id, file_hash)
        expected = counters.staged_records + counters.structural_malformed
        result.rows_read = counters.source_records
        result.rows_parsed = counters.source_records
        result.rows_flagged = counters.quality_issues
        result.rows_rejected = counters.structural_malformed

        if counters.source_records != expected or staged_count != counters.staged_records:
            store.insert_quality_batch(
                [
                    QualityIssue(
                        load_run_id=load_run_id,
                        source_file=lineage.source_file,
                        source_path=lineage.source_path,
                        file_hash=file_hash,
                        issuer_id=lineage.issuer_id,
                        coverage_year=lineage.coverage_year,
                        row_number_in_file=None,
                        physical_line_number=None,
                        column_name=None,
                        invalid_value=None,
                        issue_code=DQ_COUNT_MISMATCH,
                        issue_message=(
                            f"Count reconcile failed: source={counters.source_records} "
                            f"staged={counters.staged_records} "
                            f"malformed={counters.structural_malformed} "
                            f"stage_table={staged_count}"
                        ),
                        expected_column_count=None,
                        observed_column_count=None,
                        raw_record=None,
                    ).as_dict()
                ]
            )
            raise CountMismatchError(
                f"source={counters.source_records} staged={counters.staged_records} "
                f"malformed={counters.structural_malformed} stage_table={staged_count}"
            )

        t_promote = time.perf_counter()
        with store.promote_transaction() as txn:
            promoted = txn.promote_stage_to_raw(load_run_id, file_hash, loaded_at)
            if promoted != counters.staged_records:
                raise CountMismatchError(
                    f"promoted={promoted} staged={counters.staged_records}"
                )
        metrics.promote_duration_ms = (time.perf_counter() - t_promote) * 1000.0

        store.delete_stage(load_run_id, file_hash)
        store.update_file_log(
            file_id,
            processing_status=FILE_STATUS_SUCCESS,
            file_disposition=disposition,
            rows_read=counters.source_records,
            rows_parsed=counters.source_records,
            rows_loaded=promoted,
            rows_flagged=counters.quality_issues,
            rows_rejected=counters.structural_malformed,
            error_message=(
                None
                if not is_replacement
                else "Same logical identity, different SHA-256; previous file preserved"
            ),
            load_run_id=load_run_id,
            completed_at=_utcnow(),
            loaded_at=loaded_at,
        )
        result.rows_loaded = promoted
        result.processing_status = FILE_STATUS_SUCCESS
        result.file_disposition = disposition
    except Exception as exc:
        completed = _utcnow()
        store.update_file_log(
            file_id,
            processing_status=FILE_STATUS_FAILED,
            file_disposition=disposition,
            load_run_id=load_run_id,
            completed_at=completed,
            error_message=str(exc)[:4000],
            rows_read=result.rows_read,
            rows_parsed=result.rows_parsed,
            rows_loaded=0,
            rows_flagged=result.rows_flagged,
            rows_rejected=result.rows_rejected,
        )
        result.processing_status = FILE_STATUS_FAILED
        result.rows_loaded = 0
        result.error_message = str(exc)
        logger.exception("RCNI file load failed: %s", file_path.name)

    metrics.total_file_duration_ms = (time.perf_counter() - t_file) * 1000.0
    logger.info(
        "RCNI %s status=%s disposition=%s loaded=%d flagged=%d batches=%d "
        "file=%.0fms stage=%.0fms promote=%.0fms rows/sec=%.1f",
        lineage.source_file,
        result.processing_status,
        result.file_disposition,
        result.rows_loaded,
        result.rows_flagged,
        metrics.batch_count,
        metrics.total_file_duration_ms,
        metrics.total_stage_duration_ms,
        metrics.promote_duration_ms,
        metrics.rows_per_sec,
    )
    return result
