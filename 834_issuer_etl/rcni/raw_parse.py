"""Streaming RCNI parse for Azure raw load.

Does not load the file into pandas. Uses csv.reader over:
  - gzip text stream for *.OUT.good.gz
  - plain text stream for *.OUT.good
"""

from __future__ import annotations

import csv
import gzip
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import UUID

from rcni.constants import (
    DQ_BROKEN_QUOTE,
    DQ_COLUMN_COUNT_MISMATCH,
    DQ_HEADER_MISMATCH,
    DQ_IDENTIFIER_FORMAT_WARNING,
    DQ_MULTILINE_FIELD,
    DQ_OTHER,
    DQ_SCHEMA_DRIFT,
    DQ_UNQUOTED_COMMA,
    EXPECTED_COLUMN_COUNT,
    EXPECTED_HEADER,
    NUMERIC_IDENTIFIER_COLUMNS,
    QUALITY_STATUS_CLEAN,
    QUALITY_STATUS_WARNING,
)
from rcni.csv_validator import is_numeric_identifier
from rcni.raw_schema import SOURCE_TO_SQL, header_mapping


class _LineTrackingFile:
    def __init__(self, handle):
        self._handle = handle
        self.physical_line_number = 0

    def __iter__(self):
        return self

    def __next__(self) -> str:
        line = next(self._handle)
        self.physical_line_number += 1
        return line


@contextmanager
def open_rcni_text_stream(path: str | Path):
    file_path = Path(path)
    if file_path.name.lower().endswith(".gz"):
        handle = gzip.open(file_path, "rt", encoding="utf-8", newline="", errors="replace")
    else:
        handle = file_path.open("r", encoding="utf-8", newline="", errors="replace")
    try:
        yield handle
    finally:
        handle.close()


def _raw_record(row: list[str]) -> str:
    return ",".join(row)


def classify_csv_error(exc: Exception) -> str:
    text = str(exc).lower()
    if "quote" in text or "unexpected end of data" in text:
        return DQ_BROKEN_QUOTE
    if "newline" in text or "line" in text and "field" in text:
        return DQ_MULTILINE_FIELD
    return DQ_OTHER


def classify_column_count(observed: int) -> str:
    if observed > EXPECTED_COLUMN_COUNT:
        return DQ_UNQUOTED_COMMA
    return DQ_COLUMN_COUNT_MISMATCH


@dataclass
class FileLineage:
    load_run_id: UUID | str
    file_hash: str
    issuer_id: str
    coverage_year: int | None
    processing_year: int | None
    processing_month: int | None
    processing_day: int | None
    file_timestamp: datetime | None
    source_file: str
    source_path: str
    document_type: str = "INDV_MONTHLYDISCREPANCY"
    compression_type: str = "none"


@dataclass
class QualityIssue:
    load_run_id: UUID | str | None
    source_file: str
    source_path: str
    file_hash: str
    issuer_id: str | None
    coverage_year: int | None
    row_number_in_file: int | None
    physical_line_number: int | None
    column_name: str | None
    invalid_value: str | None
    issue_code: str
    issue_message: str
    expected_column_count: int | None
    observed_column_count: int | None
    raw_record: str | None

    def as_dict(self) -> dict[str, Any]:
        return {
            "load_run_id": self.load_run_id,
            "source_file": self.source_file,
            "source_path": self.source_path,
            "file_hash": self.file_hash,
            "issuer_id": self.issuer_id,
            "coverage_year": self.coverage_year,
            "row_number_in_file": self.row_number_in_file,
            "physical_line_number": self.physical_line_number,
            "column_name": self.column_name,
            "invalid_value": self.invalid_value,
            "issue_code": self.issue_code,
            "issue_message": self.issue_message,
            "expected_column_count": self.expected_column_count,
            "observed_column_count": self.observed_column_count,
            "raw_record": self.raw_record,
        }


@dataclass
class HeaderDecision:
    header: tuple[str, ...] | None
    header_ok: bool
    mapping_safe: bool
    header_index: dict[str, int]
    drift_reason: str | None
    issues: list[QualityIssue] = field(default_factory=list)


@dataclass
class ParseCounters:
    header_ok: bool = False
    mapping_safe: bool = False
    header: tuple[str, ...] | None = None
    header_index: dict[str, int] = field(default_factory=dict)
    drift_reason: str | None = None
    source_records: int = 0
    staged_records: int = 0
    structural_malformed: int = 0
    identifier_warnings: int = 0
    quality_issues: int = 0


def _quality(
    lineage: FileLineage,
    *,
    issue_code: str,
    issue_message: str,
    row_number_in_file: int | None = None,
    physical_line_number: int | None = None,
    column_name: str | None = None,
    invalid_value: str | None = None,
    observed_column_count: int | None = None,
    raw_record: str | None = None,
) -> QualityIssue:
    return QualityIssue(
        load_run_id=lineage.load_run_id,
        source_file=lineage.source_file,
        source_path=lineage.source_path,
        file_hash=lineage.file_hash,
        issuer_id=lineage.issuer_id,
        coverage_year=lineage.coverage_year,
        row_number_in_file=row_number_in_file,
        physical_line_number=physical_line_number,
        column_name=column_name,
        invalid_value=None if invalid_value is None else str(invalid_value)[:1000],
        issue_code=issue_code,
        issue_message=issue_message[:1000],
        expected_column_count=EXPECTED_COLUMN_COUNT,
        observed_column_count=observed_column_count,
        raw_record=raw_record,
    )


def _stage_row(
    lineage: FileLineage,
    header_index: dict[str, int],
    row: list[str],
    row_number: int,
    quality_status: str,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "load_run_id": lineage.load_run_id,
        "file_hash": lineage.file_hash,
        "issuer_id": lineage.issuer_id,
        "coverage_year": lineage.coverage_year,
        "processing_year": lineage.processing_year,
        "processing_month": lineage.processing_month,
        "processing_day": lineage.processing_day,
        "file_timestamp": lineage.file_timestamp,
        "source_file": lineage.source_file,
        "source_path": lineage.source_path,
        "row_number_in_file": row_number,
        "quality_status": quality_status,
    }
    for source_name, sql_name in SOURCE_TO_SQL.items():
        payload[sql_name] = row[header_index[source_name]]
    return payload


def stream_rcni_file(
    path: str | Path,
    lineage: FileLineage,
) -> Iterator[HeaderDecision | QualityIssue | dict[str, Any] | ParseCounters]:
    """
    Yield HeaderDecision, then quality issues and staged-row dicts, then
    a final ParseCounters.

    Staged rows are mapped by header name. Structural malformed rows are
    emitted only as QualityIssue (never as staged rows, never auto-corrected).
    Identifier format warnings still yield a staged row with quality_status=WARNING.
    """
    counters = ParseCounters()
    with open_rcni_text_stream(path) as handle:
        tracker = _LineTrackingFile(handle)
        reader = csv.reader(tracker)
        try:
            header_row = next(reader)
        except StopIteration:
            counters.drift_reason = "File is empty"
            issue = _quality(
                lineage,
                issue_code=DQ_HEADER_MISMATCH,
                issue_message="File is empty; expected 19-column RCNI header",
                physical_line_number=1,
                observed_column_count=0,
                raw_record="",
            )
            counters.quality_issues += 1
            yield HeaderDecision(
                header=None,
                header_ok=False,
                mapping_safe=False,
                header_index={},
                drift_reason=counters.drift_reason,
                issues=[issue],
            )
            yield counters
            return
        except csv.Error as exc:
            counters.drift_reason = str(exc)
            issue = _quality(
                lineage,
                issue_code=classify_csv_error(exc),
                issue_message=f"CSV parser error on header: {exc}",
                physical_line_number=tracker.physical_line_number or 1,
                raw_record="",
            )
            counters.quality_issues += 1
            yield HeaderDecision(
                header=None,
                header_ok=False,
                mapping_safe=False,
                header_index={},
                drift_reason=counters.drift_reason,
                issues=[issue],
            )
            yield counters
            return

        header = tuple(header_row)
        counters.header = header
        index, mapping_safe, drift_reason = header_mapping(header)
        counters.header_index = index
        counters.mapping_safe = mapping_safe
        counters.drift_reason = drift_reason
        counters.header_ok = mapping_safe and header == EXPECTED_HEADER
        header_issues: list[QualityIssue] = []

        if not mapping_safe:
            issue = _quality(
                lineage,
                issue_code=DQ_SCHEMA_DRIFT,
                issue_message=(
                    "Unsafe header mapping; file quarantined. "
                    f"{drift_reason}. Expected {list(EXPECTED_HEADER)}; "
                    f"observed {list(header)}"
                ),
                physical_line_number=tracker.physical_line_number,
                observed_column_count=len(header),
                raw_record=_raw_record(header_row),
            )
            counters.quality_issues += 1
            header_issues.append(issue)
            yield HeaderDecision(
                header=header,
                header_ok=False,
                mapping_safe=False,
                header_index={},
                drift_reason=drift_reason,
                issues=header_issues,
            )
            yield counters
            return

        if header != EXPECTED_HEADER:
            issue = _quality(
                lineage,
                issue_code=DQ_HEADER_MISMATCH,
                issue_message=(
                    "Header names match the RCNI contract but column order differs; "
                    "rows will be mapped by name"
                ),
                physical_line_number=tracker.physical_line_number,
                observed_column_count=len(header),
                raw_record=_raw_record(header_row),
            )
            counters.quality_issues += 1
            header_issues.append(issue)

        yield HeaderDecision(
            header=header,
            header_ok=counters.header_ok,
            mapping_safe=True,
            header_index=index,
            drift_reason=None,
            issues=header_issues,
        )

        record_number = 0
        # row_number_in_file is the 1-based source data-record ordinal.
        # Header is excluded. csv.Error and column-count mismatches consume a
        # number so gzip and plain streams of the same logical content produce
        # identical numbers. Physical line number is stored separately.
        while True:
            try:
                row = next(reader)
            except StopIteration:
                break
            except csv.Error as exc:
                record_number += 1
                counters.source_records += 1
                counters.structural_malformed += 1
                counters.quality_issues += 1
                yield _quality(
                    lineage,
                    issue_code=classify_csv_error(exc),
                    issue_message=f"CSV parser error: {exc}",
                    row_number_in_file=record_number,
                    physical_line_number=tracker.physical_line_number,
                    raw_record="",
                )
                continue

            record_number += 1
            counters.source_records += 1
            observed = len(row)
            if observed != EXPECTED_COLUMN_COUNT:
                counters.structural_malformed += 1
                counters.quality_issues += 1
                yield _quality(
                    lineage,
                    issue_code=classify_column_count(observed),
                    issue_message=(
                        f"Record has {observed} fields; expected {EXPECTED_COLUMN_COUNT}. "
                        "Value was not auto-corrected."
                    ),
                    row_number_in_file=record_number,
                    physical_line_number=tracker.physical_line_number,
                    observed_column_count=observed,
                    raw_record=_raw_record(row),
                )
                continue

            row_warning = False
            for col in NUMERIC_IDENTIFIER_COLUMNS:
                value = row[index[col]]
                if not is_numeric_identifier(value):
                    row_warning = True
                    counters.identifier_warnings += 1
                    counters.quality_issues += 1
                    yield _quality(
                        lineage,
                        issue_code=DQ_IDENTIFIER_FORMAT_WARNING,
                        issue_message=(
                            f"{col} is not a numeric-looking identifier; "
                            "value preserved as text"
                        ),
                        row_number_in_file=record_number,
                        physical_line_number=tracker.physical_line_number,
                        column_name=col,
                        invalid_value=value,
                        observed_column_count=observed,
                        raw_record=_raw_record(row),
                    )

            quality_status = QUALITY_STATUS_WARNING if row_warning else QUALITY_STATUS_CLEAN
            counters.staged_records += 1
            yield _stage_row(lineage, index, row, record_number, quality_status)

    yield counters
