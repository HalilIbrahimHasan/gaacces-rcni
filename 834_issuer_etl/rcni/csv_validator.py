"""Streaming RCNI CSV validation. Does not load the full file into memory."""

from __future__ import annotations

import csv
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from pathlib import Path

from rcni.constants import (
    EXPECTED_COLUMN_COUNT,
    EXPECTED_HEADER,
    HIX_VALUE_COLUMN,
    ISSUE_FIELD_COUNT,
    ISSUE_HEADER_MISMATCH,
    ISSUE_IDENTIFIER_NOT_NUMERIC,
    ISSUE_PARSE_ERROR,
    ISSUER_VALUE_COLUMN,
    NUMERIC_IDENTIFIER_COLUMNS,
)

IssueCallback = Callable[["RowIssue"], None]


@dataclass
class RowIssue:
    source_file: str
    source_path: str
    record_number: int | None
    physical_line_number: int | None
    issue_type: str
    issue_description: str
    expected_column_count: int
    observed_column_count: int | None
    column_name: str | None
    bad_value: str | None
    raw_record: str


@dataclass
class CsvValidationResult:
    header: tuple[str, ...] | None
    header_column_count: int
    header_ok: bool
    header_mismatch_details: str | None
    parsed_records: int = 0
    clean_records: int = 0
    malformed_records: int = 0
    identifier_warnings: int = 0
    issues: list[RowIssue] = field(default_factory=list)
    held_all_rows: bool = False


class _LineTrackingFile:
    """File wrapper that tracks the last physical line number fed to csv.reader."""

    def __init__(self, handle):
        self._handle = handle
        self.physical_line_number = 0

    def __iter__(self):
        return self

    def __next__(self) -> str:
        line = next(self._handle)
        self.physical_line_number += 1
        return line


def is_numeric_identifier(value: str | None) -> bool:
    if value is None or value == "":
        return True
    return value.isdigit()


def _raw_record(row: Iterable[str]) -> str:
    return ",".join(row)


def validate_rcni_csv(
    path: str | Path,
    *,
    source_file: str | None = None,
    source_path: str | None = None,
    issue_callback: IssueCallback | None = None,
    collect_issues: bool = True,
) -> CsvValidationResult:
    """
    Stream-parse an RCNI CSV.

    Malformed rows are recorded and scanning continues.
    HIX Value / Issuer Value are never type-checked.
    Identifier columns are string-validated as numeric-looking without conversion.
    """
    file_path = Path(path)
    display_name = source_file or file_path.name
    display_path = source_path or str(file_path)

    def emit(issue: RowIssue) -> None:
        if collect_issues:
            result.issues.append(issue)
        if issue_callback is not None:
            issue_callback(issue)

    result = CsvValidationResult(
        header=None,
        header_column_count=0,
        header_ok=False,
        header_mismatch_details=None,
    )

    with file_path.open("r", encoding="utf-8", newline="", errors="replace") as raw:
        tracker = _LineTrackingFile(raw)
        reader = csv.reader(tracker)
        try:
            header_row = next(reader)
        except StopIteration:
            result.header_mismatch_details = "File is empty"
            emit(
                RowIssue(
                    source_file=display_name,
                    source_path=display_path,
                    record_number=None,
                    physical_line_number=1,
                    issue_type=ISSUE_HEADER_MISMATCH,
                    issue_description="File is empty; expected 19-column RCNI header",
                    expected_column_count=EXPECTED_COLUMN_COUNT,
                    observed_column_count=0,
                    column_name=None,
                    bad_value=None,
                    raw_record="",
                )
            )
            return result
        except csv.Error as exc:
            result.header_mismatch_details = str(exc)
            emit(
                RowIssue(
                    source_file=display_name,
                    source_path=display_path,
                    record_number=None,
                    physical_line_number=tracker.physical_line_number or 1,
                    issue_type=ISSUE_PARSE_ERROR,
                    issue_description=f"CSV parser error on header: {exc}",
                    expected_column_count=EXPECTED_COLUMN_COUNT,
                    observed_column_count=None,
                    column_name=None,
                    bad_value=None,
                    raw_record="",
                )
            )
            return result

        header = tuple(header_row)
        result.header = header
        result.header_column_count = len(header)
        if header != EXPECTED_HEADER:
            result.header_ok = False
            expected = list(EXPECTED_HEADER)
            details = (
                f"Header mismatch: expected {EXPECTED_COLUMN_COUNT} columns "
                f"{list(EXPECTED_HEADER)}; observed {len(header)} columns {list(header)}"
            )
            if len(header) != EXPECTED_COLUMN_COUNT:
                details += f" (count {len(header)} != {EXPECTED_COLUMN_COUNT})"
            else:
                diffs = [
                    f"{expected[i]!r} vs {header[i]!r}"
                    for i in range(EXPECTED_COLUMN_COUNT)
                    if expected[i] != header[i]
                ]
                details += "; differing positions: " + "; ".join(diffs)
            result.header_mismatch_details = details
            emit(
                RowIssue(
                    source_file=display_name,
                    source_path=display_path,
                    record_number=None,
                    physical_line_number=tracker.physical_line_number,
                    issue_type=ISSUE_HEADER_MISMATCH,
                    issue_description=details,
                    expected_column_count=EXPECTED_COLUMN_COUNT,
                    observed_column_count=len(header),
                    column_name=None,
                    bad_value=None,
                    raw_record=_raw_record(header_row),
                )
            )
        else:
            result.header_ok = True

        record_number = 0
        while True:
            try:
                row = next(reader)
            except StopIteration:
                break
            except csv.Error as exc:
                record_number += 1
                result.parsed_records += 1
                result.malformed_records += 1
                emit(
                    RowIssue(
                        source_file=display_name,
                        source_path=display_path,
                        record_number=record_number,
                        physical_line_number=tracker.physical_line_number,
                        issue_type=ISSUE_PARSE_ERROR,
                        issue_description=f"CSV parser error: {exc}",
                        expected_column_count=EXPECTED_COLUMN_COUNT,
                        observed_column_count=None,
                        column_name=None,
                        bad_value=None,
                        raw_record="",
                    )
                )
                continue

            record_number += 1
            result.parsed_records += 1
            observed = len(row)
            if observed != EXPECTED_COLUMN_COUNT:
                result.malformed_records += 1
                emit(
                    RowIssue(
                        source_file=display_name,
                        source_path=display_path,
                        record_number=record_number,
                        physical_line_number=tracker.physical_line_number,
                        issue_type=ISSUE_FIELD_COUNT,
                        issue_description=(
                            f"Record has {observed} fields; expected {EXPECTED_COLUMN_COUNT}. "
                            "Likely an unquoted embedded comma or a truncated row."
                        ),
                        expected_column_count=EXPECTED_COLUMN_COUNT,
                        observed_column_count=observed,
                        column_name=None,
                        bad_value=None,
                        raw_record=_raw_record(row),
                    )
                )
                continue

            row_has_identifier_warning = False
            if result.header_ok:
                for col in NUMERIC_IDENTIFIER_COLUMNS:
                    try:
                        idx = EXPECTED_HEADER.index(col)
                    except ValueError:
                        continue
                    value = row[idx]
                    if not is_numeric_identifier(value):
                        row_has_identifier_warning = True
                        result.identifier_warnings += 1
                        emit(
                            RowIssue(
                                source_file=display_name,
                                source_path=display_path,
                                record_number=record_number,
                                physical_line_number=tracker.physical_line_number,
                                issue_type=ISSUE_IDENTIFIER_NOT_NUMERIC,
                                issue_description=(
                                    f"{col} is not a numeric-looking identifier; "
                                    "value preserved as text (pipeline continues)"
                                ),
                                expected_column_count=EXPECTED_COLUMN_COUNT,
                                observed_column_count=observed,
                                column_name=col,
                                bad_value=value,
                                raw_record=_raw_record(row),
                            )
                        )

            # Touch HIX/Issuer so mixed types are explicitly allowed (no type coercion).
            _ = row[EXPECTED_HEADER.index(HIX_VALUE_COLUMN)]
            _ = row[EXPECTED_HEADER.index(ISSUER_VALUE_COLUMN)]

            if not row_has_identifier_warning:
                result.clean_records += 1
            else:
                # Parseable row with identifier anomaly — counted as parsed, not malformed.
                pass

    return result
