"""Write RCNI discovery and validation reports (CSV + JSON). Never writes Azure SQL."""

from __future__ import annotations

import csv
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from rcni.csv_validator import RowIssue
from utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class FileValidationSummary:
    issuer: str
    directory_processing_year: str
    directory_processing_month: str
    directory_processing_day: str | None
    plan_year: str
    source_filename: str
    source_sftp_path: str
    compressed_size: int
    content_hash: str
    compressed_hash: str
    header_column_count: int
    parsed_records: int
    clean_records: int
    malformed_records: int
    identifier_warnings: int
    schema_header_status: str
    filename_metadata_status: str
    overall_status: str
    issuer_mismatch: bool
    plan_year_differs_from_processing_year: bool
    local_extracted_path: str
    flags: list[str]


def _write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def write_discovery_inventory(
    reports_dir: Path,
    rows: list[dict[str, Any]],
) -> Path:
    path = reports_dir / "discovery_inventory.csv"
    fields = [
        "issuer",
        "processing_year",
        "processing_month",
        "processing_day",
        "plan_year",
        "filename",
        "logical_filename",
        "source_path",
        "nested_relative",
        "file_timestamp",
        "parsed_timestamp",
        "issuer_mismatch",
        "plan_year_differs_from_processing_year",
        "filename_parse_ok",
    ]
    _write_csv(path, rows, fields)
    logger.info("Wrote discovery inventory: %s (%d row(s))", path, len(rows))
    return path


def write_validation_summary(
    reports_dir: Path,
    summaries: list[FileValidationSummary],
) -> Path:
    path = reports_dir / "validation_summary.csv"
    rows = []
    for item in summaries:
        row = asdict(item)
        row["flags"] = "|".join(item.flags)
        rows.append(row)
    fields = [
        "issuer",
        "directory_processing_year",
        "directory_processing_month",
        "directory_processing_day",
        "plan_year",
        "source_filename",
        "source_sftp_path",
        "compressed_size",
        "content_hash",
        "compressed_hash",
        "header_column_count",
        "parsed_records",
        "clean_records",
        "malformed_records",
        "identifier_warnings",
        "schema_header_status",
        "filename_metadata_status",
        "overall_status",
        "issuer_mismatch",
        "plan_year_differs_from_processing_year",
        "local_extracted_path",
        "flags",
    ]
    _write_csv(path, rows, fields)
    logger.info("Wrote validation summary: %s (%d file(s))", path, len(rows))
    return path


def write_malformed_evidence(reports_dir: Path, issues: list[RowIssue]) -> Path:
    path = reports_dir / "malformed_records.csv"
    rows = [asdict(issue) for issue in issues]
    fields = [
        "source_file",
        "source_path",
        "record_number",
        "physical_line_number",
        "issue_type",
        "issue_description",
        "expected_column_count",
        "observed_column_count",
        "column_name",
        "bad_value",
        "raw_record",
    ]
    _write_csv(path, rows, fields)
    logger.info("Wrote malformed evidence: %s (%d issue(s))", path, len(rows))
    return path


def write_run_manifest(
    reports_dir: Path,
    payload: dict[str, Any],
) -> Path:
    path = reports_dir / "run_manifest.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        **payload,
        "written_at": datetime.now(timezone.utc).isoformat(),
        "azure_sql_writes": False,
        "source_files_modified": False,
    }
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    logger.info("Wrote run manifest: %s", path)
    return path


def print_candidate_inventory(rows: list[dict[str, Any]]) -> None:
    print("\nRCNI CANDIDATE INVENTORY")
    print("-" * 100)
    if not rows:
        print("  (no matching RCNI Monthly Discrepancy files)")
        print("-" * 100)
        return
    header = (
        f"{'issuer':<8} {'proc':<12} {'day':<6} {'plan':<6} {'mismatch':<9} {'filename'}"
    )
    print(header)
    for row in rows:
        proc = f"{row.get('processing_year')}/{row.get('processing_month')}"
        print(
            f"{str(row.get('issuer') or ''):<8} "
            f"{proc:<12} "
            f"{str(row.get('processing_day') or '?'):<6} "
            f"{str(row.get('plan_year') or '?'):<6} "
            f"{str(row.get('issuer_mismatch')):<9} "
            f"{row.get('filename')}"
        )
        print(f"         {row.get('source_path')}")
    print("-" * 100)
    print(f"Total candidates: {len(rows)}")
