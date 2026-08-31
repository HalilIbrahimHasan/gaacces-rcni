"""Row enrichment — automation metadata, derived fields, raw_json."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import Any

import pandas as pd

from connectors.base_connector import SourceFile
from inbound_automation.filename_utils import parse_filename_year_month
from src.transform.enrollment_summary import (
    _insurance_type_display,
    _resolve_enrollee_status,
)
from utils.logger import get_logger

logger = get_logger(__name__)

COVERAGE_SOURCE_CLI = "cli_filter"
COVERAGE_SOURCE_FOLDER = "folder_year"
COVERAGE_SOURCE_FILENAME = "filename_year"
COVERAGE_SOURCE_BENEFIT = "benefit_effective_year"
COVERAGE_SOURCE_UNRESOLVED = "unresolved"


def resolve_coverage_year(
    *,
    cli_year: str | None,
    folder_year: int,
    filename_year: int | None,
    benefit_effective_date: str | None,
) -> tuple[int | None, str]:
    if cli_year:
        try:
            return int(cli_year), COVERAGE_SOURCE_CLI
        except (TypeError, ValueError):
            pass
    if filename_year is not None:
        return filename_year, COVERAGE_SOURCE_FILENAME
    if folder_year is not None:
        return folder_year, COVERAGE_SOURCE_FOLDER
    if benefit_effective_date:
        text = str(benefit_effective_date).strip()
        if len(text) >= 4 and text[:4].isdigit():
            return int(text[:4]), COVERAGE_SOURCE_BENEFIT
    return None, COVERAGE_SOURCE_UNRESOLVED


def resolve_enrollee_status(row: dict[str, Any]) -> str:
    status = _resolve_enrollee_status(pd.Series(row))
    if status is None or (isinstance(status, float) and pd.isna(status)):
        return "UNMAPPED"
    return str(status).strip()


def canonical_row_json(row: dict[str, Any]) -> str:
    return json.dumps(row, sort_keys=True, default=str)


def raw_record_hash(row: dict[str, Any]) -> str:
    return hashlib.sha256(canonical_row_json(row).encode("utf-8")).hexdigest()


def build_raw_json(enriched: dict[str, Any]) -> str:
    return json.dumps(enriched, sort_keys=True, default=str)


def enrich_parser_row(
    parser_row: dict[str, Any],
    *,
    source: SourceFile,
    file_hash: str,
    row_number_in_file: int,
    load_run_id: str,
    loaded_at: datetime,
    cli_year: str | None,
    filename_file_year: int | None,
    filename_file_month: int | None,
    parser_version: str,
    runner_version: str,
    git_commit: str | None,
    file_warnings: list[str],
) -> dict[str, Any]:
    """Attach approved automation metadata and derived fields to a Parser834 row."""
    folder_year = int(source.year)
    folder_month = int(source.month)

    insurance_type = _insurance_type_display(parser_row.get("insurance_type_code"))
    enrollee_status = resolve_enrollee_status(parser_row)
    coverage_year, coverage_year_source = resolve_coverage_year(
        cli_year=cli_year,
        folder_year=folder_year,
        filename_year=filename_file_year,
        benefit_effective_date=parser_row.get("benefit_effective_date"),
    )

    enriched: dict[str, Any] = dict(parser_row)
    enriched.update(
        {
            "load_run_id": load_run_id,
            "loaded_at": loaded_at.isoformat(timespec="milliseconds"),
            "folder_year": folder_year,
            "folder_month": folder_month,
            "filename_file_year": filename_file_year,
            "filename_file_month": filename_file_month,
            "source_file": source.file_name,
            "source_file_path": str(source.file_path),
            "file_hash": file_hash,
            "row_number_in_file": row_number_in_file,
            "parser_version": parser_version,
            "runner_version": runner_version,
            "git_commit": git_commit,
            "coverage_year": coverage_year,
            "coverage_year_source": coverage_year_source,
            "warning_count": len(file_warnings),
            "insurance_type": insurance_type,
            "enrolleeStatus": enrollee_status,
        }
    )
    enriched["raw_record_hash"] = raw_record_hash(enriched)
    enriched["raw_json"] = build_raw_json(enriched)
    return enriched


def collect_file_warnings(
    source: SourceFile,
    filename_file_year: int | None,
    filename_file_month: int | None,
) -> list[str]:
    warnings: list[str] = []
    if filename_file_year is None or filename_file_month is None:
        msg = f"{source.file_name}: could not parse filename timestamp"
        logger.warning(msg)
        warnings.append(msg)
    return warnings
