"""Shared raw XML validation helpers — Parser834 only, no business pipeline."""

from __future__ import annotations

import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config.config import settings
from ingestion.file_discovery import discover_source_files
from ingestion.xml_reader import read_xml_bytes
from parsers.parser_834 import Parser834
from src.transform.enrollment_summary import (
    _insurance_type_display,
    _resolve_enrollee_status,
)
from utils.logger import get_logger

logger = get_logger(__name__)

COVERAGE_YEAR = "2025"
ISSUERS = ("13535", "15105", "43802")
OUTPUT_DIR = ROOT / "outputs" / "raw_validation"

# from_43802_GA_834_INDV_20251101013325.xml
_FILENAME_TS_AFTER_INDV = re.compile(
    r"(?:INDV|INVD)_(\d{4})(\d{2})(\d{2})\d{6}\.xml$",
    re.IGNORECASE,
)
_FILENAME_TS_FALLBACK = re.compile(r"_(\d{4})(\d{2})(\d{2})\d{6}\.xml$", re.IGNORECASE)


def _zmonth(month: str | int) -> str:
    return str(int(month)).zfill(2)


def parse_filename_year_month(filename: str) -> tuple[str, str]:
    """
    Parse YYYYMMDD from standard 834 filenames.

    Returns (year, month) as strings, or ("", "") when not parseable.
    """
    name = Path(str(filename)).name
    match = _FILENAME_TS_AFTER_INDV.search(name) or _FILENAME_TS_FALLBACK.search(name)
    if not match:
        return "", ""
    year = match.group(1)
    month = str(int(match.group(2)))
    return year, month


def resolve_status(row: pd.Series) -> str:
    status = _resolve_enrollee_status(row)
    if status is None or (isinstance(status, float) and pd.isna(status)):
        return "UNMAPPED"
    return str(status).strip()


def parse_raw_rows() -> tuple[pd.DataFrame, list[str], list[str], list[str], int]:
    """
    Parse all source_data XML for configured issuers/year.

    Returns:
        work: one row per parsed enrollee with validation columns attached
        months_seen: sorted folder months discovered
        failed_files: parse failure messages
        filename_parse_warnings: files where filename timestamp could not be parsed
        total_source_files: distinct source files discovered
    """
    parser = Parser834()
    source_root = settings.source_data_path
    all_rows: list[dict] = []
    failed_files: list[str] = []
    filename_parse_warnings: list[str] = []
    months_seen: set[str] = set()
    source_files: set[str] = set()

    for issuer in ISSUERS:
        sources = discover_source_files(
            source_root,
            issuer_filter=issuer,
            year_filter=COVERAGE_YEAR,
        )
        for src in sources:
            source_files.add(src.file_name)
            months_seen.add(_zmonth(src.month))
            fn_year, fn_month = parse_filename_year_month(src.file_name)
            if not fn_year or not fn_month:
                msg = f"{src.file_name}: could not parse filename timestamp"
                logger.warning(msg)
                filename_parse_warnings.append(msg)
            try:
                xml_bytes = read_xml_bytes(src)
                records = parser.parse_file(
                    xml_bytes,
                    issuer=src.issuer,
                    year=src.year,
                    month=src.month,
                    file_name=src.file_name,
                    file_path=str(src.file_path),
                )
                for rec in records:
                    rec["source_file"] = src.file_name
                    rec["_filename_file_year"] = fn_year
                    rec["_filename_file_month"] = fn_month
                all_rows.extend(records)
            except Exception as exc:
                msg = f"{src.file_path}: {exc}"
                logger.error("Parse failed %s", msg)
                failed_files.append(msg)

    if not all_rows:
        return pd.DataFrame(), sorted(months_seen), failed_files, filename_parse_warnings, len(source_files)

    work = pd.DataFrame(all_rows)
    work["Issuer"] = work["issuer"].astype(str)
    work["Coverage_Year"] = int(COVERAGE_YEAR)
    work["Folder_FileYear"] = pd.to_numeric(work["year"], errors="coerce").astype("Int64")
    work["Folder_FileMonth"] = pd.to_numeric(work["month"], errors="coerce").astype("Int64")
    # Legacy aliases used by folder-month file-level runner
    work["File_Year"] = work["Folder_FileYear"]
    work["File_Month"] = work["Folder_FileMonth"]
    work["Filename_FileYear"] = work["_filename_file_year"].apply(
        lambda v: pd.NA if not str(v).strip() else int(str(v).strip())
    ).astype("Int64")
    work["Filename_FileMonth"] = work["_filename_file_month"].apply(
        lambda v: pd.NA if not str(v).strip() else int(str(v).strip())
    ).astype("Int64")
    work["Insurance_Type"] = work["insurance_type_code"].map(_insurance_type_display)
    work["enrolleeStatus"] = work.apply(resolve_status, axis=1)
    work["Source_File"] = work["source_file"].astype(str)
    return work, sorted(months_seen), failed_files, filename_parse_warnings, len(source_files)


def build_validation_info(
    *,
    total_raw: int,
    distinct_policies: int,
    distinct_members: int,
    months_processed: list[str],
    total_source_files: int,
    failed_files: list[str],
    output_path: Path,
    report_type: str,
    filename_parse_warnings: list[str] | None = None,
) -> pd.DataFrame:
    warnings = filename_parse_warnings or []
    rows = [
        ("Report type", report_type),
        ("Coverage Year", COVERAGE_YEAR),
        ("Issuers processed", ", ".join(ISSUERS)),
        ("Months processed (folder)", ", ".join(months_processed) if months_processed else "(none)"),
        ("Total source files", total_source_files),
        ("Total raw records", total_raw),
        ("Distinct policies", distinct_policies),
        ("Distinct members", distinct_members),
        ("Failed files", "; ".join(failed_files) if failed_files else "(none)"),
        (
            "Filename timestamp parse warnings",
            "; ".join(warnings) if warnings else "(none)",
        ),
        ("Execution timestamp", datetime.now(timezone.utc).isoformat(timespec="seconds")),
        ("Output path", str(output_path)),
    ]
    return pd.DataFrame(rows, columns=["Field", "Value"])
