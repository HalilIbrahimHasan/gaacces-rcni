"""
Load XML enrollee rows from source_data (primary) or existing staging SQLite.

Never reads from assets/.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd

from azure_reconciliation.data_source_audit import record_xml_load
from config.config import settings
from ingestion.file_discovery import discover_source_files
from ingestion.xml_reader import read_xml_bytes
from parsers.parser_834 import Parser834
from utils.logger import get_logger

logger = get_logger(__name__)

XML_STAGING_COLUMNS = [
    "issuer", "year", "month", "policy_id", "member_id", "subscriber_id",
    "insurance_type_code", "maintenance_type_code", "additional_maint_reason_code",
    "member_maint_effective_date", "benefit_effective_date", "benefit_end_date",
    "transaction_classification", "coverage_status", "raw_xml_path",
    "exchg_assigned_enrollee_id", "request_submit_timestamp",
    "enrollment_action_code", "enrollee_event_type_code", "enrollee_event_reason_code",
    "household_or_employee_case_id", "subscriber_flag", "relationship",
    "qtyn", "qtyy", "qtyt", "file_name",
]


def _parse_source_data(
    issuer_filter: str | None = None,
    year_filter: str | None = None,
    month_filter: str | None = None,
) -> pd.DataFrame:
    """Parse all XML under source_data matching filters."""
    parser = Parser834()
    sources = discover_source_files(
        settings.source_data_path,
        issuer_filter=issuer_filter,
        year_filter=year_filter,
        month_filter=month_filter,
    )
    rows: list[dict] = []
    for src in sources:
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
            for r in records:
                r["file_name"] = src.file_name
            rows.extend(records)
        except Exception as exc:
            logger.error("Parse failed %s: %s", src.file_name, exc)
    if not rows:
        record_xml_load(rows=0, files_read=len(sources), load_path="source_data_direct")
        return pd.DataFrame(columns=XML_STAGING_COLUMNS)
    df = pd.DataFrame(rows)
    record_xml_load(rows=len(df), files_read=len(sources), load_path="source_data_direct")
    logger.info("Parsed %d XML row(s) from source_data", len(df))
    return df


def _load_from_staging(
    db_path: Path,
    issuer_filter: str | None = None,
    year_filter: str | None = None,
    month_filter: str | None = None,
) -> pd.DataFrame:
    """Read stg_834_records from existing pipeline SQLite DB."""
    if not db_path.exists():
        return pd.DataFrame(columns=XML_STAGING_COLUMNS)

    sql = """
        SELECT s.*, f.file_name
        FROM stg_834_records s
        LEFT JOIN raw_file_inventory f ON s.file_id = f.file_id
        WHERE 1=1
    """
    params: list = []
    if issuer_filter:
        sql += " AND s.issuer = ?"
        params.append(issuer_filter)
    if year_filter:
        sql += " AND s.year = ?"
        params.append(year_filter)
    if month_filter:
        sql += " AND s.month = ?"
        params.append(str(month_filter).zfill(2))

    conn = sqlite3.connect(db_path)
    try:
        df = pd.read_sql_query(sql, conn, params=params)
    finally:
        conn.close()
    record_xml_load(rows=len(df), files_read=0, load_path="staging_sqlite")
    logger.info("Loaded %d row(s) from staging SQLite", len(df))
    return df


def load_xml_rows(
    *,
    prefer_staging: bool = True,
    issuer_filter: str | None = None,
    year_filter: str | None = None,
    month_filter: str | None = None,
) -> pd.DataFrame:
    """
    Load XML-derived rows for reconciliation.

    Uses staging DB when populated; otherwise parses source_data directly.
    """
    df = pd.DataFrame()
    if prefer_staging and settings.database_path.exists():
        df = _load_from_staging(
            settings.database_path,
            issuer_filter=issuer_filter,
            year_filter=year_filter,
            month_filter=month_filter,
        )

    if df.empty:
        logger.info("Staging empty or unavailable — parsing source_data directly")
        df = _parse_source_data(
            issuer_filter=issuer_filter,
            year_filter=year_filter,
            month_filter=month_filter,
        )

    if "insurance_type" not in df.columns and "insurance_type_code" in df.columns:
        df = df.copy()
        df["insurance_type"] = df["insurance_type_code"]

    return df


def xml_column_inventory(df: pd.DataFrame) -> list[str]:
    """Return sorted column names present in XML dataset."""
    return sorted(df.columns.astype(str).tolist())
