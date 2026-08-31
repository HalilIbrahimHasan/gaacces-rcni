"""
Per-issuer Azure fetch diagnostics — explains zero-row fetches dynamically.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from azure_reconciliation.fixed_azure_candidate import (
    FIXED_DATE_COL,
    build_fixed_profile,
    fixed_table_name,
)
from azure_reconciliation.partition_discovery import Partition
from azure_reconciliation.safe_export import safe_write_csv
from config.config import settings
from utils.logger import get_logger

logger = get_logger(__name__)

ZERO_REASON_ISSUER = "AZURE_ZERO_ROWS_FOR_ISSUER"
ZERO_REASON_SCOPE = "AZURE_ZERO_ROWS_FOR_SOURCE_SCOPE"
ZERO_REASON_DATE = "AZURE_DATE_FILTER_EXCLUDED_ROWS"
ZERO_REASON_MAPPING = "AZURE_ISSUER_MAPPING_NOT_FOUND"


def _distinct_values(df: pd.DataFrame, col: str, limit: int = 50) -> str:
    if df.empty or col not in df.columns:
        return ""
    vals = df[col].dropna().astype(str).str.strip()
    vals = vals[(vals != "") & (vals.str.upper() != "NAN")]
    return ";".join(sorted(vals.unique())[:limit])


def _count_query(engine: Engine, sql: str, params: dict[str, Any]) -> int:
    try:
        with engine.connect() as conn:
            row = conn.execute(text(sql), params).fetchone()
        return int(row[0]) if row else 0
    except Exception as exc:
        logger.warning("Azure diagnostic count failed: %s", exc)
        return -1


def _sample_query(engine: Engine, sql: str, params: dict[str, Any]) -> pd.DataFrame:
    try:
        with engine.connect() as conn:
            return pd.read_sql(text(sql), conn, params=params)
    except Exception as exc:
        logger.warning("Azure diagnostic sample failed: %s", exc)
        return pd.DataFrame()


def classify_zero_row_reason(
    *,
    issuer_only_rows: int,
    scope_year_rows: int,
    date_filtered_rows: int,
    canonical_rows: int,
) -> str:
    """Classify why Azure produced zero canonical rows for an issuer."""
    if canonical_rows > 0:
        return ""
    if issuer_only_rows <= 0:
        return ZERO_REASON_ISSUER
    if scope_year_rows <= 0 and date_filtered_rows <= 0:
        return ZERO_REASON_SCOPE
    if issuer_only_rows > 0 and date_filtered_rows <= 0:
        return ZERO_REASON_DATE
    if date_filtered_rows > 0 and canonical_rows <= 0:
        return ZERO_REASON_MAPPING
    return ZERO_REASON_ISSUER


def run_azure_fetch_diagnostics(
    engine: Engine,
    *,
    issuer: str,
    partitions: list[Partition],
    profile=None,
    schema: str = "dbo",
    canonical_azure: pd.DataFrame | None = None,
) -> tuple[pd.DataFrame, str]:
    """
    Run diagnostic counts for Azure fetch and return (diagnostics_df, zero_row_reason).
    """
    from azure_reconciliation.azure_client import list_table_columns

    issuer_parts = [p for p in partitions if str(p.issuer) == str(issuer)]
    years = sorted({str(p.year) for p in issuer_parts})
    months = sorted({f"{p.year}-{str(p.month).zfill(2)}" for p in issuer_parts})
    scope_label = ", ".join(months) if months else "none"

    cols = list_table_columns(engine, schema, fixed_table_name())
    if profile is None:
        profile = build_fixed_profile(cols)

    full_table = f"[{profile.schema}].[{profile.table}]"
    issuer_col = profile.issuer_col or "GAA_HIOS_ID"
    date_col = profile.file_date_col or FIXED_DATE_COL
    year_col = profile.year_col

    issuer_only_sql = (
        f"SELECT COUNT(*) AS cnt FROM {full_table} "
        f"WHERE CAST([{issuer_col}] AS VARCHAR(20)) = :issuer"
    )
    issuer_only_rows = _count_query(engine, issuer_only_sql, {"issuer": str(issuer)})

    scope_clauses = [f"CAST([{issuer_col}] AS VARCHAR(20)) = :issuer"]
    scope_params: dict[str, Any] = {"issuer": str(issuer)}
    if year_col and years:
        yr_list = ", ".join(f"'{y}'" for y in years)
        scope_clauses.append(f"CAST([{year_col}] AS VARCHAR(4)) IN ({yr_list})")
    scope_sql = f"SELECT COUNT(*) AS cnt FROM {full_table} WHERE {' AND '.join(scope_clauses)}"
    scope_year_rows = _count_query(engine, scope_sql, scope_params)

    date_filtered_rows = 0
    where_samples: list[str] = []
    for part in issuer_parts:
        clauses = [f"CAST([{issuer_col}] AS VARCHAR(20)) = :issuer"]
        params: dict[str, Any] = {"issuer": str(issuer)}
        if year_col:
            clauses.append(f"CAST([{year_col}] AS VARCHAR(4)) = :year")
            params["year"] = str(part.year)
        if date_col and date_col in profile.columns:
            clauses.append(f"YEAR([{date_col}]) = :yr")
            clauses.append(
                f"(MONTH([{date_col}]) = :mo_int OR FORMAT([{date_col}], 'MM') = :mo_str)"
            )
            params["yr"] = int(part.year)
            params["mo_int"] = int(part.month)
            params["mo_str"] = str(part.month).zfill(2)
        where = " AND ".join(clauses)
        where_samples.append(where)
        sql = f"SELECT COUNT(*) AS cnt FROM {full_table} WHERE {where}"
        cnt = _count_query(engine, sql, params)
        if cnt > 0:
            date_filtered_rows += cnt

    sample_sql = (
        f"SELECT TOP 5000 * FROM {full_table} "
        f"WHERE CAST([{issuer_col}] AS VARCHAR(20)) = :issuer"
    )
    sample_df = _sample_query(engine, sample_sql, {"issuer": str(issuer)})

    ins_col = profile.insurance_type_col
    st_col = profile.status_col or "enrolleeStatus"
    distinct_ins = _distinct_values(sample_df, ins_col) if ins_col else ""
    if not distinct_ins:
        for alt in ("planCoverageDescription", "Insurance_Type", "insurance_type"):
            if alt in sample_df.columns:
                distinct_ins = _distinct_values(sample_df, alt)
                if distinct_ins:
                    break
    distinct_status = _distinct_values(sample_df, st_col) if st_col in sample_df.columns else ""

    distinct_years = ""
    distinct_months = ""
    if not sample_df.empty and date_col in sample_df.columns:
        dates = pd.to_datetime(sample_df[date_col], errors="coerce")
        distinct_years = ";".join(sorted(dates.dt.year.dropna().astype(int).astype(str).unique()[:20]))
        distinct_months = ";".join(sorted(dates.dt.month.dropna().astype(int).map(lambda m: f"{m:02d}").unique()[:24]))

    canonical_rows = len(canonical_azure) if isinstance(canonical_azure, pd.DataFrame) else 0
    zero_reason = classify_zero_row_reason(
        issuer_only_rows=max(issuer_only_rows, 0),
        scope_year_rows=max(scope_year_rows, 0),
        date_filtered_rows=max(date_filtered_rows, 0),
        canonical_rows=canonical_rows,
    )

    row = {
        "issuer": str(issuer),
        "source_data_years": ";".join(years),
        "source_data_months": scope_label,
        "sql_table": f"{profile.schema}.{profile.table}",
        "issuer_column": issuer_col,
        "date_column": date_col,
        "year_column": year_col or "",
        "raw_rows_without_date_filter": max(issuer_only_rows, 0),
        "raw_rows_scope_year_only": max(scope_year_rows, 0),
        "raw_rows_with_date_filter": max(date_filtered_rows, 0),
        "raw_rows_after_canonical_normalization": canonical_rows,
        "azure_distinct_years": distinct_years,
        "azure_distinct_months": distinct_months,
        "azure_distinct_insurance_types": distinct_ins,
        "azure_distinct_statuses": distinct_status,
        "query_where_clause_used": " | ".join(where_samples) if where_samples else issuer_only_sql,
        "zero_row_reason": zero_reason,
    }
    return pd.DataFrame([row]), zero_reason


def write_issuer_diagnostics(
    engine: Engine,
    *,
    issuer: str,
    partitions: list[Partition],
    canonical_azure: pd.DataFrame | None = None,
    xml_raw_rows: int = 0,
    xml_files: int = 0,
) -> tuple[Path, str]:
    """Write per-issuer Azure diagnostics and data source audit files."""
    dbg = settings.outputs_path / "debug"
    dbg.mkdir(parents=True, exist_ok=True)

    diag_df, zero_reason = run_azure_fetch_diagnostics(
        engine,
        issuer=issuer,
        partitions=partitions,
        canonical_azure=canonical_azure,
    )
    diag_path = dbg / f"{issuer}_azure_fetch_diagnostics.csv"
    safe_write_csv(diag_path, diag_df, table_name=f"{issuer}_azure_fetch_diagnostics", drop_duplicate_value_columns=False)
    logger.info("Wrote Azure fetch diagnostics: %s (reason=%s)", diag_path, zero_reason or "none")

    if zero_reason:
        reason_path = dbg / f"{issuer}_model_h_zero_row_reason.txt"
        lines = [
            f"issuer: {issuer}",
            f"zero_row_reason: {zero_reason}",
            "",
            "Azure returned zero rows for issuer/scope; comparison cannot determine business match.",
            "",
        ]
        if not diag_df.empty:
            d = diag_df.iloc[0].to_dict()
            lines.extend([
                f"sql_table: {d.get('sql_table')}",
                f"issuer_column: {d.get('issuer_column')}",
                f"date_column: {d.get('date_column')}",
                f"source_data_months: {d.get('source_data_months')}",
                f"raw_rows_without_date_filter: {d.get('raw_rows_without_date_filter')}",
                f"raw_rows_scope_year_only: {d.get('raw_rows_scope_year_only')}",
                f"raw_rows_with_date_filter: {d.get('raw_rows_with_date_filter')}",
                f"raw_rows_after_canonical_normalization: {d.get('raw_rows_after_canonical_normalization')}",
                f"azure_distinct_insurance_types: {d.get('azure_distinct_insurance_types')}",
            ])
        reason_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        logger.info("Wrote zero row reason: %s", reason_path)

    audit_path = dbg / f"{issuer}_data_source_audit.txt"
    issuer_parts = [p for p in partitions if str(p.issuer) == str(issuer)]
    audit_lines = [
        f"DATA SOURCE AUDIT — issuer {issuer}",
        "=" * 40,
        f"issuer_partition: {issuer}",
        f"source_data_partitions: {', '.join(p.label() for p in issuer_parts) or 'none'}",
        f"xml_raw_rows: {xml_raw_rows}",
        f"xml_files_read: {xml_files}",
        f"azure_table: dbo.834_Inbound_test",
        f"azure_zero_row_reason: {zero_reason or 'none'}",
    ]
    if not diag_df.empty:
        d = diag_df.iloc[0]
        audit_lines.extend([
            f"azure_rows_without_date_filter: {d.get('raw_rows_without_date_filter')}",
            f"azure_rows_with_date_filter: {d.get('raw_rows_with_date_filter')}",
            f"azure_canonical_rows: {d.get('raw_rows_after_canonical_normalization')}",
            f"azure_distinct_insurance_types: {d.get('azure_distinct_insurance_types')}",
        ])
    audit_path.write_text("\n".join(audit_lines) + "\n", encoding="utf-8")
    logger.info("Wrote issuer data source audit: %s", audit_path)

    return diag_path, zero_reason
