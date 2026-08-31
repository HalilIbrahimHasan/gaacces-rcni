"""Azure query helpers — issuer/year SQL fetch + active-coverage month mapping."""

from __future__ import annotations

from typing import Any

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from azure_reconciliation.azure_client import (
    _azure_settings,
    _pick_filter_column,
    list_table_columns,
)
from azure_reconciliation.azure_mirror.columns import resolve_column
from utils.logger import get_logger

logger = get_logger(__name__)

MONTH_MAPPING_METHOD = "benefit_effective_date / benefit_end_date active coverage"

DIAGNOSTIC_DATE_COLUMNS: list[str] = [
    "GAA_Load_Date",
    "enrollment_create_date",
    "enrollment_last_update_date",
    "enrollee_last_update_date",
    "benefit_effective_date",
    "benefit_end_date",
]


def _table_ref(schema: str, table: str) -> str:
    return f"[{schema}].[{table}]"


def month_window(year: str, month: str) -> tuple[pd.Timestamp, pd.Timestamp]:
    """First and last calendar day of a discovered source_data month."""
    y = int(year)
    m = int(str(month).lstrip("0") or "0") or int(month)
    start = pd.Timestamp(year=y, month=m, day=1)
    end = start + pd.offsets.MonthEnd(0)
    return start, end


def build_issuer_year_sql(
    columns: list[str],
    *,
    schema: str,
    table: str,
    issuer: str,
    year: str,
) -> tuple[str, dict[str, Any], str, str | None, str | None]:
    """Build SELECT for issuer + coverage_year only (no month filter)."""
    full_table = _table_ref(schema, table)
    issuer_col = _pick_filter_column(columns, ["hios_issuer_id", "issuer_id", "hios_id"])
    year_col = _pick_filter_column(columns, ["coverage_year", "plan_year", "benefit_year"])

    if not issuer_col:
        raise ValueError(f"Azure table {full_table} missing issuer column")

    clauses = [f"CAST([{issuer_col}] AS VARCHAR(20)) = :issuer"]
    params: dict[str, Any] = {"issuer": str(issuer)}

    if year_col:
        clauses.append(f"CAST([{year_col}] AS VARCHAR(4)) = :year")
        params["year"] = str(year)

    sql = f"SELECT * FROM {full_table} WHERE {' AND '.join(clauses)}"
    return sql, params, full_table, issuer_col, year_col


def fetch_enrollments_issuer_year(
    engine: Engine,
    *,
    issuer: str,
    year: str,
    schema: str | None = None,
    table: str | None = None,
    columns: list[str] | None = None,
) -> tuple[pd.DataFrame, str, dict[str, Any], str]:
    """Fetch rows for issuer + coverage_year only. Returns (df, sql, params, full_table)."""
    cfg = _azure_settings()
    schema = schema or cfg["schema"]
    table = table or cfg["table"]
    full_table = _table_ref(schema, table)

    cols = columns or list_table_columns(engine, schema, table)
    if not cols:
        logger.error("Azure table %s has no readable columns", full_table)
        return pd.DataFrame(), "", {}, full_table

    sql, params, _, _, _ = build_issuer_year_sql(
        cols, schema=schema, table=table, issuer=issuer, year=year
    )
    logger.info("Azure query SQL: %s", sql)
    logger.info("Azure query parameters: %s", params)

    with engine.connect() as conn:
        df = pd.read_sql(text(sql), conn, params=params)

    logger.info(
        "Azure issuer/year base row count: %d (issuer=%s year=%s)",
        len(df), issuer, year,
    )
    return df, sql, params, full_table


def count_missing_benefit_dates(df: pd.DataFrame) -> int:
    """Rows missing benefit_effective_date and/or benefit_end_date."""
    if df.empty:
        return 0
    eff_col = resolve_column(list(df.columns), "benefit_effective_date")
    end_col = resolve_column(list(df.columns), "benefit_end_date")
    if not eff_col or not end_col:
        return len(df)
    eff = pd.to_datetime(df[eff_col], errors="coerce")
    end = pd.to_datetime(df[end_col], errors="coerce")
    return int((eff.isna() | end.isna()).sum())


def filter_by_active_coverage(
    df: pd.DataFrame,
    *,
    year: str,
    month: str,
) -> tuple[pd.DataFrame, str]:
    """
    Map Azure rows to a source_data month using active coverage window.

    Row is active in month when:
      benefit_effective_date <= month_end
      AND benefit_end_date >= month_start

    Rows missing benefit dates are included in every month (coverage_year fallback).
    """
    if df.empty:
        return df, MONTH_MAPPING_METHOD

    eff_col = resolve_column(list(df.columns), "benefit_effective_date")
    end_col = resolve_column(list(df.columns), "benefit_end_date")
    month_start, month_end = month_window(year, month)

    if not eff_col or not end_col:
        logger.warning(
            "Missing benefit date columns; using coverage_year fallback for %s/%s.",
            year, month,
        )
        return df.copy(), "coverage_year_fallback (missing date columns)"

    eff = pd.to_datetime(df[eff_col], errors="coerce")
    end = pd.to_datetime(df[end_col], errors="coerce")
    missing = eff.isna() | end.isna()

    if missing.any():
        logger.warning(
            "Missing benefit dates; using coverage_year fallback for %d row(s) in %s/%s.",
            int(missing.sum()), year, month,
        )

    active = (eff <= month_end) & (end >= month_start)
    mask = active | missing
    filtered = df.loc[mask].copy()
    method = MONTH_MAPPING_METHOD
    if missing.any():
        method = f"{MONTH_MAPPING_METHOD} + coverage_year fallback ({int(missing.sum())} rows)"
    return filtered, method


def map_partitions_active_coverage(
    year_data: dict[str, pd.DataFrame],
    partitions: list,
) -> tuple[dict[str, pd.DataFrame], pd.DataFrame, int]:
    """Build partition frames and active-rows-per-month summary."""
    partition_frames: dict[str, pd.DataFrame] = {}
    summary_rows: list[dict[str, Any]] = []
    missing_total = 0

    for part in partitions:
        base = year_data.get(part.year, pd.DataFrame())
        missing_total += count_missing_benefit_dates(base)
        filtered, method = filter_by_active_coverage(
            base, year=part.year, month=part.month
        )
        partition_frames[part.label()] = filtered
        ms, me = month_window(part.year, part.month)
        summary_rows.append({
            "issuer": part.issuer,
            "year": part.year,
            "month": str(part.month).zfill(2),
            "base_row_count": len(base),
            "active_row_count": len(filtered),
            "month_mapping_method": method,
            "month_start": str(ms.date()),
            "month_end": str(me.date()),
        })

    return partition_frames, pd.DataFrame(summary_rows), missing_total
