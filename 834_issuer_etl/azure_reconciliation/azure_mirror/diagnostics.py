"""Azure query diagnostics before mirror report generation."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy.engine import Engine

from azure_reconciliation.azure_client import _azure_settings
from azure_reconciliation.azure_mirror.columns import (
    log_missing_columns,
    resolve_column,
)
from azure_reconciliation.azure_mirror.query import (
    DIAGNOSTIC_DATE_COLUMNS,
    MONTH_MAPPING_METHOD,
    fetch_enrollments_issuer_year,
    map_partitions_active_coverage,
)
from azure_reconciliation.partition_discovery import Partition
from utils.logger import get_logger

logger = get_logger(__name__)


def _distinct_values(df: pd.DataFrame, canonical: str) -> list[str]:
    actual = resolve_column(list(df.columns), canonical)
    if not actual or actual not in df.columns or df.empty:
        return []
    return sorted(df[actual].dropna().astype(str).unique().tolist())


def _date_min_max(df: pd.DataFrame, canonical: str) -> dict[str, Any]:
    actual = resolve_column(list(df.columns), canonical)
    if not actual or actual not in df.columns or df.empty:
        return {
            "column": canonical,
            "actual_column": actual or "",
            "present": bool(actual),
            "min": None,
            "max": None,
            "non_null_count": 0,
        }
    dates = pd.to_datetime(df[actual], errors="coerce")
    valid = dates.dropna()
    return {
        "column": canonical,
        "actual_column": actual,
        "present": True,
        "min": str(valid.min()) if not valid.empty else None,
        "max": str(valid.max()) if not valid.empty else None,
        "non_null_count": int(valid.shape[0]),
    }


def _discovered_months_table(partitions: list[Partition]) -> pd.DataFrame:
    return pd.DataFrame([
        {"issuer": p.issuer, "year": p.year, "month": str(p.month).zfill(2)}
        for p in partitions
    ])


def log_issuer_diagnostics(
    *,
    table: str,
    issuer: str,
    partitions: list[Partition],
    year_data: dict[str, pd.DataFrame],
    active_rows_per_month: pd.DataFrame,
    missing_benefit_date_count: int,
) -> None:
    logger.info("Azure connection successful")
    logger.info("Azure table used: %s", table)
    for year, df in year_data.items():
        logger.info(
            "Azure issuer/year base row count: issuer=%s year=%s count=%d",
            issuer, year, len(df),
        )
    discovered = _discovered_months_table(partitions)
    logger.info(
        "source_data discovered months for issuer %s: %s",
        issuer,
        discovered.to_dict(orient="records") if not discovered.empty else "[]",
    )
    logger.info("Azure month mapping method: %s", MONTH_MAPPING_METHOD)
    logger.info("Azure missing benefit date count: %d", missing_benefit_date_count)
    if not active_rows_per_month.empty:
        for _, row in active_rows_per_month.iterrows():
            logger.info(
                "Azure active rows per month: %s/%s/%s base=%d active=%d",
                row["issuer"], row["year"], row["month"],
                row["base_row_count"], row["active_row_count"],
            )


def run_issuer_diagnostics(
    engine: Engine,
    *,
    issuer: str,
    partitions: list[Partition],
    table_columns: list[str],
) -> dict[str, Any]:
    cfg = _azure_settings()
    schema, table = cfg["schema"], cfg["table"]
    full_table = f"[{schema}].[{table}]"
    missing = log_missing_columns(table_columns, context=full_table)

    years = sorted({p.year for p in partitions})
    year_data: dict[str, pd.DataFrame] = {}
    year_queries: dict[str, tuple[str, dict[str, Any]]] = {}
    query_log: list[dict[str, Any]] = []

    for year in years:
        df, sql, params, tbl = fetch_enrollments_issuer_year(
            engine,
            issuer=issuer,
            year=year,
            schema=schema,
            table=table,
            columns=table_columns,
        )
        year_data[year] = df
        year_queries[year] = (sql, params)
        query_log.append({
            "issuer": issuer,
            "year": year,
            "month": "(all)",
            "table": tbl,
            "sql": sql,
            "params": str(params),
            "base_row_count": len(df),
            "active_row_count": len(df),
            "month_mapping_method": "issuer+year SQL only",
            "note": "no SQL month filter",
        })

    combined = (
        pd.concat([year_data[y] for y in years if not year_data[y].empty], ignore_index=True)
        if any(not year_data[y].empty for y in years)
        else pd.DataFrame()
    )

    partition_frames, active_rows_per_month, missing_benefit_date_count = (
        map_partitions_active_coverage(year_data, partitions)
    )

    for part in partitions:
        base = year_data.get(part.year, pd.DataFrame())
        sql, params = year_queries.get(part.year, ("", {}))
        active_count = len(partition_frames.get(part.label(), pd.DataFrame()))
        query_log.append({
            "issuer": part.issuer,
            "year": part.year,
            "month": part.month,
            "table": full_table,
            "sql": sql,
            "params": str(params),
            "base_row_count": len(base),
            "active_row_count": active_count,
            "month_mapping_method": MONTH_MAPPING_METHOD,
            "note": "benefit_effective_date <= month_end AND benefit_end_date >= month_start",
        })

    log_issuer_diagnostics(
        table=full_table,
        issuer=issuer,
        partitions=partitions,
        year_data=year_data,
        active_rows_per_month=active_rows_per_month,
        missing_benefit_date_count=missing_benefit_date_count,
    )

    raw_sample = combined.head(500) if not combined.empty else pd.DataFrame({"note": ["no rows"]})
    row_counts_by_year = pd.DataFrame([
        {"year": y, "row_count": len(year_data[y])} for y in years
    ])

    if not combined.empty:
        issuer_col = resolve_column(list(combined.columns), "hios_issuer_id")
        if issuer_col:
            row_counts_by_issuer = (
                combined.groupby(issuer_col).size().reset_index(name="row_count")
                .rename(columns={issuer_col: "hios_issuer_id"})
            )
        else:
            row_counts_by_issuer = pd.DataFrame({
                "hios_issuer_id": [issuer], "row_count": [len(combined)],
            })
    else:
        row_counts_by_issuer = pd.DataFrame(columns=["hios_issuer_id", "row_count"])

    date_min_max = pd.DataFrame([_date_min_max(combined, col) for col in DIAGNOSTIC_DATE_COLUMNS])
    missing_df = (
        pd.DataFrame({"missing_column": missing})
        if missing
        else pd.DataFrame({"note": ["all required columns present"]})
    )

    mapping_summary = pd.DataFrame([{
        "month_mapping_method": MONTH_MAPPING_METHOD,
        "missing_benefit_date_count": missing_benefit_date_count,
        "discovered_partition_count": len(partitions),
        "issuer_year_base_rows": len(combined),
        "total_active_mapped_rows": int(active_rows_per_month["active_row_count"].sum())
        if not active_rows_per_month.empty else 0,
        "distinct_hios_issuer_id": ", ".join(_distinct_values(combined, "hios_issuer_id")),
        "distinct_coverage_year": ", ".join(_distinct_values(combined, "coverage_year")),
    }])

    diagnostic_sheets = {
        "raw_sample": raw_sample,
        "row_counts_by_year": row_counts_by_year,
        "row_counts_by_issuer": row_counts_by_issuer,
        "date_min_max": date_min_max,
        "discovered_source_data_months": _discovered_months_table(partitions),
        "active_rows_per_month": active_rows_per_month,
        "month_mapping_summary": mapping_summary,
        "missing_columns": missing_df,
        "query_log": pd.DataFrame(query_log),
    }

    base_row_total = sum(len(year_data[y]) for y in years)
    active_row_total = sum(len(df) for df in partition_frames.values())

    return {
        "year_data": year_data,
        "partition_frames": partition_frames,
        "combined": combined,
        "month_mapping_method": MONTH_MAPPING_METHOD,
        "active_rows_per_month": active_rows_per_month,
        "missing_benefit_date_count": missing_benefit_date_count,
        "diagnostic_sheets": diagnostic_sheets,
        "full_table": full_table,
        "base_row_total": base_row_total,
        "active_row_total": active_row_total,
    }


def write_query_diagnostic_workbook(path: Path, sheets: dict[str, pd.DataFrame]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        for name, frame in sheets.items():
            export = frame if not frame.empty else pd.DataFrame({"note": ["no data"]})
            export.to_excel(writer, sheet_name=name[:31], index=False)
    logger.info("Azure query diagnostic written: %s", path)
    return path


def write_no_data_html(
    *,
    issuer: str,
    partitions: list[Partition],
    output_path: Path,
    diagnostic_xlsx: Path,
    reason: str,
    base_row_count: int = 0,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    parts_html = "".join(f"<li>{p.issuer}/{p.year}/{p.month}</li>" for p in partitions)
    html = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>Azure — No Data — {issuer}</title>
<style>
body {{ font-family: Arial, sans-serif; margin: 24px; background: #121212; color: #eee; }}
</style></head><body>
<h1>No Azure rows mapped to monthly reports</h1>
<p>Issuer <strong>{issuer}</strong></p>
<p>{reason}</p>
<p>Issuer/year base rows from Azure: <strong>{base_row_count}</strong></p>
<ul>{parts_html}</ul>
<p>See <strong>{diagnostic_xlsx.name}</strong></p>
<p>Month mapping: benefit_effective_date &lt;= month_end AND benefit_end_date &gt;= month_start.
GAA_Load_Date is diagnostic only.</p>
</body></html>"""
    output_path.write_text(html, encoding="utf-8")
    logger.warning("Azure mirror: no mapped data — wrote %s", output_path)
