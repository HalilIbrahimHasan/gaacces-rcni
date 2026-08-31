"""
Raw 834 DB storage estimator — first-layer staging table footprint only.

Estimates storage for stg_834_records (parsed raw 834 XML rows).
Does not run business reporting, canonical, lifecycle, Model H, or exports.
"""

from __future__ import annotations

import sqlite3
from typing import Any

import pandas as pd

from azure_reconciliation.partition_discovery import Partition, discover_partitions
from azure_reconciliation.safe_export import safe_write_csv, safe_write_excel
from config.config import settings
from database.loaders import STG_COLUMNS
from ingestion.file_discovery import discover_source_files
from ingestion.xml_reader import read_xml_bytes
from parsers.parser_834 import Parser834
from utils.logger import get_logger

logger = get_logger(__name__)

STORAGE_METHOD_EXACT = "EXACT_DB_TABLE_SIZE"
STORAGE_METHOD_ESTIMATED = "ESTIMATED_FROM_PARSED_RAW"
STORAGE_METHOD_ALL_ISSUERS = "ESTIMATED_FROM_ALL_ISSUER_PARSED_RAW"
STORAGE_METHOD_MIXED = "MIXED"

# SQLite per-row header / alignment overhead (bytes), applied on top of column payloads.
SQLITE_ROW_OVERHEAD_BYTES = 24

MONTH_LABELS = {
    "01": "Jan", "02": "Feb", "03": "Mar", "04": "Apr",
    "05": "May", "06": "Jun", "07": "Jul", "08": "Aug",
    "09": "Sep", "10": "Oct", "11": "Nov", "12": "Dec",
}

_SUMMARY_COLUMNS = [
    "Year",
    "Month",
    "Raw_834_DB_Storage",
    "Raw_834_DB_Storage_MB",
    "Raw_834_DB_Storage_GB",
    "Parsed_Row_Count",
    "Issuer_Count",
    "Storage_Method",
]

_ALL_ISSUERS_SUMMARY_COLUMNS = [
    "Year",
    "Month",
    "Raw_834_DB_Storage",
    "Raw_834_DB_Storage_MB",
    "Raw_834_DB_Storage_GB",
    "Parsed_Row_Count",
    "Issuer_Count",
    "Source_File_Count",
    "Storage_Method",
]

_DETAIL_COLUMNS = [
    "issuer",
    "year",
    "month",
    "parsed_row_count",
    "raw_db_storage_mb",
    "storage_method",
]


def _zmonth(m: str) -> str:
    return str(m).strip().zfill(2)


def _month_label(zmonth: str) -> str:
    return MONTH_LABELS.get(_zmonth(zmonth), _zmonth(zmonth))


def _bytes_to_mb(num_bytes: int | float) -> float:
    return float(num_bytes) / (1024 * 1024)


def _format_storage(mb: float) -> str:
    """Human-readable storage: MB below 1024 MB, else GB."""
    if mb >= 1024:
        return f"{mb / 1024:.2f} GB"
    if mb >= 100:
        return f"{int(round(mb))} MB"
    return f"{mb:.1f} MB"


def _stg_table_columns(conn: sqlite3.Connection) -> list[str]:
    """Return column names present in stg_834_records (handles schema drift)."""
    rows = conn.execute("PRAGMA table_info(stg_834_records)").fetchall()
    return [str(r[1]) for r in rows]


def _content_bytes_expr(columns: list[str]) -> str:
    """SQL expression summing on-disk payload lengths for available stg columns."""
    parts: list[str] = []
    for col in columns:
        if col == "file_id":
            parts.append("8")  # INTEGER primary-key column
        elif col == "record_id":
            parts.append("8")
        else:
            parts.append(f"LENGTH(COALESCE(CAST({col} AS TEXT), ''))")
    return " + ".join(parts) if parts else "0"


def _exact_db_stats(
    *,
    issuer_filter: str | None,
    year_filter: str | None,
    month_filter: str | None,
) -> dict[tuple[str, str, str], dict[str, Any]]:
    """
    Per-partition stats from stg_834_records when the pipeline SQLite DB exists.

    Returns {(issuer, year, month): {parsed_row_count, storage_bytes, storage_method}}.
    """
    db_path = settings.database_path
    if not db_path.exists():
        return {}

    params: list[str] = []
    where_clauses: list[str] = []
    if issuer_filter:
        issuers = sorted({p.strip() for p in str(issuer_filter).split(",") if p.strip()})
        if issuers:
            where_clauses.append(f"issuer IN ({','.join('?' * len(issuers))})")
            params.extend(issuers)
    if year_filter:
        years = sorted({p.strip() for p in str(year_filter).split(",") if p.strip()})
        if years:
            where_clauses.append(f"year IN ({','.join('?' * len(years))})")
            params.extend(years)
    if month_filter:
        months = sorted({_zmonth(p) for p in str(month_filter).split(",") if p.strip()})
        if months:
            where_clauses.append(f"month IN ({','.join('?' * len(months))})")
            params.extend(months)

    conn = sqlite3.connect(db_path)
    try:
        exists = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='stg_834_records'",
        ).fetchone()
        if not exists:
            return {}
        stg_cols = _stg_table_columns(conn)
        if not stg_cols:
            return {}
        content_sql = _content_bytes_expr(stg_cols)
        sql = f"""
            SELECT
                issuer,
                year,
                month,
                COUNT(*) AS parsed_row_count,
                SUM({content_sql}) + COUNT(*) * {SQLITE_ROW_OVERHEAD_BYTES} AS storage_bytes
            FROM stg_834_records
        """
        if where_clauses:
            sql += " WHERE " + " AND ".join(where_clauses)
        sql += " GROUP BY issuer, year, month"
        rows = conn.execute(sql, params).fetchall()
    finally:
        conn.close()

    out: dict[tuple[str, str, str], dict[str, Any]] = {}
    for issuer, year, month, count, storage_bytes in rows:
        key = (str(issuer), str(year), _zmonth(str(month)))
        existing = out.get(key)
        if existing:
            existing["parsed_row_count"] += int(count)
            existing["storage_bytes"] += int(storage_bytes or 0)
        else:
            out[key] = {
                "parsed_row_count": int(count),
                "storage_bytes": int(storage_bytes or 0),
                "storage_method": STORAGE_METHOD_EXACT,
            }
    if out:
        logger.info("Exact DB stats for %d partition(s) from %s", len(out), db_path)
    return out


def _estimate_storage_bytes_from_records(records: list[dict[str, Any]]) -> int:
    """Estimate stg_834_records footprint from parsed row dicts (no DB insert)."""
    if not records:
        return 0
    total = 0
    for rec in records:
        row_bytes = SQLITE_ROW_OVERHEAD_BYTES + 8  # file_id placeholder
        for col in STG_COLUMNS:
            if col == "file_id":
                continue
            val = rec.get(col)
            if val is None:
                continue
            row_bytes += len(str(val).encode("utf-8"))
        total += row_bytes
    return total


def _parse_partition(part: Partition) -> tuple[int, int, int]:
    """Parse XML for one partition; return (row_count, storage_bytes, source_file_count)."""
    parser = Parser834()
    sources = discover_source_files(
        settings.source_data_path,
        issuer_filter=part.issuer,
        year_filter=part.year,
        month_filter=part.month,
    )
    records: list[dict[str, Any]] = []
    for src in sources:
        try:
            xml_bytes = read_xml_bytes(src)
            parsed = parser.parse_file(
                xml_bytes,
                issuer=src.issuer,
                year=src.year,
                month=src.month,
                file_name=src.file_name,
                file_path=str(src.file_path),
            )
            records.extend(parsed)
        except Exception as exc:
            logger.warning("Parse failed %s: %s", src.file_name, exc)
    return len(records), _estimate_storage_bytes_from_records(records), len(sources)


def build_issuer_month_detail(
    *,
    issuer_filter: str | None = None,
    year_filter: str | None = None,
    month_filter: str | None = None,
) -> pd.DataFrame:
    """Per-issuer/month raw DB storage detail."""
    partitions = discover_partitions(
        issuer_filter=issuer_filter,
        year_filter=year_filter,
        month_filter=month_filter,
        use_env_filters=False,
    )
    if not partitions:
        return pd.DataFrame(columns=_DETAIL_COLUMNS)

    exact = _exact_db_stats(
        issuer_filter=issuer_filter,
        year_filter=year_filter,
        month_filter=month_filter,
    )

    rows: list[dict[str, Any]] = []
    for part in partitions:
        key = (part.issuer, part.year, _zmonth(part.month))
        if key in exact and exact[key]["parsed_row_count"] > 0:
            count = exact[key]["parsed_row_count"]
            storage_bytes = exact[key]["storage_bytes"]
            method = STORAGE_METHOD_EXACT
        else:
            count, storage_bytes, _file_count = _parse_partition(part)
            method = STORAGE_METHOD_ESTIMATED

        mb = _bytes_to_mb(storage_bytes)
        rows.append({
            "issuer": part.issuer,
            "year": part.year,
            "month": _zmonth(part.month),
            "parsed_row_count": count,
            "raw_db_storage_mb": round(mb, 4),
            "storage_method": method,
        })

    return pd.DataFrame(rows).sort_values(["year", "month", "issuer"]).reset_index(drop=True).assign(
        month=lambda df: df["month"].astype(str).map(_zmonth),
    )


def build_monthly_summary(detail: pd.DataFrame) -> pd.DataFrame:
    """Aggregate issuer/month detail to year/month summary rows."""
    if detail.empty:
        return pd.DataFrame(columns=_SUMMARY_COLUMNS)

    rows: list[dict[str, Any]] = []
    for (year, month), grp in detail.groupby(["year", "month"], sort=True):
        total_bytes = float((grp["raw_db_storage_mb"] * 1024 * 1024).sum())
        total_mb = _bytes_to_mb(total_bytes)
        methods = set(grp["storage_method"].astype(str))
        if len(methods) == 1:
            storage_method = next(iter(methods))
        else:
            storage_method = STORAGE_METHOD_MIXED

        rows.append({
            "Year": str(year),
            "Month": _month_label(str(month)),
            "Raw_834_DB_Storage": _format_storage(total_mb),
            "Raw_834_DB_Storage_MB": round(total_mb, 4),
            "Raw_834_DB_Storage_GB": round(total_mb / 1024, 6),
            "Parsed_Row_Count": int(grp["parsed_row_count"].sum()),
            "Issuer_Count": int(grp["issuer"].nunique()),
            "Storage_Method": storage_method,
        })

    return pd.DataFrame(rows)[_SUMMARY_COLUMNS]


def build_all_issuers_partition_detail(
    *,
    issuer_filter: str | None = None,
    year_filter: str | None = None,
    month_filter: str | None = None,
) -> pd.DataFrame:
    """
    Parse every source_data partition and estimate raw DB (stg_834_records) storage.

    When issuer_filter is None, walks source_data/*/year/month for all issuers.
    """
    partitions = discover_partitions(
        issuer_filter=issuer_filter,
        year_filter=year_filter,
        month_filter=month_filter,
        use_env_filters=False,
    )
    if not partitions:
        return pd.DataFrame(columns=[
            "issuer", "year", "month", "parsed_row_count",
            "raw_db_storage_mb", "source_file_count", "storage_method",
        ])

    rows: list[dict[str, Any]] = []
    for i, part in enumerate(partitions, start=1):
        logger.info(
            "Parsing partition %d/%d: %s/%s/%s",
            i, len(partitions), part.issuer, part.year, part.month,
        )
        count, storage_bytes, file_count = _parse_partition(part)
        mb = _bytes_to_mb(storage_bytes)
        rows.append({
            "issuer": part.issuer,
            "year": part.year,
            "month": _zmonth(part.month),
            "parsed_row_count": count,
            "raw_db_storage_mb": round(mb, 4),
            "source_file_count": file_count,
            "storage_method": STORAGE_METHOD_ALL_ISSUERS,
        })

    return pd.DataFrame(rows).sort_values(["year", "month", "issuer"]).reset_index(drop=True)


def build_all_issuers_monthly_summary(detail: pd.DataFrame) -> pd.DataFrame:
    """Aggregate all-issuer partition detail by year/month with a TOTAL row."""
    if detail.empty:
        return pd.DataFrame(columns=_ALL_ISSUERS_SUMMARY_COLUMNS)

    rows: list[dict[str, Any]] = []
    for (year, month), grp in detail.groupby(["year", "month"], sort=True):
        total_mb = float(grp["raw_db_storage_mb"].sum())
        rows.append({
            "Year": str(year),
            "Month": _month_label(str(month)),
            "Raw_834_DB_Storage": _format_storage(total_mb),
            "Raw_834_DB_Storage_MB": round(total_mb, 4),
            "Raw_834_DB_Storage_GB": round(total_mb / 1024, 6),
            "Parsed_Row_Count": int(grp["parsed_row_count"].sum()),
            "Issuer_Count": int(grp["issuer"].nunique()),
            "Source_File_Count": int(grp["source_file_count"].sum()),
            "Storage_Method": STORAGE_METHOD_ALL_ISSUERS,
        })

    summary = pd.DataFrame(rows)
    years = sorted(summary["Year"].astype(str).unique())
    for year in years:
        year_rows = summary[summary["Year"].astype(str) == year]
        year_detail = detail[detail["year"].astype(str) == year]
        total_mb = float(year_rows["Raw_834_DB_Storage_MB"].sum())
        rows.append({
            "Year": str(year),
            "Month": "TOTAL",
            "Raw_834_DB_Storage": _format_storage(total_mb),
            "Raw_834_DB_Storage_MB": round(total_mb, 4),
            "Raw_834_DB_Storage_GB": round(total_mb / 1024, 6),
            "Parsed_Row_Count": int(year_rows["Parsed_Row_Count"].sum()),
            "Issuer_Count": int(year_detail["issuer"].nunique()),
            "Source_File_Count": int(year_rows["Source_File_Count"].sum()),
            "Storage_Method": STORAGE_METHOD_ALL_ISSUERS,
        })

    out = pd.DataFrame(rows)
    month_order = list(MONTH_LABELS.values()) + ["TOTAL"]
    out["_month_sort"] = out["Month"].map(
        lambda m: month_order.index(m) if m in month_order else 99,
    )
    out = out.sort_values(["Year", "_month_sort"]).drop(columns=["_month_sort"])
    return out[_ALL_ISSUERS_SUMMARY_COLUMNS].reset_index(drop=True)


def _write_all_issuers_summary_md(
    summary: pd.DataFrame,
    detail: pd.DataFrame,
    *,
    year_filter: str | None,
) -> str:
    from datetime import datetime, timezone

    lines = [
        "# Raw 834 DB Storage — All Issuers",
        "",
        f"**Generated:** {datetime.now(timezone.utc).isoformat(timespec='seconds')}",
        f"**Year filter:** {year_filter or 'all'}",
        "",
        "Estimated storage for `stg_834_records` (parsed raw 834 XML rows) across all issuers.",
        "This is **not** compressed XML, on-disk XML file size, canonical, business-ready, or report storage.",
        "",
        f"**Storage method:** `{STORAGE_METHOD_ALL_ISSUERS}`",
        "",
        f"- Issuer-month partitions parsed: **{len(detail)}**",
        f"- Distinct issuers: **{detail['issuer'].nunique() if not detail.empty else 0}**",
        f"- Total source XML files: **{int(detail['source_file_count'].sum()) if not detail.empty else 0:,}**",
        f"- Total parsed rows: **{int(detail['parsed_row_count'].sum()) if not detail.empty else 0:,}**",
        "",
        "## Monthly rollup (all issuers)",
        "",
        "| Year | Month | Storage | MB | GB | Rows | Issuers | Files |",
        "|------|-------|---------|---:|---:|-----:|--------:|------:|",
    ]
    for _, row in summary.iterrows():
        lines.append(
            f"| {row['Year']} | {row['Month']} | {row['Raw_834_DB_Storage']} "
            f"| {row['Raw_834_DB_Storage_MB']:,.2f} | {row['Raw_834_DB_Storage_GB']:.4f} "
            f"| {int(row['Parsed_Row_Count']):,} | {int(row['Issuer_Count'])} "
            f"| {int(row['Source_File_Count']):,} |"
        )
    lines.append("")
    return "\n".join(lines)


def run_raw_db_storage_all_issuers(
    *,
    year_filter: str,
    issuer_filter: str | None = None,
    month_filter: str | None = None,
) -> dict[str, Any]:
    """
    Estimate raw 834 DB storage for ALL issuers under source_data and write Snowflake-planning exports.

    Writes only:
      outputs/storage_estimates/raw_834_db_storage_all_issuers.xlsx
      outputs/storage_estimates/raw_834_db_storage_all_issuers.csv
      outputs/storage_estimates/raw_834_db_storage_summary.md
    """
    out_dir = settings.storage_estimates_path
    out_dir.mkdir(parents=True, exist_ok=True)

    detail = build_all_issuers_partition_detail(
        issuer_filter=issuer_filter,
        year_filter=year_filter,
        month_filter=month_filter,
    )
    summary = build_all_issuers_monthly_summary(detail)
    md = _write_all_issuers_summary_md(summary, detail, year_filter=year_filter)

    xlsx_path = out_dir / "raw_834_db_storage_all_issuers.xlsx"
    csv_path = out_dir / "raw_834_db_storage_all_issuers.csv"
    md_path = out_dir / "raw_834_db_storage_summary.md"

    safe_write_excel(
        xlsx_path,
        {
            "raw_834_db_storage_all_issuers": summary,
            "Issuer_Month_Detail": detail,
        },
        drop_duplicate_value_columns=False,
    )
    safe_write_csv(csv_path, summary, drop_duplicate_value_columns=False)
    md_path.write_text(md, encoding="utf-8")

    logger.info(
        "All-issuer raw DB storage: %d summary row(s), %d partition(s), %d issuer(s)",
        len(summary),
        len(detail),
        detail["issuer"].nunique() if not detail.empty else 0,
    )
    return {
        "output_dir": str(out_dir),
        "summary_xlsx": str(xlsx_path),
        "summary_csv": str(csv_path),
        "summary_md": str(md_path),
        "month_rows": len(summary),
        "partition_rows": len(detail),
        "issuer_count": int(detail["issuer"].nunique()) if not detail.empty else 0,
        "summary": summary,
        "detail": detail,
    }


def run_raw_db_storage_only(
    *,
    year_filter: str,
    issuer_filter: str | None = None,
    month_filter: str | None = None,
) -> dict[str, Any]:
    """
    Estimate raw 834 DB (stg_834_records) storage by month and write summary exports.

    Writes only:
      outputs/storage_estimates/raw_834_db_storage_by_month.xlsx
      outputs/storage_estimates/raw_834_db_storage_by_month.csv
    """
    out_dir = settings.storage_estimates_path
    out_dir.mkdir(parents=True, exist_ok=True)

    detail = build_issuer_month_detail(
        issuer_filter=issuer_filter,
        year_filter=year_filter,
        month_filter=month_filter,
    )
    summary = build_monthly_summary(detail)

    xlsx_path = out_dir / "raw_834_db_storage_by_month.xlsx"
    csv_path = out_dir / "raw_834_db_storage_by_month.csv"

    safe_write_excel(
        xlsx_path,
        {
            "raw_834_db_storage_by_month": summary,
            "Issuer_Month_Detail": detail,
        },
        drop_duplicate_value_columns=False,
    )
    safe_write_csv(csv_path, summary, drop_duplicate_value_columns=False)

    logger.info(
        "Raw DB storage estimate: %d month row(s), %d issuer-month partition(s)",
        len(summary), len(detail),
    )
    return {
        "output_dir": str(out_dir),
        "summary_xlsx": str(xlsx_path),
        "summary_csv": str(csv_path),
        "month_rows": len(summary),
        "partition_rows": len(detail),
        "summary": summary,
        "detail": detail,
    }
