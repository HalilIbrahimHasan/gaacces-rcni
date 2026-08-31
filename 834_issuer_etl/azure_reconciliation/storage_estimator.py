"""
Read-only storage estimator — file sizes and row counts without deep parsing.

Does not generate business reports.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from azure_reconciliation.partition_discovery import discover_partitions
from azure_reconciliation.safe_export import safe_write_excel
from config.config import settings
from utils.logger import get_logger

logger = get_logger(__name__)

COMPRESS_EXT = {".zip", ".gz", ".gzip", ".tar", ".tgz"}
XML_EXT = {".xml"}


def _dir_size(path: Path) -> int:
    if not path.exists():
        return 0
    if path.is_file():
        return path.stat().st_size
    return sum(f.stat().st_size for f in path.rglob("*") if f.is_file())


def _scan_partition_files(part_path: Path) -> dict[str, Any]:
    xml_count = 0
    xml_bytes = 0
    compressed_bytes = 0
    other_bytes = 0
    if not part_path.exists():
        return {
            "xml_file_count": 0, "xml_bytes": 0,
            "compressed_bytes": 0, "other_bytes": 0, "total_bytes": 0,
        }
    for f in part_path.rglob("*"):
        if not f.is_file():
            continue
        sz = f.stat().st_size
        ext = f.suffix.lower()
        if ext in XML_EXT:
            xml_count += 1
            xml_bytes += sz
        elif ext in COMPRESS_EXT:
            compressed_bytes += sz
        else:
            other_bytes += sz
    return {
        "xml_file_count": xml_count,
        "xml_bytes": xml_bytes,
        "compressed_bytes": compressed_bytes,
        "other_bytes": other_bytes,
        "total_bytes": xml_bytes + compressed_bytes + other_bytes,
    }


def _existing_row_count(issuer: str, year: str, month: str) -> dict[str, int | None]:
    """Lightweight row counts from existing exports if present."""
    out: dict[str, int | None] = {
        "business_ready_rows": None,
        "fast_business_ready_rows": None,
    }
    br_csv = (
        settings.outputs_path / "business_data_exports" / issuer / year
        / "business_ready" / "business_ready_all_months.csv"
    )
    fast_csv = (
        settings.fast_business_reports_path / issuer / year
        / "business_ready" / "business_ready_all_months.csv"
    )
    for key, path in (("business_ready_rows", br_csv), ("fast_business_ready_rows", fast_csv)):
        if path.exists():
            try:
                out[key] = sum(1 for _ in open(path, encoding="utf-8", errors="replace")) - 1
            except OSError:
                pass
    return out


def _estimate_stage_bytes(
    xml_bytes: int,
    xml_count: int,
    business_ready_rows: int | None,
) -> dict[str, int]:
    """Heuristic storage estimates per pipeline stage (bytes)."""
    parsed = int(xml_bytes * 2.5) if xml_bytes else xml_count * 2048
    canonical = int(parsed * 1.1)
    br_rows = business_ready_rows or max(xml_count, 1)
    business_ready = br_rows * 1200
    summary = max(br_rows // 50, 1) * 200
    reporting = summary * 3
    return {
        "raw_xml_bytes": xml_bytes,
        "parsed_table_bytes_est": parsed,
        "canonical_table_bytes_est": canonical,
        "business_ready_table_bytes_est": business_ready,
        "summary_reporting_bytes_est": summary + reporting,
        "total_pipeline_bytes_est": xml_bytes + parsed + canonical + business_ready + summary + reporting,
    }


def build_storage_by_partition(
    *,
    issuer_filter: str | None = None,
    year_filter: str | None = None,
    month_filter: str | None = None,
) -> pd.DataFrame:
    partitions = discover_partitions(
        issuer_filter=issuer_filter,
        year_filter=year_filter,
        month_filter=month_filter,
    )
    rows: list[dict[str, Any]] = []
    for part in partitions:
        scan = _scan_partition_files(part.path)
        row_counts = _existing_row_count(part.issuer, part.year, part.month)
        est = _estimate_stage_bytes(
            scan["xml_bytes"], scan["xml_file_count"],
            row_counts.get("fast_business_ready_rows") or row_counts.get("business_ready_rows"),
        )
        rows.append({
            "issuer": part.issuer,
            "year": part.year,
            "month": part.month,
            **scan,
            **row_counts,
            **est,
        })
    return pd.DataFrame(rows)


def build_snowflake_estimate(by_part: pd.DataFrame) -> pd.DataFrame:
    """Snowflake-style compressed columnar estimate (rough)."""
    if by_part.empty:
        return pd.DataFrame()
    agg = by_part.groupby(["issuer", "year"], as_index=False).agg({
        "xml_bytes": "sum",
        "xml_file_count": "sum",
        "total_pipeline_bytes_est": "sum",
        "business_ready_rows": "sum",
    })
    agg["snowflake_compressed_bytes_est"] = (agg["total_pipeline_bytes_est"] * 0.25).astype(int)
    agg["snowflake_1yr_bytes_est"] = agg["snowflake_compressed_bytes_est"]
    agg["snowflake_3yr_bytes_est"] = (agg["snowflake_compressed_bytes_est"] * 3).astype(int)
    agg["snowflake_5yr_bytes_est"] = (agg["snowflake_compressed_bytes_est"] * 5).astype(int)
    return agg


def build_storage_summary(by_part: pd.DataFrame) -> pd.DataFrame:
    if by_part.empty:
        return pd.DataFrame([{"note": "no partitions found"}])
    total_xml = int(by_part["xml_bytes"].sum())
    total_files = int(by_part["xml_file_count"].sum())
    total_est = int(by_part["total_pipeline_bytes_est"].sum())
    db_bytes = _dir_size(settings.database_path)
    sqlite_bytes = _dir_size(settings.outputs_path / "xml_business_reports" / "xml_business.sqlite")
    fast_bytes = _dir_size(settings.fast_business_reports_path)
    return pd.DataFrame([{
        "generated_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "partition_count": len(by_part),
        "issuer_count": by_part["issuer"].nunique(),
        "xml_file_count": total_files,
        "source_xml_bytes": total_xml,
        "estimated_pipeline_bytes": total_est,
        "estimated_1yr_storage_bytes": total_est,
        "estimated_3yr_storage_bytes": total_est * 3,
        "estimated_5yr_storage_bytes": total_est * 5,
        "sqlite_db_bytes": sqlite_bytes,
        "local_db_bytes": db_bytes,
        "fast_reports_bytes": fast_bytes,
    }])


def _write_summary_md(summary: pd.DataFrame, by_part: pd.DataFrame) -> str:
    lines = [
        "# Storage Estimate Summary",
        "",
        f"**Generated:** {datetime.now(timezone.utc).isoformat(timespec='seconds')}",
        "",
        "Read-only estimate based on source_data file sizes and lightweight row counts.",
        "Does not deep-parse XML.",
        "",
    ]
    if not summary.empty:
        row = summary.iloc[0]
        lines.extend([
            f"- Partitions scanned: **{row.get('partition_count', 0)}**",
            f"- XML files: **{row.get('xml_file_count', 0):,}**",
            f"- Source XML size: **{row.get('source_xml_bytes', 0):,}** bytes",
            f"- Estimated pipeline storage: **{row.get('estimated_pipeline_bytes', 0):,}** bytes",
            f"- Estimated 1-year: **{row.get('estimated_1yr_storage_bytes', 0):,}** bytes",
            f"- Estimated 3-year: **{row.get('estimated_3yr_storage_bytes', 0):,}** bytes",
            f"- Estimated 5-year: **{row.get('estimated_5yr_storage_bytes', 0):,}** bytes",
            "",
        ])
    if not by_part.empty:
        lines.append("## Top partitions by XML size")
        lines.append("")
        lines.append("| issuer | year | month | xml_files | xml_bytes |")
        lines.append("|--------|------|-------|----------:|----------:|")
        top = by_part.nlargest(10, "xml_bytes")
        for _, row in top.iterrows():
            lines.append(
                f"| {row['issuer']} | {row['year']} | {row['month']} "
                f"| {int(row['xml_file_count'])} | {int(row['xml_bytes']):,} |"
            )
    lines.append("")
    return "\n".join(lines)


def run_storage_estimator(
    *,
    issuer_filter: str | None = None,
    year_filter: str | None = None,
    month_filter: str | None = None,
    all_issuers: bool = False,
) -> dict[str, Any]:
    """Run read-only storage estimation."""
    if all_issuers:
        issuer_filter = None

    root = settings.storage_estimates_path
    root.mkdir(parents=True, exist_ok=True)

    by_part = build_storage_by_partition(
        issuer_filter=issuer_filter,
        year_filter=year_filter,
        month_filter=month_filter,
    )
    summary = build_storage_summary(by_part)
    snowflake = build_snowflake_estimate(by_part)
    md = _write_summary_md(summary, by_part)

    summary_xlsx = root / "storage_summary.xlsx"
    snowflake_xlsx = root / "snowflake_storage_estimate.xlsx"
    by_part_xlsx = root / "storage_by_issuer_year_month.xlsx"
    md_path = root / "storage_summary.md"

    safe_write_excel(summary_xlsx, {"Storage_Summary": summary, "By_Partition": by_part})
    safe_write_excel(snowflake_xlsx, {"Snowflake_Estimate": snowflake})
    safe_write_excel(by_part_xlsx, {"By_Issuer_Year_Month": by_part})
    md_path.write_text(md, encoding="utf-8")

    logger.info("Wrote storage estimates → %s", root)
    return {
        "output_root": str(root),
        "summary_xlsx": str(summary_xlsx),
        "snowflake_xlsx": str(snowflake_xlsx),
        "by_part_xlsx": str(by_part_xlsx),
        "summary_md": str(md_path),
        "partitions": len(by_part),
    }
