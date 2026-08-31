"""
Prior-year benefit effective filter — read-only comparison layer.

For reporting year Y:
  - exclude records where benefit_effective_year < Y
  - keep records where benefit_effective_year >= Y
  - keep null benefit_effective_date (flagged in audit)

Does not modify the unfiltered production pipeline or existing exports.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from azure_reconciliation.chandra_nan_safe import is_missing, safe_int, safe_optional_int

from azure_reconciliation.full_data_exports import write_full_export_excel
from azure_reconciliation.safe_export import safe_write_csv, safe_write_excel, ExportErrors
from config.config import settings
from utils.logger import get_logger

logger = get_logger(__name__)

AUDIT_COLUMNS = [
    "issuer",
    "reporting_year",
    "source_file",
    "policy_id",
    "member_id",
    "canonical_enrollment_id",
    "canonical_enrollee_id",
    "benefit_effective_date",
    "benefit_effective_year",
    "selected_transaction_date",
    "selected_transaction_year",
    "filter_action",
    "filter_reason",
]

FILTER_ACTION_EXCLUDE = "EXCLUDE_PRIOR_YEAR"
FILTER_ACTION_KEEP_CURRENT = "KEEP_CURRENT_OR_FUTURE_YEAR"
FILTER_ACTION_KEEP_NULL = "KEEP_NULL_BENEFIT_EFFECTIVE_DATE"

# Prior-year filter uses benefit_effective_date only — never selected_transaction_date.
BENEFIT_DATE_COLS = (
    "benefit_effective_date",
    "benefit_effective_begin_date",
)


def filtered_review_root() -> Path:
    return settings.outputs_path / "business_review_filtered"


def _debug_dir() -> Path:
    d = settings.outputs_path / "debug"
    d.mkdir(parents=True, exist_ok=True)
    return d


def resolve_reporting_year(
    *,
    cli_year: str | None = None,
    partition_year: str | None = None,
) -> str:
    """
    Reporting year precedence: CLI --year → settings.reporting_year → partition year.
    """
    if cli_year and str(cli_year).strip():
        return str(cli_year).strip()
    if settings.reporting_year and str(settings.reporting_year).strip():
        return str(settings.reporting_year).strip()
    if partition_year and str(partition_year).strip():
        return str(partition_year).strip()
    raise ValueError("reporting year required — pass --year, set REPORTING_YEAR, or provide partition year")


def parse_benefit_effective_year(value: Any) -> int | None:
    """Extract 4-digit year from a benefit effective date value."""
    if value is None:
        return None
    s = str(value).strip()
    if not s or s.lower() in ("nan", "none", "nat", ""):
        return None
    dt = pd.to_datetime(s[:10].replace("/", "-"), errors="coerce")
    if pd.notna(dt):
        return int(dt.year)
    if len(s) >= 4 and s[:4].isdigit():
        return int(s[:4])
    return None


def _benefit_date_series(df: pd.DataFrame) -> pd.Series:
    for col in BENEFIT_DATE_COLS:
        if col in df.columns:
            return df[col]
    return pd.Series([""] * len(df), index=df.index, dtype=str)


def _source_file_series(df: pd.DataFrame) -> pd.Series:
    for col in ("source_file", "file_name", "raw_xml_path"):
        if col in df.columns:
            s = df[col].astype(str)
            if col == "raw_xml_path":
                return s.map(lambda p: Path(p).name if p and p != "nan" else "")
            return s
    return pd.Series([""] * len(df), index=df.index, dtype=str)


def _policy_id_series(df: pd.DataFrame) -> pd.Series:
    for col in ("policy_id", "exchg_assigned_policy_id", "health_coverage_policy_no"):
        if col in df.columns:
            return df[col].astype(str)
    return pd.Series([""] * len(df), index=df.index, dtype=str)


def _member_id_series(df: pd.DataFrame) -> pd.Series:
    for col in ("member_id", "exchg_assigned_enrollee_id", "exchg_indiv_identifier", "canonical_enrollee_id"):
        if col in df.columns:
            return df[col].astype(str)
    return pd.Series([""] * len(df), index=df.index, dtype=str)


def _canonical_enrollment_id_series(df: pd.DataFrame) -> pd.Series:
    for col in ("canonical_enrollment_id", "policy_id", "enrollment_id", "health_coverage_policy_no"):
        if col in df.columns:
            return df[col].astype(str)
    return pd.Series([""] * len(df), index=df.index, dtype=str)


def _canonical_enrollee_id_series(df: pd.DataFrame) -> pd.Series:
    for col in ("canonical_enrollee_id", "member_id", "exchg_assigned_enrollee_id"):
        if col in df.columns:
            return df[col].astype(str)
    return pd.Series([""] * len(df), index=df.index, dtype=str)


def _selected_transaction_date_series(df: pd.DataFrame) -> pd.Series:
    if "selected_transaction_date" in df.columns:
        return df["selected_transaction_date"].astype(str)
    return pd.Series([""] * len(df), index=df.index, dtype=str)


def _audit_year_out(y: Any) -> str | int:
    if is_missing(y):
        return ""
    return safe_optional_int(y, default="")


def classify_benefit_filter(
    benefit_effective_year: int | None,
    reporting_year: int | str,
) -> tuple[str, str]:
    """Return (filter_action, filter_reason)."""
    if is_missing(benefit_effective_year):
        return (
            FILTER_ACTION_KEEP_NULL,
            "benefit_effective_date is null — kept and flagged for review",
        )
    y = safe_int(reporting_year, 0)
    bey = safe_int(benefit_effective_year, -1)
    if bey < 0:
        return (
            FILTER_ACTION_KEEP_NULL,
            "benefit_effective_date is null — kept and flagged for review",
        )
    if bey < y:
        return (
            FILTER_ACTION_EXCLUDE,
            f"benefit_effective_year {bey} < reporting_year {y}",
        )
    return (
        FILTER_ACTION_KEEP_CURRENT,
        f"benefit_effective_year {bey} >= reporting_year {y}",
    )


def _benefit_date_column_used(df: pd.DataFrame) -> str | None:
    """Return benefit_effective_date column if present with any non-null values."""
    for col in BENEFIT_DATE_COLS:
        if col in df.columns:
            s = df[col].astype(str).str.strip()
            if s.replace("nan", "").replace("None", "").ne("").any():
                return col
    return None


def _prior_year_candidate_count(df: pd.DataFrame, reporting_year: str) -> int:
    if df.empty:
        return 0
    date_col = _benefit_date_column_used(df)
    if not date_col:
        return 0
    years = df[date_col].map(parse_benefit_effective_year)
    y = safe_int(reporting_year, 0)
    if y <= 0:
        return 0
    return safe_int(((years.notna()) & (years < y)).sum(), 0)


def _raw_filter_status(stats: dict[str, int]) -> str:
    if stats["before"] == 0:
        return "PASS"
    return "PASS" if stats["before"] - stats["excluded"] == stats["after"] else "FAIL"


def _business_ready_filter_note(
    business_df: pd.DataFrame,
    business_audit: pd.DataFrame,
    *,
    reporting_year: str,
) -> str:
    """Explain business-ready filter outcome using benefit_effective_date only."""
    if "benefit_effective_date" not in business_df.columns and not _benefit_date_column_used(business_df):
        return (
            "business_ready export missing benefit_effective_date — "
            "prior-year filter could not be applied; re-run business_ready_exports."
        )
    candidates = _prior_year_candidate_count(business_df, reporting_year)
    excluded = int((business_audit["filter_action"] == FILTER_ACTION_EXCLUDE).sum()) if not business_audit.empty else 0
    if candidates == 0 and excluded == 0:
        return "No business-ready records had prior-year benefit_effective_date values."
    if excluded > 0:
        return (
            f"Excluded {excluded} business-ready record(s) where "
            f"benefit_effective_year < {reporting_year} (filter uses benefit_effective_date only)."
        )
    return (
        f"{candidates} record(s) had benefit_effective_year < {reporting_year} "
        "but were not excluded — review FILTER_AUDIT."
    )


def build_filter_audit(
    df: pd.DataFrame,
    *,
    issuer: str,
    reporting_year: str,
    dataset_label: str = "",
) -> pd.DataFrame:
    """Build per-record audit rows for a dataframe."""
    if df.empty:
        return pd.DataFrame(columns=AUDIT_COLUMNS)

    work = df.copy()
    benefit_dates = _benefit_date_series(work)
    benefit_years = benefit_dates.map(parse_benefit_effective_year)
    benefit_year_out = benefit_years.map(_audit_year_out)
    selected_dates = _selected_transaction_date_series(work)
    selected_years = selected_dates.map(parse_benefit_effective_year)
    selected_year_out = selected_years.map(_audit_year_out)
    actions: list[str] = []
    reasons: list[str] = []
    for bey in benefit_years:
        action, reason = classify_benefit_filter(bey, reporting_year)
        actions.append(action)
        if dataset_label:
            reason = f"[{dataset_label}] {reason}"
        reasons.append(reason)

    return pd.DataFrame({
        "issuer": issuer,
        "reporting_year": str(reporting_year),
        "source_file": _source_file_series(work),
        "policy_id": _policy_id_series(work),
        "member_id": _member_id_series(work),
        "canonical_enrollment_id": _canonical_enrollment_id_series(work),
        "canonical_enrollee_id": _canonical_enrollee_id_series(work),
        "benefit_effective_date": benefit_dates.astype(str),
        "benefit_effective_year": benefit_year_out,
        "selected_transaction_date": selected_dates.astype(str),
        "selected_transaction_year": selected_year_out,
        "filter_action": actions,
        "filter_reason": reasons,
    })


def apply_prior_year_benefit_filter(
    df: pd.DataFrame,
    *,
    reporting_year: str,
) -> pd.DataFrame:
    """Return rows that pass the prior-year benefit effective filter."""
    if df.empty:
        return df.copy()
    audit = build_filter_audit(df, issuer="", reporting_year=reporting_year)
    keep = audit["filter_action"] != FILTER_ACTION_EXCLUDE
    return df.loc[keep.values].reset_index(drop=True)


def _dataset_audit_stats(audit: pd.DataFrame) -> dict[str, int]:
    """Per-dataset counts from a filter audit frame."""
    if audit.empty:
        return {"before": 0, "excluded": 0, "after": 0, "null": 0}
    before = len(audit)
    excluded = safe_int((audit["filter_action"] == FILTER_ACTION_EXCLUDE).sum(), 0)
    after = safe_int((audit["filter_action"] != FILTER_ACTION_EXCLUDE).sum(), 0)
    null_count = safe_int((audit["filter_action"] == FILTER_ACTION_KEEP_NULL).sum(), 0)
    return {"before": before, "excluded": excluded, "after": after, "null": null_count}


def build_filter_summary(
    raw_audit: pd.DataFrame,
    business_audit: pd.DataFrame,
    *,
    reporting_year: str,
    business_df: pd.DataFrame | None = None,
) -> tuple[pd.DataFrame, str, pd.DataFrame]:
    """Summary with separate raw and business-ready sections (not combined primary counts)."""
    raw = _dataset_audit_stats(raw_audit)
    biz = _dataset_audit_stats(business_audit)
    filter_note = _business_ready_filter_note(
        business_df if business_df is not None else pd.DataFrame(),
        business_audit,
        reporting_year=reporting_year,
    )

    summary_df = pd.DataFrame([{
        "reporting_year": str(reporting_year),
        "raw_before_count": raw["before"],
        "raw_excluded_prior_year_count": raw["excluded"],
        "raw_after_count": raw["after"],
        "raw_null_benefit_effective_count": raw["null"],
        "raw_filter_status": _raw_filter_status(raw),
        "business_ready_before_count": biz["before"],
        "business_ready_excluded_prior_year_count": biz["excluded"],
        "business_ready_after_count": biz["after"],
        "business_ready_null_benefit_effective_count": biz["null"],
        "business_ready_filter_status": _raw_filter_status(biz),
        "business_ready_filter_note": filter_note,
    }])

    combined = pd.concat([raw_audit, business_audit], ignore_index=True)
    by_year_rows: list[dict[str, Any]] = []
    if not combined.empty:
        for bey, grp in combined.groupby(
            combined["benefit_effective_year"].fillna("null").astype(str),
            dropna=False,
        ):
            by_year_rows.append({
                "benefit_effective_year": bey,
                "record_count": len(grp),
                "excluded_count": safe_int((grp["filter_action"] == FILTER_ACTION_EXCLUDE).sum(), 0),
                "kept_count": safe_int((grp["filter_action"] != FILTER_ACTION_EXCLUDE).sum(), 0),
            })
    by_year_df = pd.DataFrame(by_year_rows).sort_values("benefit_effective_year")

    md_lines = [
        "# Prior-Year Benefit Effective Filter Summary",
        "",
        f"**Reporting year:** {reporting_year}",
        f"**Generated:** {datetime.now(timezone.utc).isoformat(timespec='seconds')}",
        "",
        "## Rule",
        "",
        f"For reporting year **{reporting_year}**:",
        f"- Exclude records where `benefit_effective_year < {reporting_year}`",
        f"- Keep records where `benefit_effective_year >= {reporting_year}`",
        "- Keep records with null `benefit_effective_date` (flagged in audit)",
        "",
        "## RAW DATASET FILTER SUMMARY",
        "",
        "Use **raw filtered** counts to compare against Chandra raw extract.",
        "",
        f"| Metric | Value |",
        f"|--------|------:|",
        f"| raw_before_count | {raw['before']:,} |",
        f"| raw_excluded_prior_year_count | {raw['excluded']:,} |",
        f"| raw_after_count | {raw['after']:,} |",
        f"| raw_null_benefit_effective_count | {raw['null']:,} |",
        f"| raw_filter_status | {_raw_filter_status(raw)} |",
        "",
        "## BUSINESS READY DATASET FILTER SUMMARY",
        "",
        "Use **business ready filtered** counts for reporting input — not raw extract comparison.",
        "",
        f"| Metric | Value |",
        f"|--------|------:|",
        f"| business_ready_before_count | {biz['before']:,} |",
        f"| business_ready_excluded_prior_year_count | {biz['excluded']:,} |",
        f"| business_ready_after_count | {biz['after']:,} |",
        f"| business_ready_null_benefit_effective_count | {biz['null']:,} |",
        f"| business_ready_filter_status | {_raw_filter_status(biz)} |",
        "",
        f"**Note:** {filter_note}",
        "",
        "## Counts by benefit_effective_year",
        "",
    ]
    if by_year_df.empty:
        md_lines.append("(no records)")
    else:
        md_lines.append("| benefit_effective_year | records | excluded | kept |")
        md_lines.append("|------------------------|--------:|---------:|-----:|")
        for _, row in by_year_df.iterrows():
            md_lines.append(
                f"| {row['benefit_effective_year']} | {int(row['record_count'])} "
                f"| {int(row['excluded_count'])} | {int(row['kept_count'])} |"
            )
    md_lines.extend([
        "",
        "Unfiltered raw and business exports are unchanged. "
        "Filtered outputs live under `outputs/business_review_filtered/`.",
        "",
    ])
    return summary_df, "\n".join(md_lines), by_year_df


def filter_xml_raw_for_reporting(xml_raw: pd.DataFrame, reporting_year: str) -> pd.DataFrame:
    """Apply prior-year benefit filter to XML rows before business pipeline."""
    return apply_prior_year_benefit_filter(xml_raw, reporting_year=reporting_year)


def _load_raw_export(issuer: str, year: str) -> pd.DataFrame:
    path = settings.outputs_path / "full_data_exports" / issuer / year / "raw" / "raw_all_months.csv"
    if path.exists():
        return pd.read_csv(path, dtype=str, low_memory=False)
    xlsx = path.with_suffix(".xlsx")
    if xlsx.exists():
        return pd.read_excel(xlsx, sheet_name=0, dtype=str)
    return pd.DataFrame()


def _load_business_ready_export(issuer: str, year: str) -> pd.DataFrame:
    path = (
        settings.outputs_path / "business_data_exports" / issuer / year
        / "business_ready" / "business_ready_all_months.csv"
    )
    if path.exists():
        return pd.read_csv(path, dtype=str, low_memory=False)
    xlsx = path.with_suffix(".xlsx")
    if xlsx.exists():
        return pd.read_excel(xlsx, sheet_name="BUSINESS_READY_ALL_MONTHS", dtype=str)
    return pd.DataFrame()


def write_filtered_package(
    *,
    issuer: str,
    year: str,
    reporting_year: str,
    raw_df: pd.DataFrame,
    business_df: pd.DataFrame,
) -> dict[str, Any]:
    """Write filtered comparison outputs for one issuer/year."""
    root = filtered_review_root() / issuer / year
    root.mkdir(parents=True, exist_ok=True)

    raw_audit = build_filter_audit(raw_df, issuer=issuer, reporting_year=reporting_year, dataset_label="RAW")
    biz_audit = build_filter_audit(
        business_df, issuer=issuer, reporting_year=reporting_year, dataset_label="BUSINESS_READY",
    )
    filter_audit = pd.concat([raw_audit, biz_audit], ignore_index=True)

    raw_filtered = apply_prior_year_benefit_filter(raw_df, reporting_year=reporting_year)
    biz_filtered = apply_prior_year_benefit_filter(business_df, reporting_year=reporting_year)

    summary_df, summary_md, by_year_df = build_filter_summary(
        raw_audit, biz_audit, reporting_year=reporting_year, business_df=business_df,
    )
    raw_stats = _dataset_audit_stats(raw_audit)
    biz_stats = _dataset_audit_stats(biz_audit)

    comparison_xlsx = root / "filtered_comparison.xlsx"
    write_full_export_excel(
        comparison_xlsx,
        {
            "README": pd.DataFrame({
                "note": [
                    f"Prior-year benefit filter for reporting year {reporting_year}.",
                    "Filter uses benefit_effective_date / benefit_effective_year only.",
                    "selected_transaction_date is included for comparison but NOT used for filtering.",
                    "RAW_FILTERED and BUSINESS_READY_FILTERED exclude benefit_effective_year < reporting_year.",
                    "Null benefit_effective_date rows are kept and flagged in FILTER_AUDIT.",
                    "Original unfiltered exports are not modified.",
                ],
            }),
            "RAW_FILTERED": raw_filtered,
            "BUSINESS_READY_FILTERED": biz_filtered,
            "FILTER_AUDIT": filter_audit,
            "FILTER_SUMMARY": summary_df,
            "COUNTS_BY_BENEFIT_YEAR": by_year_df,
        },
        csv_fallback_paths={
            "RAW_FILTERED": root / "raw_filtered.csv",
            "BUSINESS_READY_FILTERED": root / "business_ready_filtered.csv",
        },
    )
    safe_write_csv(root / "raw_filtered.csv", raw_filtered, table_name="RAW_FILTERED",
                   drop_duplicate_value_columns=False)
    safe_write_csv(
        root / "business_ready_filtered.csv", biz_filtered,
        table_name="BUSINESS_READY_FILTERED", drop_duplicate_value_columns=False,
    )

    readme = root / "README.md"
    readme_body = summary_md + (
        "\n## About this package\n\n"
        "This is the **filtered** Chandra-aligned comparison view.\n"
        "Prior-year benefit effective records are excluded dynamically by reporting year.\n"
        "Filter uses **benefit_effective_date** only — not selected_transaction_date.\n"
        "Full unfiltered source truth is preserved under `outputs/business_review/`.\n"
        "Filtered reports are under `assets_filtered/` and `outputs/xml_business_reports_filtered/`.\n\n"
        "## Files\n\n- `filtered_comparison.xlsx`\n"
    )
    readme.write_text(readme_body, encoding="utf-8")

    return {
        "issuer": issuer,
        "year": year,
        "reporting_year": reporting_year,
        "comparison_xlsx": str(comparison_xlsx),
        "raw_before": len(raw_df),
        "raw_after": len(raw_filtered),
        "raw_excluded": raw_stats["excluded"],
        "business_before": len(business_df),
        "business_after": len(biz_filtered),
        "business_excluded": biz_stats["excluded"],
        "excluded": raw_stats["excluded"] + biz_stats["excluded"],
        "filter_audit": filter_audit,
        "summary_df": summary_df,
        "summary_md": summary_md,
        "by_year_df": by_year_df,
    }


def write_global_diagnostics(results: list[dict[str, Any]]) -> dict[str, str]:
    """Write consolidated debug audit xlsx and summary md."""
    if not results:
        return {}

    all_audit = pd.concat([r["filter_audit"] for r in results], ignore_index=True)
    all_summary = pd.concat([r["summary_df"] for r in results], ignore_index=True)
    by_year_parts = [r["by_year_df"].assign(reporting_year=r["reporting_year"]) for r in results]
    all_by_year = pd.concat(by_year_parts, ignore_index=True) if by_year_parts else pd.DataFrame()

    audit_path = _debug_dir() / "prior_year_benefit_filter_audit.xlsx"
    safe_write_excel(
        audit_path,
        {
            "FILTER_AUDIT": all_audit,
            "SUMMARY": all_summary,
            "COUNTS_BY_BENEFIT_YEAR": all_by_year,
        },
        drop_duplicate_value_columns=False,
    )

    md_parts = [r["summary_md"] for r in results]
    md_path = _debug_dir() / "prior_year_benefit_filter_summary.md"
    md_path.write_text("\n\n---\n\n".join(md_parts), encoding="utf-8")

    logger.info("Wrote filter diagnostics → %s, %s", audit_path, md_path)
    return {"audit_xlsx": str(audit_path), "summary_md": str(md_path)}


def run_prior_year_benefit_filter(
    *,
    issuer: str,
    year: str,
    reporting_year: str | None = None,
) -> dict[str, Any]:
    """Run filtered export for one issuer/year using existing unfiltered exports as input."""
    ry = reporting_year or resolve_reporting_year(partition_year=year)
    logger.info(
        "Prior-year benefit filter: issuer=%s partition_year=%s reporting_year=%s",
        issuer, year, ry,
    )

    raw_df = _load_raw_export(issuer, year)
    business_df = _load_business_ready_export(issuer, year)
    if raw_df.empty and business_df.empty:
        raise RuntimeError(
            f"No raw or business-ready exports found for {issuer}/{year}. "
            "Run full_data_exports and business_ready_exports first."
        )

    result = write_filtered_package(
        issuer=issuer,
        year=year,
        reporting_year=ry,
        raw_df=raw_df,
        business_df=business_df,
    )
    return result


def run_prior_year_benefit_filter_batch(
    pairs: list[tuple[str, str]],
    *,
    reporting_year_override: str | None = None,
) -> dict[str, Any]:
    """Run filter for multiple issuer/year pairs; write global diagnostics."""
    results: list[dict[str, Any]] = []
    for issuer, year in pairs:
        try:
            ry = reporting_year_override or resolve_reporting_year(partition_year=year)
            results.append(run_prior_year_benefit_filter(issuer=issuer, year=year, reporting_year=ry))
        except Exception as exc:
            logger.error("Filter failed for %s/%s: %s", issuer, year, exc)

    diagnostics = write_global_diagnostics(results)
    raw_excluded = sum(r.get("raw_excluded", 0) for r in results)
    biz_excluded = sum(r.get("business_excluded", 0) for r in results)
    ry = results[0]["reporting_year"] if results else ""
    logger.info(
        "FILTER_PRIOR_YEAR_BENEFIT_EFFECTIVE=true reporting_year=%s "
        "excluded_prior_year_raw_count=%d excluded_prior_year_business_ready_count=%d "
        "filtered output root=%s",
        ry, raw_excluded, biz_excluded, filtered_review_root(),
    )
    return {
        "issuer_years": len(results),
        "results": results,
        "diagnostics": diagnostics,
        "output_root": str(filtered_review_root()),
    }


def run_filtered_business_reporting(
    issuer: str,
    year: str,
    *,
    parse_source: bool = False,
    export_errors: ExportErrors | None = None,
) -> dict[str, Any]:
    """
    Generate filtered XML business + assets reports using prior-year benefit filter.

    Writes to assets_filtered/ and xml_business_reports_filtered/ — does not touch unfiltered outputs.
    """
    from azure_reconciliation.partition_discovery import discover_partitions
    from azure_reconciliation.xml_business_reports import (
        export_issuer_reports,
        process_issuer_xml_business,
        write_sqlite_db,
    )
    from azure_reconciliation.xml_loader import load_xml_rows
    from azure_reconciliation.assets_style_reports import export_assets_style_reports

    logger.info("Running filtered reports for issuer=%s, year=%s", issuer, year)
    logger.info("Writing filtered assets to assets_filtered/%s/%s", issuer, year)

    ry = resolve_reporting_year(partition_year=year)
    partitions = discover_partitions(settings.source_data_path, issuer_filter=issuer, year_filter=year)
    xml_raw = load_xml_rows(
        prefer_staging=not parse_source,
        issuer_filter=issuer,
        year_filter=year,
    )
    if xml_raw.empty:
        raise RuntimeError(f"No XML rows for filtered reporting {issuer}/{year}")

    before = len(xml_raw)
    filtered = filter_xml_raw_for_reporting(xml_raw, ry)
    logger.info(
        "Filtered reporting input %s/%s: %d → %d rows (reporting_year=%s)",
        issuer, year, before, len(filtered), ry,
    )

    result = process_issuer_xml_business(issuer, filtered, partitions)
    xml_root = settings.xml_business_reports_filtered_path / issuer
    export_issuer_reports(result, export_errors=export_errors, reports_root=xml_root)
    assets_root = export_assets_style_reports(
        [result], export_errors=export_errors, assets_root=settings.assets_filtered_path,
    )
    db_path = settings.xml_business_reports_filtered_path / "xml_business_filtered.sqlite"
    write_sqlite_db([result], export_errors=export_errors, db_path=db_path)

    return {
        "issuer": issuer,
        "year": year,
        "reporting_year": ry,
        "xml_rows_before": before,
        "xml_rows_after": len(filtered),
        "xml_rows_excluded": before - len(filtered),
        "xml_reports_root": str(xml_root),
        "assets_root": str(assets_root),
        "sqlite": str(db_path),
    }


def run_prior_year_filter_end_to_end(
    *,
    issuer_filter: str | None = None,
    year_filter: str | None = None,
    parse_source: bool = False,
    export_errors: ExportErrors | None = None,
) -> dict[str, Any]:
    """
    End-to-end filtered pipeline: exports → filter comparison → filtered reports.

    Unfiltered outputs are produced separately by callers; this only writes filtered roots.
    """
    from azure_reconciliation.business_ready_exports import run_business_ready_exports
    from azure_reconciliation.full_data_exports import discover_issuer_year_pairs, run_full_data_exports

    if not settings.filter_prior_year_benefit_effective:
        return {"enabled": False}

    pairs = discover_issuer_year_pairs(issuer_filter=issuer_filter, year_filter=year_filter)
    if not pairs:
        logger.warning("Prior-year filter: no issuer/year pairs found")
        return {"enabled": True, "issuer_years": 0}

    logger.info("FILTER_PRIOR_YEAR_BENEFIT_EFFECTIVE=true")
    errors = export_errors or ExportErrors()

    for issuer, year in pairs:
        run_full_data_exports(
            issuer_filter=issuer, year_filter=year,
            parse_source=parse_source, export_errors=errors,
        )
        run_business_ready_exports(
            issuer_filter=issuer, year_filter=year,
            parse_source=parse_source, export_errors=errors,
        )

    reporting_override = resolve_reporting_year(cli_year=year_filter) if year_filter else None
    filter_stats = run_prior_year_benefit_filter_batch(
        pairs, reporting_year_override=reporting_override,
    )

    filtered_reporting: list[dict[str, Any]] = []
    for issuer, year in pairs:
        logger.info("Running filtered reports for issuer=%s, year=%s", issuer, year)
        logger.info("Writing filtered assets to assets_filtered/%s/%s", issuer, year)
        try:
            fr = run_filtered_business_reporting(
                issuer, year, parse_source=parse_source, export_errors=errors,
            )
            filtered_reporting.append(fr)
        except Exception as exc:
            logger.error("Filtered reporting failed for %s/%s: %s", issuer, year, exc)
            errors.record(f"Filtered reporting {issuer}/{year}: {exc}")

    return {
        "enabled": True,
        "filter_stats": filter_stats,
        "filtered_reporting": filtered_reporting,
        "filtered_output_root": str(filtered_review_root()),
        "assets_filtered_root": str(settings.assets_filtered_path),
        "xml_reports_filtered_root": str(settings.xml_business_reports_filtered_path),
    }
