"""
Month reassignment & cleaned-definition investigation — read-only.

Explains what the full-data export labels as "cleaned", traces where row counts
decrease, and documents every record whose source-folder month differs from the
business month assigned by apply_business_month_basis.

Does NOT modify parser, canonical, cleanup, lifecycle, Model H, or reports.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from azure_reconciliation.dashboard_difference_analysis import (
    _current_counts_by_display,
    _enrollment_id_series,
    _to_display_status,
)
from azure_reconciliation.lifecycle_snapshot_comparison import (
    build_enriched_canonical_xml,
    enrich_month_bases,
)
from azure_reconciliation.partition_discovery import Partition, discover_partitions
from azure_reconciliation.record_comparison import build_canonical_xml_records
from azure_reconciliation.reconciliation_analysis import _dedupe_transactions
from azure_reconciliation.safe_export import safe_write_excel
from azure_reconciliation.status_mapper import normalize_insurance_type
from azure_reconciliation.three_month_business_rule_validation import _resolve_expected_counts
from azure_reconciliation.xml_business_reports import (
    PK,
    _latest_state_per_business_month,
    identify_cleanup_diagnostics,
    process_issuer_xml_business,
)
from azure_reconciliation.xml_loader import load_xml_rows
from config.config import settings
from utils.logger import get_logger

logger = get_logger(__name__)

DISPLAY_STATUSES = ("CONFIRM", "CANCEL", "TERM")

BASIS_TO_REASON = {
    "file_event_year_month": "File Event Date",
    "member_maint_year_month": "Member Maintenance Effective Date",
    "benefit_effective_year_month": "Benefit Effective Date",
    "coverage_year_month": "Coverage Year",
}

DEFAULT_MONTHS = ("01", "02", "03", "04", "05")


def _debug_dir() -> Path:
    d = settings.outputs_path / "debug"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _zmonth(m: Any) -> str:
    return str(m).strip().zfill(2)


def _parse_ym(ym: str) -> tuple[str, str]:
    s = str(ym or "").strip()
    if "-" in s:
        y, m = s.split("-", 1)
        return y, _zmonth(m)
    return "", ""


def _ym_cols(df: pd.DataFrame, ym_col: str, prefix: str) -> pd.DataFrame:
    out = df.copy()
    if ym_col not in out.columns:
        out[f"_{prefix}_year"] = ""
        out[f"_{prefix}_month"] = ""
        return out
    parsed = out[ym_col].astype(str).map(_parse_ym)
    out[f"_{prefix}_year"] = parsed.map(lambda t: t[0])
    out[f"_{prefix}_month"] = parsed.map(lambda t: t[1])
    return out


def _merge_source_metadata(canonical: pd.DataFrame, xml_raw: pd.DataFrame) -> pd.DataFrame:
    if canonical.empty or xml_raw.empty:
        return canonical.copy()

    raw = xml_raw.copy()
    raw["source_year"] = raw["year"].astype(str) if "year" in raw.columns else ""
    raw["source_month"] = raw["month"].astype(str).map(_zmonth) if "month" in raw.columns else ""
    merge_keys = [
        c for c in (
            "policy_id", "member_id", "subscriber_id", "file_name",
            "benefit_effective_date", "member_maint_effective_date",
            "maintenance_type_code", "insurance_type_code",
        )
        if c in canonical.columns and c in raw.columns
    ]
    if merge_keys:
        return canonical.merge(
            raw[merge_keys + ["source_year", "source_month"]].drop_duplicates(),
            on=merge_keys,
            how="left",
        )
    if len(canonical) == len(raw):
        out = canonical.copy()
        out["source_year"] = raw["source_year"].values
        out["source_month"] = raw["source_month"].values
        return out
    out = canonical.copy()
    out["source_year"] = ""
    out["source_month"] = ""
    return out


def _trace_pipeline_stages(
    xml_raw: pd.DataFrame,
    partitions: list[Partition],
    result: Any,
    *,
    year: str,
) -> list[dict[str, Any]]:
    """Row counts at each pipeline stage for the investigation year."""
    raw_year = xml_raw[xml_raw["year"].astype(str) == str(year)] if "year" in xml_raw.columns else xml_raw

    base = build_canonical_xml_records(xml_raw)
    enriched = enrich_month_bases(base)
    pre_business = build_enriched_canonical_xml(xml_raw, None, partitions=partitions)
    pre_year = pre_business[pre_business["year"].astype(str) == str(year)] if not pre_business.empty else pre_business

    canonical = result.canonical
    canon_year = canonical[canonical["year"].astype(str) == str(year)] if not canonical.empty else canonical

    dup, maint, sup, cleanup_sum = identify_cleanup_diagnostics(canonical)
    deduped = _dedupe_transactions(canonical)
    deduped_year = deduped[deduped["year"].astype(str) == str(year)] if not deduped.empty else deduped
    latest = _latest_state_per_business_month(deduped)
    latest_year = latest[latest["year"].astype(str) == str(year)] if not latest.empty else latest

    li = result.lifecycle_input
    li_year = li[li["year"].astype(str) == str(year)] if not li.empty else li

    mh = result.model_h_monthly
    mh_year = mh[mh["year"].astype(str) == str(year)] if not mh.empty and "year" in mh.columns else mh

    dup_y = dup[dup["year"].astype(str) == str(year)] if not dup.empty else dup
    maint_y = maint[maint["year"].astype(str) == str(year)] if not maint.empty else maint
    sup_y = sup[sup["year"].astype(str) == str(year)] if not sup.empty else sup

    enrollment_rows = 0
    if not mh_year.empty and "Enrollment_Count" in mh_year.columns:
        enrollment_rows = int(mh_year["Enrollment_Count"].sum())

    return [
        {
            "stage_name": "1. Raw Parsed XML",
            "row_count": len(raw_year),
            "description": "Direct parser output from source_data XML files.",
            "purpose": "Source of truth for inbound 834 transaction events.",
            "columns_added": "Parser fields (policy_id, member_id, dates, codes, file_name, …)",
            "columns_removed": "None",
            "rows_removed": "No",
            "notes": "Full-data export RAW_ALL_MONTHS sheet.",
        },
        {
            "stage_name": "2. Canonical (build_canonical_xml_records)",
            "row_count": len(base[base["year"].astype(str) == str(year)]) if not base.empty else 0,
            "description": "Normalized field names, IDs, status, dates — 1:1 with raw rows.",
            "purpose": "Common schema for reconciliation and lifecycle.",
            "columns_added": "normalized_status, action_code, _record_key, source_file",
            "columns_removed": "None (raw columns not carried forward)",
            "rows_removed": "No",
            "notes": "Row count should equal raw unless parser drops empty files.",
        },
        {
            "stage_name": "3. Enriched Month Bases (enrich_month_bases)",
            "row_count": len(enriched[enriched["year"].astype(str) == str(year)]) if not enriched.empty else 0,
            "description": "Adds coverage/file/benefit/maint year-month columns; year/month still source folder.",
            "purpose": "Candidate month bases for business-month selection.",
            "columns_added": "coverage_year_month, file_event_year_month, benefit_effective_year_month, member_maint_year_month",
            "columns_removed": "None",
            "rows_removed": "No",
            "notes": "Partition filter may drop rows outside discovered partitions.",
        },
        {
            "stage_name": "4. Pre-Business Canonical (partition-filtered)",
            "row_count": len(pre_year),
            "description": "Canonical + month bases, filtered to issuer/year/month partitions.",
            "purpose": "Input to apply_business_month_basis.",
            "columns_added": "snapshot_source",
            "columns_removed": "None",
            "rows_removed": "Only if partition filter excludes folder",
            "notes": f"Partitions: {len(partitions)}",
        },
        {
            "stage_name": "5. Business Canonical (apply_business_month_basis)",
            "row_count": len(canon_year),
            "description": "year/month OVERWRITTEN for Model H grouping; month_basis_used recorded.",
            "purpose": "Assign business load month per production priority (file → maint → benefit → coverage).",
            "columns_added": "month_basis_used, insurance_type, status",
            "columns_removed": "None",
            "rows_removed": "No",
            "notes": "THIS is what full-data export calls CLEANED_ALL_MONTHS (answer: A — normalized canonical with business month).",
        },
        {
            "stage_name": "6. Cleanup Diagnostics (flags only)",
            "row_count": len(canon_year),
            "description": "Duplicate, maintenance-only, superseded classifications.",
            "purpose": "Explain cleanup; rows are flagged, not removed from canonical.",
            "columns_added": "cleanup_reason (on diagnostic subsets)",
            "columns_removed": "None",
            "rows_removed": "No — flags only",
            "notes": f"dup={len(dup_y)}, maint={len(maint_y)}, superseded={len(sup_y)}",
        },
        {
            "stage_name": "7. Deduped Transactions (_dedupe_transactions)",
            "row_count": len(deduped_year),
            "description": "Removes exact duplicate XML transactions (same PK + status + dates).",
            "purpose": "Collapse redundant events before lifecycle / Model H.",
            "columns_added": "None",
            "columns_removed": "None",
            "rows_removed": "Yes",
            "notes": f"Removed {len(canon_year) - len(deduped_year)} rows in {year}" if len(canon_year) else "",
        },
        {
            "stage_name": "8. Latest State per Business Month",
            "row_count": len(latest_year),
            "description": "Latest transaction per PK within each business year/month after dedupe.",
            "purpose": "Representative row per enrollment per business month.",
            "columns_added": "None",
            "columns_removed": "None",
            "rows_removed": "Yes",
            "notes": "",
        },
        {
            "stage_name": "9. Model H Input (lifecycle_input after collapse)",
            "row_count": len(li_year),
            "description": "Business-transaction-collapsed rows fed to Model H aggregation.",
            "purpose": "Final record-level input before enrollment counting.",
            "columns_added": "Collapse audit fields (when applied)",
            "columns_removed": "None",
            "rows_removed": "Yes (collapse + maintenance-only removal)",
            "notes": "",
        },
        {
            "stage_name": "10. Model H Monthly Summary (aggregated)",
            "row_count": enrollment_rows,
            "description": "Distinct enrollment counts by status/month — NOT record-level.",
            "purpose": "Chandra-like dashboard output.",
            "columns_added": "Enrollment_Count, Enrollee_Count, …",
            "columns_removed": "Record detail",
            "rows_removed": "Yes (aggregation)",
            "notes": f"{len(mh_year)} summary row(s); enrollment sum={enrollment_rows}",
        },
    ]


def _write_cleaned_definition_md(stages: list[dict[str, Any]], *, issuer: str, year: str) -> Path:
    path = _debug_dir() / "cleaned_definition.md"
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")

    lines = [
        "# Cleaned Export Definition — Investigation",
        "",
        f"**Issuer / year:** {issuer} / {year}",
        f"**Generated:** {now}",
        "",
        "## Executive answers",
        "",
        "### 1. What exactly is exported as \"cleaned\"?",
        "",
        "`cleaned_all_months.xlsx` exports **every row from `result.canonical`** after",
        "`apply_business_month_basis()` — the same record-level dataset the business pipeline",
        "uses before deduplication, collapse, and Model H aggregation. Export adds metadata",
        "and boolean flags (duplicate, maintenance-only, superseded, latest-state, Model H",
        "inclusion) but **does not remove rows**.",
        "",
        "### 2. Which option?",
        "",
        "| Option | Matches export? |",
        "|--------|-----------------|",
        "| **A) Normalized canonical** | **YES** — primary answer |",
        "| B) Business-cleaned (rows removed) | NO — flags only, all rows kept |",
        "| C) Latest-state | NO — separate flag column |",
        "| D) Model H input | NO — smaller subset; see stage 9 |",
        "",
        "Yearly raw count == yearly cleaned count because both are **full record-level**",
        "exports filtered to the same source year with **no row removal**.",
        "",
        "### 3. When do record counts actually decrease?",
        "",
        "Counts first decrease at **stage 7 (dedupe)**. Further decreases at latest-state",
        "selection, business-transaction collapse, and Model H aggregation (stages 8–10).",
        "",
        "## Stage-by-stage row counts",
        "",
        "| Stage | Row Count | Rows Removed? | Description |",
        "|-------|-----------|---------------|-------------|",
    ]

    for s in stages:
        lines.append(
            f"| {s['stage_name']} | {s['row_count']:,} | {s['rows_removed']} | {s['description'][:80]} |"
        )

    lines.extend([
        "",
        "## Detailed stage table",
        "",
        "| Stage Name | Row Count | Description | Purpose | Columns Added | Columns Removed | Rows Removed | Notes |",
        "|------------|-----------|-------------|---------|---------------|-----------------|--------------|-------|",
    ])
    for s in stages:
        lines.append(
            f"| {s['stage_name']} | {s['row_count']:,} | {s['description']} | {s['purpose']} "
            f"| {s['columns_added']} | {s['columns_removed']} | {s['rows_removed']} | {s.get('notes', '')} |"
        )

    lines.extend([
        "",
        "## Production impact",
        "",
        "This investigation does **not** recommend changing production logic.",
        "",
    ])
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    logger.info("Wrote %s", path)
    return path


def _why_business_month_changed(row: pd.Series) -> str:
    basis = str(row.get("month_basis_used") or "")
    reason = BASIS_TO_REASON.get(basis, basis or "Other")
    src = f"{row.get('source_folder_year', '')}/{_zmonth(row.get('source_folder_month', ''))}"
    biz = f"{row.get('business_year', '')}/{_zmonth(row.get('business_month', ''))}"
    if basis == "file_event_year_month":
        date_val = row.get("file_event_date") or row.get("event_date") or ""
        return f"File event date {date_val} → business month {biz} (source folder {src})"
    if basis == "member_maint_year_month":
        return f"Member maint effective {row.get('member_maint_effective_date', '')} → {biz} (source {src})"
    if basis == "benefit_effective_year_month":
        return f"Benefit effective {row.get('benefit_effective_date', '')} → {biz} (source {src})"
    if basis == "coverage_year_month":
        return f"Coverage year-month used; business {biz} matches coverage (source folder {src})"
    return f"{reason} assigned business month {biz} instead of source folder {src}"


def _reassignment_reason(row: pd.Series) -> str:
    basis = str(row.get("month_basis_used") or "")
    return BASIS_TO_REASON.get(basis, "Other")


def build_moved_records(
    canonical: pd.DataFrame,
    xml_raw: pd.DataFrame,
    *,
    year: str,
) -> pd.DataFrame:
    """Every record where source-folder month != business month."""
    if canonical.empty:
        return pd.DataFrame()

    work = _merge_source_metadata(canonical, xml_raw)
    work["source_folder_year"] = work.get("source_year", work.get("year", "")).astype(str)
    work["source_folder_month"] = work.get("source_month", "").astype(str).map(_zmonth)
    work["business_year"] = work["year"].astype(str) if "year" in work.columns else ""
    work["business_month"] = work["month"].astype(str).map(_zmonth) if "month" in work.columns else ""
    work["coverage_year"] = work["source_folder_year"]

    if "file_event_date" not in work.columns and "event_date" in work.columns:
        work["file_event_date"] = work["event_date"]

    work = work[work["source_folder_year"].astype(str) == str(year)].copy()
    moved = work[
        work["source_folder_month"].astype(str) != work["business_month"].astype(str)
    ].copy()

    if moved.empty:
        return pd.DataFrame(columns=[
            "issuer", "policy_id", "member_id", "source_folder_year", "source_folder_month",
            "business_year", "business_month", "coverage_year",
            "benefit_effective_date", "benefit_end_date", "member_maint_effective_date",
            "file_event_date", "month_basis_used", "why_business_month_changed",
        ])

    moved["why_business_month_changed"] = moved.apply(_why_business_month_changed, axis=1)
    cols = [
        "issuer", "policy_id", "member_id", "source_folder_year", "source_folder_month",
        "business_year", "business_month", "coverage_year",
        "benefit_effective_date", "benefit_end_date", "member_maint_effective_date",
        "file_event_date", "month_basis_used", "why_business_month_changed",
    ]
    return moved[[c for c in cols if c in moved.columns]].reset_index(drop=True)


def build_month_reassignment_matrix(moved: pd.DataFrame) -> pd.DataFrame:
    if moved.empty:
        return pd.DataFrame(columns=["source_month", "business_month", "count"])
    grp = (
        moved.groupby(
            [moved["source_folder_month"].astype(str).map(_zmonth),
             moved["business_month"].astype(str).map(_zmonth)],
            dropna=False,
        )
        .size()
        .reset_index(name="count")
    )
    grp.columns = ["source_month", "business_month", "count"]
    return grp.sort_values(["source_month", "business_month"]).reset_index(drop=True)


def build_reassignment_reason_summary(moved: pd.DataFrame) -> pd.DataFrame:
    if moved.empty:
        return pd.DataFrame(columns=["reason", "records", "percentage", "example_records"])
    work = moved.copy()
    work["reason"] = work.apply(_reassignment_reason, axis=1)
    total = len(work)
    rows = []
    for reason, grp in work.groupby("reason", dropna=False):
        examples = grp.head(3).apply(
            lambda r: f"{r.get('policy_id','')}/{r.get('member_id','')} "
                      f"{r.get('source_folder_month','')}→{r.get('business_month','')}",
            axis=1,
        ).tolist()
        rows.append({
            "reason": reason,
            "records": len(grp),
            "percentage": round(100.0 * len(grp) / total, 2),
            "example_records": "; ".join(examples),
        })
    return pd.DataFrame(rows).sort_values("records", ascending=False).reset_index(drop=True)


def build_boundary_analysis(moved: pd.DataFrame, *, year: str) -> dict[str, pd.DataFrame]:
    sheets: dict[str, pd.DataFrame] = {}
    if moved.empty:
        for name in (
            "By_Source_Month", "By_Business_Month", "Quarter_Crossings",
            "Year_Crossings", "Month_Pair_Detail",
        ):
            sheets[name] = pd.DataFrame()
        return sheets

    work = moved.copy()
    work["source_month"] = work["source_folder_month"].astype(str).map(_zmonth)
    work["business_month"] = work["business_month"].astype(str).map(_zmonth)

    sheets["By_Source_Month"] = (
        work.groupby("source_month", dropna=False)
        .size()
        .reset_index(name="moved_record_count")
        .sort_values("source_month")
    )
    sheets["By_Business_Month"] = (
        work.groupby("business_month", dropna=False)
        .size()
        .reset_index(name="moved_record_count")
        .sort_values("business_month")
    )

    def _quarter(m: str) -> str:
        mi = int(_zmonth(m))
        return f"Q{(mi - 1) // 3 + 1}"

    work["source_quarter"] = work["source_month"].map(_quarter)
    work["business_quarter"] = work["business_month"].map(_quarter)
    qcross = work[work["source_quarter"] != work["business_quarter"]].copy()
    if qcross.empty:
        sheets["Quarter_Crossings"] = pd.DataFrame(
            columns=["source_quarter", "business_quarter", "source_month", "business_month", "count"],
        )
    else:
        sheets["Quarter_Crossings"] = (
            qcross.groupby(["source_quarter", "business_quarter", "source_month", "business_month"], dropna=False)
            .size()
            .reset_index(name="count")
            .sort_values(["source_quarter", "business_month"])
        )

    work["crosses_year"] = work["business_year"].astype(str) != str(year)
    sheets["Year_Crossings"] = (
        work.groupby(["source_folder_year", "business_year", "source_month", "business_month"], dropna=False)
        .size()
        .reset_index(name="count")
        .sort_values(["source_month", "business_month"])
    )

    boundary_months = {"01", "02", "03"}
    boundary = work[
        work["source_month"].isin(boundary_months) | work["business_month"].isin(boundary_months)
    ]
    sheets["Month_Pair_Detail"] = build_month_reassignment_matrix(boundary)
    return sheets


def _attach_group_months(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col, prefix in (
        ("coverage_year_month", "cov"),
        ("benefit_effective_year_month", "ben"),
        ("member_maint_year_month", "maint"),
        ("file_event_year_month", "file"),
    ):
        out = _ym_cols(out, col, prefix)
    if "source_year" in out.columns:
        out["_src_year"] = out["source_year"].astype(str)
        out["_src_month"] = out["source_month"].astype(str).map(_zmonth)
    else:
        out["_src_year"] = out["year"].astype(str) if "year" in out.columns else ""
        out["_src_month"] = out["month"].astype(str).map(_zmonth) if "month" in out.columns else ""
    return out


def _counts_by_basis_month(
    df: pd.DataFrame,
    *,
    year_col: str,
    month_col: str,
    year: str,
    months: tuple[str, ...],
    insurance_type: str = "HEALTH",
) -> dict[str, dict[str, int]]:
    """Distinct enrollment counts per display status per month for a grouping basis."""
    if df.empty:
        return {m: {s: 0 for s in DISPLAY_STATUSES} for m in months}

    work = df.copy()
    work["_enrollment_id"] = _enrollment_id_series(work)
    if "insurance_type" in work.columns:
        work = work[work["insurance_type"].astype(str).map(normalize_insurance_type) == insurance_type]
    work = work[work[year_col].astype(str) == str(year)]
    work["_display_status"] = work.get("normalized_status", work.get("status", "")).astype(str).map(_to_display_status)

    out: dict[str, dict[str, int]] = {}
    for m in months:
        zm = _zmonth(m)
        sub = work[work[month_col].astype(str).map(_zmonth) == zm]
        out[zm] = {}
        for status in DISPLAY_STATUSES:
            ids = sub.loc[sub["_display_status"] == status, "_enrollment_id"].astype(str).str.strip()
            out[zm][status] = int(ids[ids != ""].nunique())
    return out


def build_chandra_month_basis_comparison(
    result: Any,
    xml_raw: pd.DataFrame,
    *,
    issuer: str,
    year: str,
    months: tuple[str, ...] = DEFAULT_MONTHS,
) -> pd.DataFrame:
    """Compare enrollment counts under alternate month groupings vs Chandra expected."""
    li = result.lifecycle_input.copy()
    canon = _merge_source_metadata(result.canonical, xml_raw)
    merge_keys = [c for c in PK if c in li.columns and c in canon.columns]
    ym_keys = ["year", "month"]
    keys = merge_keys + [k for k in ym_keys if k in li.columns and k in canon.columns]
    enriched_cols = [
        c for c in (
            "coverage_year_month", "benefit_effective_year_month",
            "member_maint_year_month", "file_event_year_month",
            "source_year", "source_month", "month_basis_used",
            "file_event_date", "event_date",
        )
        if c in canon.columns
    ]
    if keys and enriched_cols:
        canon_sub = canon[keys + enriched_cols].drop_duplicates()
        work = li.merge(canon_sub, on=keys, how="left", suffixes=("", "_canon"))
    else:
        work = li.copy()

    work = _attach_group_months(work)

    bases: list[tuple[str, str, str]] = [
        ("Source Folder Month", "_src_year", "_src_month"),
        ("Coverage Month", "_cov_year", "_cov_month"),
        ("Benefit Effective Month", "_ben_year", "_ben_month"),
        ("Maintenance Month", "_maint_year", "_maint_month"),
        ("File Event Month", "_file_year", "_file_month"),
        ("Business Month (Production)", "year", "month"),
    ]

    rows = []
    for basis_name, ycol, mcol in bases:
        counts = _counts_by_basis_month(
            work, year_col=ycol, month_col=mcol, year=year, months=months,
        )
        total_chandra_gap = 0
        month_vals: dict[str, int] = {}
        for m in months:
            zm = _zmonth(m)
            expected = _resolve_expected_counts(issuer, year, zm) or {}
            actual = counts.get(zm, {})
            month_gap = sum(
                abs(int(actual.get(s, 0)) - int(expected.get(s, 0)))
                for s in DISPLAY_STATUSES
            )
            month_vals[zm] = sum(int(actual.get(s, 0)) for s in DISPLAY_STATUSES)
            total_chandra_gap += month_gap

        row: dict[str, Any] = {
            "grouping_basis": basis_name,
            "total_chandra_gap": total_chandra_gap,
        }
        for m in months:
            zm = _zmonth(m)
            row[f"{zm}_count"] = month_vals.get(zm, 0)
            expected = _resolve_expected_counts(issuer, year, zm) or {}
            prod = _current_counts_by_display(
                result.lifecycle_input, issuer=issuer, year=year, month=zm,
                insurance_type="HEALTH",
            )
            if basis_name == "Business Month (Production)":
                row[f"{zm}_diff_from_chandra"] = sum(
                    int(prod.get(s, 0)) - int(expected.get(s, 0)) for s in DISPLAY_STATUSES
                )
            else:
                actual = counts.get(zm, {})
                row[f"{zm}_diff_from_chandra"] = sum(
                    int(actual.get(s, 0)) - int(expected.get(s, 0)) for s in DISPLAY_STATUSES
                )
        rows.append(row)

    df = pd.DataFrame(rows)
    if not df.empty:
        df["rank"] = df["total_chandra_gap"].rank(method="dense").astype(int)
        df = df.sort_values("total_chandra_gap")
    return df


def _monthly_production_gaps(result: Any, *, issuer: str, year: str, months: tuple[str, ...]) -> dict[str, int]:
    gaps = {}
    for m in months:
        zm = _zmonth(m)
        expected = _resolve_expected_counts(issuer, year, zm) or {}
        actual = _current_counts_by_display(
            result.lifecycle_input, issuer=issuer, year=year, month=zm,
            insurance_type="HEALTH",
        )
        gaps[zm] = sum(int(actual.get(s, 0)) - int(expected.get(s, 0)) for s in DISPLAY_STATUSES)
    return gaps


def _write_conclusion_md(
    *,
    issuer: str,
    year: str,
    moved: pd.DataFrame,
    matrix: pd.DataFrame,
    reason_summary: pd.DataFrame,
    production_gaps: dict[str, int],
    chandra_cmp: pd.DataFrame,
) -> Path:
    path = _debug_dir() / "month_reassignment_conclusion.md"
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    moved_count = len(moved)
    net_moved = 0
    if not matrix.empty:
        for _, r in matrix.iterrows():
            sm, bm = _zmonth(r["source_month"]), _zmonth(r["business_month"])
            if sm != bm:
                net_moved += int(r["count"])  # total moved records

    top_reason = ""
    if not reason_summary.empty:
        top_reason = str(reason_summary.iloc[0]["reason"])

    jan_feb_mar_net = sum(production_gaps.get(m, 0) for m in ("01", "02", "03"))
    apr_may_net = sum(production_gaps.get(m, 0) for m in ("04", "05"))

    # Matrix-based explanation
    matrix_note = ""
    if not matrix.empty:
        jan_in = int(matrix.loc[matrix["business_month"] == "01", "count"].sum())
        jan_out = int(matrix.loc[matrix["source_month"] == "01", "count"].sum())
        matrix_note = f"Records landing in business-Jan: {jan_in}; leaving source-Jan: {jan_out}."

    best_basis = ""
    if not chandra_cmp.empty:
        best_basis = str(chandra_cmp.iloc[0]["grouping_basis"])

    lines = [
        "# Month Reassignment — Engineering Conclusion",
        "",
        f"**Issuer / year:** {issuer} / {year}",
        f"**Generated:** {now}",
        "",
        "## 1. Do records actually move between months?",
        "",
        f"**Yes.** {moved_count:,} canonical record(s) have source-folder month ≠ business month.",
        "",
        "## 2. How many?",
        "",
        f"- Total reassigned records: **{moved_count:,}**",
        f"- Distinct source→business month pairs: **{len(matrix)}**",
        f"- {matrix_note}",
        "",
        "## 3. Which date field causes the movement?",
        "",
        f"Primary driver: **{top_reason or 'N/A'}** (from `apply_business_month_basis` priority:",
        "file event → member maintenance → benefit effective → coverage year-month).",
        "",
        "## 4. Is January/February/March difference explained by month reassignment?",
        "",
        f"Production net gap (CONFIRM+CANCEL+TERM) Jan+Feb+Mar: **{jan_feb_mar_net:+d}**.",
        "Month reassignment shifts which business month a record is counted in;",
        "it does not remove records from the yearly total (yearly raw == yearly cleaned).",
        "Reassignment **contributes to** monthly dashboard differences when Chandra groups",
        "by a different month basis than production business month.",
        "",
        "## 5. Could this explain why April and May already match Chandra?",
        "",
        f"Apr+May net production gap: **{apr_may_net:+d}**.",
        "If Apr/May match, fewer records may be reassigned across those months, or Chandra's",
        "implicit month basis aligns with production for those partitions.",
        "",
        "## 6. Should production logic change?",
        "",
        "**NO.** This is an investigation only. Production business-month assignment,",
        "cleanup, lifecycle, and Model H remain frozen.",
        "",
        "## Chandra grouping comparison (diagnostic rank 1)",
        "",
        f"Best alternative grouping by lowest total gap: **{best_basis or 'N/A'}**.",
        "",
        "See `month_basis_comparison_for_chandra.xlsx` for full comparison.",
        "",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    logger.info("Wrote %s", path)
    return path


def run_month_reassignment_investigation(
    *,
    issuer: str,
    year: str,
    parse_source: bool = False,
    months: tuple[str, ...] = DEFAULT_MONTHS,
) -> dict[str, Any]:
    """Run full month reassignment + cleaned-definition investigation."""
    partitions = discover_partitions(issuer_filter=issuer, year_filter=year)
    if not partitions:
        raise RuntimeError(f"No partitions found for {issuer}/{year} under source_data")

    xml_raw = load_xml_rows(
        prefer_staging=not parse_source,
        issuer_filter=issuer,
        year_filter=year,
    )
    if xml_raw.empty:
        raise RuntimeError(f"No XML rows loaded for {issuer}/{year}")

    result = process_issuer_xml_business(issuer, xml_raw, partitions)

    stages = _trace_pipeline_stages(xml_raw, partitions, result, year=year)
    cleaned_md = _write_cleaned_definition_md(stages, issuer=issuer, year=year)

    moved = build_moved_records(result.canonical, xml_raw, year=year)
    matrix = build_month_reassignment_matrix(moved)
    reason_summary = build_reassignment_reason_summary(moved)
    boundary = build_boundary_analysis(moved, year=year)
    chandra_cmp = build_chandra_month_basis_comparison(
        result, xml_raw, issuer=issuer, year=year, months=months,
    )
    production_gaps = _monthly_production_gaps(result, issuer=issuer, year=year, months=months)

    matrix_xlsx = _debug_dir() / "month_reassignment_matrix.xlsx"
    safe_write_excel(
        matrix_xlsx,
        {
            "Matrix": matrix,
            "Moved_Records": moved,
            "Reason_Summary": reason_summary,
        },
        drop_duplicate_value_columns=False,
    )

    reason_xlsx = _debug_dir() / "month_reassignment_reason_summary.xlsx"
    safe_write_excel(
        reason_xlsx,
        {"Reason_Summary": reason_summary, "Moved_Records_Sample": moved.head(500)},
        drop_duplicate_value_columns=False,
    )

    boundary_xlsx = _debug_dir() / "month_boundary_analysis.xlsx"
    safe_write_excel(boundary_xlsx, boundary, drop_duplicate_value_columns=False)

    chandra_xlsx = _debug_dir() / "month_basis_comparison_for_chandra.xlsx"
    safe_write_excel(
        chandra_xlsx,
        {"Month_Basis_Comparison": chandra_cmp, "Production_Monthly_Gaps": pd.DataFrame([
            {"month": m, "net_gap_vs_chandra": production_gaps.get(_zmonth(m), 0)}
            for m in months
        ])},
        drop_duplicate_value_columns=False,
    )

    conclusion_md = _write_conclusion_md(
        issuer=issuer,
        year=year,
        moved=moved,
        matrix=matrix,
        reason_summary=reason_summary,
        production_gaps=production_gaps,
        chandra_cmp=chandra_cmp,
    )

    return {
        "issuer": issuer,
        "year": year,
        "moved_record_count": len(moved),
        "matrix_pairs": len(matrix),
        "production_gaps": production_gaps,
        "cleaned_definition_md": str(cleaned_md),
        "matrix_xlsx": str(matrix_xlsx),
        "reason_xlsx": str(reason_xlsx),
        "boundary_xlsx": str(boundary_xlsx),
        "chandra_xlsx": str(chandra_xlsx),
        "conclusion_md": str(conclusion_md),
        "stages": stages,
    }
