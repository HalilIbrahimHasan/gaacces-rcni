"""
XML-only Chandra-like business reporting — no Azure dependency.

Reads source_data dynamically, applies lifecycle cleanup, Model H aggregation,
and writes HTML/XLSX/CSV/SQLite under outputs/xml_business_reports/.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from azure_reconciliation.chandra_nan_safe import safe_int
from azure_reconciliation.lifecycle_engine import build_all_lifecycle_snapshots
from azure_reconciliation.lifecycle_snapshot_comparison import (
    _sort_chronological,
    build_enriched_canonical_xml,
    collapse_to_snapshot,
)
from azure_reconciliation.partition_discovery import Partition, discover_partitions
from azure_reconciliation.record_comparison import (
    LIFECYCLE_PRIMARY_JOIN,
    join_key_series,
)
from azure_reconciliation.reconciliation_analysis import (
    DASHBOARD_GROUP_KEYS,
    MAINT_ACTION_PREFIXES,
    XML_SUBSCRIBER_ID_COLS,
    _chandra_dashboard,
    _dedupe_transactions,
    build_model_h_count_column_audit,
)
from azure_reconciliation.status_mapper import normalize_insurance_type, normalize_status
from azure_reconciliation.safe_export import (
    ExportErrors,
    safe_write_csv,
    safe_write_excel,
    safe_write_html_report,
    safe_write_sqlite,
)
from azure_reconciliation.df_utils import find_col, normalize_id, normalize_id_series
from azure_reconciliation.xml_loader import load_xml_rows
from config.config import settings
from utils.logger import get_logger

logger = get_logger(__name__)

PK = list(LIFECYCLE_PRIMARY_JOIN)

BUSINESS_SUMMARY_COLS = [
    "issuer", "year", "month", "insurance_type", "status",
    "Enrollment_Count", "Enrollee_Count", "Subscriber_Count",
    "Raw_XML_Row_Count", "Canonical_Row_Count", "Latest_State_Record_Count",
    "Duplicate_Count", "Maintenance_Only_Count", "Superseded_Count",
    "Month_Basis_Used",
]

MONTH_BASIS_PRIORITY: list[tuple[str, str]] = [
    ("file_event_year_month", "file_event_year_month"),
    ("member_maint_year_month", "member_maint_year_month"),
    ("benefit_effective_year_month", "benefit_effective_year_month"),
    ("coverage_year_month", "coverage_year_month"),
]


def xml_business_reports_root() -> Path:
    d = settings.xml_business_reports_path
    d.mkdir(parents=True, exist_ok=True)
    return d


def _zmonth(m: str) -> str:
    return str(m).strip().zfill(2)


def _filter_year_month_lifecycle(df: pd.DataFrame, year: str, month: str) -> pd.DataFrame:
    if df.empty:
        return df
    work = df[df["year"].astype(str) == str(year)].copy()
    if work.empty or "month" not in work.columns:
        return work
    return work[work["month"].astype(str).map(_zmonth) == _zmonth(month)].copy()


def _status_series(df: pd.DataFrame) -> pd.Series:
    for c in ("normalized_status", "canonical_status", "status"):
        if c in df.columns:
            return df[c].astype(str).map(normalize_status)
    return pd.Series(["UNKNOWN"] * len(df), index=df.index)


def _insurance_series(df: pd.DataFrame) -> pd.Series:
    for c in ("insurance_type", "insurance_type_code", "Insurance_Type", "planCoverageDescription"):
        if c in df.columns:
            return df[c].astype(str).map(normalize_insurance_type)
    return pd.Series(["UNKNOWN"] * len(df), index=df.index)


def _group_mask(df: pd.DataFrame, row: pd.Series) -> pd.Series:
    if df.empty:
        return pd.Series(dtype=bool)
    work = df.copy()
    work["year"] = work["year"].astype(str).str.zfill(4) if "year" in work.columns else ""
    work["month"] = work["month"].astype(str).str.zfill(2) if "month" in work.columns else ""
    mask = work["issuer"].astype(str) == str(row["issuer"])
    mask &= work["year"].astype(str) == str(row["year"])
    mask &= work["month"].astype(str) == _zmonth(str(row["month"]))
    if "insurance_type" in work.columns:
        mask &= work["insurance_type"].astype(str) == str(row["insurance_type"])
    if "status" in work.columns:
        mask &= _status_series(work).astype(str) == str(row["status"])
    return mask


def apply_business_month_basis(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Choose business load month per row (file_event → maint → benefit → coverage).
    Overwrites year/month for Model H grouping; records month_basis_used.
    """
    if df.empty:
        return df, pd.DataFrame(columns=["month_basis_used", "row_count", "pct"])

    work = df.copy()
    basis_used: list[str] = []
    years: list[str] = []
    months: list[str] = []

    for _, row in work.iterrows():
        chosen = ""
        y, m = "", ""
        for col, label in MONTH_BASIS_PRIORITY:
            ym = str(row.get(col, "") or "").strip()
            if ym and "-" in ym:
                parts = ym.split("-", 1)
                y, m = parts[0], _zmonth(parts[1])
                chosen = label
                break
        if not chosen:
            y = str(row.get("year", "") or "")
            m = _zmonth(str(row.get("month", "") or ""))
            chosen = "coverage_year_month"
        basis_used.append(chosen)
        years.append(y)
        months.append(m)

    work["month_basis_used"] = basis_used
    work["year"] = years
    work["month"] = months
    work["insurance_type"] = _insurance_series(work)
    work["status"] = _status_series(work)

    audit = (
        work.groupby("month_basis_used", dropna=False)
        .size()
        .reset_index(name="row_count")
    )
    total = max(len(work), 1)
    audit["pct"] = (audit["row_count"] / total * 100).round(2)
    return work, audit


def identify_cleanup_diagnostics(
    canonical: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Classify duplicate, maintenance-only, and superseded XML transactions."""
    if canonical.empty:
        empty = pd.DataFrame()
        summary = pd.DataFrame([{
            "raw_canonical_rows": 0,
            "duplicate_count": 0,
            "maintenance_only_count": 0,
            "superseded_count": 0,
            "deduped_count": 0,
            "lifecycle_collapsed_count": 0,
        }])
        return empty, empty, empty, summary

    work = canonical.copy()
    work["insurance_type"] = _insurance_series(work)
    work["status"] = _status_series(work)

    dedupe_keys = [
        c for c in PK + ["normalized_status", "benefit_effective_date", "member_maint_effective_date"]
        if c in work.columns
    ]
    dup_mask = work.duplicated(subset=dedupe_keys, keep="last")
    duplicate_df = work[dup_mask].copy()
    duplicate_df["cleanup_reason"] = "DUPLICATE_XML_TRANSACTION"

    if "action_code" in work.columns:
        ac = work["action_code"].astype(str).str.strip().str[:3]
        maint_mask = ac.isin(MAINT_ACTION_PREFIXES)
    elif "maintenance_type_code" in work.columns:
        ac = work["maintenance_type_code"].astype(str).str.strip().str[:3]
        maint_mask = ac.isin(MAINT_ACTION_PREFIXES)
    else:
        maint_mask = pd.Series([False] * len(work), index=work.index)
    maintenance_df = work[maint_mask].copy()
    maintenance_df["cleanup_reason"] = "MAINTENANCE_ONLY_EVENT"

    sorted_work = _sort_chronological(work)
    sorted_work["_pk"] = join_key_series(sorted_work, PK)
    final_idx = sorted_work.groupby("_pk", dropna=False).tail(1).index
    superseded_df = sorted_work[~sorted_work.index.isin(final_idx)].copy()
    superseded_df["cleanup_reason"] = "SUPERSEDED_BY_LATER_XML_EVENT"

    deduped = _dedupe_transactions(work)
    lifecycle_collapsed = collapse_to_snapshot(deduped, PK)

    summary = pd.DataFrame([{
        "raw_canonical_rows": len(work),
        "duplicate_count": int(dup_mask.sum()),
        "maintenance_only_count": int(maint_mask.sum()),
        "superseded_count": len(superseded_df),
        "deduped_count": len(deduped),
        "lifecycle_collapsed_count": len(lifecycle_collapsed),
    }])
    return duplicate_df, maintenance_df, superseded_df, summary


def _latest_state_per_business_month(df: pd.DataFrame) -> pd.DataFrame:
    """Latest lifecycle state per PK within each business year/month — no cross-month stack."""
    if df.empty:
        return df
    work = _sort_chronological(_dedupe_transactions(df))
    group_keys = [k for k in PK + ["year", "month"] if k in work.columns]
    if len(group_keys) <= len([k for k in PK if k in work.columns]):
        return collapse_to_snapshot(work, PK)
    return work.groupby(group_keys, dropna=False, as_index=False).last()


def _build_model_h_lifecycle_input(
    canonical: pd.DataFrame,
    lifecycle_snap: pd.DataFrame,
) -> tuple[pd.DataFrame, Any]:
    """
    Model H input: deduped canonical with business transaction collapse applied.

    Does not stack all partition lifecycle snapshots or re-apply business month basis
    to replay output (that path multiplied rows across months).
    """
    from azure_reconciliation.business_transaction_collapse import (
        CollapseResult,
        apply_business_transaction_collapse,
    )

    deduped = _dedupe_transactions(canonical)
    if not deduped.empty:
        collapse_result = apply_business_transaction_collapse(
            deduped,
            fallback_latest_state_fn=_latest_state_per_business_month,
        )
        return collapse_result.collapsed, collapse_result

    empty_result = CollapseResult(collapsed=pd.DataFrame(), applied=False)
    if lifecycle_snap.empty:
        return pd.DataFrame(), empty_result

    mapped = lifecycle_snapshots_to_model_input(lifecycle_snap)
    dedupe_keys = [k for k in PK + ["year", "month"] if k in mapped.columns]
    if dedupe_keys:
        return mapped.drop_duplicates(subset=dedupe_keys, keep="last"), empty_result
    return mapped, empty_result


def lifecycle_snapshots_to_model_input(snapshots: pd.DataFrame) -> pd.DataFrame:
    """Map lifecycle replay output to Model H-compatible canonical shape."""
    if snapshots.empty:
        return snapshots
    out = snapshots.copy()
    out["policy_id"] = out.get("enrollment_id", "")
    out["member_id"] = out.get("enrollee_id", "")
    out["year"] = out.get("coverage_year", out.get("last_event_year", ""))
    out["month"] = out.get("snapshot_month", out.get("last_event_month", "")).astype(str).map(_zmonth)
    out["normalized_status"] = out.get("canonical_status", "UNKNOWN").astype(str).map(normalize_status)
    out["status"] = out["normalized_status"]
    out["insurance_type"] = out.get("insurance_type", "UNKNOWN").astype(str).map(normalize_insurance_type)
    return out


def _attach_canonical_subscriber_columns(
    lifecycle_input: pd.DataFrame,
    canonical: pd.DataFrame,
) -> pd.DataFrame:
    """
    Copy subscriber ID columns from canonical onto Model H input.

    Lifecycle replay does not emit subscriber fields; Model H coalescing requires
    them on the input frame. Uses existing canonical values and primary join keys only.
    """
    if lifecycle_input.empty or canonical.empty:
        return lifecycle_input

    merge_keys = [k for k in PK if k in lifecycle_input.columns and k in canonical.columns]
    if len(merge_keys) < 3:
        return lifecycle_input

    sub_cols: list[str] = []
    for cand in XML_SUBSCRIBER_ID_COLS:
        col = find_col(canonical, cand)
        if col and col not in merge_keys and col not in sub_cols:
            sub_cols.append(col)

    if not sub_cols:
        return lifecycle_input

    canon_pick = collapse_to_snapshot(canonical, PK)
    pick = merge_keys + sub_cols
    canon_sub = canon_pick[pick].drop_duplicates(subset=merge_keys, keep="last")
    if canon_sub.duplicated(subset=merge_keys).any():
        canon_sub = canon_sub.drop_duplicates(subset=merge_keys, keep="last")
    before = len(lifecycle_input)
    merged = lifecycle_input.merge(
        canon_sub,
        on=merge_keys,
        how="left",
        validate="many_to_one",
    )
    if len(merged) != before:
        logger.error(
            "Subscriber attach changed row count %d → %d (merge_keys=%s)",
            before, len(merged), merge_keys,
        )
    attached = sum(
        int((normalize_id_series(merged[c]).astype(str).str.strip() != "").sum())
        for c in sub_cols if c in merged.columns
    )
    if attached:
        logger.info(
            "Attached %d subscriber field value(s) from canonical onto Model H input",
            attached,
        )
    return merged


def build_extended_business_summary(
    model_h: pd.DataFrame,
    *,
    xml_raw: pd.DataFrame,
    canonical: pd.DataFrame,
    lifecycle_input: pd.DataFrame,
    duplicate_df: pd.DataFrame,
    maintenance_df: pd.DataFrame,
    superseded_df: pd.DataFrame,
) -> pd.DataFrame:
    """Chandra-like summary with cleanup diagnostics per dashboard group."""
    if model_h.empty:
        return pd.DataFrame(columns=BUSINESS_SUMMARY_COLS)

    raw_prep = xml_raw.copy() if not xml_raw.empty else pd.DataFrame()
    if not raw_prep.empty:
        raw_prep["issuer"] = raw_prep.get("issuer", "").astype(str)
        raw_prep["year"] = raw_prep.get("year", "").astype(str)
        raw_prep["month"] = raw_prep.get("month", "").astype(str).map(_zmonth)
        raw_prep["insurance_type"] = _insurance_series(raw_prep)
        raw_prep["status"] = _status_series(raw_prep)

    rows: list[dict[str, Any]] = []
    for _, row in model_h.iterrows():
        gmask_c = _group_mask(canonical, row) if not canonical.empty else pd.Series(dtype=bool)
        gmask_l = _group_mask(lifecycle_input, row) if not lifecycle_input.empty else pd.Series(dtype=bool)
        gmask_r = _group_mask(raw_prep, row) if not raw_prep.empty else pd.Series(dtype=bool)
        gmask_d = _group_mask(duplicate_df, row) if not duplicate_df.empty else pd.Series(dtype=bool)
        gmask_m = _group_mask(maintenance_df, row) if not maintenance_df.empty else pd.Series(dtype=bool)
        gmask_s = _group_mask(superseded_df, row) if not superseded_df.empty else pd.Series(dtype=bool)

        month_basis = ""
        if gmask_c.any() and "month_basis_used" in canonical.columns:
            vc = canonical.loc[gmask_c, "month_basis_used"].value_counts()
            if not vc.empty:
                month_basis = str(vc.index[0])

        rows.append({
            "issuer": row["issuer"],
            "year": row["year"],
            "month": _zmonth(str(row["month"])),
            "insurance_type": row["insurance_type"],
            "status": row["status"],
            "Enrollment_Count": safe_int(row.get("enrollment_count", 0), 0),
            "Enrollee_Count": safe_int(row.get("enrollee_count", 0), 0),
            "Subscriber_Count": safe_int(row.get("subscriber_count", 0), 0),
            "Raw_XML_Row_Count": safe_int(gmask_r.sum(), 0),
            "Canonical_Row_Count": safe_int(gmask_c.sum(), 0),
            "Latest_State_Record_Count": safe_int(gmask_l.sum(), 0),
            "Duplicate_Count": safe_int(gmask_d.sum(), 0),
            "Maintenance_Only_Count": safe_int(gmask_m.sum(), 0),
            "Superseded_Count": safe_int(gmask_s.sum(), 0),
            "Month_Basis_Used": month_basis,
        })
    return pd.DataFrame(rows)


def yearly_rollup(monthly: pd.DataFrame) -> pd.DataFrame:
    """Sum monthly business summary to issuer/year grain."""
    if monthly.empty:
        return monthly
    sum_cols = [
        "Enrollment_Count", "Enrollee_Count", "Subscriber_Count",
        "Raw_XML_Row_Count", "Canonical_Row_Count", "Latest_State_Record_Count",
        "Duplicate_Count", "Maintenance_Only_Count", "Superseded_Count",
    ]
    keys = ["issuer", "year", "insurance_type", "status"]
    agg = {c: "sum" for c in sum_cols if c in monthly.columns}
    out = monthly.groupby(keys, dropna=False).agg(agg).reset_index()
    out["month"] = "ALL"
    if "Month_Basis_Used" in monthly.columns:
        basis = monthly.groupby(keys, dropna=False)["Month_Basis_Used"].agg(
            lambda s: s.mode().iloc[0] if not s.mode().empty else ""
        ).reset_index()
        out = out.merge(basis, on=keys, how="left")
    return out[BUSINESS_SUMMARY_COLS] if all(c in out.columns for c in BUSINESS_SUMMARY_COLS) else out


@dataclass
class IssuerBusinessResult:
    issuer: str
    partitions: list[Partition]
    xml_raw: pd.DataFrame = field(default_factory=pd.DataFrame)
    canonical: pd.DataFrame = field(default_factory=pd.DataFrame)
    lifecycle_snapshots: pd.DataFrame = field(default_factory=pd.DataFrame)
    lifecycle_input: pd.DataFrame = field(default_factory=pd.DataFrame)
    duplicate_df: pd.DataFrame = field(default_factory=pd.DataFrame)
    maintenance_df: pd.DataFrame = field(default_factory=pd.DataFrame)
    superseded_df: pd.DataFrame = field(default_factory=pd.DataFrame)
    cleanup_summary: pd.DataFrame = field(default_factory=pd.DataFrame)
    month_basis_audit: pd.DataFrame = field(default_factory=pd.DataFrame)
    count_column_audit: pd.DataFrame = field(default_factory=pd.DataFrame)
    model_h_monthly: pd.DataFrame = field(default_factory=pd.DataFrame)
    business_monthly: pd.DataFrame = field(default_factory=pd.DataFrame)
    business_yearly: pd.DataFrame = field(default_factory=pd.DataFrame)
    business_rollup: pd.DataFrame = field(default_factory=pd.DataFrame)
    collapse_result: Any = None


def process_issuer_xml_business(
    issuer: str,
    xml_raw: pd.DataFrame,
    partitions: list[Partition],
) -> IssuerBusinessResult:
    """Full XML-only cleanup + Model H pipeline for one issuer."""
    result = IssuerBusinessResult(issuer=issuer, partitions=partitions, xml_raw=xml_raw)

    canonical = build_enriched_canonical_xml(xml_raw, None, partitions=partitions)
    canonical, month_audit = apply_business_month_basis(canonical)
    result.canonical = canonical
    result.month_basis_audit = month_audit

    dup, maint, sup, cleanup_sum = identify_cleanup_diagnostics(canonical)
    result.duplicate_df = dup
    result.maintenance_df = maint
    result.superseded_df = sup
    result.cleanup_summary = cleanup_sum

    lifecycle_snap = build_all_lifecycle_snapshots(xml_raw, partitions)
    result.lifecycle_snapshots = lifecycle_snap

    lifecycle_input, collapse_result = _build_model_h_lifecycle_input(canonical, lifecycle_snap)
    result.collapse_result = collapse_result
    if collapse_result is not None and not collapse_result.before_model_h_input.empty:
        collapse_result.before_model_h_input = _attach_canonical_subscriber_columns(
            collapse_result.before_model_h_input, canonical,
        )
    lifecycle_input = _attach_canonical_subscriber_columns(lifecycle_input, canonical)
    if collapse_result is not None:
        collapse_result.collapsed = lifecycle_input.copy()
    result.lifecycle_input = lifecycle_input

    if collapse_result is not None:
        try:
            from azure_reconciliation.business_transaction_collapse import write_collapse_audits

            write_collapse_audits(collapse_result, issuer=issuer, partitions=partitions)
        except Exception as exc:
            logger.warning("Collapse audit export skipped — %s", exc)

    if issuer == "15105" and any(p.year == "2026" and _zmonth(p.month) == "01" for p in partitions):
        try:
            from azure_reconciliation.lineage_audit import write_lineage_audit

            write_lineage_audit(xml_raw, partitions, issuer="15105", year="2026", month="01")
        except Exception as exc:
            logger.warning("Lineage audit skipped — %s", exc)

    model_h = _chandra_dashboard(lifecycle_input, source="xml")
    result.model_h_monthly = model_h

    result.count_column_audit = build_model_h_count_column_audit(lifecycle_input, pd.DataFrame())

    business_monthly = build_extended_business_summary(
        model_h,
        xml_raw=xml_raw,
        canonical=canonical,
        lifecycle_input=lifecycle_input,
        duplicate_df=dup,
        maintenance_df=maint,
        superseded_df=sup,
    )
    result.business_monthly = business_monthly
    result.business_yearly = yearly_rollup(business_monthly)
    result.business_rollup = business_monthly.copy()
    return result


def _write_bundle(
    base: Path,
    prefix: str,
    summary: pd.DataFrame,
    detail: pd.DataFrame,
    *,
    export_errors: ExportErrors | None = None,
    processing_html: str = "",
) -> None:
    base.mkdir(parents=True, exist_ok=True)
    safe_write_csv(base / f"{prefix}_detail.csv", detail, export_errors=export_errors)
    safe_write_excel(
        base / f"{prefix}_summary.xlsx",
        {"summary": summary},
        drop_duplicate_value_columns=False,
        export_errors=export_errors,
    )
    safe_write_html_report(
        base / f"{prefix}_summary.html",
        title=f"{prefix} summary",
        summary_df=summary,
        detail_df=detail.head(500) if not detail.empty else pd.DataFrame(),
        extra_html=processing_html,
        export_errors=export_errors,
    )


def export_issuer_reports(
    result: IssuerBusinessResult,
    *,
    export_errors: ExportErrors | None = None,
    reports_root: Path | None = None,
) -> Path:
    """Write per-issuer HTML/XLSX/CSV tree."""
    from azure_reconciliation.business_output_validation import (
        build_business_processing_summary,
        lifecycle_snapshot_export_df,
        processing_summary_html,
    )
    from azure_reconciliation.chandra_business_format import (
        chandra_year_rollup,
        to_chandra_business_summary,
        write_chandra_business_html,
        write_chandra_business_xlsx,
        write_model_h_month_html,
    )

    root = reports_root or (xml_business_reports_root() / result.issuer)
    root.mkdir(parents=True, exist_ok=True)
    issuer_proc = processing_summary_html(build_business_processing_summary(result))
    chandra_monthly = to_chandra_business_summary(result.business_monthly)
    chandra_yearly = chandra_year_rollup(chandra_monthly)

    safe_write_excel(
        root / "issuer_summary.xlsx",
        {
            "Enrollment_Summary": chandra_monthly,
            "Year_Rollup": chandra_yearly,
            "Processing_Diagnostics": build_business_processing_summary(result),
            "Technical_Internal": result.business_monthly,
        },
        drop_duplicate_value_columns=False,
        export_errors=export_errors,
    )
    write_chandra_business_html(
        root / "issuer_summary.html",
        chandra_monthly,
        title=f"Issuer {result.issuer} — Enrollment Summary",
    )

    diag = root / "diagnostics"
    diag.mkdir(parents=True, exist_ok=True)
    safe_write_excel(
        diag / "cleanup_summary.xlsx",
        {"cleanup_summary": result.cleanup_summary},
        export_errors=export_errors,
    )
    safe_write_html_report(
        diag / "cleanup_summary.html",
        title=f"Issuer {result.issuer} — cleanup summary",
        summary_df=result.cleanup_summary,
        export_errors=export_errors,
    )
    safe_write_csv(diag / "duplicate_transactions.csv", result.duplicate_df, export_errors=export_errors)
    safe_write_csv(diag / "maintenance_only_events.csv", result.maintenance_df, export_errors=export_errors)
    safe_write_csv(diag / "superseded_events.csv", result.superseded_df, export_errors=export_errors)
    safe_write_csv(diag / "lifecycle_snapshot.csv", lifecycle_snapshot_export_df(result.lifecycle_snapshots), export_errors=export_errors)
    safe_write_csv(diag / "model_h_count_column_audit.csv", result.count_column_audit, export_errors=export_errors)
    safe_write_csv(diag / "month_basis_audit.csv", result.month_basis_audit, export_errors=export_errors)

    rollup_dir = root / "rollup"
    write_chandra_business_xlsx(rollup_dir / "xml_all_months_summary.xlsx", chandra_monthly)
    write_chandra_business_html(
        rollup_dir / "xml_all_months_summary.html",
        chandra_monthly,
        title=f"Issuer {result.issuer} — All Months Enrollment Summary",
    )
    safe_write_csv(rollup_dir / "xml_all_months_detail.csv", result.lifecycle_input, export_errors=export_errors)

    for year in sorted({p.year for p in result.partitions}):
        year_sum = result.business_monthly[
            result.business_monthly["year"].astype(str) == str(year)
        ]
        chandra_year_sum = chandra_year_rollup(to_chandra_business_summary(year_sum))
        year_detail = result.lifecycle_input[
            result.lifecycle_input["year"].astype(str) == str(year)
        ]
        year_dir = root / "yearly" / str(year)
        year_dir.mkdir(parents=True, exist_ok=True)
        write_chandra_business_xlsx(year_dir / "xml_year_summary.xlsx", chandra_year_sum)
        write_chandra_business_html(
            year_dir / "xml_year_summary.html",
            chandra_year_sum,
            title=f"Issuer {result.issuer} {year} — Year Enrollment Summary",
        )
        safe_write_csv(year_dir / "xml_year_detail.csv", year_detail, export_errors=export_errors)

        for part in sorted(
            (p for p in result.partitions if p.year == year),
            key=lambda p: p.month,
        ):
            month_internal = year_sum[
                year_sum["month"].astype(str).map(_zmonth) == _zmonth(part.month)
            ]
            chandra_month = to_chandra_business_summary(month_internal)
            month_detail = _filter_year_month_lifecycle(result.lifecycle_input, year, part.month)
            month_dir = root / "monthly" / str(year) / _zmonth(part.month)
            month_dir.mkdir(parents=True, exist_ok=True)
            month_proc = processing_summary_html(
                build_business_processing_summary(result, part),
            )
            write_chandra_business_xlsx(
                month_dir / "enrollment_summary.xlsx",
                chandra_month,
            )
            write_chandra_business_html(
                month_dir / "enrollment_summary.html",
                chandra_month,
                title=f"{result.issuer}/{year}/{_zmonth(part.month)} — Enrollment Summary",
            )
            write_model_h_month_html(
                month_dir / "model_h_month_summary.html",
                business_df=chandra_month,
                processing_html=month_proc,
                title=f"{result.issuer}/{year}/{_zmonth(part.month)} — Model H Month Summary",
            )
            safe_write_excel(
                month_dir / "model_h_month_summary.xlsx",
                {
                    "Enrollment_Summary": chandra_month,
                    "Processing_Diagnostics": build_business_processing_summary(result, part),
                    "Technical_Internal": month_internal,
                },
                drop_duplicate_value_columns=False,
                export_errors=export_errors,
            )
            diag = month_dir / "diagnostics"
            diag.mkdir(parents=True, exist_ok=True)
            safe_write_csv(diag / "lifecycle_detail.csv", month_detail, export_errors=export_errors)

    logger.info("Wrote XML business reports for issuer %s → %s", result.issuer, root)
    return root


def export_all_issuers_reports(
    results: list[IssuerBusinessResult],
    *,
    export_errors: ExportErrors | None = None,
) -> Path:
    """Cross-issuer monthly/yearly rollups."""
    from azure_reconciliation.chandra_business_format import (
        chandra_year_rollup,
        to_chandra_business_summary,
        write_chandra_business_html,
        write_chandra_business_xlsx,
    )

    all_dir = xml_business_reports_root() / "all_issuers"
    all_dir.mkdir(parents=True, exist_ok=True)

    monthly_internal = pd.concat(
        [r.business_monthly for r in results if not r.business_monthly.empty], ignore_index=True,
    )
    monthly_chandra = to_chandra_business_summary(monthly_internal)
    yearly_chandra = chandra_year_rollup(monthly_chandra)
    detail = pd.concat([r.lifecycle_input for r in results if not r.lifecycle_input.empty], ignore_index=True)

    write_chandra_business_xlsx(all_dir / "all_issuers_monthly_summary.xlsx", monthly_chandra)
    write_chandra_business_html(
        all_dir / "all_issuers_monthly_summary.html", monthly_chandra,
        title="All Issuers — Monthly Enrollment Summary",
    )
    write_chandra_business_xlsx(all_dir / "all_issuers_yearly_summary.xlsx", yearly_chandra)
    write_chandra_business_html(
        all_dir / "all_issuers_yearly_summary.html", yearly_chandra,
        title="All Issuers — Yearly Enrollment Summary",
    )
    safe_write_csv(all_dir / "all_issuers_detail.csv", detail, export_errors=export_errors)
    return all_dir


def write_index_reports(
    results: list[IssuerBusinessResult],
    *,
    export_errors: ExportErrors | None = None,
    reports_root: Path | None = None,
) -> Path:
    """Master index.html / index.xlsx at xml_business_reports root."""
    from azure_reconciliation.business_output_validation import (
        build_business_processing_summary,
        processing_summary_html,
    )
    from azure_reconciliation.chandra_business_format import (
        chandra_all_years_rollup,
        chandra_year_rollup,
        to_chandra_business_summary,
    )

    root = reports_root or xml_business_reports_root()
    monthly_internal = pd.concat(
        [r.business_monthly for r in results if not r.business_monthly.empty], ignore_index=True,
    )
    yearly_internal = pd.concat(
        [r.business_yearly for r in results if not r.business_yearly.empty], ignore_index=True,
    )
    monthly_chandra = to_chandra_business_summary(monthly_internal)
    yearly_chandra = chandra_year_rollup(to_chandra_business_summary(monthly_internal))
    all_years_chandra = chandra_all_years_rollup(monthly_chandra)
    issuer_rows = []
    for r in results:
        issuer_rows.append({
            "issuer": r.issuer,
            "partitions": len(r.partitions),
            "raw_xml_rows": len(r.xml_raw),
            "canonical_rows": len(r.canonical),
            "lifecycle_rows": len(r.lifecycle_input),
            "model_h_groups": len(r.business_monthly),
            "duplicate_count": len(r.duplicate_df),
            "maintenance_only_count": len(r.maintenance_df),
            "superseded_count": len(r.superseded_df),
        })
    issuer_summary = pd.DataFrame(issuer_rows)
    cleanup = pd.concat([r.cleanup_summary.assign(issuer=r.issuer) for r in results], ignore_index=True)
    count_audit = pd.concat([r.count_column_audit for r in results if not r.count_column_audit.empty], ignore_index=True)
    month_audit = pd.concat([r.month_basis_audit.assign(issuer=r.issuer) for r in results if not r.month_basis_audit.empty], ignore_index=True)

    exec_rows = [{
        "generated_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "mode": "XML_ONLY_BUSINESS",
        "issuer_count": len(results),
        "total_raw_xml_rows": sum(len(r.xml_raw) for r in results),
        "total_model_h_groups": len(monthly_chandra),
        "azure_connection": "DISABLED",
    }]
    executive = pd.DataFrame(exec_rows)

    safe_write_excel(
        root / "index.xlsx",
        {
            "Executive_Summary": executive,
            "All_Issuers_Monthly": monthly_chandra,
            "All_Issuers_Yearly": yearly_chandra,
            "Issuer_Summary": issuer_summary,
            "Model_H_Monthly": monthly_chandra,
            "Model_H_Yearly": yearly_chandra,
            "Model_H_All_Years": all_years_chandra,
            "Technical_Monthly_Internal": monthly_internal,
            "Technical_Yearly_Internal": yearly_internal,
            "Cleanup_Diagnostics": cleanup,
            "Count_Audit": count_audit,
            "Month_Basis_Audit": month_audit,
        },
        drop_duplicate_value_columns=False,
        export_errors=export_errors,
    )

    index_lines = [
        "<!DOCTYPE html><html><head><meta charset='utf-8'>",
        "<title>XML Business Reports</title>",
        "<style>body{font-family:system-ui,sans-serif;margin:2rem}",
        "table{border-collapse:collapse}td,th{border:1px solid #ccc;padding:4px 8px}</style>",
        "</head><body>",
        "<h1>XML-Only Business Reports (Model H)</h1>",
        f"<p>Generated: {exec_rows[0]['generated_utc']}</p>",
        f"<p>Issuers processed: {len(results)}</p>",
    ]
    if results:
        combined = build_business_processing_summary(results[0])
        for r in results[1:]:
            combined = pd.concat([combined, build_business_processing_summary(r)], ignore_index=True)
        index_lines.append(processing_summary_html(combined))
    index_lines.append("<h2>Issuer summary</h2>")
    if not issuer_summary.empty:
        index_lines.append(issuer_summary.to_html(index=False))
    index_lines.append("<h2>Navigation</h2><ul>")
    for r in results:
        index_lines.append(f"<li><a href='{r.issuer}/issuer_summary.html'>{r.issuer}</a></li>")
    index_lines.append("<li><a href='all_issuers/all_issuers_monthly_summary.html'>All issuers monthly</a></li>")
    index_lines.append("</ul></body></html>")
    (root / "index.html").write_text("\n".join(index_lines), encoding="utf-8")
    logger.info("Wrote index → %s", root / "index.html")
    return root


def write_sqlite_db(
    results: list[IssuerBusinessResult],
    *,
    export_errors: ExportErrors | None = None,
    db_path: Path | None = None,
) -> Path:
    """Persist canonical, lifecycle, Model H, and audit tables."""
    db_path = db_path or (xml_business_reports_root() / "xml_business.sqlite")
    db_path.parent.mkdir(parents=True, exist_ok=True)

    canonical = pd.concat([r.canonical for r in results if not r.canonical.empty], ignore_index=True)
    lifecycle = pd.concat([r.lifecycle_snapshots for r in results if not r.lifecycle_snapshots.empty], ignore_index=True)
    monthly_internal = pd.concat([r.business_monthly for r in results if not r.business_monthly.empty], ignore_index=True)
    yearly_internal = pd.concat([r.business_yearly for r in results if not r.business_yearly.empty], ignore_index=True)
    from azure_reconciliation.chandra_business_format import to_chandra_business_summary

    monthly = to_chandra_business_summary(monthly_internal)
    yearly = to_chandra_business_summary(yearly_internal)
    all_issuers = monthly.copy()
    cleanup = pd.concat([
        r.cleanup_summary.assign(issuer=r.issuer) for r in results
    ], ignore_index=True)
    count_audit = pd.concat([r.count_column_audit for r in results if not r.count_column_audit.empty], ignore_index=True)
    month_audit = pd.concat([
        r.month_basis_audit.assign(issuer=r.issuer) for r in results if not r.month_basis_audit.empty
    ], ignore_index=True)

    with sqlite3.connect(db_path) as conn:
        tables = {
            "canonical_xml_records": canonical,
            "xml_lifecycle_snapshot": lifecycle,
            "model_h_monthly_summary": monthly,
            "model_h_yearly_rollup": yearly,
            "model_h_all_issuers_summary": all_issuers,
            "cleanup_diagnostics": cleanup,
            "count_column_audit": count_audit,
            "month_basis_audit": month_audit,
        }
        for name, df in tables.items():
            safe_write_sqlite(conn, name, df, if_exists="replace", export_errors=export_errors)
    logger.info("Wrote SQLite → %s", db_path)
    return db_path


def run_xml_business_reporting(
    *,
    issuer: str | None = None,
    parse_source: bool = False,
    export_errors: ExportErrors | None = None,
    disable_azure: bool = True,
) -> dict[str, Any]:
    """
    Main entry — discover issuers, process XML, write all outputs.
    Never connects to Azure.
    """
    if disable_azure:
        settings.apply_xml_only_business_mode(True)
    settings.ensure_dirs()

    partitions = discover_partitions(settings.source_data_path, issuer_filter=issuer)
    issuers = sorted({p.issuer for p in partitions})
    if not issuers:
        raise RuntimeError("No source_data partitions found")

    logger.info("XML business reporting — issuers: %s", issuers)
    results: list[IssuerBusinessResult] = []

    for iss in issuers:
        parts = [p for p in partitions if p.issuer == iss]
        xml_raw = load_xml_rows(
            prefer_staging=not parse_source,
            issuer_filter=iss,
        )
        if xml_raw.empty:
            logger.warning("No XML rows for issuer %s — skipping", iss)
            continue
        result = process_issuer_xml_business(iss, xml_raw, parts)
        export_issuer_reports(result, export_errors=export_errors)
        results.append(result)

    if not results:
        raise RuntimeError("No XML data processed for any issuer")

    from azure_reconciliation.business_output_validation import (
        write_business_validation_md,
        write_subscriber_mapping_audit,
    )

    write_subscriber_mapping_audit(results)
    write_business_validation_md(results)

    export_all_issuers_reports(results, export_errors=export_errors)
    write_index_reports(results, export_errors=export_errors)
    db_path = write_sqlite_db(results, export_errors=export_errors)

    from azure_reconciliation.assets_style_reports import export_assets_style_reports

    assets_root = export_assets_style_reports(results, export_errors=export_errors)

    return {
        "issuers": [r.issuer for r in results],
        "output_root": str(xml_business_reports_root()),
        "assets_root": str(assets_root),
        "sqlite": str(db_path),
        "model_h_groups": sum(len(r.business_monthly) for r in results),
    }
