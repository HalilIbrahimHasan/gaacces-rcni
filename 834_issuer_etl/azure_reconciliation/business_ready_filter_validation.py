"""
Read-only QA investigation — Business Ready prior-year benefit filter validation.

Does not modify production parser, canonical, lifecycle, Model H, or reporting logic.
Generates evidence under outputs/debug/.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from azure_reconciliation.business_ready_exports import build_business_ready_records
from azure_reconciliation.full_data_exports import write_full_export_excel
from azure_reconciliation.partition_discovery import discover_partitions
from azure_reconciliation.prior_year_benefit_filter import (
    parse_benefit_effective_year,
    resolve_reporting_year,
)
from azure_reconciliation.reconciliation_analysis import _dedupe_transactions
from azure_reconciliation.safe_export import safe_write_excel
from azure_reconciliation.xml_business_reports import (
    _latest_state_per_business_month,
    process_issuer_xml_business,
)
from azure_reconciliation.xml_loader import load_xml_rows
from config.config import settings
from utils.logger import get_logger

logger = get_logger(__name__)

_DATE_PRIORITY = (
    "member_maint_effective_date",
    "benefit_effective_date",
    "file_event_date",
    "event_date",
)


def _debug_dir() -> Path:
    d = settings.outputs_path / "debug"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _zmonth(m: Any) -> str:
    return str(m).strip().zfill(2)


def _load_frame(path: Path, *, sheet: str | None = None) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    if path.suffix.lower() == ".xlsx":
        return pd.read_excel(path, sheet_name=sheet or 0, dtype=str)
    return pd.read_csv(path, dtype=str, low_memory=False)


def _load_business_ready_filtered(issuer: str, year: str) -> pd.DataFrame:
    root = settings.outputs_path / "business_review_filtered" / issuer / year
    for path in (
        root / "business_ready_filtered.csv",
        root / "filtered_comparison.xlsx",
    ):
        if path.suffix == ".csv" and path.exists():
            return _load_frame(path)
        if path.suffix == ".xlsx" and path.exists():
            return _load_frame(path, sheet="BUSINESS_READY_FILTERED")
    return pd.DataFrame()


def _load_business_ready_unfiltered(issuer: str, year: str) -> pd.DataFrame:
    base = settings.outputs_path / "business_data_exports" / issuer / year / "business_ready"
    for path in (base / "business_ready_all_months.csv", base / "business_ready_all_months.xlsx"):
        if path.suffix == ".csv" and path.exists():
            return _load_frame(path)
        if path.suffix == ".xlsx" and path.exists():
            return _load_frame(path, sheet="BUSINESS_READY_ALL_MONTHS")
    return pd.DataFrame()


def _load_raw_filtered(issuer: str, year: str) -> pd.DataFrame:
    root = settings.outputs_path / "business_review_filtered" / issuer / year
    for path in (root / "raw_filtered.csv", root / "filtered_comparison.xlsx"):
        if path.suffix == ".csv" and path.exists():
            return _load_frame(path)
        if path.suffix == ".xlsx" and path.exists():
            return _load_frame(path, sheet="RAW_FILTERED")
    return pd.DataFrame()


def _load_raw_unfiltered(issuer: str, year: str) -> pd.DataFrame:
    base = settings.outputs_path / "full_data_exports" / issuer / year / "raw"
    for path in (base / "raw_all_months.csv", base / "raw_all_months.xlsx"):
        if path.suffix == ".csv" and path.exists():
            return _load_frame(path)
        if path.suffix == ".xlsx" and path.exists():
            return _load_frame(path, sheet=0)
    return pd.DataFrame()


def build_column_inventory(df: pd.DataFrame, *, dataset_label: str) -> pd.DataFrame:
    """Column inventory: name, dtype, non-null, null, sample values."""
    if df.empty:
        return pd.DataFrame(columns=[
            "dataset", "column_name", "data_type", "non_null_count", "null_count", "sample_values",
        ])
    rows: list[dict[str, Any]] = []
    n = len(df)
    for col in df.columns:
        s = df[col]
        non_null = int(s.notna().sum())
        null_count = n - non_null
        samples = s.dropna().astype(str).str.strip()
        samples = samples[samples.ne("")].head(3).tolist()
        rows.append({
            "dataset": dataset_label,
            "column_name": col,
            "data_type": str(s.dtype),
            "non_null_count": non_null,
            "null_count": null_count,
            "sample_values": "; ".join(samples) if samples else "",
        })
    return pd.DataFrame(rows)


def _date_column_used(row: pd.Series) -> str:
    """Mirror business_ready_exports._selected_transaction_date source priority."""
    for col in _DATE_PRIORITY:
        if col in row.index:
            val = str(row.get(col, "") or "").strip()
            if val and val.lower() not in ("nan", "none", "nat"):
                return col
    return "none"


def _benefit_year_series(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series([None] * len(df), index=df.index, dtype=object)
    return df[col].map(parse_benefit_effective_year)


def _prior_year_mask(years: pd.Series, reporting_year: str) -> pd.Series:
    y = int(reporting_year)
    return years.notna() & (years < y)


def _join_lifecycle_dates(
    business_df: pd.DataFrame,
    lifecycle_input: pd.DataFrame,
) -> pd.DataFrame:
    """Attach lifecycle source date columns to business-ready rows (read-only join)."""
    if business_df.empty:
        return business_df
    out = business_df.copy()
    if lifecycle_input.empty:
        for col in ("benefit_effective_date", "member_maint_effective_date"):
            out[col] = ""
        out["date_used_for_business_ready"] = "selected_transaction_date_only_in_export"
        return out

    join_cols = [c for c in ("policy_id", "member_id", "year", "month") if c in business_df.columns and c in lifecycle_input.columns]
    if not join_cols:
        out["date_used_for_business_ready"] = "join_failed"
        return out

    li = lifecycle_input.copy()
    li["year"] = li["year"].astype(str)
    li["month"] = li["month"].astype(str).map(_zmonth)
    work = out.copy()
    work["year"] = work["year"].astype(str)
    work["month"] = work["month"].astype(str).map(_zmonth)

    date_cols = [c for c in _DATE_PRIORITY if c in li.columns]
    pick = join_cols + date_cols
    li_sub = li[pick].drop_duplicates(subset=join_cols, keep="last")
    merged = work.merge(li_sub, on=join_cols, how="left", suffixes=("", "_li"))

    date_used: list[str] = []
    for _, row in merged.iterrows():
        date_used.append(_date_column_used(row))
    merged["date_used_for_business_ready"] = date_used
    return merged


def build_date_trace_report(
    business_df: pd.DataFrame,
    lifecycle_input: pd.DataFrame,
) -> pd.DataFrame:
    """Per-record date source trace for business-ready rows."""
    merged = _join_lifecycle_dates(business_df, lifecycle_input)
    cols = [
        "canonical_enrollment_id", "member_id", "policy_id",
        "benefit_effective_date", "member_maint_effective_date",
        "selected_transaction_date", "business_month", "date_used_for_business_ready",
    ]
    if "canonical_enrollee_id" in merged.columns and "member_id" not in merged.columns:
        merged["member_id"] = merged["canonical_enrollee_id"]
    if "canonical_enrollment_id" not in merged.columns and "policy_id" in merged.columns:
        merged["canonical_enrollment_id"] = merged["policy_id"]
    keep = [c for c in cols if c in merged.columns]
    return merged[keep].copy()


def build_prior_year_candidates(
    business_df: pd.DataFrame,
    lifecycle_input: pd.DataFrame,
    *,
    reporting_year: str,
) -> pd.DataFrame:
    """Records that would filter under benefit_effective vs selected_transaction_date."""
    merged = _join_lifecycle_dates(business_df, lifecycle_input)
    if merged.empty:
        return pd.DataFrame(columns=[
            "canonical_enrollment_id", "policy_id", "member_id",
            "benefit_effective_date", "benefit_effective_year",
            "selected_transaction_date", "selected_transaction_year",
            "business_month",
            "would_filter_using_benefit_effective",
            "would_filter_using_selected_transaction",
        ])

    if "canonical_enrollment_id" not in merged.columns:
        merged["canonical_enrollment_id"] = merged.get("policy_id", "")
    if "member_id" not in merged.columns:
        merged["member_id"] = merged.get("canonical_enrollee_id", "")

    bey = _benefit_year_series(merged, "benefit_effective_date")
    sty = _benefit_year_series(merged, "selected_transaction_date")
    merged["benefit_effective_year"] = bey.apply(lambda y: "" if y is None else int(y))
    merged["selected_transaction_year"] = sty.apply(lambda y: "" if y is None else int(y))
    merged["would_filter_using_benefit_effective"] = _prior_year_mask(bey, reporting_year)
    merged["would_filter_using_selected_transaction"] = _prior_year_mask(sty, reporting_year)

    cols = [
        "canonical_enrollment_id", "policy_id", "member_id",
        "benefit_effective_date", "benefit_effective_year",
        "selected_transaction_date", "selected_transaction_year",
        "business_month",
        "would_filter_using_benefit_effective",
        "would_filter_using_selected_transaction",
    ]
    return merged[[c for c in cols if c in merged.columns]].copy()


def _count_prior_year(df: pd.DataFrame, date_col: str, reporting_year: str) -> dict[str, int]:
    if df.empty or date_col not in df.columns:
        return {"total": len(df), "prior_year": 0, "null_date": len(df)}
    years = df[date_col].map(parse_benefit_effective_year)
    prior = int(_prior_year_mask(years, reporting_year).sum())
    null_date = int(years.isna().sum())
    return {"total": len(df), "prior_year": prior, "null_date": null_date}


def build_pipeline_stage_prior_year_counts(
    *,
    issuer: str,
    year: str,
    reporting_year: str,
    parse_source: bool,
) -> tuple[pd.DataFrame, pd.DataFrame, Any]:
    """
    Count prior-year benefit_effective records at each pipeline stage (read-only replay).

    Returns (stage_counts_df, dropped_enrollment_ids_df, pipeline_result).
    """
    partitions = discover_partitions(settings.source_data_path, issuer_filter=issuer, year_filter=year)
    xml_raw = load_xml_rows(
        prefer_staging=not parse_source,
        issuer_filter=issuer,
        year_filter=year,
    )
    if xml_raw.empty:
        raise RuntimeError(f"No XML rows for {issuer}/{year}")

    result = process_issuer_xml_business(issuer, xml_raw, partitions)
    canonical = result.canonical[result.canonical["year"].astype(str) == str(year)].copy()
    deduped = _dedupe_transactions(canonical)
    latest_state = _latest_state_per_business_month(deduped)
    lifecycle_input = result.lifecycle_input[
        result.lifecycle_input["year"].astype(str) == str(year)
    ].copy()
    business_ready = build_business_ready_records(
        result, issuer=issuer, year=year, xml_raw=xml_raw,
    )
    # Business-ready export drops benefit_effective_date; use lifecycle join for stage 5
    br_enriched = _join_lifecycle_dates(business_ready, lifecycle_input)

    stages: list[dict[str, Any]] = []
    stage_frames: list[tuple[str, pd.DataFrame]] = [
        ("1_raw_xml", xml_raw),
        ("2_canonical (business month applied)", canonical),
        ("3_after_dedupe (_dedupe_transactions)", deduped),
        ("4_latest_state_per_business_month", latest_state),
        ("5_business_transaction_collapse → lifecycle_input", lifecycle_input),
        ("6_business_ready_export (lifecycle-joined benefit dates)", br_enriched),
    ]
    for stage_name, frame in stage_frames:
        counts = _count_prior_year(frame, "benefit_effective_date", reporting_year)
        stages.append({
            "stage": stage_name,
            "row_count": counts["total"],
            "prior_year_benefit_effective_count": counts["prior_year"],
            "null_benefit_effective_date_count": counts["null_date"],
            "reporting_year": reporting_year,
        })

    # Enrollment IDs present in raw prior-year but absent from business_ready
    def _enroll_series(df: pd.DataFrame) -> pd.Series:
        for col in ("policy_id", "canonical_enrollment_id", "enrollment_id", "health_coverage_policy_no"):
            if col in df.columns:
                return df[col].astype(str).str.strip()
        return pd.Series([""] * len(df), index=df.index, dtype=str)

    raw_years = (
        xml_raw["benefit_effective_date"].map(parse_benefit_effective_year)
        if "benefit_effective_date" in xml_raw.columns
        else pd.Series([None] * len(xml_raw), index=xml_raw.index, dtype=object)
    )
    raw_prior_mask = _prior_year_mask(raw_years, reporting_year) if len(raw_years) else pd.Series(dtype=bool)
    raw_prior = xml_raw[raw_prior_mask.reindex(xml_raw.index, fill_value=False)].copy()
    br_enroll = set(_enroll_series(business_ready)[_enroll_series(business_ready).ne("")].unique())
    raw_prior_enroll = _enroll_series(raw_prior)
    missing_mask = raw_prior_enroll.ne("") & ~raw_prior_enroll.isin(br_enroll)
    if missing_mask.any():
        dropped = raw_prior[missing_mask.reindex(raw_prior.index, fill_value=False)].copy()
        dropped_ids = pd.DataFrame({
            "canonical_enrollment_id": _enroll_series(dropped).values,
            "policy_id": dropped["policy_id"].astype(str).values if "policy_id" in dropped.columns else _enroll_series(dropped).values,
            "member_id": dropped["member_id"].astype(str).values if "member_id" in dropped.columns else "",
            "benefit_effective_date": dropped["benefit_effective_date"].astype(str).values if "benefit_effective_date" in dropped.columns else "",
            "benefit_effective_year": dropped["benefit_effective_date"].map(parse_benefit_effective_year).astype(str).values if "benefit_effective_date" in dropped.columns else "",
            "stage_where_lost": "between_raw_and_business_ready",
        })
    else:
        dropped_ids = pd.DataFrame(columns=[
            "canonical_enrollment_id", "policy_id", "member_id",
            "benefit_effective_date", "benefit_effective_year", "stage_where_lost",
        ])

    return pd.DataFrame(stages), dropped_ids, result


def _compare_filter_methods(
    business_df: pd.DataFrame,
    lifecycle_input: pd.DataFrame,
    *,
    reporting_year: str,
) -> dict[str, Any]:
    """Method A (benefit_effective_date) vs Method B (selected_transaction_date)."""
    enriched = _join_lifecycle_dates(business_df, lifecycle_input)
    before = len(enriched)

    method_a = enriched.copy()
    if "benefit_effective_date" in method_a.columns:
        years_a = method_a["benefit_effective_date"].map(parse_benefit_effective_year)
        keep_a = ~_prior_year_mask(years_a, reporting_year)
        after_a = int(keep_a.sum())
        excluded_a = before - after_a
    else:
        after_a = before
        excluded_a = 0

    method_b = enriched.copy()
    if "selected_transaction_date" in method_b.columns:
        years_b = method_b["selected_transaction_date"].map(parse_benefit_effective_year)
        keep_b = ~_prior_year_mask(years_b, reporting_year)
        after_b = int(keep_b.sum())
        excluded_b = before - after_b
    else:
        after_b = before
        excluded_b = 0

    return {
        "method_a_label": "benefit_effective_date (from lifecycle join)",
        "method_a_before": before,
        "method_a_excluded": excluded_a,
        "method_a_after": after_a,
        "method_b_label": "selected_transaction_date (export column)",
        "method_b_before": before,
        "method_b_excluded": excluded_b,
        "method_b_after": after_b,
    }


def _first_stage_removing_prior_year(stage_df: pd.DataFrame) -> str:
    """Identify first stage where prior-year count drops to zero from a positive prior count."""
    if stage_df.empty:
        return "unknown"
    prev_prior = None
    for _, row in stage_df.iterrows():
        prior = int(row["prior_year_benefit_effective_count"])
        if prev_prior is not None and prev_prior > 0 and prior == 0:
            return str(row["stage"])
        prev_prior = prior
    if int(stage_df.iloc[0]["prior_year_benefit_effective_count"]) == 0:
        return "no prior-year records at raw stage"
    last = stage_df[stage_df["prior_year_benefit_effective_count"] > 0]
    if last.empty:
        return "unknown"
    final_stage = str(last.iloc[-1]["stage"])
    if int(stage_df.iloc[-1]["prior_year_benefit_effective_count"]) == 0:
        return f"removed by stage after {final_stage}"
    return final_stage


def build_engineering_conclusion(
    *,
    reporting_year: str,
    candidates: pd.DataFrame,
    stage_counts: pd.DataFrame,
    method_compare: dict[str, Any],
    column_inventory: pd.DataFrame,
) -> str:
    """Answer the four engineering questions with evidence."""
    has_benefit_col = (
        "benefit_effective_date" in column_inventory["column_name"].values
        if not column_inventory.empty else False
    )
    export_has_benefit = bool(
        not column_inventory.empty
        and (
            (column_inventory["dataset"] == "BUSINESS_READY_FILTERED")
            & (column_inventory["column_name"] == "benefit_effective_date")
            & (column_inventory["non_null_count"] > 0)
        ).any()
    )
    export_has_selected = bool(
        not column_inventory.empty
        and (
            (column_inventory["dataset"] == "BUSINESS_READY_FILTERED")
            & (column_inventory["column_name"] == "selected_transaction_date")
            & (column_inventory["non_null_count"] > 0)
        ).any()
    )

    cand_a = int(candidates["would_filter_using_benefit_effective"].sum()) if not candidates.empty and "would_filter_using_benefit_effective" in candidates.columns else 0
    cand_b = int(candidates["would_filter_using_selected_transaction"].sum()) if not candidates.empty and "would_filter_using_selected_transaction" in candidates.columns else 0

    br_prior_at_export = 0
    if not stage_counts.empty:
        br_row = stage_counts[stage_counts["stage"].str.contains("business_ready", case=False)]
        if not br_row.empty:
            br_prior_at_export = int(br_row.iloc[-1]["prior_year_benefit_effective_count"])

    is_free = cand_a == 0 and br_prior_at_export == 0
    removal_stage = _first_stage_removing_prior_year(stage_counts)

    lines = [
        "# Business Ready Filter — Engineering Conclusion",
        "",
        f"**Generated:** {datetime.now(timezone.utc).isoformat(timespec='seconds')}",
        f"**Reporting year:** {reporting_year}",
        "",
        "## Column evidence (BUSINESS_READY_FILTERED export)",
        "",
        f"- `benefit_effective_date` in export: **{'YES' if export_has_benefit else 'NO'}**",
        f"- `selected_transaction_date` in export: **{'YES' if export_has_selected else 'NO'}**",
        "",
        "## Questions",
        "",
        "### 1. Is Business Ready already free of prior-year benefit records?",
        "",
        f"**{'YES' if is_free else 'NO'}**",
        "",
        f"- Prior-year candidates (Method A — benefit_effective_date via lifecycle join): **{cand_a}**",
        f"- Prior-year candidates (Method B — selected_transaction_date): **{cand_b}**",
        f"- Prior-year count at business_ready pipeline stage (benefit_effective_date): **{br_prior_at_export}**",
        "",
        "### 2. If YES — which pipeline stage removed them?",
        "",
        removal_stage if is_free else "N/A — prior-year records still present or date column missing in export",
        "",
        "### 3. If NO — how many remain?",
        "",
        f"**{max(cand_a, cand_b, br_prior_at_export)}** (max of Method A, Method B, pipeline-stage count)",
        "",
        "### 4. Should Business Ready filtering still exist?",
        "",
    ]
    if is_free and cand_b == 0:
        lines.extend([
            "**NO** — for reporting alignment, prior-year exclusion is already enforced upstream.",
            "Business-ready rows are collapsed to current-year transaction dates; re-filtering on",
            "`selected_transaction_date` duplicates raw-level exclusion without removing additional rows.",
            "Keep raw-level filter for Chandra raw extract comparison only.",
        ])
    elif cand_a > 0 or br_prior_at_export > 0:
        lines.extend([
            "**YES** — prior-year benefit effective records remain in business-ready input when judged",
            "by `benefit_effective_date`. Filter on lifecycle-joined benefit dates, not only",
            "`selected_transaction_date`.",
        ])
    else:
        lines.extend([
            "**OPTIONAL** — export lacks `benefit_effective_date`; filter on `selected_transaction_date`",
            "may exclude rows that raw filter already removed. Document that business-ready filter",
            "is informational when upstream collapse has already removed prior-year benefit rows.",
        ])
    lines.append("")
    return "\n".join(lines)


def build_validation_markdown(
    *,
    issuer: str,
    year: str,
    reporting_year: str,
    column_inventory: pd.DataFrame,
    date_trace: pd.DataFrame,
    candidates: pd.DataFrame,
    stage_counts: pd.DataFrame,
    dropped_ids: pd.DataFrame,
    method_compare: dict[str, Any],
    raw_filtered_count: int,
    raw_prior_excluded: int,
) -> str:
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    cand_a = int(candidates["would_filter_using_benefit_effective"].sum()) if not candidates.empty and "would_filter_using_benefit_effective" in candidates.columns else 0
    cand_b = int(candidates["would_filter_using_selected_transaction"].sum()) if not candidates.empty and "would_filter_using_selected_transaction" in candidates.columns else 0

    date_used_vc = ""
    if not date_trace.empty and "date_used_for_business_ready" in date_trace.columns:
        vc = date_trace["date_used_for_business_ready"].value_counts()
        date_used_vc = "\n".join(f"- `{k}`: {int(v)}" for k, v in vc.items())

    lines = [
        "# Business Ready Filter Validation",
        "",
        f"**Issuer / year:** {issuer} / {year}",
        f"**Reporting year:** {reporting_year}",
        f"**Generated:** {now}",
        "",
        "## Raw filter (validated)",
        "",
        f"- Raw before: (see FILTER_SUMMARY)",
        f"- Raw filtered row count in export: {raw_filtered_count}",
        f"- Raw prior-year excluded (from FILTER_SUMMARY): {raw_prior_excluded}",
        "",
        "## Step 1 — BUSINESS_READY_FILTERED column inventory",
        "",
        "See `business_ready_column_inventory.xlsx` sheet `Column_Inventory`.",
        "",
        "Key finding:",
        "",
    ]
    filtered_inv = column_inventory[column_inventory["dataset"] == "BUSINESS_READY_FILTERED"] if not column_inventory.empty else pd.DataFrame()
    has_bed = not filtered_inv[filtered_inv["column_name"] == "benefit_effective_date"].empty if not filtered_inv.empty else False
    has_std = not filtered_inv[filtered_inv["column_name"] == "selected_transaction_date"].empty if not filtered_inv.empty else False
    if has_bed:
        row = filtered_inv[filtered_inv["column_name"] == "benefit_effective_date"].iloc[0]
        lines.append(f"- `benefit_effective_date` present — non-null: {row['non_null_count']}")
    else:
        lines.append("- `benefit_effective_date` **not exported** in BUSINESS_READY_FILTERED")
    if has_std:
        row = filtered_inv[filtered_inv["column_name"] == "selected_transaction_date"].iloc[0]
        lines.append(f"- `selected_transaction_date` present — non-null: {row['non_null_count']}")
    else:
        lines.append("- `selected_transaction_date` not found")

    lines.extend([
        "",
        "## Step 2 — Date used for Business Ready",
        "",
        "Priority (from `business_ready_exports._selected_transaction_date`):",
        "1. member_maint_effective_date",
        "2. benefit_effective_date",
        "3. file_event_date",
        "4. event_date",
        "",
        "Distribution in business-ready records (lifecycle join):",
        "",
        date_used_vc or "(no records)",
        "",
        "Full per-record trace: `business_ready_column_inventory.xlsx` sheet `Date_Trace`.",
        "",
        "## Step 3 — Prior-year candidates",
        "",
        f"- Would filter using **benefit_effective_date** (lifecycle join): **{cand_a}**",
        f"- Would filter using **selected_transaction_date**: **{cand_b}**",
        "",
        "Full candidate list: `business_ready_prior_year_candidates.xlsx`",
        "",
        "## Step 4 — Pipeline stage analysis (benefit_effective_date)",
        "",
        "Prior-year counts by stage (read-only pipeline replay):",
        "",
        "| Stage | Rows | Prior-year benefit | Null benefit date |",
        "|-------|-----:|-------------------:|------------------:|",
    ])
    for _, row in stage_counts.iterrows():
        lines.append(
            f"| {row['stage']} | {int(row['row_count'])} "
            f"| {int(row['prior_year_benefit_effective_count'])} "
            f"| {int(row['null_benefit_effective_date_count'])} |"
        )

    lines.extend([
        "",
        f"**First stage where prior-year count reaches zero:** {_first_stage_removing_prior_year(stage_counts)}",
        "",
    ])

    if cand_a == 0 and cand_b == 0:
        lines.extend([
            "### Why prior-year records disappeared before Business Ready",
            "",
            "Evidence from pipeline stage counts (not assumed):",
            "",
        ])
        raw_prior = int(stage_counts.iloc[0]["prior_year_benefit_effective_count"]) if not stage_counts.empty else 0
        if raw_prior > 0:
            for i in range(1, len(stage_counts)):
                prev = int(stage_counts.iloc[i - 1]["prior_year_benefit_effective_count"])
                cur = int(stage_counts.iloc[i]["prior_year_benefit_effective_count"])
                if prev > 0 and cur < prev:
                    lines.append(
                        f"- Drop at **{stage_counts.iloc[i]['stage']}**: "
                        f"{prev} → {cur} prior-year records"
                    )
            lines.extend([
                "",
                "Business Ready `selected_transaction_date` reflects the **winning transaction date**",
                "after dedupe, latest-state selection, and business-transaction collapse — typically",
                "`member_maint_effective_date` when present, which is often in the reporting year even",
                "when the original XML `benefit_effective_date` was prior year.",
                "",
                f"Sample enrollment IDs in raw prior-year but not in business_ready: **{len(dropped_ids)}**",
                "(see `Dropped_Raw_Prior_Year` sheet)",
            ])
        else:
            lines.append("- No prior-year benefit_effective_date records found even at raw stage for this issuer/year.")
    else:
        lines.extend([
            "## Step 5 — Method A vs Method B comparison",
            "",
            f"| Method | Before | Excluded | After |",
            f"|--------|-------:|---------:|------:|",
            f"| A: {method_compare['method_a_label']} | {method_compare['method_a_before']} | {method_compare['method_a_excluded']} | {method_compare['method_a_after']} |",
            f"| B: {method_compare['method_b_label']} | {method_compare['method_b_before']} | {method_compare['method_b_excluded']} | {method_compare['method_b_after']} |",
            "",
        ])

    lines.append("")
    return "\n".join(lines)


def run_business_ready_filter_validation(
    *,
    issuer: str,
    year: str,
    reporting_year: str | None = None,
    parse_source: bool = False,
) -> dict[str, Any]:
    """Run full read-only Business Ready filter validation investigation."""
    ry = reporting_year or resolve_reporting_year(partition_year=year)
    debug = _debug_dir()

    biz_filtered = _load_business_ready_filtered(issuer, year)
    biz_unfiltered = _load_business_ready_unfiltered(issuer, year)
    business_df = biz_filtered if not biz_filtered.empty else biz_unfiltered

    if business_df.empty:
        raise RuntimeError(
            f"No business-ready export for {issuer}/{year}. "
            "Run business_ready_exports or filtered comparison first."
        )

    # Column inventory across filtered + unfiltered exports
    inv_parts = [build_column_inventory(business_df, dataset_label="BUSINESS_READY_FILTERED")]
    if not biz_unfiltered.empty and biz_filtered is not business_df:
        inv_parts.append(build_column_inventory(biz_unfiltered, dataset_label="BUSINESS_READY_UNFILTERED"))
    column_inventory = pd.concat(inv_parts, ignore_index=True)

    # Pipeline replay for lifecycle join + stage counts
    stage_counts, dropped_ids, pipeline_result = build_pipeline_stage_prior_year_counts(
        issuer=issuer, year=year, reporting_year=ry, parse_source=parse_source,
    )
    lifecycle_input = pipeline_result.lifecycle_input[
        pipeline_result.lifecycle_input["year"].astype(str) == str(year)
    ]

    date_trace = build_date_trace_report(business_df, lifecycle_input)
    candidates = build_prior_year_candidates(business_df, lifecycle_input, reporting_year=ry)
    method_compare = _compare_filter_methods(business_df, lifecycle_input, reporting_year=ry)

    # Raw filter reference from FILTER_SUMMARY if available
    raw_filtered = _load_raw_filtered(issuer, year)
    raw_unfiltered = _load_raw_unfiltered(issuer, year)
    raw_prior_excluded = 0
    summary_path = settings.outputs_path / "business_review_filtered" / issuer / year / "filtered_comparison.xlsx"
    if summary_path.exists():
        try:
            summ = pd.read_excel(summary_path, sheet_name="FILTER_SUMMARY")
            if not summ.empty and "raw_excluded_prior_year_count" in summ.columns:
                raw_prior_excluded = int(summ.iloc[0]["raw_excluded_prior_year_count"])
        except Exception:
            pass

    validation_md = build_validation_markdown(
        issuer=issuer,
        year=year,
        reporting_year=ry,
        column_inventory=column_inventory,
        date_trace=date_trace,
        candidates=candidates,
        stage_counts=stage_counts,
        dropped_ids=dropped_ids,
        method_compare=method_compare,
        raw_filtered_count=len(raw_filtered) if not raw_filtered.empty else len(raw_unfiltered),
        raw_prior_excluded=raw_prior_excluded,
    )
    conclusion_md = build_engineering_conclusion(
        reporting_year=ry,
        candidates=candidates,
        stage_counts=stage_counts,
        method_compare=method_compare,
        column_inventory=column_inventory,
    )

    inv_path = debug / "business_ready_column_inventory.xlsx"
    cand_path = debug / "business_ready_prior_year_candidates.xlsx"
    val_path = debug / "business_ready_filter_validation.md"
    concl_path = debug / "business_ready_filter_engineering_conclusion.md"

    write_full_export_excel(
        inv_path,
        {
            "Column_Inventory": column_inventory,
            "Date_Trace": date_trace,
            "Pipeline_Stage_Prior_Year": stage_counts,
            "Method_Comparison": pd.DataFrame([method_compare]),
            "Dropped_Raw_Prior_Year": dropped_ids,
        },
    )
    safe_write_excel(
        cand_path,
        {
            "Prior_Year_Candidates": candidates,
            "All_Would_Filter_Benefit": candidates[candidates["would_filter_using_benefit_effective"]] if not candidates.empty and "would_filter_using_benefit_effective" in candidates.columns else candidates,
            "All_Would_Filter_Selected": candidates[candidates["would_filter_using_selected_transaction"]] if not candidates.empty and "would_filter_using_selected_transaction" in candidates.columns else candidates,
        },
        drop_duplicate_value_columns=False,
    )
    val_path.write_text(validation_md + "\n\n---\n\n" + conclusion_md, encoding="utf-8")
    concl_path.write_text(conclusion_md, encoding="utf-8")

    logger.info("Wrote column inventory → %s", inv_path)
    logger.info("Wrote prior-year candidates → %s", cand_path)
    logger.info("Wrote validation report → %s", val_path)
    logger.info("Wrote engineering conclusion → %s", concl_path)

    return {
        "column_inventory": str(inv_path),
        "prior_year_candidates": str(cand_path),
        "validation_md": str(val_path),
        "engineering_conclusion": str(concl_path),
        "prior_year_candidates_benefit": int(candidates["would_filter_using_benefit_effective"].sum()) if not candidates.empty and "would_filter_using_benefit_effective" in candidates.columns else 0,
        "prior_year_candidates_selected": int(candidates["would_filter_using_selected_transaction"].sum()) if not candidates.empty and "would_filter_using_selected_transaction" in candidates.columns else 0,
        "method_compare": method_compare,
        "stage_counts": stage_counts.to_dict(orient="records"),
    }
