"""
Temporary lineage audit — row counts and key cardinality at each pipeline step.

Debug-only; does not change transformation logic.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from azure_reconciliation.df_utils import find_col, normalize_id_series
from azure_reconciliation.lifecycle_engine import build_all_lifecycle_snapshots
from azure_reconciliation.lifecycle_snapshot_comparison import (
    _sort_chronological,
    build_enriched_canonical_xml,
    collapse_to_snapshot,
)
from azure_reconciliation.partition_discovery import Partition
from azure_reconciliation.record_comparison import join_key_series
from azure_reconciliation.reconciliation_analysis import (
    MAINT_ACTION_PREFIXES,
    _chandra_dashboard,
    _dedupe_transactions,
)
from azure_reconciliation.status_mapper import normalize_insurance_type, normalize_status
from azure_reconciliation.xml_business_reports import (
    PK,
    _attach_canonical_subscriber_columns,
    _insurance_series,
    _status_series,
    _zmonth,
    apply_business_month_basis,
    identify_cleanup_diagnostics,
    lifecycle_snapshots_to_model_input,
)
from config.config import settings
from utils.logger import get_logger

logger = get_logger(__name__)

LINEAGE_KEY_COLS = [
    "issuer",
    "enrollment_id",
    "enrollee_id",
    "insurance_type",
    "canonical_status",
    "snapshot_month",
]


def _debug_dir() -> Path:
    d = settings.outputs_path / "debug"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _enrollment_series(df: pd.DataFrame) -> pd.Series:
    for name in ("enrollment_id", "policy_id"):
        col = find_col(df, name)
        if col:
            return normalize_id_series(df[col])
    return pd.Series([""] * len(df), index=df.index, dtype=str)


def _enrollee_series(df: pd.DataFrame) -> pd.Series:
    for name in ("enrollee_id", "member_id"):
        col = find_col(df, name)
        if col:
            return normalize_id_series(df[col])
    return pd.Series([""] * len(df), index=df.index, dtype=str)


def _subscriber_series(df: pd.DataFrame) -> pd.Series:
    for name in ("subscriber_id", "exchg_subscriber_identifier", "issuer_subscriber_identifier"):
        col = find_col(df, name)
        if col:
            return normalize_id_series(df[col])
    return pd.Series([""] * len(df), index=df.index, dtype=str)


def _snapshot_month_series(df: pd.DataFrame) -> pd.Series:
    if "snapshot_month" in df.columns:
        return df["snapshot_month"].astype(str).map(_zmonth)
    if "month" in df.columns:
        return df["month"].astype(str).map(_zmonth)
    return pd.Series([""] * len(df), index=df.index, dtype=str)


def _canonical_status_series(df: pd.DataFrame) -> pd.Series:
    for col in ("canonical_status", "normalized_status", "status"):
        if col in df.columns:
            return df[col].astype(str).map(normalize_status)
    return pd.Series(["UNKNOWN"] * len(df), index=df.index)


def _lineage_key_series(df: pd.DataFrame) -> pd.Series:
    if df.empty:
        return pd.Series(dtype=str)
    work = df.copy()
    work["_issuer"] = work.get("issuer", "").astype(str)
    work["_enrollment_id"] = _enrollment_series(work)
    work["_enrollee_id"] = _enrollee_series(work)
    work["_insurance_type"] = _insurance_series(work)
    work["_canonical_status"] = _canonical_status_series(work)
    work["_snapshot_month"] = _snapshot_month_series(work)
    return (
        work["_issuer"] + "|"
        + work["_enrollment_id"] + "|"
        + work["_enrollee_id"] + "|"
        + work["_insurance_type"] + "|"
        + work["_canonical_status"] + "|"
        + work["_snapshot_month"]
    )


def _filter_audit_scope(
    df: pd.DataFrame,
    *,
    issuer: str,
    year: str,
    month: str,
) -> pd.DataFrame:
    """Rows relevant to issuer / business or snapshot month (inclusive for lifecycle stacks)."""
    if df.empty:
        return df
    work = df.copy()
    mask = work.get("issuer", pd.Series(dtype=str)).astype(str) == str(issuer)
    ym = f"{year}-{_zmonth(month)}"
    if "year" in work.columns and "month" in work.columns:
        biz = (
            (work["year"].astype(str) == str(year))
            & (work["month"].astype(str).map(_zmonth) == _zmonth(month))
        )
        mask &= biz
    elif "coverage_year" in work.columns and "snapshot_month" in work.columns:
        snap = (
            (work["coverage_year"].astype(str) == str(year))
            & (work["snapshot_month"].astype(str).map(_zmonth) == _zmonth(month))
        )
        mask &= snap
    elif "coverage_year_month" in work.columns:
        mask &= work["coverage_year_month"].astype(str) == ym
    elif "file_event_year_month" in work.columns:
        mask &= work["file_event_year_month"].astype(str) == ym
    return work[mask].copy()


def _audit_row(
    df: pd.DataFrame,
    *,
    step_name: str,
    function_name: str,
    dataframe_name: str,
    issuer: str,
    year: str,
    month: str,
) -> dict[str, Any]:
    scoped = _filter_audit_scope(df, issuer=issuer, year=year, month=month)
    keys = _lineage_key_series(scoped)
    distinct_keys = int(keys.nunique()) if not keys.empty else 0
    row_count = len(scoped)
    dup_keys = max(row_count - distinct_keys, 0)
    sample = ""
    if not keys.empty:
        sample = str(keys[keys.astype(str).str.strip() != ""].iloc[0])
    return {
        "step_name": step_name,
        "function_name": function_name,
        "dataframe_name": dataframe_name,
        "row_count": row_count,
        "distinct_enrollment_id_count": int(_enrollment_series(scoped).replace("", pd.NA).dropna().nunique()),
        "distinct_enrollee_id_count": int(_enrollee_series(scoped).replace("", pd.NA).dropna().nunique()),
        "distinct_subscriber_id_count": int(_subscriber_series(scoped).replace("", pd.NA).dropna().nunique()),
        "duplicate_key_count": dup_keys,
        "sample_key": sample,
    }


def _after_maintenance_filter(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    work = df.copy()
    if "action_code" in work.columns:
        ac = work["action_code"].astype(str).str.strip().str[:3]
        return work[~ac.isin(MAINT_ACTION_PREFIXES)].copy()
    if "maintenance_type_code" in work.columns:
        ac = work["maintenance_type_code"].astype(str).str.strip().str[:3]
        return work[~ac.isin(MAINT_ACTION_PREFIXES)].copy()
    return work


def _after_superseded_filter(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    sorted_work = _sort_chronological(df)
    sorted_work["_pk"] = join_key_series(sorted_work, PK)
    final_idx = sorted_work.groupby("_pk", dropna=False).tail(1).index
    return sorted_work[sorted_work.index.isin(final_idx)].copy()


def _audit_row_unscoped(
    df: pd.DataFrame,
    *,
    step_name: str,
    function_name: str,
    dataframe_name: str,
    issuer: str,
) -> dict[str, Any]:
    scoped = df[df.get("issuer", pd.Series(dtype=str)).astype(str) == str(issuer)].copy() if not df.empty and "issuer" in df.columns else df
    keys = _lineage_key_series(scoped)
    distinct_keys = int(keys.nunique()) if not keys.empty else 0
    row_count = len(scoped)
    return {
        "step_name": step_name,
        "function_name": function_name,
        "dataframe_name": dataframe_name,
        "row_count": row_count,
        "distinct_enrollment_id_count": int(_enrollment_series(scoped).replace("", pd.NA).dropna().nunique()),
        "distinct_enrollee_id_count": int(_enrollee_series(scoped).replace("", pd.NA).dropna().nunique()),
        "distinct_subscriber_id_count": int(_subscriber_series(scoped).replace("", pd.NA).dropna().nunique()),
        "duplicate_key_count": max(row_count - distinct_keys, 0),
        "sample_key": str(keys.iloc[0]) if not keys.empty else "",
    }


def build_lineage_audit_rows(
    xml_raw: pd.DataFrame,
    partitions: list[Partition],
    *,
    issuer: str,
    year: str,
    month: str,
) -> pd.DataFrame:
    """Collect row-count metrics at each pipeline step for one issuer/month."""
    rows: list[dict[str, Any]] = []

    def add(df: pd.DataFrame, step: str, func: str, name: str) -> None:
        rows.append(_audit_row(
            df, step_name=step, function_name=func, dataframe_name=name,
            issuer=issuer, year=year, month=month,
        ))

    add(xml_raw, "1_raw_xml_rows", "load_xml_rows", "xml_raw")

    canonical = build_enriched_canonical_xml(xml_raw, None, partitions=partitions)
    canonical, _ = apply_business_month_basis(canonical)
    add(canonical, "2_canonical_records", "build_enriched_canonical_xml", "canonical")

    dup, maint, sup, _ = identify_cleanup_diagnostics(canonical)
    after_dup = _dedupe_transactions(canonical)
    add(after_dup, "3_after_duplicate_cleanup", "_dedupe_transactions", "deduped_canonical")

    after_maint = _after_maintenance_filter(after_dup)
    add(after_maint, "4_after_maintenance_only_handling", "_after_maintenance_filter", "deduped_no_maintenance")

    after_sup = _after_superseded_filter(after_maint)
    add(after_sup, "5_after_superseded_handling", "_after_superseded_filter", "deduped_final_state")

    lifecycle_snap = build_all_lifecycle_snapshots(xml_raw, partitions)
    rows.append(_audit_row_unscoped(
        lifecycle_snap,
        step_name="7_lifecycle_output_total",
        function_name="build_all_lifecycle_snapshots",
        dataframe_name="lifecycle_snapshots_all_partitions",
        issuer=issuer,
    ))
    add(lifecycle_snap, "7_lifecycle_output", "build_all_lifecycle_snapshots", "lifecycle_snapshots")

    lifecycle_mapped = lifecycle_snapshots_to_model_input(lifecycle_snap)
    lifecycle_with_basis, _ = apply_business_month_basis(lifecycle_mapped)
    add(lifecycle_with_basis, "6_lifecycle_input_pre_subscriber", "lifecycle_snapshots_to_model_input+apply_business_month_basis", "lifecycle_input_pre_subscriber")

    lifecycle_input_legacy = _attach_canonical_subscriber_columns(lifecycle_with_basis, canonical)
    add(lifecycle_input_legacy, "6_lifecycle_input", "_attach_canonical_subscriber_columns", "lifecycle_input")

    add(lifecycle_input_legacy, "8_model_h_input", "process_issuer_xml_business", "model_h_input")

    model_h = _chandra_dashboard(lifecycle_input_legacy, source="xml")
    scoped_mh = model_h[
        (model_h["issuer"].astype(str) == str(issuer))
        & (model_h["year"].astype(str) == str(year))
        & (model_h["month"].astype(str).map(_zmonth) == _zmonth(month))
    ]
    rows.append({
        "step_name": "9_model_h_grouped_output",
        "function_name": "_chandra_dashboard",
        "dataframe_name": "model_h_monthly",
        "row_count": len(scoped_mh),
        "distinct_enrollment_id_count": int(scoped_mh["enrollment_count"].sum()) if "enrollment_count" in scoped_mh.columns else 0,
        "distinct_enrollee_id_count": int(scoped_mh["enrollee_count"].sum()) if "enrollee_count" in scoped_mh.columns else 0,
        "distinct_subscriber_id_count": int(scoped_mh["subscriber_count"].sum()) if "subscriber_count" in scoped_mh.columns else 0,
        "duplicate_key_count": 0,
        "sample_key": (
            f"groups={len(scoped_mh)};enrollment_sum={scoped_mh['enrollment_count'].sum()}"
            if not scoped_mh.empty and "enrollment_count" in scoped_mh.columns else ""
        ),
    })

    # Fixed-path preview (post-fix build) for comparison in same audit run
    from azure_reconciliation.xml_business_reports import _build_model_h_lifecycle_input

    fixed_input = _build_model_h_lifecycle_input(canonical, lifecycle_snap)
    fixed_input = _attach_canonical_subscriber_columns(fixed_input, canonical)
    add(fixed_input, "8b_model_h_input_fixed", "_build_model_h_lifecycle_input", "model_h_input_fixed")
    fixed_mh = _chandra_dashboard(fixed_input, source="xml")
    fixed_scoped = fixed_mh[
        (fixed_mh["issuer"].astype(str) == str(issuer))
        & (fixed_mh["year"].astype(str) == str(year))
        & (fixed_mh["month"].astype(str).map(_zmonth) == _zmonth(month))
    ]
    rows.append({
        "step_name": "9b_model_h_grouped_output_fixed",
        "function_name": "_chandra_dashboard",
        "dataframe_name": "model_h_monthly_fixed",
        "row_count": len(fixed_scoped),
        "distinct_enrollment_id_count": int(fixed_scoped["enrollment_count"].sum()) if "enrollment_count" in fixed_scoped.columns else 0,
        "distinct_enrollee_id_count": int(fixed_scoped["enrollee_count"].sum()) if "enrollee_count" in fixed_scoped.columns else 0,
        "distinct_subscriber_id_count": int(fixed_scoped["subscriber_count"].sum()) if "subscriber_count" in fixed_scoped.columns else 0,
        "duplicate_key_count": 0,
        "sample_key": (
            f"groups={len(fixed_scoped)};enrollment_sum={fixed_scoped['enrollment_count'].sum()}"
            if not fixed_scoped.empty and "enrollment_count" in fixed_scoped.columns else ""
        ),
    })

    return pd.DataFrame(rows)


def _first_explosion_step(audit: pd.DataFrame) -> str:
    if audit.empty:
        return "unknown"
    counts = audit["row_count"].tolist()
    names = audit["step_name"].tolist()
    prev = counts[0] if counts else 0
    for i in range(1, len(counts)):
        if counts[i] > prev * 1.5 and counts[i] > prev + 100:
            return names[i]
        prev = counts[i]
    return "none detected"


def write_lineage_audit(
    xml_raw: pd.DataFrame,
    partitions: list[Partition],
    *,
    issuer: str,
    year: str,
    month: str,
) -> tuple[Path, Path]:
    """Write CSV + markdown lineage audit for one issuer/month."""
    audit = build_lineage_audit_rows(
        xml_raw, partitions, issuer=issuer, year=year, month=month,
    )
    tag = f"{issuer}_{year}_{_zmonth(month)}"
    csv_path = _debug_dir() / f"lineage_audit_{tag}.csv"
    md_path = _debug_dir() / f"lineage_audit_{tag}.md"
    audit.to_csv(csv_path, index=False)

    explosion = _first_explosion_step(audit[~audit["step_name"].str.endswith("_fixed") & ~audit["step_name"].str.contains("total")])
    lines = [
        f"# Lineage Audit — {issuer} / {year} / {_zmonth(month)}",
        "",
        "| step | function | dataframe | rows | distinct enrollment | distinct enrollee | dup keys |",
        "|------|----------|-----------|------|---------------------|-------------------|----------|",
    ]
    for _, r in audit.iterrows():
        lines.append(
            f"| {r['step_name']} | {r['function_name']} | {r['dataframe_name']} | "
            f"{r['row_count']} | {r['distinct_enrollment_id_count']} | "
            f"{r['distinct_enrollee_id_count']} | {r['duplicate_key_count']} |"
        )
    lines.extend([
        "",
        f"**First unexpected row increase:** `{explosion}`",
        "",
        "## Key",
        "",
        "Lineage key: `issuer + enrollment_id + enrollee_id + insurance_type + canonical_status + snapshot_month`",
        "",
        "## Notes",
        "",
        "- Steps 6–9 (legacy path) stack all partition lifecycle snapshots then apply business month basis.",
        "- Step 8b/9b use latest-state per business month from deduped canonical (fix path).",
        "- Row counts scoped to issuer/year/month where columns exist.",
    ])
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    logger.info("Wrote lineage audit → %s, %s", csv_path, md_path)
    return csv_path, md_path
