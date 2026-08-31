"""
Business-ready record export — read-only validation layer.

Exports the exact record-level dataset fed to Model H aggregation (after dedupe,
latest-state selection, and business-transaction collapse). Does not modify
pipeline logic or existing exports.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd

from azure_reconciliation.chandra_nan_safe import is_missing, safe_int, safe_optional_int, safe_status_id, safe_sum
from azure_reconciliation.chandra_business_format import (
    enrollee_status_display,
    insurance_type_display,
)
from azure_reconciliation.dashboard_difference_analysis import _enrollment_id_series
from azure_reconciliation.full_data_exports import (
    discover_issuer_year_pairs,
    write_full_export_excel,
)
from azure_reconciliation.partition_discovery import discover_partitions
from azure_reconciliation.prior_year_benefit_filter import parse_benefit_effective_year
from azure_reconciliation.safe_export import ExportErrors, safe_write_csv
from azure_reconciliation.status_mapper import normalize_insurance_type
from azure_reconciliation.xml_business_reports import (
    PK,
    process_issuer_xml_business,
)
from azure_reconciliation.xml_loader import load_xml_rows
from config.config import settings
from utils.logger import get_logger

logger = get_logger(__name__)

BUSINESS_READY_COLUMNS = [
    "issuer",
    "year",
    "month",
    "insurance_type",
    "status_Id",
    "enrolleeStatus",
    "canonical_enrollment_id",
    "canonical_enrollee_id",
    "canonical_subscriber_id",
    "policy_id",
    "member_id",
    "subscriber_id",
    "business_month",
    "benefit_effective_date",
    "benefit_effective_year",
    "selected_transaction_date",
    "selection_reason",
    "collapsed_event_count",
    "duplicate_flag",
    "maintenance_only_flag",
    "superseded_flag",
    "latest_state_flag",
    "model_h_included_flag",
    "dashboard_group_key",
    "raw_transaction_count",
    "raw_source_files",
    "raw_transaction_keys",
]

_COLLAPSE_GROUP_COLS = ["issuer", "policy_id", "member_id", "insurance_type", "year", "month"]


def export_root() -> Path:
    return settings.outputs_path / "business_data_exports"


def _zmonth(m: Any) -> str:
    return str(m).strip().zfill(2)


def _selected_transaction_date(row: pd.Series) -> str:
    for col in (
        "member_maint_effective_date",
        "benefit_effective_date",
        "file_event_date",
        "event_date",
    ):
        if col in row.index:
            val = str(row.get(col, "") or "").strip()
            if val:
                return val
    return ""


def _transaction_lineage_key(row: pd.Series) -> str:
    maint = ""
    for col in ("maintenance_type_code", "action_code", "enrollment_action_code"):
        if col in row.index and str(row.get(col, "") or "").strip():
            maint = str(row[col]).strip()
            break
    src = ""
    for col in ("file_name", "source_file", "raw_xml_path"):
        if col in row.index and str(row.get(col, "") or "").strip():
            src = Path(str(row[col])).name if col == "raw_xml_path" else str(row[col]).strip()
            break
    parts = [
        str(row.get("policy_id", "") or row.get("enrollment_id", "")),
        str(row.get("member_id", "") or row.get("enrollee_id", "")),
        maint,
        str(row.get("benefit_effective_date", "") or ""),
        str(row.get("member_maint_effective_date", "") or ""),
        src,
    ]
    return "|".join(parts)


def _pipe_join_unique(values: pd.Series) -> str:
    seen: list[str] = []
    for v in values.astype(str).str.strip():
        if v and v not in seen:
            seen.append(v)
    return "|".join(seen)


def _collapse_group_lineage(collapse_result: Any) -> pd.DataFrame:
    """Aggregate full collapse-audit groups (all events per business-ready selection)."""
    empty_cols = _COLLAPSE_GROUP_COLS + [
        "raw_transaction_count", "raw_source_files", "raw_transaction_keys",
    ]
    if collapse_result is None or not getattr(collapse_result, "audit", pd.DataFrame()).size:
        return pd.DataFrame(columns=empty_cols)

    audit = collapse_result.audit
    if audit.empty:
        return pd.DataFrame(columns=empty_cols)

    rows: list[dict[str, Any]] = []
    group_cols = [c for c in _COLLAPSE_GROUP_COLS if c in audit.columns]
    for key, grp in audit.groupby(group_cols, dropna=False):
        key_vals = dict(zip(group_cols, key if isinstance(key, tuple) else (key,)))
        txn_keys = grp.apply(_transaction_lineage_key, axis=1)
        rows.append({
            **key_vals,
            "raw_transaction_count": int(len(grp)),
            "raw_source_files": _pipe_join_unique(grp.get("source_file", pd.Series(dtype=str))),
            "raw_transaction_keys": _pipe_join_unique(txn_keys),
        })
    return pd.DataFrame(rows)


def _xml_lineage_lookup(
    xml_raw: pd.DataFrame,
    canonical: pd.DataFrame,
) -> dict[tuple[str, str, str, str], dict[str, Any]]:
    """
    Map (enrollment_id, business_year, business_month, insurance_type) → raw XML lineage.
    """
    if xml_raw.empty or canonical.empty:
        return {}

    xml = xml_raw.copy()
    xml["_enroll"] = _enrollment_id_series(xml)
    xml["_txn_key"] = xml.apply(_transaction_lineage_key, axis=1)
    if "file_name" in xml.columns:
        xml["_source_file"] = xml["file_name"].astype(str)
    elif "raw_xml_path" in xml.columns:
        xml["_source_file"] = xml["raw_xml_path"].astype(str).map(lambda p: Path(p).name)
    else:
        xml["_source_file"] = ""

    itype_col = "insurance_type_code" if "insurance_type_code" in xml.columns else "insurance_type"
    if itype_col in xml.columns:
        xml["_insurance_type"] = xml[itype_col].astype(str).map(normalize_insurance_type)
    else:
        xml["_insurance_type"] = "HEALTH"

    merge_keys = [
        c for c in (
            "policy_id", "member_id", "benefit_effective_date",
            "member_maint_effective_date", "maintenance_type_code", "file_name",
        )
        if c in xml.columns and c in canonical.columns
    ]
    if merge_keys:
        canon_sub = canonical[merge_keys + ["year", "month"]].drop_duplicates(subset=merge_keys, keep="last")
        xml = xml.merge(
            canon_sub.rename(columns={"year": "_biz_year", "month": "_biz_month"}),
            on=merge_keys,
            how="left",
        )
    else:
        xml["_biz_year"] = xml["year"].astype(str) if "year" in xml.columns else ""
        xml["_biz_month"] = xml["month"].astype(str).map(_zmonth) if "month" in xml.columns else ""

    xml["_biz_month"] = xml["_biz_month"].astype(str).map(_zmonth)

    lookup: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    for (enroll, by, bm, itype), grp in xml.groupby(
        ["_enroll", "_biz_year", "_biz_month", "_insurance_type"], dropna=False,
    ):
        enroll_s = str(enroll).strip()
        if not enroll_s:
            continue
        lookup[(enroll_s, str(by), _zmonth(str(bm)), str(itype))] = {
            "raw_transaction_count": len(grp),
            "raw_source_files": _pipe_join_unique(grp["_source_file"]),
            "raw_transaction_keys": _pipe_join_unique(grp["_txn_key"]),
        }
    return lookup


def _attach_raw_lineage(
    work: pd.DataFrame,
    *,
    result: Any,
    xml_raw: pd.DataFrame,
) -> pd.DataFrame:
    """Attach raw XML transaction lineage to business-ready rows."""
    out = work.copy()
    collapse_lineage = _collapse_group_lineage(result.collapse_result)
    xml_lookup = _xml_lineage_lookup(xml_raw, result.canonical)

    merge_keys = [c for c in _COLLAPSE_GROUP_COLS if c in out.columns and c in collapse_lineage.columns]
    if merge_keys and not collapse_lineage.empty:
        out = out.merge(
            collapse_lineage,
            on=merge_keys,
            how="left",
            suffixes=("", "_collapse"),
        )

    raw_counts: list[int] = []
    raw_files: list[str] = []
    raw_keys: list[str] = []

    for _, row in out.iterrows():
        enroll = str(row.get("canonical_enrollment_id", "") or row.get("policy_id", "")).strip()
        by = str(row.get("year", ""))
        bm = _zmonth(str(row.get("month", "")))
        itype = str(
            row.get("_insurance_type_norm")
            or normalize_insurance_type(str(row.get("insurance_type", "")))
        )

        xml_hit = xml_lookup.get((enroll, by, bm, itype), {})
        count = xml_hit.get("raw_transaction_count")
        files = xml_hit.get("raw_source_files", "")
        keys = xml_hit.get("raw_transaction_keys", "")

        if is_missing(count):
            count = safe_int(
                row.get("raw_transaction_count", row.get("collapsed_event_count", 1)), 1,
            )

        raw_counts.append(safe_int(count, 1))
        raw_files.append(files)
        raw_keys.append(keys)

    out["raw_transaction_count"] = raw_counts
    out["raw_source_files"] = raw_files
    out["raw_transaction_keys"] = raw_keys

    drop_cols = [c for c in out.columns if c.endswith("_collapse")]
    return out.drop(columns=drop_cols, errors="ignore")


def _dashboard_group_key(row: pd.Series) -> str:
    ins = str(
        row.get("_insurance_type_norm")
        or normalize_insurance_type(str(row.get("insurance_type", "")))
    )
    status = str(row.get("enrolleeStatus", "") or row.get("status", ""))
    return "|".join([
        str(row.get("issuer", "")),
        str(row.get("year", "")),
        _zmonth(str(row.get("month", ""))),
        ins,
        status,
    ])


def _attach_benefit_effective_dates(
    work: pd.DataFrame,
    canonical: pd.DataFrame,
) -> pd.DataFrame:
    """Ensure benefit_effective_date and benefit_effective_year on business-ready rows."""
    out = work.copy()
    bed_missing = (
        "benefit_effective_date" not in out.columns
        or out["benefit_effective_date"].astype(str).str.strip().isin(["", "nan", "None"]).all()
    )
    if bed_missing and not canonical.empty:
        merge_keys = [
            c for c in PK + ["year", "month"]
            if c in out.columns and c in canonical.columns
        ]
        if merge_keys:
            canon_sub = canonical[merge_keys + ["benefit_effective_date"]].drop_duplicates(
                subset=merge_keys, keep="last",
            )
            out = out.merge(canon_sub, on=merge_keys, how="left", suffixes=("", "_canon"))
            if "benefit_effective_date_canon" in out.columns:
                if "benefit_effective_date" not in out.columns:
                    out["benefit_effective_date"] = out["benefit_effective_date_canon"]
                else:
                    mask = out["benefit_effective_date"].astype(str).str.strip().isin(["", "nan", "None"])
                    out.loc[mask, "benefit_effective_date"] = out.loc[mask, "benefit_effective_date_canon"]
                out = out.drop(columns=["benefit_effective_date_canon"], errors="ignore")

    if "benefit_effective_date" not in out.columns:
        out["benefit_effective_date"] = ""
    out["benefit_effective_year"] = out["benefit_effective_date"].map(
        lambda v: (
            ""
            if parse_benefit_effective_year(v) is None or is_missing(parse_benefit_effective_year(v))
            else safe_optional_int(parse_benefit_effective_year(v), default="")
        ),
    )
    return out


def _merge_collapse_audit(
    lifecycle_input: pd.DataFrame,
    collapse_result: Any,
) -> pd.DataFrame:
    """Attach selection_reason and collapsed_event_count from collapse audit."""
    if lifecycle_input.empty:
        return lifecycle_input

    work = lifecycle_input.copy()
    if collapse_result is None or not getattr(collapse_result, "audit", pd.DataFrame()).size:
        work["selection_reason"] = work.get("selection_reason", "no_collapse_audit")
        if "collapsed_event_count" not in work.columns:
            work["collapsed_event_count"] = 1
        return work

    audit = collapse_result.audit
    kept = audit[audit["kept_for_model_h"] == True].copy()  # noqa: E712
    if kept.empty:
        work["selection_reason"] = "no_collapse_audit"
        if "collapsed_event_count" not in work.columns:
            work["collapsed_event_count"] = 1
        return work

    merge_keys = [
        c for c in ("issuer", "policy_id", "member_id", "insurance_type", "year", "month")
        if c in work.columns and c in kept.columns
    ]
    pick = merge_keys + ["reason", "collapsed_event_count"]
    kept_sub = kept[pick].rename(columns={"reason": "selection_reason"})
    if merge_keys:
        work = work.merge(kept_sub, on=merge_keys, how="left", suffixes=("", "_audit"))
    work["selection_reason"] = work["selection_reason"].fillna(
        work.get("selection_reason_audit", "kept_final_business_state"),
    )
    if "collapsed_event_count_audit" in work.columns:
        work["collapsed_event_count"] = work["collapsed_event_count"].fillna(
            work["collapsed_event_count_audit"],
        )
    work["collapsed_event_count"] = pd.to_numeric(
        work.get("collapsed_event_count", pd.Series([1] * len(work))), errors="coerce",
    ).fillna(1).apply(lambda v: safe_int(v, 1))
    work["selection_reason"] = work["selection_reason"].fillna("kept_final_business_state")
    drop_cols = [c for c in work.columns if c.endswith("_audit")]
    return work.drop(columns=drop_cols, errors="ignore")


def _attach_lineage_flags(work: pd.DataFrame, result: Any) -> pd.DataFrame:
    """Annotate flags — business-ready rows are Model H input by definition."""
    out = work.copy()
    out["model_h_included_flag"] = True
    out["latest_state_flag"] = True
    out["duplicate_flag"] = False
    out["maintenance_only_flag"] = False
    out["superseded_flag"] = False

    canonical = result.canonical
    if canonical.empty or out.empty:
        return out

    dup_idx = set(result.duplicate_df.index)
    maint_idx = set(result.maintenance_df.index)
    sup_idx = set(result.superseded_df.index)

    merge_keys = [
        c for c in PK + ["year", "month", "benefit_effective_date", "member_maint_effective_date"]
        if c in out.columns and c in canonical.columns
    ]
    if not merge_keys:
        return out

    canon_sub = canonical.copy()
    canon_sub["_canon_idx"] = canon_sub.index
    merged = out.merge(
        canon_sub[merge_keys + ["_canon_idx"]].drop_duplicates(),
        on=merge_keys,
        how="left",
    )
    if "_canon_idx" in merged.columns:
        merged["duplicate_flag"] = merged["_canon_idx"].isin(dup_idx)
        merged["maintenance_only_flag"] = merged["_canon_idx"].isin(maint_idx)
        merged["superseded_flag"] = merged["_canon_idx"].isin(sup_idx)
        merged = merged.drop(columns=["_canon_idx"], errors="ignore")
    return merged


def build_business_ready_records(
    result: Any,
    *,
    issuer: str,
    year: str,
    xml_raw: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Build business-ready export frame from process_issuer_xml_business result."""
    li = result.lifecycle_input
    if li.empty:
        return pd.DataFrame(columns=BUSINESS_READY_COLUMNS)

    work = li[li["year"].astype(str) == str(year)].copy()
    if work.empty:
        return pd.DataFrame(columns=BUSINESS_READY_COLUMNS)

    work = _merge_collapse_audit(work, result.collapse_result)
    work = _attach_lineage_flags(work, result)

    work["issuer"] = issuer
    work["year"] = work["year"].astype(str)
    work["month"] = work["month"].astype(str).map(_zmonth)
    work["business_month"] = work["year"] + "-" + work["month"]

    work["canonical_enrollment_id"] = _enrollment_id_series(work)
    work["canonical_enrollee_id"] = work["member_id"].astype(str) if "member_id" in work.columns else ""
    if "subscriber_id" in work.columns:
        work["canonical_subscriber_id"] = work["subscriber_id"].astype(str)
    else:
        work["canonical_subscriber_id"] = ""

    status_raw = work.get("normalized_status", work.get("status", "")).astype(str)
    work["enrolleeStatus"] = status_raw.map(enrollee_status_display)
    work["status_Id"] = [
        safe_status_id(None, enrollee_status=es) for es in work["enrolleeStatus"]
    ]
    work["_insurance_type_norm"] = work.get("insurance_type", "").astype(str).map(normalize_insurance_type)
    work["insurance_type"] = work["_insurance_type_norm"].map(insurance_type_display)

    work = _attach_benefit_effective_dates(work, result.canonical)
    work["selected_transaction_date"] = work.apply(_selected_transaction_date, axis=1)

    if "policy_id" not in work.columns:
        work["policy_id"] = work["canonical_enrollment_id"]
    if "member_id" not in work.columns:
        work["member_id"] = work["canonical_enrollee_id"]
    if "subscriber_id" not in work.columns:
        work["subscriber_id"] = work["canonical_subscriber_id"]

    if "collapsed_event_count" not in work.columns:
        work["collapsed_event_count"] = 1
    else:
        work["collapsed_event_count"] = pd.to_numeric(
            work["collapsed_event_count"], errors="coerce",
        ).fillna(1).apply(lambda v: safe_int(v, 1))

    if "selection_reason" not in work.columns:
        work["selection_reason"] = "kept_final_business_state"

    if xml_raw is not None:
        work = _attach_raw_lineage(work, result=result, xml_raw=xml_raw)

    work["dashboard_group_key"] = work.apply(_dashboard_group_key, axis=1)

    cols = [c for c in BUSINESS_READY_COLUMNS if c in work.columns]
    return work[cols].reset_index(drop=True)


def business_ready_monthly_counts(df: pd.DataFrame, issuer: str, year: str) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=[
            "issuer", "year", "month", "business_ready_record_count",
            "distinct_enrollment_count", "distinct_enrollee_count",
            "distinct_subscriber_count", "model_h_included_count",
        ])
    rows = []
    for month, grp in df.groupby(df["month"].astype(str).map(_zmonth), dropna=False):
        enroll = grp["canonical_enrollment_id"].astype(str).str.strip()
        enrollee = grp["canonical_enrollee_id"].astype(str).str.strip()
        sub = grp["canonical_subscriber_id"].astype(str).str.strip()
        rows.append({
            "issuer": issuer,
            "year": year,
            "month": month,
            "business_ready_record_count": len(grp),
            "distinct_enrollment_count": int(enroll[enroll != ""].nunique()),
            "distinct_enrollee_count": int(enrollee[enrollee != ""].nunique()),
            "distinct_subscriber_count": int(sub[sub != ""].nunique()),
            "model_h_included_count": safe_int(grp["model_h_included_flag"].sum(), len(grp)) if "model_h_included_flag" in grp.columns else len(grp),
        })
    return pd.DataFrame(rows).sort_values("month").reset_index(drop=True)


def business_ready_dashboard_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Business-facing dashboard-group summary (no subscriber fields)."""
    cols = [
        "issuer", "year", "month", "insurance_type", "status",
        "dashboard_group_key",
        "business_ready_records", "distinct_enrollment_ids", "distinct_enrollee_ids",
    ]
    if df.empty:
        return pd.DataFrame(columns=cols)

    rows = []
    for key, grp in df.groupby("dashboard_group_key", dropna=False):
        parts = str(key).split("|", 4)
        issuer = parts[0] if len(parts) > 0 else ""
        yr = parts[1] if len(parts) > 1 else ""
        month = _zmonth(parts[2]) if len(parts) > 2 else ""
        itype = parts[3] if len(parts) > 3 else ""
        status = parts[4] if len(parts) > 4 else ""
        enroll = grp["canonical_enrollment_id"].astype(str).str.strip()
        enrollee = grp["canonical_enrollee_id"].astype(str).str.strip()
        rows.append({
            "issuer": issuer,
            "year": yr,
            "month": month,
            "insurance_type": itype,
            "status": status,
            "dashboard_group_key": key,
            "business_ready_records": len(grp),
            "distinct_enrollment_ids": int(enroll[enroll != ""].nunique()),
            "distinct_enrollee_ids": int(enrollee[enrollee != ""].nunique()),
        })
    return pd.DataFrame(rows).sort_values(["year", "month", "insurance_type", "status"]).reset_index(drop=True)


def business_ready_internal_traceability(df: pd.DataFrame) -> pd.DataFrame:
    """Internal traceability summary — may include subscriber and raw lineage fields."""
    cols = [
        "issuer", "year", "month", "insurance_type", "status",
        "business_ready_records", "distinct_enrollment_ids", "distinct_enrollee_ids",
        "distinct_subscriber_ids", "dashboard_group_key",
        "raw_transaction_count", "raw_source_files", "raw_transaction_keys",
    ]
    if df.empty:
        return pd.DataFrame(columns=cols)

    rows = []
    for key, grp in df.groupby("dashboard_group_key", dropna=False):
        parts = str(key).split("|", 4)
        issuer = parts[0] if len(parts) > 0 else ""
        yr = parts[1] if len(parts) > 1 else ""
        month = _zmonth(parts[2]) if len(parts) > 2 else ""
        itype = parts[3] if len(parts) > 3 else ""
        status = parts[4] if len(parts) > 4 else ""
        enroll = grp["canonical_enrollment_id"].astype(str).str.strip()
        enrollee = grp["canonical_enrollee_id"].astype(str).str.strip()
        sub = grp["canonical_subscriber_id"].astype(str).str.strip()
        raw_tx_count = safe_sum(grp["raw_transaction_count"]) if "raw_transaction_count" in grp.columns else len(grp)
        raw_files = ""
        if "raw_source_files" in grp.columns:
            raw_files = "; ".join(sorted({str(v).strip() for v in grp["raw_source_files"] if str(v).strip()}))
        raw_keys = ""
        if "raw_transaction_keys" in grp.columns:
            raw_keys = "; ".join(sorted({str(v).strip() for v in grp["raw_transaction_keys"] if str(v).strip()}))
        rows.append({
            "issuer": issuer,
            "year": yr,
            "month": month,
            "insurance_type": itype,
            "status": status,
            "business_ready_records": len(grp),
            "distinct_enrollment_ids": int(enroll[enroll != ""].nunique()),
            "distinct_enrollee_ids": int(enrollee[enrollee != ""].nunique()),
            "distinct_subscriber_ids": int(sub[sub != ""].nunique()),
            "dashboard_group_key": key,
            "raw_transaction_count": raw_tx_count,
            "raw_source_files": raw_files,
            "raw_transaction_keys": raw_keys,
        })
    return pd.DataFrame(rows).sort_values(["year", "month", "insurance_type", "status"]).reset_index(drop=True)


def business_ready_yearly_counts(df: pd.DataFrame, issuer: str, year: str) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame([{
            "issuer": issuer,
            "year": year,
            "business_ready_record_count": 0,
            "distinct_enrollment_count": 0,
            "distinct_enrollee_count": 0,
            "distinct_subscriber_count": 0,
            "months_covered": 0,
        }])
    enroll = df["canonical_enrollment_id"].astype(str).str.strip()
    enrollee = df["canonical_enrollee_id"].astype(str).str.strip()
    sub = df["canonical_subscriber_id"].astype(str).str.strip()
    return pd.DataFrame([{
        "issuer": issuer,
        "year": year,
        "business_ready_record_count": len(df),
        "distinct_enrollment_count": int(enroll[enroll != ""].nunique()),
        "distinct_enrollee_count": int(enrollee[enrollee != ""].nunique()),
        "distinct_subscriber_count": int(sub[sub != ""].nunique()),
        "months_covered": int(df["month"].astype(str).nunique()),
    }])


@dataclass
class BusinessReadyExportResult:
    issuer: str
    year: str
    record_count: int = 0
    excel_path: Path | None = None
    csv_path: Path | None = None
    monthly_counts_path: Path | None = None
    yearly_counts_path: Path | None = None
    summary_path: Path | None = None


def export_issuer_year(
    issuer: str,
    year: str,
    *,
    parse_source: bool = False,
    export_errors: ExportErrors | None = None,
) -> BusinessReadyExportResult:
    """Export business-ready records for one issuer/year."""
    logger.info("Business-ready export: %s / %s", issuer, year)
    out = BusinessReadyExportResult(issuer=issuer, year=year)
    base = export_root() / issuer / year / "business_ready"
    base.mkdir(parents=True, exist_ok=True)

    partitions = discover_partitions(issuer_filter=issuer, year_filter=year)
    xml_raw = load_xml_rows(
        prefer_staging=not parse_source,
        issuer_filter=issuer,
        year_filter=year,
    )
    result = process_issuer_xml_business(issuer, xml_raw, partitions)
    df = build_business_ready_records(result, issuer=issuer, year=year, xml_raw=xml_raw)
    out.record_count = len(df)

    monthly = business_ready_monthly_counts(df, issuer, year)
    yearly = business_ready_yearly_counts(df, issuer, year)
    summary = business_ready_dashboard_summary(df)
    traceability = business_ready_internal_traceability(df)

    csv_path = base / "business_ready_all_months.csv"
    xlsx_path = base / "business_ready_all_months.xlsx"
    safe_write_csv(
        csv_path, df, table_name="BUSINESS_READY_ALL_MONTHS",
        export_errors=export_errors, drop_duplicate_value_columns=False,
    )
    write_full_export_excel(
        xlsx_path,
        {
            "README": pd.DataFrame({
                "note": [
                    "One row per business-ready record fed to Model H (after dedupe, latest-state, collapse).",
                    "Trace Enrollment_Count via dashboard_group_key and canonical_enrollment_id.",
                    "raw_transaction_count = original XML transactions represented by this record.",
                    "Engineering validation only — does not change production logic.",
                ],
            }),
            "BUSINESS_READY_ALL_MONTHS": df,
        },
        csv_fallback_paths={"BUSINESS_READY_ALL_MONTHS": csv_path},
        export_errors=export_errors,
    )
    out.csv_path = csv_path
    out.excel_path = xlsx_path

    monthly_xlsx = base / "business_ready_monthly_counts.xlsx"
    write_full_export_excel(
        monthly_xlsx,
        {"BUSINESS_READY_MONTHLY_COUNTS": monthly},
        export_errors=export_errors,
    )
    out.monthly_counts_path = monthly_xlsx

    yearly_xlsx = base / "business_ready_yearly_counts.xlsx"
    write_full_export_excel(
        yearly_xlsx,
        {"BUSINESS_READY_YEARLY_COUNTS": yearly},
        export_errors=export_errors,
    )
    out.yearly_counts_path = yearly_xlsx

    summary_xlsx = base / "business_ready_summary.xlsx"
    write_full_export_excel(
        summary_xlsx,
        {
            "Business_Ready_Summary": summary,
            "Internal_Traceability": traceability,
        },
        export_errors=export_errors,
    )
    out.summary_path = summary_xlsx

    return out


def run_business_ready_exports(
    *,
    issuer_filter: str | None = None,
    year_filter: str | None = None,
    parse_source: bool = False,
    export_errors: ExportErrors | None = None,
) -> dict[str, Any]:
    """Run business-ready exports for all matching issuer/year pairs."""
    root = export_root()
    root.mkdir(parents=True, exist_ok=True)
    pairs = discover_issuer_year_pairs(issuer_filter=issuer_filter, year_filter=year_filter)
    if not pairs:
        logger.warning("No issuer/year partitions found under source_data")
        return {"issuer_years": 0, "results": []}

    results: list[BusinessReadyExportResult] = []
    for issuer, year in pairs:
        try:
            results.append(
                export_issuer_year(
                    issuer, year, parse_source=parse_source, export_errors=export_errors,
                )
            )
        except Exception as exc:
            msg = f"Business-ready export failed for {issuer}/{year}: {exc}"
            logger.error(msg)
            if export_errors:
                export_errors.record(msg)

    return {
        "issuer_years": len(results),
        "output_root": str(root),
        "results": results,
    }
