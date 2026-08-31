"""
Comparison-only identifier and dedup-ordering analysis.

Produces reports comparing member-based vs enrollee-ID-based distinct counts
and maintenance-date vs request-submit-timestamp dedup ordering.

NOT used by enrollment summary, dashboards, or KPI totals.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

DEDUP_GROUP_COLUMNS = [
    "issuer_id",
    "source_year",
    "source_month",
    "Insurance_Type",
    "enrolleeStatus",
]

INSURANCE_TYPE_DISPLAY = {
    "HLT": "Health",
    "HEALTH": "Health",
    "H": "Health",
    "DEN": "Dental",
    "DENTAL": "Dental",
    "VIS": "Vision",
    "VISION": "Vision",
}

STATUS_TO_ID = {
    "CONFIRM": 1,
    "CANCEL": 2,
    "TERM": 3,
}


def _normalize_key_value(value: object) -> str | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    text = str(value).strip()
    return text if text else None


def _count_distinct(series: pd.Series) -> int:
    cleaned = series.dropna().astype(str).str.strip()
    cleaned = cleaned[cleaned != ""]
    return int(cleaned.nunique())


def _is_populated(series: pd.Series) -> bool:
    if series.empty:
        return False
    as_str = series.astype(str).str.strip()
    return bool((series.notna() & (as_str != "") & (as_str.str.lower() != "nan")).any())


def _member_column(df: pd.DataFrame) -> str:
    if "exchg_indiv_identifier" in df.columns:
        return "exchg_indiv_identifier"
    if "member_id" in df.columns:
        return "member_id"
    raise ValueError("No member identifier column found")


def _enrollee_id_column(df: pd.DataFrame) -> str | None:
    if "exchg_assigned_enrollee_id" in df.columns:
        return "exchg_assigned_enrollee_id"
    return None


def _policy_column(df: pd.DataFrame) -> str:
    if "exchg_assigned_policy_id" in df.columns:
        return "exchg_assigned_policy_id"
    if "policy_id" in df.columns:
        return "policy_id"
    raise ValueError("No policy identifier column found")


def _issuer_column(df: pd.DataFrame) -> str:
    if "issuer_id" in df.columns:
        return "issuer_id"
    if "issuer" in df.columns:
        return "issuer"
    raise ValueError("No issuer column found")


def _insurance_type_display(code: str | None) -> str:
    if code is None or (isinstance(code, float) and pd.isna(code)):
        return "Health"
    key = str(code).strip().upper()
    if not key:
        return "Health"
    return INSURANCE_TYPE_DISPLAY.get(key, "Health")


def _resolve_enrollee_status(row: pd.Series) -> str | None:
    """Same status resolution as enrollment_summary (for comparison grouping only)."""
    reason = str(row.get("additional_maint_reason_code") or "").strip().upper()
    if reason == "REINSTATE":
        return "CONFIRM"
    if reason in STATUS_TO_ID:
        return reason

    txn = str(row.get("transaction_classification") or "").strip().upper()
    if txn == "CONFIRMATION":
        return "CONFIRM"
    if txn == "CANCELLATION":
        return "CANCEL"
    if txn == "TERMINATION":
        return "TERM"

    action_desc = str(row.get("action_code_description") or "").upper()
    action_code = str(
        row.get("enrollee_event_type_code")
        or row.get("event_type_code")
        or row.get("action_code")
        or ""
    ).upper()

    for text in (reason, action_desc, action_code):
        if "REINSTATE" in text:
            return "CONFIRM"
        if "CONFIRM" in text:
            return "CONFIRM"
        if "CANCEL" in text:
            return "CANCEL"
        if "TERM" in text:
            return "TERM"

    return None


def _prepare_work(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy()
    if "source_year" not in work.columns and "year" in work.columns:
        work["source_year"] = work["year"].astype(str)
    if "source_month" not in work.columns and "month" in work.columns:
        work["source_month"] = work["month"].astype(str)
    if "source_file" not in work.columns and "file_name" in work.columns:
        work["source_file"] = work["file_name"]
    if "insurance_type_code" in work.columns:
        work["Insurance_Type"] = work["insurance_type_code"].map(_insurance_type_display)
    else:
        work["Insurance_Type"] = "Health"
    work["GAA_HIOS_ID"] = work[_issuer_column(work)].astype(str)
    work["enrolleeStatus"] = work.apply(_resolve_enrollee_status, axis=1)
    return work


def _dedupe_latest(
    work: pd.DataFrame,
    member_col: str,
    sort_col: str,
    *,
    sort_label: str,
) -> pd.DataFrame:
    """Keep latest row per member within status partition using given sort column."""
    if work.empty:
        return work

    file_col = "source_file" if "source_file" in work.columns else "file_name"
    sort_cols = [sort_col, file_col]
    for col in sort_cols:
        if col not in work.columns:
            work = work.copy()
            work[col] = ""

    sorted_work = work.sort_values(
        by=sort_cols,
        ascending=[False, False],
        na_position="last",
        kind="mergesort",
    )
    subset = [c for c in DEDUP_GROUP_COLUMNS if c in sorted_work.columns] + [member_col]
    deduped = sorted_work.drop_duplicates(subset=subset, keep="first")
    return deduped.assign(_dedup_sort_column=sort_label)


def build_identifier_comparison(df: pd.DataFrame, issuer_id: str) -> dict[str, Any]:
    """
    Compare member-based vs enrollee-ID-based counts and dedup orderings.

    Returns a dict with summary and detail DataFrames for validation export only.
    """
    empty_result: dict[str, Any] = {
        "issuer_id": issuer_id,
        "summary": pd.DataFrame(),
        "dedup_ordering_detail": pd.DataFrame(),
        "note": "No rows to compare",
    }
    if df.empty:
        return empty_result

    work = _prepare_work(df)
    work = work[work["enrolleeStatus"].notna()].copy()
    if work.empty:
        return empty_result

    member_col = _member_column(work)
    policy_col = _policy_column(work)
    enrollee_id_col = _enrollee_id_column(work)
    has_enrollee_id = enrollee_id_col is not None and _is_populated(work[enrollee_id_col])
    has_request_ts = (
        "request_submit_timestamp" in work.columns
        and _is_populated(work["request_submit_timestamp"])
    )

    maint_col = "member_maint_effective_date"
    if maint_col not in work.columns:
        maint_col = "benefit_effective_begin_date"

    dedup_maint = _dedupe_latest(work, member_col, maint_col, sort_label="member_maint_effective_date")
    ts_col = "request_submit_timestamp" if has_request_ts else maint_col
    ts_label = "request_submit_timestamp" if has_request_ts else "member_maint_effective_date (fallback)"
    dedup_ts = _dedupe_latest(work, member_col, ts_col, sort_label=ts_label)

    summary_rows: list[dict[str, Any]] = []
    for status in sorted(work["enrolleeStatus"].dropna().unique()):
        for ins_type in sorted(work["Insurance_Type"].dropna().unique()):
            mask = (work["enrolleeStatus"] == status) & (work["Insurance_Type"] == ins_type)
            raw_grp = work.loc[mask]
            maint_grp = dedup_maint.loc[
                (dedup_maint["enrolleeStatus"] == status)
                & (dedup_maint["Insurance_Type"] == ins_type)
            ]
            ts_grp = dedup_ts.loc[
                (dedup_ts["enrolleeStatus"] == status)
                & (dedup_ts["Insurance_Type"] == ins_type)
            ]

            row: dict[str, Any] = {
                "issuer_id": issuer_id,
                "enrolleeStatus": status,
                "Insurance_Type": ins_type,
                "raw_row_count": int(len(raw_grp)),
                "distinct_policy_ids_raw": _count_distinct(raw_grp[policy_col]),
                "distinct_member_ids_raw": _count_distinct(raw_grp[member_col]),
                "distinct_enrollee_ids_raw": (
                    _count_distinct(raw_grp[enrollee_id_col]) if has_enrollee_id else None
                ),
                "member_based_enrollment_count_maint_dedup": _count_distinct(
                    maint_grp[policy_col]
                ),
                "member_based_enrollee_count_maint_dedup": _count_distinct(
                    maint_grp[member_col]
                ),
                "enrollee_id_based_enrollee_count_maint_dedup": (
                    _count_distinct(maint_grp[enrollee_id_col])
                    if has_enrollee_id
                    else None
                ),
                "member_based_enrollee_count_ts_dedup": _count_distinct(ts_grp[member_col]),
                "enrollee_id_based_enrollee_count_ts_dedup": (
                    _count_distinct(ts_grp[enrollee_id_col]) if has_enrollee_id else None
                ),
                "dedup_rows_removed_maint_vs_raw": int(len(raw_grp) - len(maint_grp)),
                "dedup_rows_removed_ts_vs_raw": int(len(raw_grp) - len(ts_grp)),
                "maint_vs_ts_dedup_agreement": int(len(maint_grp) == len(ts_grp)),
            }
            summary_rows.append(row)

    detail_rows: list[dict[str, Any]] = []
    dup_members = work.loc[work.duplicated(subset=[member_col], keep=False), member_col]
    for member_val in dup_members.dropna().astype(str).unique()[:50]:
        member_rows = work[work[member_col].astype(str) == member_val]
        maint_pick = dedup_maint[dedup_maint[member_col].astype(str) == member_val]
        ts_pick = dedup_ts[dedup_ts[member_col].astype(str) == member_val]
        detail_rows.append({
            "issuer_id": issuer_id,
            "member_id": member_val,
            "duplicate_row_count": int(len(member_rows)),
            "maint_dedup_kept_maint_date": (
                maint_pick[maint_col].iloc[0] if not maint_pick.empty and maint_col in maint_pick else None
            ),
            "ts_dedup_kept_sort_value": (
                ts_pick[ts_col].iloc[0] if not ts_pick.empty else None
            ),
            "same_row_selected": (
                maint_pick.index[0] == ts_pick.index[0]
                if not maint_pick.empty and not ts_pick.empty
                else None
            ),
            "enrollee_id": (
                member_rows[enrollee_id_col].iloc[0]
                if has_enrollee_id and enrollee_id_col
                else None
            ),
        })

    return {
        "issuer_id": issuer_id,
        "summary": pd.DataFrame(summary_rows),
        "dedup_ordering_detail": pd.DataFrame(detail_rows),
        "comparison_notes": {
            "enrollee_id_field_available": has_enrollee_id,
            "request_submit_timestamp_available": has_request_ts,
            "maint_dedup_sort": "member_maint_effective_date DESC, source_file DESC",
            "timestamp_dedup_sort": (
                "request_submit_timestamp DESC, source_file DESC"
                if has_request_ts
                else "member_maint_effective_date DESC (timestamp unavailable)"
            ),
            "purpose": "Comparison only — not used in enrollment summary or dashboards",
        },
    }
