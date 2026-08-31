"""
Build Hari-format enrollment summary reports with business-level deduplication.

Output columns (fixed order):
    Coverage_Year, GAA_HIOS_ID, GAA_Load_Date, Insurance_Type,
    status_id, enrolleeStatus, Enrollment_Count, Enrollee_Count
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pandas as pd

from utils.logger import get_logger

logger = get_logger(__name__)

OUTPUT_COLUMNS = [
    "Coverage_Year",
    "GAA_HIOS_ID",
    "GAA_Load_Date",
    "Insurance_Type",
    "status_id",
    "enrolleeStatus",
    "Enrollment_Count",
    "Enrollee_Count",
]

SUMMARY_GROUP_COLUMNS = [
    "Coverage_Year",
    "GAA_HIOS_ID",
    "GAA_Load_Date",
    "Insurance_Type",
    "status_id",
    "enrolleeStatus",
]

DEDUP_GROUP_COLUMNS = [
    "GAA_HIOS_ID",
    "source_year",
    "source_month",
    "Insurance_Type",
    "enrolleeStatus",
]

DEDUPLICATION_RULE_USED = (
    "Within each issuer/year/month/status/insurance_type group, sort by "
    "member_maint_effective_date descending, then source_file descending; "
    "keep the latest row per member_id; aggregate Enrollment_Count as "
    "COUNT(DISTINCT enrollment_key) and Enrollee_Count as COUNT(DISTINCT member_id). "
    "enrollment_key uses policy_id, else subscriber_id, else household_or_employee_case_id. "
    "REINSTATE rows are mapped to CONFIRM before deduplication."
)

STATUS_TO_ID = {
    "CONFIRM": 1,
    "CANCEL": 2,
    "TERM": 3,
}

INSURANCE_TYPE_DISPLAY = {
    "HLT": "Health",
    "HEALTH": "Health",
    "H": "Health",
    "DEN": "Dental",
    "DENTAL": "Dental",
    "VIS": "Vision",
    "VISION": "Vision",
}


@dataclass
class EnrollmentDedupDebug:
    """Debug metrics for enrollment summary deduplication."""

    raw_row_count: int = 0
    duplicate_row_count: int = 0
    distinct_policy_id_count_before_dedup: int = 0
    distinct_member_id_count_before_dedup: int = 0
    distinct_policy_id_count_after_dedup: int = 0
    distinct_member_id_count_after_dedup: int = 0
    reinstate_rows_mapped_to_confirm: int = 0
    deduplication_rule_used: str = DEDUPLICATION_RULE_USED
    enrollment_rows_using_policy_id: int = 0
    enrollment_rows_using_subscriber_id_fallback: int = 0
    enrollment_rows_using_household_fallback: int = 0
    enrollment_rows_missing_enrollment_key: int = 0
    distinct_enrollment_key_count_before_dedup: int = 0
    distinct_enrollment_key_count_after_dedup: int = 0
    enrollment_key_fallback_examples: list[dict[str, Any]] = field(default_factory=list)
    duplicate_member_examples: list[dict[str, Any]] = field(default_factory=list)
    duplicate_policy_examples: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "raw_row_count": self.raw_row_count,
            "duplicate_row_count": self.duplicate_row_count,
            "distinct_policy_id_count_before_dedup": self.distinct_policy_id_count_before_dedup,
            "distinct_member_id_count_before_dedup": self.distinct_member_id_count_before_dedup,
            "distinct_policy_id_count_after_dedup": self.distinct_policy_id_count_after_dedup,
            "distinct_member_id_count_after_dedup": self.distinct_member_id_count_after_dedup,
            "distinct_enrollment_key_count_before_dedup": self.distinct_enrollment_key_count_before_dedup,
            "distinct_enrollment_key_count_after_dedup": self.distinct_enrollment_key_count_after_dedup,
            "enrollment_rows_using_policy_id": self.enrollment_rows_using_policy_id,
            "enrollment_rows_using_subscriber_id_fallback": self.enrollment_rows_using_subscriber_id_fallback,
            "enrollment_rows_using_household_fallback": self.enrollment_rows_using_household_fallback,
            "enrollment_rows_missing_enrollment_key": self.enrollment_rows_missing_enrollment_key,
            "reinstate_rows_mapped_to_confirm": self.reinstate_rows_mapped_to_confirm,
            "deduplication_rule_used": self.deduplication_rule_used,
            "enrollment_key_fallback_examples": self.enrollment_key_fallback_examples,
            "duplicate_member_examples": self.duplicate_member_examples,
            "duplicate_policy_examples": self.duplicate_policy_examples,
        }


def _gaa_load_date(year: str | int, month: str | int) -> str:
    """Format load date as M/D/YYYY (first day of partition month)."""
    return f"{int(month)}/1/{int(year)}"


def _insurance_type_display(code: str | None) -> str:
    if code is None or (isinstance(code, float) and pd.isna(code)):
        return "Health"
    key = str(code).strip().upper()
    if not key:
        return "Health"
    return INSURANCE_TYPE_DISPLAY.get(key, "Health")


def _member_column(df: pd.DataFrame) -> str:
    if "exchg_indiv_identifier" in df.columns:
        return "exchg_indiv_identifier"
    if "member_id" in df.columns:
        return "member_id"
    raise ValueError("No member identifier column found for enrollee count")


def _policy_column(df: pd.DataFrame) -> str:
    if "exchg_assigned_policy_id" in df.columns:
        return "exchg_assigned_policy_id"
    if "policy_id" in df.columns:
        return "policy_id"
    raise ValueError("No policy identifier column found for enrollment count")


def _subscriber_column(df: pd.DataFrame) -> str:
    if "exchg_subscriber_identifier" in df.columns:
        return "exchg_subscriber_identifier"
    if "subscriber_id" in df.columns:
        return "subscriber_id"
    raise ValueError("No subscriber identifier column found")


def _household_column(df: pd.DataFrame) -> str:
    if "household_or_employee_case_id" in df.columns:
        return "household_or_employee_case_id"
    raise ValueError("No household identifier column found")


def _normalize_key_value(value: object) -> str | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    text = str(value).strip()
    return text if text else None


def _enrollment_key_from_row(row: pd.Series) -> tuple[str | None, str]:
    """
    Build a stable enrollment business key with fallback order:
    policy_id -> subscriber_id -> household_or_employee_case_id.
    """
    for col in ("exchg_assigned_policy_id", "policy_id"):
        policy = _normalize_key_value(row.get(col)) if col in row.index else None
        if policy:
            return f"policy:{policy}", "policy_id"

    for col in ("exchg_subscriber_identifier", "subscriber_id"):
        subscriber = _normalize_key_value(row.get(col)) if col in row.index else None
        if subscriber:
            return f"subscriber:{subscriber}", "subscriber_id"

    household = (
        _normalize_key_value(row.get("household_or_employee_case_id"))
        if "household_or_employee_case_id" in row.index
        else None
    )
    if household:
        return f"household:{household}", "household_or_employee_case_id"

    return None, "missing"


def _add_enrollment_keys(work: pd.DataFrame) -> pd.DataFrame:
    """Add enrollment_key and enrollment_key_source columns for business counting."""
    keys = work.apply(_enrollment_key_from_row, axis=1, result_type="expand")
    work = work.copy()
    work["enrollment_key"] = keys[0]
    work["enrollment_key_source"] = keys[1]
    return work


def _enrollment_key_fallback_stats(work: pd.DataFrame) -> dict[str, int]:
    if work.empty or "enrollment_key_source" not in work.columns:
        return {
            "enrollment_rows_using_policy_id": 0,
            "enrollment_rows_using_subscriber_id_fallback": 0,
            "enrollment_rows_using_household_fallback": 0,
            "enrollment_rows_missing_enrollment_key": 0,
        }
    counts = work["enrollment_key_source"].value_counts()
    return {
        "enrollment_rows_using_policy_id": int(counts.get("policy_id", 0)),
        "enrollment_rows_using_subscriber_id_fallback": int(counts.get("subscriber_id", 0)),
        "enrollment_rows_using_household_fallback": int(
            counts.get("household_or_employee_case_id", 0)
        ),
        "enrollment_rows_missing_enrollment_key": int(counts.get("missing", 0)),
    }


def _enrollment_key_fallback_examples(
    work: pd.DataFrame,
    *,
    limit: int = 10,
) -> list[dict[str, Any]]:
    if work.empty or "enrollment_key_source" not in work.columns:
        return []

    member_col = _member_column(work)
    file_col = _source_file_column(work)
    policy_col = "exchg_assigned_policy_id" if "exchg_assigned_policy_id" in work.columns else "policy_id"
    subscriber_col = (
        "exchg_subscriber_identifier"
        if "exchg_subscriber_identifier" in work.columns
        else "subscriber_id"
    )
    household_col = "household_or_employee_case_id"

    fallback = work[work["enrollment_key_source"] != "policy_id"].copy()
    if fallback.empty:
        return []

    examples: list[dict[str, Any]] = []
    for _, row in fallback.head(limit).iterrows():
        examples.append(
            {
                "enrollment_key": row.get("enrollment_key"),
                "enrollment_key_source": row.get("enrollment_key_source"),
                "member_id": row.get(member_col),
                "policy_id": row.get(policy_col),
                "subscriber_id": row.get(subscriber_col),
                "household_or_employee_case_id": row.get(household_col),
                "source_file": row.get(file_col),
                "enrolleeStatus": row.get("enrolleeStatus"),
            }
        )
    return examples


def _issuer_column(df: pd.DataFrame) -> str:
    if "issuer_id" in df.columns:
        return "issuer_id"
    if "issuer" in df.columns:
        return "issuer"
    raise ValueError("No issuer column found")


def _maint_date_column(df: pd.DataFrame) -> str:
    for col in ("member_maint_effective_date", "benefit_effective_begin_date"):
        if col in df.columns:
            return col
    return "member_maint_effective_date"


def _source_file_column(df: pd.DataFrame) -> str:
    if "source_file" in df.columns:
        return "source_file"
    if "file_name" in df.columns:
        return "file_name"
    return "source_file"


def _resolve_enrollee_status_legacy(row: pd.Series) -> str | None:
    """Legacy status mapping — REINSTATE excluded (pre-business-logic behavior)."""
    reason = str(row.get("additional_maint_reason_code") or "").strip().upper()
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
    action_code = str(row.get("event_type_code") or row.get("action_code") or "").upper()

    for text in (reason, action_desc, action_code):
        if "CONFIRM" in text:
            return "CONFIRM"
        if "CANCEL" in text:
            return "CANCEL"
        if "TERM" in text:
            return "TERM"

    return None


def _resolve_enrollee_status(row: pd.Series) -> str | None:
    """Map a row to CONFIRM, CANCEL, or TERM. REINSTATE maps to CONFIRM."""
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
    action_code = str(row.get("event_type_code") or row.get("action_code") or "").upper()

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


def _is_reinstate_row(row: pd.Series) -> bool:
    reason = str(row.get("additional_maint_reason_code") or "").strip().upper()
    if reason == "REINSTATE":
        return True
    action_desc = str(row.get("action_code_description") or "").upper()
    return "REINSTATE" in action_desc


def _prepare_work_frame(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy()
    issuer_col = _issuer_column(work)

    if "source_year" not in work.columns and "year" in work.columns:
        work["source_year"] = work["year"].astype(str)
    if "source_month" not in work.columns and "month" in work.columns:
        work["source_month"] = work["month"].astype(str)

    work["Coverage_Year"] = work["source_year"].astype(str)
    work["GAA_HIOS_ID"] = work[issuer_col].astype(str)
    work["GAA_Load_Date"] = work.apply(
        lambda r: _gaa_load_date(r["source_year"], r["source_month"]),
        axis=1,
    )
    if "insurance_type_code" in work.columns:
        work["Insurance_Type"] = work["insurance_type_code"].map(_insurance_type_display)
    else:
        work["Insurance_Type"] = "Health"

    if _source_file_column(work) not in work.columns:
        work["source_file"] = ""

    return work


def _count_distinct(series: pd.Series) -> int:
    cleaned = series.dropna().astype(str).str.strip()
    cleaned = cleaned[cleaned != ""]
    return int(cleaned.nunique())


def _apply_debug_fallback_stats(debug: EnrollmentDedupDebug, work: pd.DataFrame) -> None:
    stats = _enrollment_key_fallback_stats(work)
    debug.enrollment_rows_using_policy_id = stats["enrollment_rows_using_policy_id"]
    debug.enrollment_rows_using_subscriber_id_fallback = stats[
        "enrollment_rows_using_subscriber_id_fallback"
    ]
    debug.enrollment_rows_using_household_fallback = stats[
        "enrollment_rows_using_household_fallback"
    ]
    debug.enrollment_rows_missing_enrollment_key = stats[
        "enrollment_rows_missing_enrollment_key"
    ]
    debug.enrollment_key_fallback_examples = _enrollment_key_fallback_examples(work)


def _duplicate_examples(
    df: pd.DataFrame,
    key_col: str,
    *,
    limit: int = 10,
) -> list[dict[str, Any]]:
    if df.empty or key_col not in df.columns:
        return []

    member_col = _member_column(df)
    policy_col = _policy_column(df)
    file_col = _source_file_column(df)
    maint_col = _maint_date_column(df)

    dup_mask = df.duplicated(subset=[key_col], keep=False)
    dupes = df.loc[dup_mask].copy()
    if dupes.empty:
        return []

    examples: list[dict[str, Any]] = []
    for key_val, grp in dupes.groupby(key_col, dropna=False):
        if pd.isna(key_val) or str(key_val).strip() == "":
            continue
        examples.append(
            {
                key_col: str(key_val),
                "row_count": int(len(grp)),
                "member_ids": sorted(grp[member_col].dropna().astype(str).unique().tolist())[:5],
                "policy_ids": sorted(grp[policy_col].dropna().astype(str).unique().tolist())[:5],
                "source_files": sorted(grp[file_col].dropna().astype(str).unique().tolist())[:5],
                "member_maint_dates": sorted(
                    grp[maint_col].dropna().astype(str).unique().tolist(),
                    reverse=True,
                )[:5],
            }
        )
        if len(examples) >= limit:
            break
    return examples


def _dedupe_business_rows(work: pd.DataFrame) -> tuple[pd.DataFrame, EnrollmentDedupDebug]:
    """Deduplicate to latest row per member within issuer/year/month/status/insurance."""
    member_col = _member_column(work)
    policy_col = _policy_column(work)
    file_col = _source_file_column(work)
    maint_col = _maint_date_column(work)

    debug = EnrollmentDedupDebug(
        raw_row_count=len(work),
        distinct_policy_id_count_before_dedup=_count_distinct(work[policy_col]),
        distinct_member_id_count_before_dedup=_count_distinct(work[member_col]),
        distinct_enrollment_key_count_before_dedup=_count_distinct(work["enrollment_key"]),
        duplicate_member_examples=_duplicate_examples(work, member_col),
        duplicate_policy_examples=_duplicate_examples(work, "enrollment_key"),
    )
    _apply_debug_fallback_stats(debug, work)

    if work.empty:
        return work, debug

    sort_cols = [maint_col, file_col]
    for col in sort_cols:
        if col not in work.columns:
            work[col] = ""

    sorted_work = work.sort_values(
        by=sort_cols,
        ascending=[False, False],
        na_position="last",
        kind="mergesort",
    )

    deduped = (
        sorted_work.drop_duplicates(subset=DEDUP_GROUP_COLUMNS + [member_col], keep="first")
        .copy()
    )

    debug.duplicate_row_count = debug.raw_row_count - len(deduped)
    debug.distinct_policy_id_count_after_dedup = _count_distinct(deduped[policy_col])
    debug.distinct_member_id_count_after_dedup = _count_distinct(deduped[member_col])
    debug.distinct_enrollment_key_count_after_dedup = _count_distinct(deduped["enrollment_key"])
    return deduped, debug


def build_enrollment_summary_legacy(df: pd.DataFrame) -> pd.DataFrame:
    """Previous raw-row aggregation (for comparison reports only)."""
    if df.empty:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    work = _prepare_work_frame(df)
    member_col = _member_column(work)

    work["enrolleeStatus"] = work.apply(_resolve_enrollee_status_legacy, axis=1)
    work = work[work["enrolleeStatus"].notna()].copy()
    if work.empty:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    work["status_id"] = work["enrolleeStatus"].map(STATUS_TO_ID)

    grouped = (
        work.groupby(SUMMARY_GROUP_COLUMNS, dropna=False)
        .agg(
            Enrollment_Count=(member_col, "count"),
            Enrollee_Count=(member_col, "nunique"),
        )
        .reset_index()
    )

    grouped["status_id"] = grouped["status_id"].astype(int)
    grouped["Enrollment_Count"] = grouped["Enrollment_Count"].astype(int)
    grouped["Enrollee_Count"] = grouped["Enrollee_Count"].astype(int)
    return grouped[OUTPUT_COLUMNS].sort_values(
        ["Coverage_Year", "GAA_Load_Date", "status_id"]
    ).reset_index(drop=True)


def build_enrollment_summary_with_debug(
    df: pd.DataFrame,
) -> tuple[pd.DataFrame, EnrollmentDedupDebug]:
    """
    Build business-level enrollment summary with deduplication metrics.

    Enrollment_Count = COUNT(DISTINCT enrollment_key) after dedup.
    Enrollee_Count = COUNT(DISTINCT member_id) after dedup.
    enrollment_key fallback: policy_id -> subscriber_id -> household_or_employee_case_id.
    """
    empty_debug = EnrollmentDedupDebug()
    if df.empty:
        return pd.DataFrame(columns=OUTPUT_COLUMNS), empty_debug

    work = _prepare_work_frame(df)
    member_col = _member_column(work)

    work["enrolleeStatus"] = work.apply(_resolve_enrollee_status, axis=1)
    reinstate_mask = work.apply(_is_reinstate_row, axis=1)
    reinstate_count = int(reinstate_mask.sum())

    work = work[work["enrolleeStatus"].notna()].copy()
    if work.empty:
        empty_debug.reinstate_rows_mapped_to_confirm = reinstate_count
        return pd.DataFrame(columns=OUTPUT_COLUMNS), empty_debug

    work["status_id"] = work["enrolleeStatus"].map(STATUS_TO_ID)
    work = _add_enrollment_keys(work)

    deduped, debug = _dedupe_business_rows(work)
    debug.reinstate_rows_mapped_to_confirm = reinstate_count

    if deduped.empty:
        return pd.DataFrame(columns=OUTPUT_COLUMNS), debug

    grouped = (
        deduped.groupby(SUMMARY_GROUP_COLUMNS, dropna=False)
        .agg(
            Enrollment_Count=("enrollment_key", lambda s: _count_distinct(s)),
            Enrollee_Count=(member_col, lambda s: _count_distinct(s)),
        )
        .reset_index()
    )

    grouped["status_id"] = grouped["status_id"].astype(int)
    grouped["Enrollment_Count"] = grouped["Enrollment_Count"].astype(int)
    grouped["Enrollee_Count"] = grouped["Enrollee_Count"].astype(int)

    result = grouped[OUTPUT_COLUMNS].sort_values(
        ["Coverage_Year", "GAA_Load_Date", "status_id"]
    ).reset_index(drop=True)

    logger.info(
        "Built business enrollment summary: %d row(s), raw=%d deduped=%d removed=%d "
        "reinstate_mapped=%d issuer(s)=%s",
        len(result),
        debug.raw_row_count,
        debug.raw_row_count - debug.duplicate_row_count,
        debug.duplicate_row_count,
        debug.reinstate_rows_mapped_to_confirm,
        ", ".join(sorted(result["GAA_HIOS_ID"].unique())) if not result.empty else "",
    )
    return result, debug


def build_enrollment_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Build cleaned business enrollment summary for dashboards and exports."""
    summary, _ = build_enrollment_summary_with_debug(df)
    return summary
