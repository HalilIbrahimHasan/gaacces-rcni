"""
Parser optional-field coverage reporting.

Produces validation-only summaries of which optional Companion Guide fields
were present in parsed staging data. Does not affect dashboards or counts.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

OPTIONAL_FIELDS: list[tuple[str, str]] = [
    ("exchg_assigned_enrollee_id", "Exchange Assigned Enrollee ID (REF*4A)"),
    ("request_submit_timestamp", "REQUEST SUBMIT TIMESTAMP (2750)"),
    ("enrollee_event_type_code", "Enrollee event type (eventTypeLkp)"),
    ("enrollee_event_reason_code", "Enrollee event reason (eventReasonLookUp)"),
    ("enrollment_action_code", "Enrollment action code (actionCode)"),
    ("issuer_subscriber_identifier", "Issuer subscriber identifier"),
    ("issuer_indiv_identifier", "Issuer individual identifier"),
    ("last_premium_paid_date", "Last premium paid date (DTP*543)"),
    ("qtyn", "QTYn (dependent total)"),
    ("qtyy", "QTYy"),
    ("qtyt", "QTYt (enrollee total)"),
]


def _is_populated(series: pd.Series) -> pd.Series:
    if series.empty:
        return pd.Series(dtype=bool)
    as_str = series.astype(str).str.strip()
    return series.notna() & (as_str != "") & (as_str.str.lower() != "nan")


def _count_distinct(series: pd.Series) -> int:
    mask = _is_populated(series)
    if not mask.any():
        return 0
    return int(series.loc[mask].astype(str).str.strip().nunique())


def build_parser_field_report(df: pd.DataFrame, issuer_id: str) -> dict[str, Any]:
    """Build summary dict for optional parser field availability."""
    total = len(df)
    field_rows: list[dict[str, Any]] = []

    for col, label in OPTIONAL_FIELDS:
        if col not in df.columns:
            field_rows.append({
                "issuer_id": issuer_id,
                "field_name": col,
                "field_label": label,
                "status": "column_absent",
                "populated_rows": 0,
                "missing_rows": total,
                "populated_pct": 0.0,
            })
            continue
        populated = int(_is_populated(df[col]).sum())
        field_rows.append({
            "issuer_id": issuer_id,
            "field_name": col,
            "field_label": label,
            "status": "present" if populated > 0 else "missing",
            "populated_rows": populated,
            "missing_rows": total - populated,
            "populated_pct": round(100.0 * populated / total, 2) if total else 0.0,
        })

    qty_summary: dict[str, Any] = {}
    for qty_col in ("qtyn", "qtyy", "qtyt"):
        if qty_col in df.columns and _is_populated(df[qty_col]).any():
            vals = df.loc[_is_populated(df[qty_col]), qty_col].astype(str).str.strip().unique()
            qty_summary[qty_col] = sorted(vals.tolist())

    return {
        "issuer_id": issuer_id,
        "total_rows": total,
        "distinct_policy_ids": _count_distinct(
            df["policy_id"] if "policy_id" in df.columns else pd.Series(dtype=object)
        ),
        "distinct_member_ids": _count_distinct(
            df["member_id"] if "member_id" in df.columns else pd.Series(dtype=object)
        ),
        "distinct_enrollee_ids": _count_distinct(
            df["exchg_assigned_enrollee_id"]
            if "exchg_assigned_enrollee_id" in df.columns
            else pd.Series(dtype=object)
        ),
        "enrollee_id_available": (
            "exchg_assigned_enrollee_id" in df.columns
            and _is_populated(df["exchg_assigned_enrollee_id"]).any()
        ),
        "event_type_available": (
            "enrollee_event_type_code" in df.columns
            and _is_populated(df["enrollee_event_type_code"]).any()
        ),
        "request_submit_timestamp_available": (
            "request_submit_timestamp" in df.columns
            and _is_populated(df["request_submit_timestamp"]).any()
        ),
        "qty_values_observed": qty_summary,
        "optional_fields": field_rows,
    }


def parser_field_report_to_dataframe(report: dict[str, Any]) -> pd.DataFrame:
    """Flatten optional-field rows for Excel export."""
    rows = report.get("optional_fields", [])
    if not rows:
        return pd.DataFrame(
            columns=[
                "issuer_id",
                "field_name",
                "field_label",
                "status",
                "populated_rows",
                "missing_rows",
                "populated_pct",
            ]
        )
    return pd.DataFrame(rows)


def parser_field_summary_to_dataframe(report: dict[str, Any]) -> pd.DataFrame:
    """Single-row summary of distinct IDs and availability flags."""
    return pd.DataFrame([{
        "issuer_id": report.get("issuer_id"),
        "total_rows": report.get("total_rows", 0),
        "distinct_policy_ids": report.get("distinct_policy_ids", 0),
        "distinct_member_ids": report.get("distinct_member_ids", 0),
        "distinct_enrollee_ids": report.get("distinct_enrollee_ids", 0),
        "enrollee_id_available": report.get("enrollee_id_available", False),
        "event_type_available": report.get("event_type_available", False),
        "request_submit_timestamp_available": report.get(
            "request_submit_timestamp_available", False
        ),
        "qty_values_observed": str(report.get("qty_values_observed", {})),
    }])
