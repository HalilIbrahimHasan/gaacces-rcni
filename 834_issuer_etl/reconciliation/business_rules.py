"""
90-day cancellation / termination business rules layer.

Rules:
- Cancel within 90 days of coverage effective date → cancellation, refund may apply.
- Cancel after 90 days → reclassified as termination, no refund.
- Explicit termination → no refund for valid covered months.
- Computes timing vs effective date, transaction date, benefit end, reporting month.
"""

from __future__ import annotations

import math
from datetime import datetime
from typing import Any

import pandas as pd

from config.config import settings
from database.db import Database
from utils.logger import get_logger

logger = get_logger(__name__)

WINDOW_DAYS = 90  # overridden by settings at runtime

# Action classification
CLASS_CONFIRMATION = "CONFIRMATION"
CLASS_CANCELLATION = "CANCELLATION"
CLASS_TERMINATION = "TERMINATION"
CLASS_OTHER = "OTHER"
CLASS_UNKNOWN_REVIEW = "UNKNOWN_REVIEW"

STATUS_WITHIN = "WITHIN_90_DAYS"
STATUS_OUTSIDE = "OUTSIDE_90_DAYS"
STATUS_UNKNOWN = "UNKNOWN_DATE"
STATUS_NA = "N/A"

REFUND_REQUIRED = "REFUND_REQUIRED"
NO_REFUND = "NO_REFUND_TERMINATION"
REVIEW = "REVIEW_REQUIRED"
REFUND_NA = "N/A"


def _clean_value(value: Any) -> Any:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    return value


def _clean_row(row: dict[str, Any]) -> dict[str, Any]:
    return {key: _clean_value(val) for key, val in row.items()}


def _safe_float(value: Any) -> float:
    value = _clean_value(value)
    if value is None:
        return 0.0
    try:
        result = float(value)
    except (TypeError, ValueError):
        return 0.0
    if math.isnan(result) or math.isinf(result):
        return 0.0
    return result


def parse_date(s: str | None) -> datetime | None:
    s = _clean_value(s)
    if not s:
        return None
    s = str(s)
    cleaned = s[:10].replace("-", "")
    if len(cleaned) >= 8 and cleaned[:8].isdigit():
        try:
            return datetime.strptime(cleaned[:8], "%Y%m%d")
        except ValueError:
            pass
    try:
        return datetime.fromisoformat(s[:10])
    except ValueError:
        return None


def _is_cancel(action: str, reason: str) -> bool:
    return reason == "CANCEL" or "CANCEL" in action


def _is_term(action: str, reason: str) -> bool:
    return reason == "TERM" or "TERM" in action


def _is_confirm(action: str, reason: str) -> bool:
    return reason == "CONFIRM" or "CONFIRM" in action


def classify_record(row: dict[str, Any], window_days: int) -> dict[str, Any]:
    """
    Apply business rules to a single staging record.

    Returns dict of fields to UPDATE on stg_834_records.
    """
    row = _clean_row(row)
    action = str(row.get("action_code_description") or "").upper()
    reason = str(row.get("additional_maint_reason_code") or "").upper()

    eff = parse_date(row.get("benefit_effective_date"))
    txn = parse_date(row.get("member_maint_effective_date"))
    end = parse_date(row.get("benefit_end_date"))

    year = str(row.get("year") or "")
    month_raw = row.get("month")
    month = str(month_raw).zfill(2) if month_raw not in (None, "") else ""
    reporting_month = f"{year}-{month}" if year and month else None

    days_eff_to_txn = None
    months_eff_to_txn = None
    days_eff_to_end = None
    days_txn_to_end = None

    if eff and txn:
        days_eff_to_txn = (txn - eff).days
        months_eff_to_txn = round(days_eff_to_txn / 30.44, 1)
    if eff and end:
        days_eff_to_end = (end - eff).days
    if txn and end:
        days_txn_to_end = (end - txn).days

    window_status = STATUS_UNKNOWN
    refund = REVIEW
    txn_class = CLASS_OTHER
    revenue_at_risk = 0.0
    withheld_fee = 0.0
    expected_fee = _safe_float(row.get("expected_user_fee"))
    if expected_fee == 0.0:
        expected_fee = _safe_float(row.get("user_fee_amount"))

    if _is_confirm(action, reason):
        txn_class = CLASS_CONFIRMATION
        window_status = STATUS_NA
        refund = REFUND_NA

    elif _is_cancel(action, reason) or _is_term(action, reason):
        if not eff or not txn:
            window_status = STATUS_UNKNOWN
            refund = REVIEW
            txn_class = CLASS_CANCELLATION if _is_cancel(action, reason) else CLASS_TERMINATION
        elif _is_term(action, reason):
            # Explicit termination — always outside window, no refund
            txn_class = CLASS_TERMINATION
            window_status = STATUS_OUTSIDE
            refund = NO_REFUND
        elif days_eff_to_txn is not None and days_eff_to_txn <= window_days:
            # Cancel within 90 days → true cancellation, refund eligible
            txn_class = CLASS_CANCELLATION
            window_status = STATUS_WITHIN
            refund = REFUND_REQUIRED
            revenue_at_risk = expected_fee
        elif days_eff_to_txn is not None:
            # Cancel after 90 days → reclassify as termination, no refund
            txn_class = CLASS_TERMINATION
            window_status = STATUS_OUTSIDE
            refund = NO_REFUND
            withheld_fee = expected_fee  # fee collected, no refund owed

    return {
        "days_between_effective_and_cancel": days_eff_to_txn,
        "months_between_effective_and_cancel": months_eff_to_txn,
        "days_between_effective_and_end": days_eff_to_end,
        "days_between_transaction_and_end": days_txn_to_end,
        "reporting_month": reporting_month,
        "cancellation_window_status": window_status,
        "refund_eligibility": refund,
        "transaction_classification": txn_class,
        "revenue_at_risk": revenue_at_risk,
        "withheld_user_fee": withheld_fee,
        "non_refundable_user_fee": expected_fee if refund == NO_REFUND else 0.0,
        "refund_eligible_user_fee": expected_fee if refund == REFUND_REQUIRED else 0.0,
    }


def unknown_review_result(row: dict[str, Any], error: str) -> dict[str, Any]:
    """Fallback classification when a record cannot be processed safely."""
    row = _clean_row(row)
    expected_fee = _safe_float(row.get("expected_user_fee"))
    if expected_fee == 0.0:
        expected_fee = _safe_float(row.get("user_fee_amount"))
    year = str(row.get("year") or "")
    month_raw = row.get("month")
    month = str(month_raw).zfill(2) if month_raw not in (None, "") else ""
    reporting_month = f"{year}-{month}" if year and month else None
    return {
        "days_between_effective_and_cancel": None,
        "months_between_effective_and_cancel": None,
        "days_between_effective_and_end": None,
        "days_between_transaction_and_end": None,
        "reporting_month": reporting_month,
        "cancellation_window_status": STATUS_UNKNOWN,
        "refund_eligibility": REVIEW,
        "transaction_classification": CLASS_UNKNOWN_REVIEW,
        "revenue_at_risk": 0.0,
        "withheld_user_fee": 0.0,
        "non_refundable_user_fee": 0.0,
        "refund_eligible_user_fee": 0.0,
        "_audit_error": error,
    }


def _log_classification_error(row: dict[str, Any], exc: Exception) -> None:
    row = _clean_row(row)
    logger.error(
        "Business rule classification failed record_id=%s issuer=%s year=%s month=%s "
        "subscriber_id=%s member_id=%s policy_id=%s "
        "benefit_effective_date=%s member_maint_effective_date=%s benefit_end_date=%s "
        "action=%s reason=%s expected_user_fee=%s user_fee_amount=%s error=%s",
        row.get("record_id"),
        row.get("issuer"),
        row.get("year"),
        row.get("month"),
        row.get("subscriber_id"),
        row.get("member_id"),
        row.get("policy_id"),
        row.get("benefit_effective_date"),
        row.get("member_maint_effective_date"),
        row.get("benefit_end_date"),
        row.get("action_code_description"),
        row.get("additional_maint_reason_code"),
        row.get("expected_user_fee"),
        row.get("user_fee_amount"),
        exc,
        exc_info=True,
    )


def apply_business_rules(db: Database) -> int:
    """Apply 90-day cancellation/termination rules to all staging records."""
    window_days = settings.cancellation_window_days
    df = pd.read_sql_query(
        """SELECT record_id, issuer, year, month,
                  benefit_effective_date, benefit_end_date, member_maint_effective_date,
                  action_code_description, additional_maint_reason_code,
                  expected_user_fee, user_fee_amount,
                  subscriber_id, member_id, policy_id
           FROM stg_834_records""",
        db.conn,
    )
    updated = 0
    review_count = 0
    for _, row in df.iterrows():
        cleaned = _clean_row(row.to_dict())
        try:
            result = classify_record(cleaned, window_days)
        except Exception as exc:
            review_count += 1
            _log_classification_error(cleaned, exc)
            result = unknown_review_result(cleaned, str(exc))
        db.execute(
            """UPDATE stg_834_records SET
               days_between_effective_and_cancel = ?,
               months_between_effective_and_cancel = ?,
               days_between_effective_and_end = ?,
               days_between_transaction_and_end = ?,
               reporting_month = ?,
               cancellation_window_status = ?,
               refund_eligibility = ?,
               transaction_classification = ?,
               revenue_at_risk = ?,
               withheld_user_fee = ?,
               non_refundable_user_fee = ?,
               refund_eligible_user_fee = ?
               WHERE record_id = ?""",
            (
                result["days_between_effective_and_cancel"],
                result["months_between_effective_and_cancel"],
                result["days_between_effective_and_end"],
                result["days_between_transaction_and_end"],
                result["reporting_month"],
                result["cancellation_window_status"],
                result["refund_eligibility"],
                result["transaction_classification"],
                result["revenue_at_risk"],
                result["withheld_user_fee"],
                result["non_refundable_user_fee"],
                result["refund_eligible_user_fee"],
                row["record_id"],
            ),
        )
        updated += 1
    db.commit()
    logger.info(
        "Business rules applied to %d records (window=%d days, unknown_review=%d)",
        updated, window_days, review_count,
    )
    return updated
