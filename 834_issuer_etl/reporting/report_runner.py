"""Generate all KPI and reconciliation reports."""

from __future__ import annotations

from config.config import settings
from database.db import Database
from reconciliation.cancellation_analysis import (
    cancel_without_confirm_report,
    cancellation_gap_report,
    repeated_cancel_report,
)
from reconciliation.cancellation_window import cancellation_window_summary
from reconciliation.policy_lifecycle import refund_eligibility_detail
from reconciliation.policy_lifecycle import (
    household_member_counts,
    issuer_kpi_summary,
    rolling_3_month_kpi,
)
from reconciliation.premium_validation import premium_mismatch_report
from reconciliation.user_fee_calculation import (
    refund_user_fee_report,
    user_fee_summary,
)
from reporting.excel_writer import write_excel
from utils.logger import get_logger

logger = get_logger(__name__)


def run_kpi_reports(db: Database, issuer: str | None = None) -> None:
    out = settings.reports_path / "kpi"
    issuer_label = issuer or "all"

    kpi = issuer_kpi_summary(db, issuer)
    write_excel(kpi, out / "issuer_kpi_summary.xlsx", "kpi_summary")

    fee = user_fee_summary(db, issuer)
    write_excel(
        fee, out / "user_fee_validation.xlsx", "user_fee",
        extra_sheets={"refunds": refund_user_fee_report(db, issuer)},
    )

    write_excel(
        repeated_cancel_report(db, issuer),
        out / "repeated_cancel_report.xlsx", "repeated_cancels",
    )
    write_excel(
        cancellation_gap_report(db, issuer),
        out / "cancellation_gap_report.xlsx", "cancel_gaps",
    )
    write_excel(
        cancel_without_confirm_report(db, issuer),
        out / f"{issuer_label}_cancel_without_confirm.xlsx", "gaps",
    )
    write_excel(
        premium_mismatch_report(db, issuer),
        out / "premium_mismatch_report.xlsx", "mismatches",
    )
    write_excel(
        cancellation_window_summary(db, issuer),
        out / "cancellation_window_summary.xlsx", "window_summary",
    )
    write_excel(
        rolling_3_month_kpi(db, issuer),
        out / "rolling_3_month_kpi_summary.xlsx", "rolling_3mo",
    )
    write_excel(
        refund_eligibility_detail(db, issuer),
        out / "refund_eligibility_report.xlsx", "refund_eligibility",
        extra_sheets={"refund_summary": refund_user_fee_report(db, issuer)},
    )
    write_excel(
        household_member_counts(db, issuer),
        out / f"{issuer_label}_household_counts.xlsx", "households",
    )

    logger.info("KPI reports written to %s", out)
