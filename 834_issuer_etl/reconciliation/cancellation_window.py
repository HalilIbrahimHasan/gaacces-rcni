"""90-day cancellation window summaries — delegates to business_rules."""

from __future__ import annotations

import pandas as pd

from database.db import Database
from reconciliation.business_rules import apply_business_rules
from utils.logger import get_logger

logger = get_logger(__name__)


def apply_cancellation_window(db: Database) -> int:
    """Backward-compatible alias for apply_business_rules."""
    return apply_business_rules(db)


def cancellation_window_summary(db: Database, issuer: str | None = None) -> pd.DataFrame:
    where = "WHERE issuer = ?" if issuer else ""
    params = (issuer,) if issuer else ()
    return pd.read_sql_query(
        f"""SELECT issuer, year, month, reporting_month,
                   transaction_classification,
                   cancellation_window_status,
                   refund_eligibility,
                   COUNT(*) AS record_count,
                   SUM(refund_eligible_user_fee) AS refund_eligible_user_fee,
                   SUM(non_refundable_user_fee) AS non_refundable_user_fee,
                   SUM(revenue_at_risk) AS revenue_at_risk,
                   SUM(withheld_user_fee) AS withheld_user_fee,
                   AVG(days_between_effective_and_cancel) AS avg_days_eff_to_txn,
                   AVG(months_between_effective_and_cancel) AS avg_months_eff_to_txn
            FROM stg_834_records {where}
            GROUP BY issuer, year, month, reporting_month,
                     transaction_classification, cancellation_window_status,
                     refund_eligibility
            ORDER BY issuer, year, month""",
        db.conn, params=params,
    )
