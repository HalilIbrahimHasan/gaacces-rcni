"""User fee calculations and revenue metrics."""

from __future__ import annotations

import pandas as pd

from config.config import settings
from database.db import Database
from utils.logger import get_logger

logger = get_logger(__name__)


def apply_user_fees(db: Database) -> int:
    rate = settings.user_fee_rate
    db.execute(
        f"""UPDATE stg_834_records
            SET expected_user_fee = ROUND(total_premium_amount * {rate}, 4),
                user_fee_amount = COALESCE(user_fee_amount,
                    ROUND(total_premium_amount * {rate}, 4))
            WHERE total_premium_amount IS NOT NULL"""
    )
    db.commit()
    count = db.table_count(
        "stg_834_records", "expected_user_fee IS NOT NULL"
    )
    logger.info("User fee applied to %d records (rate=%.4f)", count, rate)
    return count


def user_fee_summary(db: Database, issuer: str | None = None) -> pd.DataFrame:
    where = "WHERE issuer = ?" if issuer else ""
    params = (issuer,) if issuer else ()
    return pd.read_sql_query(
        f"""SELECT issuer, year, month,
                   COUNT(*) AS record_count,
                   SUM(expected_user_fee) AS total_user_fee,
                   SUM(total_premium_amount) AS total_premium
            FROM stg_834_records {where}
            GROUP BY issuer, year, month
            ORDER BY issuer, year, month""",
        db.conn, params=params,
    )


def refund_user_fee_report(db: Database, issuer: str | None = None) -> pd.DataFrame:
    where = "WHERE refund_eligibility = 'REFUND_REQUIRED'"
    params: tuple = ()
    if issuer:
        where += " AND issuer = ?"
        params = (issuer,)
    return pd.read_sql_query(
        f"""SELECT issuer, year, month, policy_id, member_id,
                   expected_user_fee, refund_eligibility, cancellation_window_status
            FROM stg_834_records {where}""",
        db.conn, params=params,
    )
