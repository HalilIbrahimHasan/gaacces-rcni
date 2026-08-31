"""Premium arithmetic validation."""

from __future__ import annotations

import pandas as pd

from database.db import Database
from utils.logger import get_logger

logger = get_logger(__name__)

TOLERANCE = 0.02


def apply_premium_validation(db: Database) -> int:
    """Update premium_validation_status on staging records."""
    df = pd.read_sql_query(
        """SELECT record_id, total_premium_amount, individual_responsibility_amount,
                  aptc_amount FROM stg_834_records""",
        db.conn,
    )
    updated = 0
    for _, row in df.iterrows():
        tp = row["total_premium_amount"]
        ir = row["individual_responsibility_amount"] or 0
        aptc = row["aptc_amount"] or 0
        if tp is None:
            status = "MISSING_PREMIUM"
        elif abs((ir + aptc) - tp) <= TOLERANCE:
            status = "PASS"
        else:
            status = "MISMATCH"
        db.execute(
            "UPDATE stg_834_records SET premium_validation_status=? WHERE record_id=?",
            (status, row["record_id"]),
        )
        updated += 1
    db.commit()
    logger.info("Premium validation applied to %d records", updated)
    return updated


def premium_mismatch_report(db: Database, issuer: str | None = None) -> pd.DataFrame:
    where = "WHERE premium_validation_status = 'MISMATCH'"
    params: tuple = ()
    if issuer:
        where += " AND issuer = ?"
        params = (issuer,)
    return pd.read_sql_query(
        f"""SELECT issuer, year, month, policy_id, member_id,
                   total_premium_amount, individual_responsibility_amount,
                   aptc_amount, premium_validation_status
            FROM stg_834_records {where}""",
        db.conn, params=params,
    )
