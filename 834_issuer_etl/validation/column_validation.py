"""Column availability and missing required fields."""

from __future__ import annotations

import pandas as pd

from database.db import Database
from utils.logger import get_logger

logger = get_logger(__name__)

REQUIRED_FIELDS = [
    "issuer", "policy_id", "member_id", "subscriber_id",
    "benefit_effective_date", "total_premium_amount",
]

OPTIONAL_KEY_FIELDS = [
    "member_first_name", "member_last_name", "action_code_description",
    "individual_responsibility_amount", "aptc_amount", "user_fee_amount",
]


def run_column_validation(db: Database, issuer: str | None = None) -> pd.DataFrame:
    where = "WHERE issuer = ?" if issuer else ""
    params = (issuer,) if issuer else ()
    total = db.table_count("stg_834_records", where.replace("WHERE ", ""), params)

    rows = []
    for col in REQUIRED_FIELDS + OPTIONAL_KEY_FIELDS:
        null_sql = f"""
            SELECT COUNT(*) FROM stg_834_records
            {where}{' AND' if where else ' WHERE'} ({col} IS NULL OR {col} = '')
        """
        null_count = db.execute(null_sql, params).fetchone()[0]
        rows.append({
            "column": col,
            "total_rows": total,
            "null_or_empty": null_count,
            "availability_pct": round(100 * (1 - null_count / total), 2) if total else 0,
            "required": col in REQUIRED_FIELDS,
        })
        logger.info(
            "Column %s: %.1f%% available (%d null/empty)",
            col, rows[-1]["availability_pct"], null_count,
        )
    return pd.DataFrame(rows)
