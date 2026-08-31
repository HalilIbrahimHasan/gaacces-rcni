"""Row count validation against source and reference targets."""

from __future__ import annotations

import pandas as pd

from config.config import settings
from database.db import Database
from utils.logger import get_logger

logger = get_logger(__name__)


def run_count_validation(db: Database, issuer: str | None = None) -> pd.DataFrame:
    where = "WHERE issuer = ?" if issuer else ""
    params = (issuer,) if issuer else ()

    queries = {
        "by_issuer": f"""
            SELECT issuer, COUNT(*) AS row_count
            FROM stg_834_records {where}
            GROUP BY issuer ORDER BY issuer
        """,
        "by_issuer_year_month": f"""
            SELECT issuer, year, month, COUNT(*) AS row_count
            FROM stg_834_records {where}
            GROUP BY issuer, year, month ORDER BY issuer, year, month
        """,
        "by_action": f"""
            SELECT issuer, year, month, action_code_description, COUNT(*) AS row_count
            FROM stg_834_records {where}
            GROUP BY issuer, year, month, action_code_description
            ORDER BY issuer, year, month, action_code_description
        """,
    }

    frames = []
    for label, sql in queries.items():
        df = pd.read_sql_query(sql, db.conn, params=params)
        df["validation_type"] = label
        frames.append(df)
        logger.info("%s: %d group(s)", label, len(df))

    result = pd.concat(frames, ignore_index=True)

    refs = settings.reference_row_counts()
    if refs:
        for ref_issuer, expected in refs.items():
            if issuer and ref_issuer != issuer:
                continue
            actual = db.table_count("stg_834_records", "issuer = ?", (ref_issuer,))
            match = actual == expected
            logger.info(
                "Reference check %s: expected=%d actual=%d %s",
                ref_issuer, expected, actual, "PASS" if match else "FAIL",
            )
            result = pd.concat([result, pd.DataFrame([{
                "validation_type": "reference_match",
                "issuer": ref_issuer,
                "row_count": actual,
                "expected_count": expected,
                "status": "PASS" if match else "FAIL",
            }])], ignore_index=True)

    return result
