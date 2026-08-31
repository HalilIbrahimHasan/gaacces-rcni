"""Cancellation gap, repeated cancel, and lifecycle analysis."""

from __future__ import annotations

import pandas as pd

from database.db import Database
from utils.logger import get_logger

logger = get_logger(__name__)


def repeated_cancel_report(db: Database, issuer: str | None = None) -> pd.DataFrame:
    where = ""
    params: tuple = ()
    if issuer:
        where = "WHERE issuer = ?"
        params = (issuer,)
    return pd.read_sql_query(
        f"""SELECT issuer, policy_id, member_id,
                   COUNT(*) AS cancel_count,
                   GROUP_CONCAT(DISTINCT year || '-' || month) AS periods
            FROM stg_834_records
            {where}
            {'AND' if where else 'WHERE'}
            (action_code_description LIKE '%Cancel%' OR additional_maint_reason_code = 'CANCEL')
            GROUP BY issuer, policy_id, member_id
            HAVING COUNT(*) > 1
            ORDER BY cancel_count DESC""",
        db.conn, params=params,
    )


def cancel_without_confirm_report(db: Database, issuer: str | None = None) -> pd.DataFrame:
    """Policies cancelled without a prior confirmation/effectuation record."""
    where = "WHERE s.issuer = ?" if issuer else ""
    params = (issuer,) if issuer else ()
    return pd.read_sql_query(
        f"""
        SELECT c.issuer, c.policy_id, c.member_id, c.year, c.month,
               c.action_code_description
        FROM stg_834_records c
        {where.replace('s.', 'c.') if where else ''}
        {'AND' if where else 'WHERE'}
        (c.action_code_description LIKE '%Cancel%'
         OR c.additional_maint_reason_code = 'CANCEL')
        AND NOT EXISTS (
            SELECT 1 FROM stg_834_records conf
            WHERE conf.policy_id = c.policy_id
              AND conf.member_id = c.member_id
              AND conf.issuer = c.issuer
              AND (conf.action_code_description LIKE '%Confirm%'
                   OR conf.additional_maint_reason_code = 'CONFIRM')
              AND (conf.year < c.year OR (conf.year = c.year AND conf.month < c.month))
        )
        """,
        db.conn, params=params,
    )


def cancellation_gap_report(db: Database, issuer: str | None = None) -> pd.DataFrame:
    """Policy active in one month but cancelled in a later month."""
    where = "WHERE issuer = ?" if issuer else ""
    params = (issuer,) if issuer else ()
    return pd.read_sql_query(
        f"""
        SELECT a.issuer, a.policy_id, a.member_id,
               a.year AS active_year, a.month AS active_month,
               c.year AS cancel_year, c.month AS cancel_month
        FROM stg_834_records a
        JOIN stg_834_records c
          ON a.policy_id = c.policy_id AND a.member_id = c.member_id
         AND a.issuer = c.issuer
        {where.replace('issuer', 'a.issuer') if where else ''}
        {'AND' if where else 'WHERE'}
        (a.action_code_description LIKE '%Confirm%'
         OR a.additional_maint_reason_code = 'CONFIRM')
        AND (c.action_code_description LIKE '%Cancel%'
             OR c.additional_maint_reason_code = 'CANCEL')
        AND (c.year > a.year OR (c.year = a.year AND c.month > a.month))
        """,
        db.conn, params=params,
    )
