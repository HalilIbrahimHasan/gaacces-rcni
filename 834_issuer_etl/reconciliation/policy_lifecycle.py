"""Policy lifecycle KPIs — monthly and rolling 3-month summaries."""

from __future__ import annotations

import pandas as pd

from database.db import Database
from reconciliation.cancellation_analysis import (
    cancel_without_confirm_report,
    repeated_cancel_report,
)
from utils.logger import get_logger

logger = get_logger(__name__)

MONTH_NAMES = {
    "01": "Jan", "02": "Feb", "03": "Mar", "04": "Apr",
    "05": "May", "06": "Jun", "07": "Jul", "08": "Aug",
    "09": "Sep", "10": "Oct", "11": "Nov", "12": "Dec",
}

ROLLING_WINDOWS = [
    ("01", "03", "Jan-Mar"),
    ("02", "04", "Feb-Apr"),
    ("03", "05", "Mar-May"),
    ("04", "06", "Apr-Jun"),
    ("05", "07", "May-Jul"),
    ("06", "08", "Jun-Aug"),
    ("07", "09", "Jul-Sep"),
    ("08", "10", "Aug-Oct"),
    ("09", "11", "Sep-Nov"),
    ("10", "12", "Oct-Dec"),
]


def _monthly_kpi_sql(where: str) -> str:
    return f"""
        SELECT issuer, year, month,
            SUM(CASE WHEN transaction_classification = 'CONFIRMATION'
                      OR additional_maint_reason_code = 'CONFIRM' THEN 1 ELSE 0 END)
                AS confirmed_policies,
            SUM(CASE WHEN transaction_classification = 'CANCELLATION' THEN 1 ELSE 0 END)
                AS cancelled_policies,
            SUM(CASE WHEN transaction_classification = 'TERMINATION' THEN 1 ELSE 0 END)
                AS terminated_policies,
            SUM(CASE WHEN cancellation_window_status = 'WITHIN_90_DAYS' THEN 1 ELSE 0 END)
                AS cancellations_within_90_days,
            SUM(CASE WHEN cancellation_window_status = 'OUTSIDE_90_DAYS'
                      AND transaction_classification = 'CANCELLATION' THEN 1 ELSE 0 END)
                AS cancellations_outside_90_days,
            SUM(CASE WHEN transaction_classification = 'TERMINATION'
                      AND cancellation_window_status = 'OUTSIDE_90_DAYS' THEN 1 ELSE 0 END)
                AS terminations_after_90_days,
            SUM(COALESCE(refund_eligible_user_fee, 0)) AS refund_eligible_user_fee,
            SUM(COALESCE(non_refundable_user_fee, 0)) AS non_refundable_user_fee,
            SUM(COALESCE(revenue_at_risk, 0)) AS revenue_at_risk,
            SUM(COALESCE(withheld_user_fee, 0)) AS withheld_user_fee,
            COUNT(DISTINCT policy_id) AS distinct_policies,
            COUNT(DISTINCT member_id) AS distinct_members,
            COUNT(*) AS total_records
        FROM stg_834_records {where}
        GROUP BY issuer, year, month
        ORDER BY issuer, year, month
    """


def issuer_kpi_summary(db: Database, issuer: str | None = None) -> pd.DataFrame:
    where = "WHERE issuer = ?" if issuer else ""
    params = (issuer,) if issuer else ()
    monthly = pd.read_sql_query(_monthly_kpi_sql(where), db.conn, params=params)

    if monthly.empty:
        return monthly

    # Attach repeated cancel + cancel-without-confirm counts per issuer/month
    repeated = repeated_cancel_report(db, issuer)
    no_confirm = cancel_without_confirm_report(db, issuer)

    monthly["repeated_cancellation_count"] = 0
    monthly["cancel_without_prior_confirmation_count"] = 0

    for idx, row in monthly.iterrows():
        iid, yr, mo = row["issuer"], row["year"], row["month"]
        period = f"{yr}-{mo}"
        if not repeated.empty:
            cnt = repeated[
                repeated["periods"].str.contains(period, na=False) & (repeated["issuer"] == iid)
            ]["cancel_count"].sum()
            monthly.at[idx, "repeated_cancellation_count"] = int(cnt)
        if not no_confirm.empty:
            cnt = len(no_confirm[
                (no_confirm["issuer"] == iid) & (no_confirm["year"] == yr) & (no_confirm["month"] == mo)
            ])
            monthly.at[idx, "cancel_without_prior_confirmation_count"] = int(cnt)

    return monthly


def household_member_counts(db: Database, issuer: str | None = None) -> pd.DataFrame:
    where = "WHERE issuer = ?" if issuer else ""
    params = (issuer,) if issuer else ()
    return pd.read_sql_query(
        f"""SELECT issuer, year, month, policy_id,
                   COUNT(DISTINCT member_id) AS member_count,
                   SUM(CASE WHEN subscriber_flag = 'Y' THEN 1 ELSE 0 END) AS subscriber_count
            FROM stg_834_records {where}
            GROUP BY issuer, year, month, policy_id
            HAVING subscriber_count > 1 OR member_count > 1
            ORDER BY member_count DESC""",
        db.conn, params=params,
    )


def rolling_3_month_kpi(db: Database, issuer: str | None = None) -> pd.DataFrame:
    """
    Calendar rolling 3-month windows: Jan-Mar, Feb-Apr, Mar-May, ... Oct-Dec.
    """
    monthly = issuer_kpi_summary(db, issuer)
    if monthly.empty:
        return monthly

    kpi_cols = [
        c for c in monthly.columns if c not in ("issuer", "year", "month")
    ]
    rows: list[dict] = []

    for issuer_name, grp in monthly.groupby("issuer"):
        grp = grp.copy()
        grp["month"] = grp["month"].astype(str).str.zfill(2)
        years = sorted(grp["year"].unique())

        for year in years:
            year_data = grp[grp["year"] == year].set_index("month")
            for start_m, end_m, label in ROLLING_WINDOWS:
                months_in_window = [
                    f"{int(m):02d}"
                    for m in range(int(start_m), int(end_m) + 1)
                ]
                available = [m for m in months_in_window if m in year_data.index]
                if not available:
                    continue
                window = year_data.loc[available]
                row = {
                    "issuer": issuer_name,
                    "year": year,
                    "rolling_3_month_period": label,
                    "period_start_month": start_m,
                    "period_end_month": end_m,
                    "months_included": ",".join(available),
                }
                for col in kpi_cols:
                    row[col] = window[col].sum()
                rows.append(row)

    result = pd.DataFrame(rows)
    logger.info("Rolling 3-month KPI: %d period(s)", len(result))
    return result


def refund_eligibility_detail(db: Database, issuer: str | None = None) -> pd.DataFrame:
    where = "WHERE refund_eligibility IN ('REFUND_REQUIRED', 'REVIEW_REQUIRED', 'NO_REFUND_TERMINATION')"
    params: tuple = ()
    if issuer:
        where += " AND issuer = ?"
        params = (issuer,)
    return pd.read_sql_query(
        f"""SELECT issuer, year, month, reporting_month,
                   policy_id, member_id,
                   benefit_effective_date, benefit_end_date, member_maint_effective_date,
                   transaction_classification, cancellation_window_status, refund_eligibility,
                   days_between_effective_and_cancel, months_between_effective_and_cancel,
                   days_between_effective_and_end, days_between_transaction_and_end,
                   expected_user_fee, refund_eligible_user_fee,
                   non_refundable_user_fee, revenue_at_risk, withheld_user_fee
            FROM stg_834_records {where}
            ORDER BY issuer, year, month, policy_id""",
        db.conn, params=params,
    )
