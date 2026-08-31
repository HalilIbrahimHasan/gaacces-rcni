"""Build monthly and rollup KPI DataFrames from Azure enrollment rows."""

from __future__ import annotations

import pandas as pd

from azure_reconciliation.azure_mirror.columns import col_series, resolve_column
from azure_reconciliation.status_mapper import normalize_insurance_type, normalize_status
from utils.logger import get_logger

logger = get_logger(__name__)

STATUS_ID_MAP = {
    "ENROLLED": 1,
    "CANCELLED": 2,
    "TERMINATED": 3,
    "PENDING": 4,
    "UNKNOWN": 0,
}

INSURANCE_DISPLAY = {
    "HEALTH": "Health",
    "DENTAL": "Dental",
    "VISION": "Vision",
}


def prepare_azure_frame(
    df: pd.DataFrame,
    *,
    issuer: str,
    year: str,
    month: str,
) -> pd.DataFrame:
    """Add normalized fields and partition tags."""
    if df.empty:
        return df
    out = df.copy()
    out["_issuer"] = str(issuer)
    out["_source_year"] = str(year)
    out["_source_month"] = str(month).zfill(2)

    enrollee_status = col_series(out, "enrollee_status_description", "")
    enroll_status = col_series(out, "enrollment_status_description", "")
    combined = enrollee_status.fillna("").astype(str)
    combined = combined.where(combined.str.strip() != "", enroll_status.astype(str))
    out["_canonical_status"] = combined.map(normalize_status)

    ins = col_series(out, "Insurance_Type", "")
    out["_insurance_type"] = ins.map(normalize_insurance_type)

    return out


def _safe_nunique(df: pd.DataFrame, canonical: str) -> int:
    if df.empty:
        return 0
    actual = resolve_column(list(df.columns), canonical)
    if not actual or actual not in df.columns:
        return 0
    return int(df[actual].nunique(dropna=True))


def _subscriber_count(df: pd.DataFrame) -> int | None:
    if df.empty:
        return None
    actual = resolve_column(list(df.columns), "person_type")
    if not actual or actual not in df.columns:
        return None
    subs = df[actual].astype(str).str.upper().str.contains("SUBSCR", na=False)
    if not subs.any():
        return None
    return int(subs.sum())


def _status_count(df: pd.DataFrame, status: str) -> int:
    if df.empty or "_canonical_status" not in df.columns:
        return 0
    return int((df["_canonical_status"] == status).sum())


def _premium_sum(df: pd.DataFrame, canonical: str) -> float:
    if df.empty:
        return 0.0
    actual = resolve_column(list(df.columns), canonical)
    if not actual or actual not in df.columns:
        return 0.0
    return float(pd.to_numeric(df[actual], errors="coerce").sum(skipna=True) or 0.0)


def build_monthly_kpi_by_insurance_type(
    df: pd.DataFrame,
    *,
    issuer: str,
    year: str,
    month: str,
) -> pd.DataFrame:
    """Monthly KPI rows grouped by insurance_type."""
    prepared = prepare_azure_frame(df, issuer=issuer, year=year, month=month)
    if prepared.empty:
        return pd.DataFrame(columns=_monthly_kpi_columns())

    rows: list[dict] = []
    for ins_type, grp in prepared.groupby("_insurance_type", dropna=False):
        rows.append(_monthly_kpi_row(grp, issuer, year, month, str(ins_type)))
    return pd.DataFrame(rows)


def _monthly_kpi_columns() -> list[str]:
    return [
        "issuer",
        "year",
        "month",
        "insurance_type",
        "enrollment_count",
        "enrollee_count",
        "household_count",
        "subscriber_count",
        "enrolled_count",
        "cancelled_count",
        "terminated_count",
        "pending_count",
        "gross_premium_total",
        "net_premium_total",
        "aptc_total",
        "csr_total",
    ]


def _monthly_kpi_row(
    grp: pd.DataFrame,
    issuer: str,
    year: str,
    month: str,
    insurance_type: str,
) -> dict:
    sub = _subscriber_count(grp)
    return {
        "issuer": issuer,
        "year": year,
        "month": str(month).zfill(2),
        "insurance_type": insurance_type,
        "enrollment_count": _safe_nunique(grp, "enrollment_id"),
        "enrollee_count": _safe_nunique(grp, "enrollee_id"),
        "household_count": _safe_nunique(grp, "household_id"),
        "subscriber_count": sub,
        "enrolled_count": _status_count(grp, "ENROLLED"),
        "cancelled_count": _status_count(grp, "CANCELLED"),
        "terminated_count": _status_count(grp, "TERMINATED"),
        "pending_count": _status_count(grp, "PENDING"),
        "gross_premium_total": _premium_sum(grp, "gross_premium_amt"),
        "net_premium_total": _premium_sum(grp, "net_premium_amt"),
        "aptc_total": _premium_sum(grp, "aptc_amt"),
        "csr_total": _premium_sum(grp, "csr_amt"),
    }


def build_enrollment_summary(
    df: pd.DataFrame,
    *,
    issuer: str,
    year: str,
    month: str,
) -> pd.DataFrame:
    """Hari-format enrollment summary from Azure rows (mirrors XML report columns)."""
    prepared = prepare_azure_frame(df, issuer=issuer, year=year, month=month)
    if prepared.empty:
        return pd.DataFrame(
            columns=[
                "Coverage_Year",
                "GAA_HIOS_ID",
                "GAA_Load_Date",
                "Insurance_Type",
                "status_id",
                "enrolleeStatus",
                "Enrollment_Count",
                "Enrollee_Count",
            ]
        )

    load_date = f"{year}-{str(month).zfill(2)}-01"
    rows: list[dict] = []
    group_cols = ["_insurance_type", "_canonical_status"]
    for (ins_type, status), grp in prepared.groupby(group_cols, dropna=False):
        rows.append({
            "Coverage_Year": year,
            "GAA_HIOS_ID": issuer,
            "GAA_Load_Date": load_date,
            "Insurance_Type": INSURANCE_DISPLAY.get(str(ins_type), str(ins_type)),
            "status_id": STATUS_ID_MAP.get(str(status), 0),
            "enrolleeStatus": str(status),
            "Enrollment_Count": _safe_nunique(grp, "enrollment_id"),
            "Enrollee_Count": _safe_nunique(grp, "enrollee_id"),
        })
    return pd.DataFrame(rows)


def build_rollup_summary(all_prepared: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """
    Rollup KPI tables: issuer totals, by year, by insurance_type, by status,
    month-over-month trend, premium totals.
    """
    empty = pd.DataFrame()
    if all_prepared.empty:
        return {
            "issuer_totals": empty,
            "year_totals": empty,
            "insurance_type_totals": empty,
            "status_totals": empty,
            "month_trend": empty,
            "premium_totals": empty,
        }

    issuer = str(all_prepared["_issuer"].iloc[0]) if "_issuer" in all_prepared.columns else ""

    issuer_totals = pd.DataFrame([{
        "issuer": issuer,
        "enrollment_count": _safe_nunique(all_prepared, "enrollment_id"),
        "enrollee_count": _safe_nunique(all_prepared, "enrollee_id"),
        "household_count": _safe_nunique(all_prepared, "household_id"),
        "subscriber_count": _subscriber_count(all_prepared),
        "enrolled_count": _status_count(all_prepared, "ENROLLED"),
        "cancelled_count": _status_count(all_prepared, "CANCELLED"),
        "terminated_count": _status_count(all_prepared, "TERMINATED"),
        "pending_count": _status_count(all_prepared, "PENDING"),
        "gross_premium_total": _premium_sum(all_prepared, "gross_premium_amt"),
        "net_premium_total": _premium_sum(all_prepared, "net_premium_amt"),
        "aptc_total": _premium_sum(all_prepared, "aptc_amt"),
        "csr_total": _premium_sum(all_prepared, "csr_amt"),
    }])

    year_totals = (
        all_prepared.groupby("_source_year", dropna=False)
        .apply(
            lambda g: pd.Series({
                "enrollment_count": _safe_nunique(g, "enrollment_id"),
                "enrollee_count": _safe_nunique(g, "enrollee_id"),
                "household_count": _safe_nunique(g, "household_id"),
            }),
            include_groups=False,
        )
        .reset_index()
        .rename(columns={"_source_year": "year"})
    )

    insurance_type_totals = (
        all_prepared.groupby("_insurance_type", dropna=False)
        .apply(
            lambda g: pd.Series({
                "enrollment_count": _safe_nunique(g, "enrollment_id"),
                "enrollee_count": _safe_nunique(g, "enrollee_id"),
            }),
            include_groups=False,
        )
        .reset_index()
        .rename(columns={"_insurance_type": "insurance_type"})
    )

    status_totals = (
        all_prepared.groupby("_canonical_status", dropna=False)
        .size()
        .reset_index(name="row_count")
        .rename(columns={"_canonical_status": "status"})
    )

    month_trend = (
        all_prepared.groupby(["_source_year", "_source_month"], dropna=False)
        .apply(
            lambda g: pd.Series({
                "enrollment_count": _safe_nunique(g, "enrollment_id"),
                "enrollee_count": _safe_nunique(g, "enrollee_id"),
                "gross_premium_total": _premium_sum(g, "gross_premium_amt"),
                "net_premium_total": _premium_sum(g, "net_premium_amt"),
            }),
            include_groups=False,
        )
        .reset_index()
        .rename(columns={"_source_year": "year", "_source_month": "month"})
        .sort_values(["year", "month"])
    )

    premium_totals = pd.DataFrame([{
        "gross_premium_total": _premium_sum(all_prepared, "gross_premium_amt"),
        "net_premium_total": _premium_sum(all_prepared, "net_premium_amt"),
        "aptc_total": _premium_sum(all_prepared, "aptc_amt"),
        "csr_total": _premium_sum(all_prepared, "csr_amt"),
    }])

    return {
        "issuer_totals": issuer_totals,
        "year_totals": year_totals,
        "insurance_type_totals": insurance_type_totals,
        "status_totals": status_totals,
        "month_trend": month_trend,
        "premium_totals": premium_totals,
    }
