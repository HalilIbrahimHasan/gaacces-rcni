"""
Chandra-like business report presentation — display format only.

Does not change Model H aggregation or transformation logic.
Maps internal dashboard summaries to Hari/Chandra enrollment summary columns.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from azure_reconciliation.chandra_nan_safe import (
    is_missing,
    safe_int,
    safe_month as safe_month_str,
    safe_status_id,
    safe_year as safe_year_str,
)
from azure_reconciliation.safe_export import safe_write_excel

# Re-export for backward compatibility
safe_count_int = safe_int

CHANDRA_BUSINESS_COLUMNS = [
    "Coverage_Year",
    "GAA_HIOS_ID",
    "GAA_Load_Date",
    "Insurance_Type",
    "status_Id",
    "enrolleeStatus",
    "Enrollment_Count",
    "Enrollee_Count",
    "Subscriber_Count",
]

CHANDRA_BUSINESS_COLUMNS_CORE = [
    "Coverage_Year",
    "GAA_HIOS_ID",
    "GAA_Load_Date",
    "Insurance_Type",
    "status_Id",
    "enrolleeStatus",
    "Enrollment_Count",
    "Enrollee_Count",
]

INSURANCE_TYPE_DISPLAY = {
    "HLT": "Health",
    "HEALTH": "Health",
    "H": "Health",
    "MEDICAL": "Health",
    "DEN": "Dental",
    "DENTAL": "Dental",
    "D": "Dental",
    "VIS": "Vision",
    "VISION": "Vision",
}

STATUS_TO_CHANDRA_DISPLAY = {
    "ENROLLED": "CONFIRM",
    "CONFIRM": "CONFIRM",
    "CONFIRMED": "CONFIRM",
    "ACTIVE": "CONFIRM",
    "EFFECTUATED": "CONFIRM",
    "REINSTATE": "CONFIRM",
    "CANCELLED": "CANCEL",
    "CANCEL": "CANCEL",
    "CANCELED": "CANCEL",
    "TERMINATED": "TERM",
    "TERM": "TERM",
    "UNKNOWN": "UNKNOWN",
}

STATUS_ID_MAP = {
    "CONFIRM": 1,
    "CANCEL": 2,
    "TERM": 3,
    "UNKNOWN": 0,
}


def _zmonth(m: str | object) -> str:
    return safe_month_str(m)


def gaa_load_date(year: str | int | object, month: str | int | object) -> str:
    """First day of partition month — Chandra M/D/YYYY format."""
    ys = safe_year_str(year)
    ms = safe_month_str(month)
    if not ys or not ms:
        return ""
    return f"{safe_int(ms, 0)}/1/{safe_int(ys, 0)}"


def insurance_type_display(raw: str | None) -> str:
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return "Health"
    key = str(raw).strip().upper()
    if not key:
        return "Health"
    if key in INSURANCE_TYPE_DISPLAY:
        return INSURANCE_TYPE_DISPLAY[key]
    if "DENTAL" in key or key.startswith("DEN"):
        return "Dental"
    if "HEALTH" in key or key.startswith("HLT"):
        return "Health"
    if "VISION" in key or key.startswith("VIS"):
        return "Vision"
    if key == "UNKNOWN":
        return "Unknown"
    return "Health"


def enrollee_status_display(raw: str | None) -> str:
    key = str(raw or "UNKNOWN").strip().upper()
    return STATUS_TO_CHANDRA_DISPLAY.get(key, "UNKNOWN")


def status_id_for_display(enrollee_status: str) -> int:
    if is_missing(enrollee_status):
        return 0
    return safe_int(STATUS_ID_MAP.get(str(enrollee_status).strip().upper(), 0), 0)


def to_chandra_business_summary(internal: pd.DataFrame) -> pd.DataFrame:
    """
    Convert internal Model H business summary to Chandra enrollment summary columns.

    Expects internal columns: issuer, year, month, insurance_type, status,
    Enrollment_Count, Enrollee_Count.
    Business-facing output excludes Subscriber_Count (often unpopulated).
    """
    if internal.empty:
        return pd.DataFrame(columns=CHANDRA_BUSINESS_COLUMNS_CORE)

    rows: list[dict] = []
    for _, row in internal.iterrows():
        year = safe_year_str(row.get("year", ""))
        month = safe_month_str(row.get("month", ""))
        if not year or not str(row.get("issuer", "")).strip() or str(row.get("issuer", "")).strip().lower() == "nan":
            continue
        enrollee_status = enrollee_status_display(row.get("status"))
        enroll_ct = row.get("Enrollment_Count", row.get("enrollment_count", 0))
        enrollee_ct = row.get("Enrollee_Count", row.get("enrollee_count", 0))
        rows.append({
            "Coverage_Year": year,
            "GAA_HIOS_ID": str(row.get("issuer", "")).strip(),
            "GAA_Load_Date": gaa_load_date(year, month) if month else "",
            "Insurance_Type": insurance_type_display(row.get("insurance_type")),
            "status_Id": safe_status_id(row.get("status_Id"), enrollee_status=enrollee_status),
            "enrolleeStatus": enrollee_status,
            "Enrollment_Count": safe_int(enroll_ct, 0),
            "Enrollee_Count": safe_int(enrollee_ct, 0),
        })
    if not rows:
        return pd.DataFrame(columns=CHANDRA_BUSINESS_COLUMNS_CORE)
    out = pd.DataFrame(rows)
    return out[CHANDRA_BUSINESS_COLUMNS_CORE]


def rollup_chandra_business(
    chandra_df: pd.DataFrame,
    group_keys: list[str],
) -> pd.DataFrame:
    """Sum count columns on Chandra-formatted data."""
    if chandra_df.empty:
        return chandra_df
    sum_cols = ["Enrollment_Count", "Enrollee_Count"]
    keys = [k for k in group_keys if k in chandra_df.columns]
    if not keys:
        return chandra_df
    agg = {c: "sum" for c in sum_cols if c in chandra_df.columns}
    out = chandra_df.groupby(keys, dropna=False).agg(agg).reset_index()
    for c in sum_cols:
        if c in out.columns:
            out[c] = out[c].apply(lambda v: safe_int(v, 0))
    return out[[c for c in group_keys + sum_cols if c in out.columns]]


def chandra_year_rollup(chandra_monthly: pd.DataFrame) -> pd.DataFrame:
    """Aggregate monthly Chandra rows to issuer/year grain (all load dates in year)."""
    if chandra_monthly.empty:
        return chandra_monthly
    keys = ["Coverage_Year", "GAA_HIOS_ID", "Insurance_Type", "status_Id", "enrolleeStatus"]
    return rollup_chandra_business(chandra_monthly, keys)


def chandra_all_years_rollup(chandra_monthly: pd.DataFrame) -> pd.DataFrame:
    """Aggregate all years/months to issuer grain."""
    if chandra_monthly.empty:
        return chandra_monthly
    keys = ["GAA_HIOS_ID", "Insurance_Type", "status_Id", "enrolleeStatus"]
    out = rollup_chandra_business(chandra_monthly, keys)
    if not out.empty:
        out.insert(0, "Coverage_Year", "ALL")
    return out


def business_table_html(df: pd.DataFrame, *, title: str = "Business Enrollment Summary") -> str:
    if df.empty:
        body = "<p><em>No business rows</em></p>"
    else:
        show = df[[c for c in CHANDRA_BUSINESS_COLUMNS_CORE if c in df.columns]]
        body = show.to_html(index=False, escape=True, border=1)
    return f"<h2>{title}</h2>\n{body}"


def diagnostics_section_html(
    processing_html: str,
    *,
    diagnostics_rel_link: str = "diagnostics/data_quality_summary.html",
) -> str:
    parts = [
        "<h2>Processing Diagnostics</h2>",
        f"<p><a href=\"{diagnostics_rel_link}\">Open data quality diagnostics</a></p>",
    ]
    if processing_html:
        parts.append(f"<details><summary>Show processing summary</summary>{processing_html}</details>")
    return "\n".join(parts)


def write_model_h_month_html(
    path: Path,
    *,
    business_df: pd.DataFrame,
    processing_html: str = "",
    title: str = "Model H Month Summary",
) -> None:
    """Two-section month report: Chandra business + collapsed diagnostics."""
    path.parent.mkdir(parents=True, exist_ok=True)
    content = "\n".join([
        "<!DOCTYPE html><html><head><meta charset='utf-8'>",
        f"<title>{title}</title>",
        "<style>body{font-family:system-ui,sans-serif;margin:1.5rem}",
        "table{border-collapse:collapse}th,td{border:1px solid #ccc;padding:4px 8px}",
        "details{margin-top:1rem}</style>",
        "</head><body>",
        f"<h1>{title}</h1>",
        business_table_html(business_df),
        diagnostics_section_html(processing_html),
        "</body></html>",
    ])
    path.write_text(content, encoding="utf-8")


def write_chandra_business_html(
    path: Path,
    business_df: pd.DataFrame,
    *,
    title: str = "Enrollment Summary",
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    content = "\n".join([
        "<!DOCTYPE html><html><head><meta charset='utf-8'>",
        f"<title>{title}</title>",
        "<style>body{font-family:system-ui,sans-serif;margin:1.5rem}",
        "table{border-collapse:collapse}th,td{border:1px solid #ccc;padding:4px 8px}</style>",
        "</head><body>",
        f"<h1>{title}</h1>",
        business_table_html(business_df),
        "</body></html>",
    ])
    path.write_text(content, encoding="utf-8")


def write_chandra_business_xlsx(
    path: Path,
    business_df: pd.DataFrame,
    *,
    sheet_name: str = "Enrollment_Summary",
) -> None:
    safe_write_excel(
        path,
        {sheet_name: business_df},
        drop_duplicate_value_columns=False,
    )
