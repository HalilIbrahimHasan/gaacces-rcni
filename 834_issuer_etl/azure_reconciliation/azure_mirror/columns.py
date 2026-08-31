"""Azure column resolution and required-field inventory."""

from __future__ import annotations

import pandas as pd

from utils.logger import get_logger

logger = get_logger(__name__)

REQUIRED_AZURE_COLUMNS = [
    "coverage_year",
    "hios_issuer_id",
    "Insurance_Type",
    "enrollment_id",
    "enrollee_id",
    "household_id",
    "ssap_application_id",
    "external_application_id",
    "application_type",
    "source",
    "application_status",
    "person_type",
    "consumer_category",
    "birth_date",
    "enrollee_first_name",
    "enrollee_last_name",
    "gross_premium_amt",
    "net_premium_amt",
    "aptc_amt",
    "csr_amt",
    "exchange_eligibility_status",
    "plan_level_combined_bronze",
    "cms_plan_id",
    "plan_id",
    "plan_name",
    "insurer_name",
    "age",
    "rating_area",
    "county",
    "zip",
    "enrollment_status_description",
    "enrollee_status_description",
    "benefit_effective_date",
    "benefit_end_date",
    "enrollment_confirmation_date",
    "enrollment_create_date",
    "enrollment_last_update_date",
    "enrollee_create_date",
    "enrollee_last_update_date",
    "enrollee_start_date",
    "enrollee_end_date",
    "application_create_date",
    "application_last_update_date",
]

COLUMN_ALIASES: dict[str, list[str]] = {
    "coverage_year": ["coverage_year", "plan_year", "benefit_year"],
    "hios_issuer_id": ["hios_issuer_id", "issuer_id", "hios_id"],
    "Insurance_Type": ["Insurance_Type", "insurance_type", "insurance_type_code"],
    "enrollment_id": ["enrollment_id", "enrollmentid"],
    "enrollee_id": ["enrollee_id", "enrolleeid"],
    "household_id": ["household_id", "householdid"],
    "person_type": ["person_type", "persontype", "relationship_type"],
    "gross_premium_amt": ["gross_premium_amt", "gross_premium"],
    "net_premium_amt": ["net_premium_amt", "net_premium"],
    "aptc_amt": ["aptc_amt", "aptc_amount"],
    "csr_amt": ["csr_amt", "csr_amount"],
    "enrollment_status_description": [
        "enrollment_status_description",
        "enrollment_status",
    ],
    "enrollee_status_description": [
        "enrollee_status_description",
        "enrollee_status",
    ],
    "rating_area": ["rating_area", "ratingarea"],
    "GAA_Load_Date": ["GAA_Load_Date", "gaa_load_date", "load_date"],
    "benefit_effective_date": ["benefit_effective_date", "benefitEffectiveBeginDate"],
    "benefit_end_date": ["benefit_end_date", "benefitEffectiveEndDate"],
}


def _norm(name: str) -> str:
    return str(name).strip().lower().replace(" ", "_")


def build_column_lookup(columns: list[str]) -> dict[str, str]:
    """Map normalized names to actual Azure column names."""
    lookup: dict[str, str] = {}
    for col in columns:
        lookup[_norm(col)] = col
    return lookup


def resolve_column(columns: list[str], canonical: str) -> str | None:
    """Return the actual column name for a canonical field, if present."""
    lookup = build_column_lookup(columns)
    for alias in COLUMN_ALIASES.get(canonical, [canonical]):
        hit = lookup.get(_norm(alias))
        if hit:
            return hit
    return None


def log_missing_columns(columns: list[str], *, context: str = "") -> list[str]:
    """Log columns not found in Azure table; return missing list."""
    missing: list[str] = []
    for req in REQUIRED_AZURE_COLUMNS:
        if resolve_column(columns, req) is None:
            missing.append(req)
    if missing:
        logger.warning(
            "Azure missing columns%s (%d): %s",
            f" [{context}]" if context else "",
            len(missing),
            ", ".join(missing[:20]) + ("..." if len(missing) > 20 else ""),
        )
    else:
        logger.info("Azure column check%s: all required columns present", f" [{context}]" if context else "")
    return missing


def col_series(df: pd.DataFrame, canonical: str, default=None) -> pd.Series:
    """Get a column by canonical name with fallback default."""
    actual = resolve_column(list(df.columns), canonical)
    if actual and actual in df.columns:
        return df[actual]
    return pd.Series([default] * len(df), index=df.index)
