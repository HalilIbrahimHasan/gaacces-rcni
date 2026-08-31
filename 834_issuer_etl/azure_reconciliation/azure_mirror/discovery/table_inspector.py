"""Azure logic discovery — table inspection and column role detection."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from azure_reconciliation.azure_client import (
    AZURE_TABLE_CANDIDATES,
    DEFAULT_SCHEMA,
    _pick_filter_column,
    list_table_columns,
)
from utils.logger import get_logger

logger = get_logger(__name__)

DISCOVERY_TABLES = AZURE_TABLE_CANDIDATES

ROLE_CANDIDATES: dict[str, list[str]] = {
    "issuer": [
        "hios_issuer_id", "GAA_HIOS_ID", "issuer_id", "hios_id",
    ],
    "year": [
        "coverage_year", "Coverage_Year", "plan_year", "invoice_year", "benefit_year",
    ],
    "month": [
        "invoice_month", "report_month", "coverage_month",
    ],
    "policy": [
        "enrollment_id", "exchgAssignedPolicyID", "exchg_assigned_policy_id", "policy_id",
    ],
    "member": [
        "enrollee_id", "exchgIndivIdentifier", "exchg_indiv_identifier", "member_id",
    ],
    "subscriber": [
        "exchgSubscriberIdentifier", "exchg_subscriber_identifier", "subscriber_id",
    ],
    "insurance_type": [
        "Insurance_Type", "insurance_type", "insurance_type_code",
    ],
    "status": [
        "enrolleeStatus", "enrollee_status_description", "enrollment_status_description",
        "enrollee_status", "enrollment_status", "status",
    ],
    "action": [
        "actionCode", "action_code", "actionCode_desc", "event_type_code",
        "event_type_code_desc", "maintenance_type_code", "enrollment_action_code",
    ],
    "event_reason": [
        "event_reason_code", "event_reason_code_desc", "additional_maint_reason_code",
    ],
    "benefit_effective": ["benefit_effective_date", "benefitEffectiveBeginDate"],
    "benefit_end": ["benefit_end_date", "benefitEffectiveEndDate"],
    "file_date": ["GAA_834_File_Date", "GAA_Load_Date", "file_date"],
    "maint_date": ["memberMaintEffectiveDate", "member_maint_effective_date"],
}

EVENT_DATE_CANDIDATES = [
    "memberMaintEffectiveDate", "member_maint_effective_date",
    "GAA_834_File_Date", "enrollment_create_date", "enrollment_last_update_date",
    "enrollee_create_date", "enrollee_last_update_date", "enrollment_confirmation_date",
    "application_create_date", "application_last_update_date", "enrolleeStatusDate",
]


@dataclass
class TableProfile:
    schema: str
    table: str
    columns: list[str]
    available: bool = True
    issuer_col: str | None = None
    year_col: str | None = None
    month_col: str | None = None
    policy_col: str | None = None
    member_col: str | None = None
    subscriber_col: str | None = None
    insurance_type_col: str | None = None
    status_col: str | None = None
    action_col: str | None = None
    event_reason_col: str | None = None
    benefit_effective_col: str | None = None
    benefit_end_col: str | None = None
    file_date_col: str | None = None
    maint_date_col: str | None = None
    event_date_cols: list[str] = field(default_factory=list)
    missing_roles: list[str] = field(default_factory=list)
    sample_row_count: int = 0

    @property
    def full_name(self) -> str:
        return f"{self.schema}.{self.table}"


def _detect(columns: list[str], role: str) -> str | None:
    return _pick_filter_column(columns, ROLE_CANDIDATES.get(role, []))


def inspect_table(engine: Engine, schema: str, table: str) -> TableProfile:
    columns = list_table_columns(engine, schema, table)
    if not columns:
        logger.warning("Table not available or no columns: %s.%s", schema, table)
        return TableProfile(schema=schema, table=table, columns=[], available=False)

    event_dates = [
        c for c in EVENT_DATE_CANDIDATES
        if _pick_filter_column(columns, [c])
    ]

    profile = TableProfile(
        schema=schema,
        table=table,
        columns=columns,
        issuer_col=_detect(columns, "issuer"),
        year_col=_detect(columns, "year"),
        month_col=_detect(columns, "month"),
        policy_col=_detect(columns, "policy"),
        member_col=_detect(columns, "member"),
        subscriber_col=_detect(columns, "subscriber"),
        insurance_type_col=_detect(columns, "insurance_type"),
        status_col=_detect(columns, "status"),
        action_col=_detect(columns, "action"),
        event_reason_col=_detect(columns, "event_reason"),
        benefit_effective_col=_detect(columns, "benefit_effective"),
        benefit_end_col=_detect(columns, "benefit_end"),
        file_date_col=_detect(columns, "file_date"),
        maint_date_col=_detect(columns, "maint_date"),
        event_date_cols=event_dates,
    )

    for role in ("issuer", "year", "policy", "member", "status"):
        if not getattr(profile, f"{role}_col", None) and role not in ("policy", "member"):
            profile.missing_roles.append(role)

    logger.info(
        "Inspected %s — cols=%d issuer=%s year=%s status=%s action=%s",
        profile.full_name, len(columns),
        profile.issuer_col, profile.year_col, profile.status_col, profile.action_col,
    )
    return profile


def fetch_issuer_year_sample(
    engine: Engine,
    profile: TableProfile,
    *,
    issuer: str,
    year: str,
    limit: int = 5000,
) -> tuple[pd.DataFrame, str, dict[str, Any]]:
    """SELECT-only fetch for one issuer/year from a candidate table."""
    if not profile.available or not profile.issuer_col:
        return pd.DataFrame(), "", {}

    full_table = f"[{profile.schema}].[{profile.table}]"
    clauses = [f"CAST([{profile.issuer_col}] AS VARCHAR(20)) = :issuer"]
    params: dict[str, Any] = {"issuer": str(issuer)}

    if profile.year_col:
        clauses.append(f"CAST([{profile.year_col}] AS VARCHAR(4)) = :year")
        params["year"] = str(year)

    sql = f"SELECT * FROM {full_table} WHERE {' AND '.join(clauses)}"
    if limit:
        sql += f" ORDER BY (SELECT NULL) OFFSET 0 ROWS FETCH NEXT {int(limit)} ROWS ONLY"

    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(sql), conn, params=params)
    except Exception as exc:
        logger.warning("Fetch failed %s: %s", profile.full_name, exc)
        return pd.DataFrame(), sql, params

    profile.sample_row_count = len(df)
    return df, sql, params


def inspect_all_tables(engine: Engine, schema: str = DEFAULT_SCHEMA) -> list[TableProfile]:
    profiles: list[TableProfile] = []
    for table in DISCOVERY_TABLES:
        p = inspect_table(engine, schema, table)
        if p.available:
            profiles.append(p)
    logger.info("Tables inspected: %d available of %d candidates", len(profiles), len(DISCOVERY_TABLES))
    return profiles


def profiles_to_dataframe(profiles: list[TableProfile]) -> pd.DataFrame:
    rows = []
    for p in profiles:
        rows.append({
            "table": p.full_name,
            "column_count": len(p.columns),
            "issuer_col": p.issuer_col,
            "year_col": p.year_col,
            "month_col": p.month_col,
            "policy_col": p.policy_col,
            "member_col": p.member_col,
            "subscriber_col": p.subscriber_col,
            "insurance_type_col": p.insurance_type_col,
            "status_col": p.status_col,
            "action_col": p.action_col,
            "event_reason_col": p.event_reason_col,
            "benefit_effective_col": p.benefit_effective_col,
            "benefit_end_col": p.benefit_end_col,
            "file_date_col": p.file_date_col,
            "maint_date_col": p.maint_date_col,
            "event_date_cols": ", ".join(p.event_date_cols),
            "missing_roles": ", ".join(p.missing_roles),
        })
    return pd.DataFrame(rows)
