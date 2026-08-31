"""
Fixed Azure candidate for FAST_MODE — dbo.834_Inbound_test / Strategy C.

No table scanning. Used when ENABLE_FULL_DISCOVERY=false.
"""

from __future__ import annotations

from typing import Any

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from azure_reconciliation.data_source_audit import record_azure_fetch
from azure_reconciliation.azure_mirror.discovery.strategies import run_strategy_c
from azure_reconciliation.azure_mirror.discovery.table_inspector import TableProfile
from azure_reconciliation.azure_fetch import AzureFetchPlan
from azure_reconciliation.discovery_engine import DiscoverySelection, StrategyCandidate
from azure_reconciliation.partition_discovery import Partition
from utils.logger import get_logger

logger = get_logger(__name__)

FIXED_SCHEMA = "dbo"
FIXED_TABLE = "834_Inbound_test"
FIXED_STRATEGY = "C"
FIXED_DATE_COL = "GAA_834_File_Date"

FIXED_COLUMNS_MAP = {
    "issuer_col": "GAA_HIOS_ID",
    "year_col": "Coverage_Year",
    "status_col": "enrolleeStatus",
    "action_col": "actionCode",
    "policy_col": "exchgAssignedPolicyID",
    "member_col": "exchgIndivIdentifier",
    "subscriber_col": "exchgSubscriberIdentifier",
    "insurance_type_col": "Insurance_Type",
    "file_date_col": "GAA_834_File_Date",
}


def fixed_table_name() -> str:
    return FIXED_TABLE


def build_fixed_profile(columns: list[str]) -> TableProfile:
    """Build TableProfile with known 834_Inbound_test column roles."""
    ins_col = FIXED_COLUMNS_MAP["insurance_type_col"]
    if ins_col not in columns:
        for alt in ("planCoverageDescription", "Insurance_Type", "insurance_type"):
            if alt in columns:
                ins_col = alt
                break

    return TableProfile(
        schema=FIXED_SCHEMA,
        table=FIXED_TABLE,
        columns=columns,
        available=True,
        issuer_col=FIXED_COLUMNS_MAP["issuer_col"] if FIXED_COLUMNS_MAP["issuer_col"] in columns else None,
        year_col=FIXED_COLUMNS_MAP["year_col"] if FIXED_COLUMNS_MAP["year_col"] in columns else None,
        policy_col=FIXED_COLUMNS_MAP["policy_col"] if FIXED_COLUMNS_MAP["policy_col"] in columns else None,
        member_col=FIXED_COLUMNS_MAP["member_col"] if FIXED_COLUMNS_MAP["member_col"] in columns else None,
        subscriber_col=FIXED_COLUMNS_MAP["subscriber_col"] if FIXED_COLUMNS_MAP["subscriber_col"] in columns else None,
        insurance_type_col=ins_col if ins_col in columns else None,
        status_col=FIXED_COLUMNS_MAP["status_col"] if FIXED_COLUMNS_MAP["status_col"] in columns else None,
        action_col=FIXED_COLUMNS_MAP["action_col"] if FIXED_COLUMNS_MAP["action_col"] in columns else None,
        file_date_col=FIXED_DATE_COL if FIXED_DATE_COL in columns else None,
        event_date_cols=[c for c in (FIXED_DATE_COL, "memberMaintEffectiveDate") if c in columns],
    )


def fixed_fetch_plan() -> AzureFetchPlan:
    return AzureFetchPlan(
        schema=FIXED_SCHEMA,
        table=FIXED_TABLE,
        strategy_id=FIXED_STRATEGY,
        issuer_col=FIXED_COLUMNS_MAP["issuer_col"],
        year_col=FIXED_COLUMNS_MAP["year_col"],
        date_col=FIXED_DATE_COL,
        status_col=FIXED_COLUMNS_MAP["status_col"],
        policy_col=FIXED_COLUMNS_MAP["policy_col"],
        member_col=FIXED_COLUMNS_MAP["member_col"],
        insurance_type_col=FIXED_COLUMNS_MAP["insurance_type_col"],
        logic_type="event",
        confidence_score=100.0,
        source="fixed_candidate",
    )


def fixed_discovery_selection(profile: TableProfile) -> DiscoverySelection:
    candidate = StrategyCandidate(
        table=FIXED_TABLE,
        schema=FIXED_SCHEMA,
        strategy_id=FIXED_STRATEGY,
        profile=profile,
        date_column=FIXED_DATE_COL,
        status_column=FIXED_COLUMNS_MAP["status_col"],
        logic_type="event",
        confidence_score=100.0,
        notes="FAST_MODE fixed candidate",
    )
    return DiscoverySelection(
        candidate=candidate,
        tables_evaluated=1,
        strategies_evaluated=1,
    )


def fetch_fixed_azure_data(
    engine: Engine,
    *,
    issuer: str,
    partitions: list[Partition],
    profile: TableProfile,
) -> pd.DataFrame:
    """Fetch 834_Inbound_test rows for issuer across partition months (date-filtered)."""
    if not profile.available or not profile.issuer_col:
        return pd.DataFrame()

    full_table = f"[{profile.schema}].[{profile.table}]"
    date_col = profile.file_date_col or FIXED_DATE_COL
    frames: list[pd.DataFrame] = []

    for part in partitions:
        if str(part.issuer) != str(issuer):
            continue
        clauses = [f"CAST([{profile.issuer_col}] AS VARCHAR(20)) = :issuer"]
        params: dict[str, Any] = {"issuer": str(issuer)}
        if profile.year_col:
            clauses.append(f"CAST([{profile.year_col}] AS VARCHAR(4)) = :year")
            params["year"] = str(part.year)
        if date_col and date_col in profile.columns:
            clauses.append(f"YEAR([{date_col}]) = :yr")
            clauses.append(
                f"(MONTH([{date_col}]) = :mo_int OR FORMAT([{date_col}], 'MM') = :mo_str)"
            )
            params["yr"] = int(part.year)
            params["mo_int"] = int(part.month)
            params["mo_str"] = str(part.month).zfill(2)
        sql = f"SELECT * FROM {full_table} WHERE {' AND '.join(clauses)}"
        try:
            with engine.connect() as conn:
                part_df = pd.read_sql(text(sql), conn, params=params)
            if not part_df.empty:
                part_df["_partition"] = part.label()
                frames.append(part_df)
                logger.info("FAST fetch %s: %d rows", part.label(), len(part_df))
        except Exception as exc:
            logger.warning("FAST fetch failed %s: %s", part.label(), exc)

    if not frames:
        record_azure_fetch(rows=0, table=f"{profile.schema}.{profile.table}")
        return pd.DataFrame()
    result = pd.concat(frames, ignore_index=True)
    record_azure_fetch(rows=len(result), table=f"{profile.schema}.{profile.table}")
    return result


def run_fixed_strategy_c(
    table_df: pd.DataFrame,
    profile: TableProfile,
    partitions: list[Partition],
) -> pd.DataFrame:
    """Run only Strategy C on fixed table."""
    if table_df.empty:
        return pd.DataFrame()
    out = run_strategy_c(table_df, profile, partitions)
    if not out.empty:
        out = out.copy()
        if "source_date_column" not in out.columns:
            out["source_date_column"] = FIXED_DATE_COL
        else:
            out["source_date_column"] = out["source_date_column"].fillna(FIXED_DATE_COL)
        if "source_status_column" not in out.columns:
            out["source_status_column"] = FIXED_COLUMNS_MAP["status_col"]
    return out
