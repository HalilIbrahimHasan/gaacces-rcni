"""
Metadata-driven Azure partition fetch.

Table/strategy selection delegated to discovery_engine — no hardcoded tables.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from azure_reconciliation.azure_client import (
    DEFAULT_SCHEMA,
    list_table_columns,
)
from azure_reconciliation.discovery_engine import (
    DiscoverySelection,
    evaluate_azure_candidates,
    selection_to_fetch_plan,
)
from azure_reconciliation.partition_discovery import Partition
from config.config import settings
from utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class AzureFetchPlan:
    schema: str
    table: str
    strategy_id: str
    issuer_col: str
    year_col: str | None
    date_col: str | None
    status_col: str | None
    policy_col: str | None
    member_col: str | None
    insurance_type_col: str | None
    logic_type: str = "event"
    confidence_score: float = 0.0
    source: str = "dynamic_discovery"

    @property
    def full_name(self) -> str:
        return f"{self.schema}.{self.table}" if self.table else ""


def discover_and_select(
    engine: Engine,
    *,
    issuer: str,
    partitions: list[Partition],
    schema: str = DEFAULT_SCHEMA,
) -> tuple[AzureFetchPlan, DiscoverySelection]:
    """Run discovery (or FAST_MODE fixed candidate) and return fetch plan."""
    if settings.use_fixed_azure_candidate and not settings.enable_full_discovery:
        from azure_reconciliation.fixed_azure_candidate import (
            build_fixed_profile,
            fixed_discovery_selection,
            fixed_fetch_plan,
            fixed_table_name,
        )
        from azure_reconciliation.azure_client import list_table_columns

        cols = list_table_columns(engine, schema, fixed_table_name())
        profile = build_fixed_profile(cols)
        selection = fixed_discovery_selection(profile)
        plan = fixed_fetch_plan()
        logger.info("FAST_MODE fetch plan: %s / strategy %s", plan.full_name, plan.strategy_id)
        return plan, selection

    selection = evaluate_azure_candidates(engine, issuer=issuer, partitions=partitions, schema=schema)
    plan = selection_to_fetch_plan(selection)
    return plan, selection


def _build_partition_sql(
    plan: AzureFetchPlan,
    partition: Partition,
    columns: list[str],
) -> tuple[str, dict[str, Any]]:
    if not plan.table or not plan.issuer_col:
        return "", {}

    full_table = f"[{plan.schema}].[{plan.table}]"
    clauses = [f"CAST([{plan.issuer_col}] AS VARCHAR(20)) = :issuer"]
    params: dict[str, Any] = {"issuer": str(partition.issuer)}

    if plan.year_col and plan.logic_type == "snapshot":
        clauses.append(f"CAST([{plan.year_col}] AS VARCHAR(4)) = :year")
        params["year"] = str(partition.year)

    if plan.date_col and plan.date_col in columns and plan.logic_type in ("event", "financial"):
        clauses.append(f"YEAR([{plan.date_col}]) = :year")
        clauses.append(
            f"(MONTH([{plan.date_col}]) = :month_int OR FORMAT([{plan.date_col}], 'MM') = :month_str)"
        )
        params["year"] = int(partition.year)
        params["month_int"] = int(partition.month)
        params["month_str"] = str(partition.month).zfill(2)

    sql = f"SELECT * FROM {full_table} WHERE {' AND '.join(clauses)}"
    return sql, params


def fetch_partition(engine: Engine, plan: AzureFetchPlan, partition: Partition) -> pd.DataFrame:
    if not plan.table:
        return pd.DataFrame()
    columns = list_table_columns(engine, plan.schema, plan.table)
    if not columns:
        return pd.DataFrame()
    sql, params = _build_partition_sql(plan, partition, columns)
    if not sql:
        return pd.DataFrame()
    logger.info("Azure fetch %s: %s", plan.full_name, sql[:240])
    with engine.connect() as conn:
        df = pd.read_sql(text(sql), conn, params=params)
    logger.info(
        "Azure fetch %s rows=%d table=%s strategy=%s score=%.1f",
        partition.label(), len(df), plan.table, plan.strategy_id, plan.confidence_score,
    )
    return df


def fetch_issuer_year_bulk(
    engine: Engine,
    plan: AzureFetchPlan,
    *,
    issuer: str,
    years: list[str],
    limit: int = 50000,
) -> pd.DataFrame:
    """Fetch issuer/year bulk for lifecycle replay."""
    if not plan.table or not plan.issuer_col:
        return pd.DataFrame()
    columns = list_table_columns(engine, plan.schema, plan.table)
    if not columns:
        return pd.DataFrame()
    full_table = f"[{plan.schema}].[{plan.table}]"
    clauses = [f"CAST([{plan.issuer_col}] AS VARCHAR(20)) = :issuer"]
    params: dict[str, Any] = {"issuer": str(issuer)}
    if plan.year_col and years:
        ph = ", ".join(f":y{i}" for i in range(len(years)))
        clauses.append(f"CAST([{plan.year_col}] AS VARCHAR(4)) IN ({ph})")
        for i, y in enumerate(years):
            params[f"y{i}"] = str(y)
    sql = f"SELECT * FROM {full_table} WHERE {' AND '.join(clauses)}"
    if limit:
        sql += f" ORDER BY (SELECT NULL) OFFSET 0 ROWS FETCH NEXT {int(limit)} ROWS ONLY"
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params=params)


def fetch_with_full_fallback(
    engine: Engine,
    partition: Partition,
    *,
    partitions: list[Partition],
    primary_plan: AzureFetchPlan | None = None,
    selection: DiscoverySelection | None = None,
) -> tuple[pd.DataFrame, AzureFetchPlan, DiscoverySelection | None]:
    """
    Fetch partition; on zero rows re-run discovery and try next-best candidates.
    """
    if primary_plan is None or selection is None:
        primary_plan, selection = discover_and_select(
            engine, issuer=partition.issuer, partitions=partitions
        )

    df = fetch_partition(engine, primary_plan, partition)
    if not df.empty:
        return df, primary_plan, selection

    if selection.all_scores.empty:
        return pd.DataFrame(), primary_plan, selection

    ranked = selection.all_scores.sort_values(
        by=["dynamic_score", "confidence_score"], ascending=False
    )
    tried = {primary_plan.table}
    for _, row in ranked.iterrows():
        tbl = str(row.get("source_table", "")).replace("dbo.", "")
        if not tbl or tbl in tried:
            continue
        tried.add(tbl)
        sid = str(row.get("strategy_id", ""))
        alt_selection = evaluate_azure_candidates(
            engine, issuer=partition.issuer, partitions=partitions
        )
        alt_plan = selection_to_fetch_plan(alt_selection)
        if alt_plan.table == tbl:
            df = fetch_partition(engine, alt_plan, partition)
            if not df.empty:
                logger.info("Fallback table %s succeeded for %s", tbl, partition.label())
                return df, alt_plan, alt_selection

    logger.warning("All Azure candidates exhausted for %s", partition.label())
    return pd.DataFrame(), primary_plan, selection
