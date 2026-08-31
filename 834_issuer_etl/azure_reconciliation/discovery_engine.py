"""
Dynamic Azure table/strategy discovery engine.

FROZEN — read-only. Do not add discovery logic here.
Use final_comparison_engine.py for business-level strategy selection.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any

import pandas as pd
from sqlalchemy.engine import Engine

from azure_reconciliation.data_source_audit import get_audit
from azure_reconciliation.azure_client import (
    DEFAULT_SCHEMA,
    _pick_filter_column,
    list_available_tables,
)
from azure_reconciliation.azure_mirror.discovery.scoring import score_strategy_vs_xml
from azure_reconciliation.azure_mirror.discovery.strategies import (
    STRATEGY_META,
    run_applicable_strategies,
)
from azure_reconciliation.azure_mirror.discovery.table_inspector import (
    EVENT_DATE_CANDIDATES,
    TableProfile,
    fetch_issuer_year_sample,
    inspect_table,
)
from azure_reconciliation.azure_mirror.discovery.xml_reference import (
    load_xml_summaries,
    xml_monthly_totals,
)
from azure_reconciliation.partition_discovery import Partition
from config.config import settings
from utils.logger import get_logger

logger = get_logger(__name__)

LOAD_DATE_ROLES = {"GAA_Load_Date", "gaa_load_date"}


@dataclass
class StrategyCandidate:
    table: str
    schema: str
    strategy_id: str
    profile: TableProfile
    date_column: str
    status_column: str
    logic_type: str
    confidence_score: float = 0.0
    row_count: int = 0
    issuer_coverage: float = 0.0
    date_coverage: float = 0.0
    status_coverage: float = 0.0
    seed_bonus: float = 0.0
    notes: str = ""

    @property
    def full_name(self) -> str:
        return f"{self.schema}.{self.table}"


@dataclass
class DiscoverySelection:
    candidate: StrategyCandidate
    all_scores: pd.DataFrame = field(default_factory=pd.DataFrame)
    tables_evaluated: int = 0
    strategies_evaluated: int = 0


def _optional_env_list(key: str) -> list[str]:
    raw = (os.getenv(key) or "").strip()
    if not raw:
        return []
    return [t.strip() for t in raw.split(",") if t.strip()]


def discover_candidate_tables(engine: Engine, schema: str = DEFAULT_SCHEMA) -> list[str]:
    """Discover inspectable tables — FAST_MODE returns fixed candidate only."""
    from azure_reconciliation.fixed_azure_candidate import fixed_table_name

    if settings.use_fixed_azure_candidate and not settings.enable_full_discovery:
        logger.info("FAST_MODE: single candidate table %s.%s", schema, fixed_table_name())
        return [fixed_table_name()]

    available = list_available_tables(engine, schema)
    hints = _optional_env_list("AZURE_TABLE_HINTS")
    from azure_reconciliation.azure_client import AZURE_TABLE_CANDIDATES

    seen: set[str] = set()
    ordered: list[str] = []
    for name in hints + AZURE_TABLE_CANDIDATES + available:
        if name not in seen and name in available:
            seen.add(name)
            ordered.append(name)
    for name in available:
        if name not in seen:
            seen.add(name)
            ordered.append(name)
    logger.info("Candidate Azure tables for evaluation: %d", len(ordered))
    return ordered


def _load_seed_rankings(issuer: str) -> dict[tuple[str, str], float]:
    """Seed bonus from prior discovery reports — not hardcoded truth."""
    bonus: dict[tuple[str, str], float] = {}
    paths = [
        settings.azure_discovery_output_path / f"recommendations_{issuer}.xlsx",
        settings.assets_path / issuer / "azurevs" / "discovery" / f"strategy_vs_xml_comparison_{issuer}.xlsx",
        settings.project_root / f"strategy_vs_xml_comparison_{issuer}.xlsx",
    ]
    for path in paths:
        if not path.is_file():
            continue
        get_audit().mark_outputs_read(path)
        try:
            df = pd.read_excel(path, sheet_name="strategy_scores")
            if df.empty:
                df = pd.read_excel(path, sheet_name="recommendations")
            for _, row in df.iterrows():
                tbl = str(row.get("source_table") or row.get("best_azure_table", "")).replace("dbo.", "")
                sid = str(row.get("strategy_id") or row.get("best_strategy_id", ""))
                score = float(row.get("confidence_score", 0) or 0)
                if tbl and sid:
                    bonus[(tbl, sid)] = min(score / 20.0, 5.0)
            logger.info("Loaded seed rankings from %s (%d entries)", path, len(bonus))
            break
        except Exception as exc:
            logger.debug("Seed ranking unavailable from %s: %s", path, exc)
    return bonus


def _pick_date_column(profile: TableProfile, strategy_id: str, strategy_rows: pd.DataFrame) -> str:
    if not strategy_rows.empty and "source_date_column" in strategy_rows.columns:
        val = str(strategy_rows["source_date_column"].iloc[0])
        if val and val != "(none — snapshot)" and val in profile.columns:
            return val
    if strategy_id == "D" and profile.maint_date_col:
        return profile.maint_date_col
    for cand in profile.event_date_cols + EVENT_DATE_CANDIDATES:
        col = _pick_filter_column(profile.columns, [cand])
        if col and col not in LOAD_DATE_ROLES:
            return col
    if profile.maint_date_col:
        return profile.maint_date_col
    if profile.file_date_col and profile.file_date_col not in LOAD_DATE_ROLES:
        return profile.file_date_col
    return profile.month_col or ""


def _profile_quality(profile: TableProfile, df: pd.DataFrame, issuer: str) -> tuple[float, float, float]:
    if df.empty:
        return 0.0, 0.0, 0.0
    issuer_cov = 1.0
    if profile.issuer_col and profile.issuer_col in df.columns:
        issuer_cov = float((df[profile.issuer_col].astype(str) == str(issuer)).mean())
    date_cov = 0.0
    for col in profile.event_date_cols + ([profile.maint_date_col] if profile.maint_date_col else []):
        if col and col in df.columns:
            date_cov = max(date_cov, 1.0 - float(df[col].isna().mean()))
    status_col = profile.status_col or profile.action_col
    status_cov = 1.0 - float(df[status_col].isna().mean()) if status_col and status_col in df.columns else 0.0
    return issuer_cov, date_cov, status_cov


def evaluate_azure_candidates(
    engine: Engine,
    *,
    issuer: str,
    partitions: list[Partition],
    schema: str = DEFAULT_SCHEMA,
) -> DiscoverySelection:
    """Evaluate candidate tables/strategies — FAST_MODE uses fixed 834_Inbound_test / C."""
    if settings.use_fixed_azure_candidate and not settings.enable_full_discovery:
        from azure_reconciliation.azure_client import list_table_columns
        from azure_reconciliation.fixed_azure_candidate import (
            build_fixed_profile,
            fixed_discovery_selection,
            fixed_table_name,
        )

        cols = list_table_columns(engine, schema, fixed_table_name())
        profile = build_fixed_profile(cols)
        logger.info(
            "FAST_MODE discovery: %s strategy C date=%s (skipped %d-table scan)",
            profile.full_name, "GAA_834_File_Date", 0,
        )
        return fixed_discovery_selection(profile)

    table_names = discover_candidate_tables(engine, schema)
    xml_raw = load_xml_summaries(issuer)
    xml_totals = xml_monthly_totals(xml_raw)
    seed_bonus = _load_seed_rankings(issuer)
    years = sorted({p.year for p in partitions if p.issuer == issuer})

    candidates: list[StrategyCandidate] = []
    score_rows: list[dict[str, Any]] = []

    for table in table_names:
        profile = inspect_table(engine, schema, table)
        if not profile.available or not profile.issuer_col:
            continue

        table_df_parts: list[pd.DataFrame] = []
        for year in years:
            df, _, _ = fetch_issuer_year_sample(engine, profile, issuer=issuer, year=year, limit=8000)
            if not df.empty:
                table_df_parts.append(df)
        if not table_df_parts:
            continue
        table_df = pd.concat(table_df_parts, ignore_index=True)
        issuer_parts = [p for p in partitions if p.issuer == issuer]
        strategies = run_applicable_strategies(table_df, profile, issuer_parts)

        for sid, strat_df in strategies.items():
            if strat_df.empty:
                continue
            meta = STRATEGY_META.get(sid, ("", "unknown", ""))
            date_col = _pick_date_column(profile, sid, strat_df)
            status_col = profile.status_col or profile.action_col or ""
            if "source_status_column" in strat_df.columns and not strat_df.empty:
                status_col = str(strat_df["source_status_column"].iloc[0]) or status_col

            try:
                xml_score = score_strategy_vs_xml(
                    strat_df, xml_totals,
                    strategy_id=sid,
                    source_table=profile.full_name,
                    logic_type=meta[1],
                    source_date_column=date_col,
                    source_status_column=status_col,
                    source_policy_column=profile.policy_col or "",
                    source_member_column=profile.member_col or "",
                    missing_column_penalty=len(profile.missing_roles) * 2.0,
                    partitions=issuer_parts,
                )
            except Exception as exc:
                logger.warning(
                    "Scoring skipped for %s/%s on %s: %s",
                    sid, profile.full_name, table, exc,
                )
                from azure_reconciliation.azure_mirror.discovery.scoring import _low_confidence_result
                xml_score = _low_confidence_result(
                    strategy_id=sid,
                    source_table=profile.full_name,
                    logic_type=meta[1],
                    source_date_column=date_col,
                    source_status_column=status_col,
                    source_policy_column=profile.policy_col or "",
                    source_member_column=profile.member_col or "",
                    missing_column_penalty=len(profile.missing_roles) * 2.0,
                    notes=f"Scoring error: {exc}",
                )
            iss_cov, date_cov, status_cov = _profile_quality(profile, table_df, issuer)
            seed = seed_bonus.get((table, sid), 0.0)
            dynamic = (
                (xml_score["confidence_score"] or 0) * 0.5
                + iss_cov * 15
                + date_cov * 15
                + status_cov * 10
                + min(len(table_df) / 1000.0, 10.0)
                + seed
            )
            dynamic = min(100.0, max(0.0, dynamic))

            cand = StrategyCandidate(
                table=table,
                schema=schema,
                strategy_id=sid,
                profile=profile,
                date_column=date_col,
                status_column=status_col,
                logic_type=meta[1],
                confidence_score=round(dynamic, 2),
                row_count=len(table_df),
                issuer_coverage=round(iss_cov, 3),
                date_coverage=round(date_cov, 3),
                status_coverage=round(status_cov, 3),
                seed_bonus=seed,
                notes=xml_score.get("notes", ""),
            )
            candidates.append(cand)
            score_rows.append({**xml_score, "dynamic_score": dynamic, "seed_bonus": seed, "row_count": len(table_df)})

    scores_df = pd.DataFrame(score_rows)
    if not candidates:
        logger.warning("No Azure strategy candidates evaluated for issuer %s", issuer)
        empty_profile = TableProfile(schema=schema, table="", columns=[], available=False)
        fallback = StrategyCandidate(
            table="", schema=schema, strategy_id="", profile=empty_profile,
            date_column="", status_column="", logic_type="unknown", confidence_score=0.0,
        )
        return DiscoverySelection(candidate=fallback, all_scores=scores_df)

    best = max(candidates, key=lambda c: c.confidence_score)
    logger.info(
        "Selected Azure table=%s strategy=%s score=%.1f rows=%d (evaluated %d tables, %d strategies)",
        best.full_name, best.strategy_id, best.confidence_score, best.row_count,
        len(table_names), len(candidates),
    )
    return DiscoverySelection(
        candidate=best,
        all_scores=scores_df,
        tables_evaluated=len(table_names),
        strategies_evaluated=len(candidates),
    )


def selection_to_fetch_plan(selection: DiscoverySelection) -> "AzureFetchPlan":
    from azure_reconciliation.azure_fetch import AzureFetchPlan

    c = selection.candidate
    p = c.profile
    return AzureFetchPlan(
        schema=c.schema,
        table=c.table,
        strategy_id=c.strategy_id,
        issuer_col=p.issuer_col or "",
        year_col=p.year_col,
        date_col=c.date_column or None,
        status_col=c.status_column or p.status_col or p.action_col,
        policy_col=p.policy_col,
        member_col=p.member_col,
        insurance_type_col=p.insurance_type_col,
        logic_type=c.logic_type,
        confidence_score=c.confidence_score,
        source="dynamic_discovery",
    )
