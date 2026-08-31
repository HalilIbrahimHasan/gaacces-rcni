"""Orchestrate Azure logic discovery for one issuer."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy.engine import Engine

from azure_reconciliation.azure_client import DEFAULT_SCHEMA, connect_azure
from azure_reconciliation.azure_mirror.discovery.scoring import (
    build_recommendation,
    closest_match_by_month,
    score_strategy_vs_xml,
)
from azure_reconciliation.azure_mirror.discovery.strategies import (
    STRATEGY_META,
    run_applicable_strategies,
)
from azure_reconciliation.azure_mirror.discovery.table_inspector import (
    TableProfile,
    fetch_issuer_year_sample,
    inspect_all_tables,
    profiles_to_dataframe,
)
from azure_reconciliation.azure_mirror.discovery.xml_reference import (
    load_xml_summaries,
    xml_monthly_totals,
)
from azure_reconciliation.discovery_store import publish_discovery_outputs
from config.config import settings
from utils.logger import get_logger

logger = get_logger(__name__)


def _discovery_dirs(issuer: str) -> Path:
    return settings.assets_path / issuer / "azurevs" / "discovery"


def _write_workbook(path: Path, sheets: dict[str, pd.DataFrame]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        for name, frame in sheets.items():
            (frame if not frame.empty else pd.DataFrame({"note": ["no data"]})).to_excel(
                writer, sheet_name=name[:31], index=False
            )
    logger.info("Discovery workbook written: %s", path)


def _table_score_penalty(profile: TableProfile) -> float:
    return len(profile.missing_roles) * 5.0


def _count_by_dimensions(df: pd.DataFrame, profile: TableProfile) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    rows: list[dict] = []
    status_col = profile.status_col
    for val, grp in (df.groupby(status_col) if status_col and status_col in df.columns else [(None, df)]):
        rows.append({
            "status": val,
            "raw_rows": len(grp),
            "policy_count": grp[profile.policy_col].nunique() if profile.policy_col and profile.policy_col in grp.columns else None,
            "member_count": grp[profile.member_col].nunique() if profile.member_col and profile.member_col in grp.columns else None,
        })
    return pd.DataFrame(rows)


def run_azure_logic_discovery(
    engine: Engine,
    *,
    issuer: str,
    partitions: list[Partition],
    schema: str = DEFAULT_SCHEMA,
) -> dict[str, Any]:
    """Run full discovery for one issuer. Returns stats and output paths."""
    out_dir = _discovery_dirs(issuer)
    out_dir.mkdir(parents=True, exist_ok=True)

    xml_raw = load_xml_summaries(issuer)
    xml_totals = xml_monthly_totals(xml_raw)

    profiles = inspect_all_tables(engine, schema)
    table_discovery = profiles_to_dataframe(profiles)

    query_log: list[dict] = []
    all_strategy_rows: list[pd.DataFrame] = []
    logic_candidates: list[dict] = []
    sample_rows: list[pd.DataFrame] = []

    years = sorted({p.year for p in partitions})

    for profile in profiles:
        if not profile.issuer_col:
            logic_candidates.append({
                "table": profile.full_name,
                "usable": False,
                "reason": "missing issuer column",
            })
            continue

        table_strategies: list[pd.DataFrame] = []
        for year in years:
            year_parts = [p for p in partitions if p.year == year]
            df, sql, params = fetch_issuer_year_sample(
                engine, profile, issuer=issuer, year=year, limit=8000
            )
            query_log.append({
                "table": profile.full_name,
                "issuer": issuer,
                "year": year,
                "sql": sql,
                "params": str(params),
                "row_count": len(df),
            })
            if df.empty:
                continue
            if len(sample_rows) < 3:
                sample_rows.append(df.head(50).assign(_source_table=profile.full_name))

            strat = run_applicable_strategies(df, profile, year_parts)
            for sid, sdf in strat.items():
                if not sdf.empty:
                    table_strategies.append(sdf)

        if table_strategies:
            combined_st = pd.concat(table_strategies, ignore_index=True)
            all_strategy_rows.append(combined_st)

        logic_candidates.append({
            "table": profile.full_name,
            "usable": bool(table_strategies),
            "issuer_col": profile.issuer_col,
            "year_col": profile.year_col,
            "status_col": profile.status_col,
            "action_col": profile.action_col,
            "policy_col": profile.policy_col,
            "member_col": profile.member_col,
            "event_date_cols": ", ".join(profile.event_date_cols),
            "strategies_tested": ", ".join(sorted(set(
                s["strategy_id"].iloc[0] for s in table_strategies if not s.empty
            ))) if table_strategies else "",
            "xml_event_match_potential": (
                "HIGH" if "834_Inbound" in profile.table
                else "MEDIUM" if profile.action_col
                else "LOW"
            ),
        })

    all_strategies = (
        pd.concat(all_strategy_rows, ignore_index=True)
        if all_strategy_rows else pd.DataFrame()
    )

    # Score each strategy+table combo
    scores: list[dict] = []
    if not all_strategies.empty:
        for (sid, tbl), grp in all_strategies.groupby(["strategy_id", "source_table"]):
            meta = STRATEGY_META.get(str(sid), ("", "unknown", ""))
            prof = next((p for p in profiles if p.full_name == tbl), None)
            penalty = _table_score_penalty(prof) if prof else 10.0
            row = grp.iloc[0]
            scores.append(score_strategy_vs_xml(
                grp, xml_totals,
                strategy_id=str(sid),
                source_table=str(tbl),
                logic_type=meta[1],
                source_date_column=str(row.get("source_date_column", "")),
                source_status_column=str(row.get("source_status_column", "")),
                source_policy_column=prof.policy_col if prof else "",
                source_member_column=prof.member_col if prof else "",
                missing_column_penalty=penalty,
                partitions=partitions,
            ))
            logger.info(
                "Strategy score: %s / %s = %.1f",
                sid, tbl, scores[-1]["confidence_score"],
            )

    scores_df = pd.DataFrame(scores)
    recommendation = build_recommendation(scores_df)
    closest = closest_match_by_month(all_strategies, xml_totals, partitions=partitions)

    if not scores_df.empty:
        best = scores_df.sort_values("confidence_score", ascending=False).iloc[0]
        logger.info(
            "Best candidate strategy: %s on %s (score=%.1f, type=%s)",
            best["strategy_id"], best["source_table"],
            best["confidence_score"], best["logic_type"],
        )
        logger.info("Recommendation: %s", recommendation.iloc[0].to_dict() if not recommendation.empty else {})

    # Per-strategy sheets for comparison workbook
    strat_sheets = {
        "A": all_strategies[all_strategies["strategy_id"] == "A"] if not all_strategies.empty else pd.DataFrame(),
        "B": all_strategies[all_strategies["strategy_id"] == "B"] if not all_strategies.empty else pd.DataFrame(),
        "C": all_strategies[all_strategies["strategy_id"] == "C"] if not all_strategies.empty else pd.DataFrame(),
        "D": all_strategies[all_strategies["strategy_id"] == "D"] if not all_strategies.empty else pd.DataFrame(),
        "E": all_strategies[all_strategies["strategy_id"] == "E"] if not all_strategies.empty else pd.DataFrame(),
    }

    comparison_path = out_dir / f"strategy_vs_xml_comparison_{issuer}.xlsx"
    _write_workbook(comparison_path, {
        "strategy_scores": scores_df,
        "active_coverage_snapshot": strat_sheets["A"],
        "enrollment_status_snapshot": strat_sheets["B"],
        "event_date_logic": strat_sheets["C"],
        "inbound_834_logic": strat_sheets["D"],
        "carrier_invoice_logic": strat_sheets["E"],
        "closest_match_by_month": closest,
        "recommendations": recommendation,
        "query_log": pd.DataFrame(query_log),
        "xml_reference": xml_totals,
        "missing_columns": table_discovery[table_discovery["missing_roles"].astype(str).str.len() > 0]
        if "missing_roles" in table_discovery.columns else pd.DataFrame(),
    })

    table_disc_path = out_dir / f"azure_table_discovery_{issuer}.xlsx"
    _write_workbook(table_disc_path, {
        "table_profiles": table_discovery,
        "logic_candidates": pd.DataFrame(logic_candidates),
        "sample_rows": pd.concat(sample_rows, ignore_index=True) if sample_rows else pd.DataFrame(),
    })

    candidates_path = out_dir / f"azure_logic_candidates_{issuer}.xlsx"
    _write_workbook(candidates_path, {
        "all_strategy_summaries": all_strategies,
        "strategy_scores": scores_df,
        "recommendations": recommendation,
    })

    html_path = out_dir / f"azure_event_candidate_summary_{issuer}.html"
    _write_html_summary(html_path, issuer, scores_df, recommendation, xml_totals, all_strategies)

    published = publish_discovery_outputs(
        issuer,
        table_profiles=table_discovery,
        logic_candidates=pd.DataFrame(logic_candidates),
        strategy_scores=scores_df,
        recommendations=recommendation,
        all_strategies=all_strategies,
        xml_reference=xml_totals,
        closest_match=closest,
    )

    return {
        "issuer": issuer,
        "tables_inspected": len(profiles),
        "strategy_rows": len(all_strategies),
        "best_score": float(scores_df["confidence_score"].max()) if not scores_df.empty else 0,
        "outputs": {
            "table_discovery": str(table_disc_path),
            "logic_candidates": str(candidates_path),
            "comparison": str(comparison_path),
            "html_summary": str(html_path),
            **published,
        },
    }


def _write_html_summary(
    path: Path,
    issuer: str,
    scores_df: pd.DataFrame,
    recommendation: pd.DataFrame,
    xml_totals: pd.DataFrame,
    all_strategies: pd.DataFrame,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    best_html = recommendation.head(1).to_html(index=False) if not recommendation.empty else "<p>No recommendation</p>"
    scores_html = scores_df.sort_values("confidence_score", ascending=False).head(10).to_html(index=False) if not scores_df.empty else "<p>No scores</p>"
    xml_html = xml_totals.head(20).to_html(index=False) if not xml_totals.empty else "<p>No XML reference loaded from assets</p>"

    html = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>Azure Logic Discovery — {issuer}</title>
<style>
body {{ font-family: Arial, sans-serif; margin: 24px; background: #121212; color: #eee; }}
table {{ border-collapse: collapse; width: 100%; margin: 12px 0; }}
th, td {{ border: 1px solid #444; padding: 6px; text-align: left; }}
th {{ background: #2B2B2B; }}
h2 {{ color: #6eb6ff; }}
</style></head><body>
<h1>Azure Event Logic Discovery — Issuer {issuer}</h1>
<h2>Recommendation</h2>
{best_html}
<h2>Strategy Scores (top 10)</h2>
{scores_html}
<h2>XML Reference (from assets summaries)</h2>
{xml_html}
<p>GAA_Load_Date is never used for monthly filtering. Strategy A (active coverage) may repeat counts across months.</p>
<p>Strategy D (834_Inbound) is the primary candidate for XML 834 event logic.</p>
</body></html>"""
    path.write_text(html, encoding="utf-8")
    logger.info("Discovery HTML written: %s", path)


def export_azure_logic_discovery(
    *,
    issuer_filter: str | None = None,
    year_filter: str | None = None,
    month_filter: str | None = None,
    engine: Engine | None = None,
) -> dict[str, Any]:
    """Standalone discovery export — requires working Azure connection."""
    # Connect first (same path as run_azure_mirror) so interactive login always triggers.
    if engine is None:
        engine, meta = connect_azure(runner_name="export_azure_logic_discovery", strict=False)
        if engine is None:
            return {"issuers": 0, "connection": meta}

    partitions = discover_partitions(
        issuer_filter=issuer_filter or settings.issuer_filter,
        year_filter=year_filter or settings.year_filter,
        month_filter=month_filter or settings.month_filter,
    )
    if not partitions:
        logger.info("No source_data partitions for discovery")
        return {"issuers": 0, "connection": {"connected": True}}

    issuers = sorted({p.issuer for p in partitions})
    results = []
    for issuer in issuers:
        issuer_parts = [p for p in partitions if p.issuer == issuer]
        logger.info("Running logic discovery for issuer %s (%d partitions)", issuer, len(issuer_parts))
        results.append(run_azure_logic_discovery(engine, issuer=issuer, partitions=issuer_parts))

    return {"issuers": len(results), "results": results}
