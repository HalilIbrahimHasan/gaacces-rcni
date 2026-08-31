"""
Unified Azure discovery outputs under outputs/azure_discovery/.

Aggregates per-issuer discovery results into reusable xlsx, sqlite, and html.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

import pandas as pd

from azure_reconciliation.excel_exporter import write_excel_report
from config.config import settings
from utils.logger import get_logger

logger = get_logger(__name__)

DISCOVERY_SCHEMA = """
CREATE TABLE IF NOT EXISTS table_profiles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    issuer TEXT, table_name TEXT, column_count INTEGER,
    issuer_col TEXT, year_col TEXT, status_col TEXT, policy_col TEXT, member_col TEXT,
    event_date_cols TEXT, missing_roles TEXT, xml_event_match_potential TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS recommendations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    issuer TEXT, best_azure_table TEXT, best_strategy_id TEXT, best_logic_type TEXT,
    best_date_column TEXT, best_status_column TEXT, best_policy_column TEXT,
    best_member_column TEXT, confidence_score REAL, snapshot_or_event TEXT,
    why_recommended TEXT, source_path TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS strategy_scores (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    issuer TEXT, strategy_id TEXT, source_table TEXT, logic_type TEXT,
    confidence_score REAL, enrollee_count_diff_total REAL,
    month_coverage_pct REAL, notes TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);
"""


class AzureDiscoveryStore:
    def __init__(self, db_path: Path | None = None) -> None:
        self.db_path = db_path or settings.azure_discovery_output_path / "azure_discovery.sqlite"
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.db_path) as conn:
            conn.executescript(DISCOVERY_SCHEMA)

    def replace_issuer_data(
        self,
        issuer: str,
        *,
        table_profiles: pd.DataFrame,
        recommendations: pd.DataFrame,
        strategy_scores: pd.DataFrame,
    ) -> None:
        with sqlite3.connect(self.db_path) as conn:
            for table in ("table_profiles", "recommendations", "strategy_scores"):
                conn.execute(f"DELETE FROM {table} WHERE issuer = ?", (issuer,))
            if not table_profiles.empty:
                tp = table_profiles.copy()
                tp["issuer"] = issuer
                tp.rename(columns={"table": "table_name"}, inplace=True, errors="ignore")
                tp.to_sql("table_profiles", conn, if_exists="append", index=False)
            if not recommendations.empty:
                rec = recommendations.copy()
                rec["issuer"] = issuer
                rec.to_sql("recommendations", conn, if_exists="append", index=False)
            if not strategy_scores.empty:
                sc = strategy_scores.copy()
                sc["issuer"] = issuer
                sc.to_sql("strategy_scores", conn, if_exists="append", index=False)


def publish_discovery_outputs(
    issuer: str,
    *,
    table_profiles: pd.DataFrame,
    logic_candidates: pd.DataFrame,
    strategy_scores: pd.DataFrame,
    recommendations: pd.DataFrame,
    all_strategies: pd.DataFrame,
    xml_reference: pd.DataFrame,
    closest_match: pd.DataFrame,
) -> dict[str, str]:
    """Write unified discovery artifacts to outputs/azure_discovery/."""
    out_dir = settings.azure_discovery_output_path
    out_dir.mkdir(parents=True, exist_ok=True)

    xlsx_path = out_dir / "azure_discovery.xlsx"
    issuer_xlsx = out_dir / f"azure_discovery_{issuer}.xlsx"
    rec_path = out_dir / f"recommendations_{issuer}.xlsx"
    html_path = out_dir / f"azure_discovery_{issuer}.html"

    sheets = {
        "Table_Profiles": table_profiles,
        "Logic_Candidates": logic_candidates,
        "Strategy_Scores": strategy_scores,
        "Recommendations": recommendations,
        "Strategy_Summaries": all_strategies,
        "XML_Reference": xml_reference,
        "Closest_Match_By_Month": closest_match,
    }
    write_excel_report(xlsx_path, sheets)
    write_excel_report(issuer_xlsx, sheets)
    write_excel_report(rec_path, {"recommendations": recommendations, "strategy_scores": strategy_scores})

    store = AzureDiscoveryStore()
    store.replace_issuer_data(
        issuer,
        table_profiles=table_profiles,
        recommendations=recommendations,
        strategy_scores=strategy_scores,
    )

    html_path.write_text(
        _discovery_html(issuer, recommendations, strategy_scores, xml_reference),
        encoding="utf-8",
    )
    global_html = out_dir / "azure_discovery.html"
    global_html.write_text(html_path.read_text(encoding="utf-8"), encoding="utf-8")

    paths = {
        "azure_discovery_xlsx": str(xlsx_path),
        "azure_discovery_issuer_xlsx": str(issuer_xlsx),
        "recommendations_xlsx": str(rec_path),
        "azure_discovery_html": str(html_path),
        "azure_discovery_sqlite": str(store.db_path),
    }
    logger.info("Published Azure discovery outputs for issuer %s: %s", issuer, out_dir)
    return paths


def _discovery_html(
    issuer: str,
    recommendations: pd.DataFrame,
    scores: pd.DataFrame,
    xml_ref: pd.DataFrame,
) -> str:
    rec_html = recommendations.head(1).to_html(index=False) if not recommendations.empty else "<p>No recommendation</p>"
    scores_html = scores.sort_values("confidence_score", ascending=False).head(10).to_html(index=False) if not scores.empty else "<p>No scores</p>"
    xml_html = xml_ref.head(20).to_html(index=False) if not xml_ref.empty else "<p>No XML reference</p>"
    return f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>Azure Discovery — {issuer}</title>
<style>
body {{ font-family: Arial, sans-serif; margin: 24px; background: #121212; color: #eee; }}
table {{ border-collapse: collapse; width: 100%; margin: 12px 0; }}
th, td {{ border: 1px solid #444; padding: 6px; text-align: left; }}
th {{ background: #2B2B2B; }}
h2 {{ color: #6eb6ff; }}
</style></head><body>
<h1>Azure Discovery — Issuer {issuer}</h1>
<p>Table/strategy selected dynamically each run via discovery engine.</p>
<h2>Recommendation</h2>
{rec_html}
<h2>Top Strategy Scores</h2>
{scores_html}
<h2>XML Reference</h2>
{xml_html}
<p>GAA_Load_Date is diagnostic only — never used for monthly filtering.</p>
</body></html>"""
