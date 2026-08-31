"""Export final comparison outputs to outputs/comparison/."""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from azure_reconciliation.excel_exporter import write_excel_report
from azure_reconciliation.final_comparison_engine import ComparisonResult
from azure_reconciliation.safe_export import (
    ExportErrors,
    safe_write_csv,
    safe_write_html_report,
    safe_write_sqlite,
    write_csv_fallback,
)
from config.config import settings
from utils.logger import get_logger

logger = get_logger(__name__)


def comparison_output_dir() -> Path:
    d = settings.outputs_path / "comparison"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _strategy_scores_df(results: list[ComparisonResult]) -> pd.DataFrame:
    score_rows = []
    for r in results:
        score_rows.append({
            "strategy_id": r.strategy_id,
            "source_table": r.source_table,
            "date_column": r.date_column,
            "status_column": r.status_column,
            "join_key": r.join_key,
            "overall_accuracy": r.overall_accuracy,
            "behavior_penalty": r.behavior_penalty,
            "behavior_bonus": r.behavior_bonus,
            "behavior_notes": r.behavior_notes,
            **r.scores,
        })
    return pd.DataFrame(score_rows)


def _build_final_result_frames(
    comparison: dict[str, Any],
    *,
    issuer: str = "",
    export_errors: ExportErrors | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    winner: ComparisonResult | None = comparison.get("winner")
    results: list[ComparisonResult] = comparison.get("results", [])
    rejected: list[dict[str, str]] = comparison.get("rejected", [])

    strategy_scores = _strategy_scores_df(results)
    detail_rows: list[dict] = []
    for r in results:
        detail_rows.extend(r.detail_rows)
    comparison_details = pd.DataFrame(detail_rows)

    if winner:
        reliable = comparison.get("accuracy_reliable", comparison.get("match_count", 0) > 0)
        raw_rates = comparison.get("raw_event_rates") or comparison.get("record_stats", {}).get("rates", {})
        lifecycle_rates = comparison.get("lifecycle_rates") or comparison.get("record_rates", {})
        best = pd.DataFrame([{
            "issuer": issuer,
            "selected_table": winner.source_table,
            "selected_strategy": winner.strategy_id,
            "selected_date_column": winner.date_column,
            "selected_status_column": winner.status_column,
            "selected_join_key": winner.join_key,
            "auto_join_mapping": comparison.get("join_mapping", ""),
            "selected_status_mapping": str(winner.status_mapping) if reliable else "not reliable",
            "status_mapping_reliable": comparison.get("status_mapping_reliable", reliable),
            "accuracy_reliable": reliable,
            "relationship_valid": comparison.get("relationship_valid", lifecycle_rates.get("relationship_valid", False)),
            "xml_raw_rows": comparison.get("xml_raw_rows"),
            "xml_lifecycle_snapshot_rows": comparison.get("xml_lifecycle_snapshot_rows"),
            "azure_raw_rows": comparison.get("azure_raw_rows"),
            "azure_lifecycle_snapshot_rows": comparison.get("azure_lifecycle_snapshot_rows"),
            "raw_event_match_rate": comparison.get("raw_event_match_rate", raw_rates.get("record_match_rate")),
            "lifecycle_snapshot_match_rate": comparison.get(
                "lifecycle_snapshot_match_rate",
                lifecycle_rates.get("lifecycle_snapshot_match_rate"),
            ),
            "status_match_rate": lifecycle_rates.get("status_match_rate"),
            "effective_date_match_rate": lifecycle_rates.get("effective_date_match_rate"),
            "file_event_month_match_rate": lifecycle_rates.get("file_event_month_match_rate"),
            "final_business_match_rate": comparison.get("final_business_match_rate"),
            "model_h_xml_groups": comparison.get("model_h_xml_groups"),
            "model_h_azure_groups": comparison.get("model_h_azure_groups"),
            "model_h_matched_groups": comparison.get("model_h_matched_groups"),
            "model_h_xml_not_in_azure": comparison.get("model_h_xml_not_in_azure"),
            "model_h_azure_not_in_xml": comparison.get("model_h_azure_not_in_xml"),
            "model_h_group_match_rate": comparison.get("model_h_group_match_rate"),
            "model_h_status_match_rate": comparison.get("model_h_status_match_rate"),
            "primary_business_model": "H",
            "lifecycle_match_count": comparison.get("match_count", 0),
            "lifecycle_xml_not_in_azure": comparison.get("xml_not_in_azure_count", 0),
            "lifecycle_azure_not_in_xml": comparison.get("azure_not_in_xml_count", 0),
            "match_count": comparison.get("match_count", 0),
            "confidence_score": winner.overall_accuracy if reliable else None,
            "overall_accuracy": winner.overall_accuracy,
            "overall_accuracy_note": "" if reliable else "NOT RELIABLE — 0 lifecycle matches; fix ID mapping",
            "comparable_date_diff_count": comparison.get("date_diff_count", 0),
            "cross_field_date_diff_count": comparison.get("cross_field_date_diff_count", 0),
            "xml_not_in_azure_remaining": comparison.get("xml_not_in_azure_count", 0),
            "azure_not_in_xml_remaining": comparison.get("azure_not_in_xml_count", 0),
            "historical_window": comparison.get("historical_window", ""),
            "xml_lifecycle_rows": comparison.get("xml_lifecycle_rows"),
            "weakest_level": min(winner.scores, key=winner.scores.get) if winner.scores else "",
            "weakest_score": min(winner.scores.values()) if winner.scores else None,
            **winner.scores,
        }])
    else:
        best = pd.DataFrame([{
            "issuer": issuer,
            "cannot_calculate_reason": comparison.get("cannot_calculate_reason", ""),
            "xml_raw_rows": comparison.get("xml_raw_rows"),
            "xml_lifecycle_rows": comparison.get("xml_lifecycle_rows"),
        }])

    rejected_df = pd.DataFrame(rejected) if rejected else pd.DataFrame()
    if export_errors and export_errors.errors:
        err_df = pd.DataFrame({"export_error": export_errors.errors})
        rejected_df = pd.concat([rejected_df, err_df], ignore_index=True)

    summary = best.copy()
    return summary, strategy_scores, pd.concat(
        [comparison_details, rejected_df], ignore_index=True,
    ) if not comparison_details.empty or not rejected_df.empty else comparison_details


def export_final_result(
    comparison: dict[str, Any],
    *,
    issuer: str = "",
    export_errors: ExportErrors | None = None,
) -> dict[str, str]:
    """Write outputs/comparison/final_result.{html,xlsx,csv}."""
    out = comparison_output_dir()
    summary, strategy_scores, details = _build_final_result_frames(
        comparison, issuer=issuer, export_errors=export_errors,
    )
    winner: ComparisonResult | None = comparison.get("winner")
    rejected = comparison.get("rejected", [])
    paths: dict[str, str] = {}

    # CSV
    csv_path = out / "final_result.csv"
    safe_write_csv(csv_path, summary, table_name="final_result", export_errors=export_errors)
    paths["final_result_csv"] = str(csv_path)
    write_csv_fallback("final_result_strategy_scores", strategy_scores, export_errors=export_errors)

    # Excel
    xlsx_path = out / "final_result.xlsx"
    result = write_excel_report(
        xlsx_path,
        {
            "final_result": summary,
            "strategy_scores": strategy_scores,
            "comparison_details": details,
            "rejected_strategies": pd.DataFrame(rejected),
        },
        export_errors=export_errors,
    )
    if result:
        paths["final_result_xlsx"] = str(xlsx_path)

    # HTML
    extra = ""
    if winner:
        reliable = comparison.get("accuracy_reliable", comparison.get("match_count", 0) > 0)
        weak = min(winner.scores, key=winner.scores.get) if winner.scores else "n/a"
        extra = (
            f"<p><strong>Selected:</strong> {winner.source_table} / Strategy {winner.strategy_id}</p>"
            f"<p><strong>Auto join:</strong> {comparison.get('join_mapping', winner.join_key)}</p>"
            f"<p><strong>Record matches:</strong> {comparison.get('match_count', 0)}</p>"
        )
        if not reliable:
            extra += (
                "<p style='color:#c00'><strong>RECORD MATCH = 0 — accuracy is NOT reliable.</strong></p>"
                "<p>Next action: review <code>outputs/debug/id_overlap_matrix.csv</code> "
                "and correct ID column mapping.</p>"
                "<p><strong>Status mapping:</strong> not reliable (suppressed)</p>"
            )
        else:
            raw_rates = comparison.get("raw_event_rates") or comparison.get("record_stats", {}).get("rates", {})
            lifecycle_rates = comparison.get("lifecycle_rates") or comparison.get("record_rates", {})
            model_h = comparison.get("model_h") or {}
            if model_h:
                xml_g = int(model_h.get("xml_output_count", 0))
                matched_g = int(model_h.get("match_count", 0))
                xml_only_g = int(model_h.get("xml_not_in_azure", 0))
                az_only_g = int(model_h.get("azure_not_in_xml", 0))
                extra += (
                    "<h3>PRIMARY: Model H dashboard aggregation</h3>"
                    f"<p><strong>Final business accuracy (Model H):</strong> "
                    f"{winner.overall_accuracy:.1f}%</p>"
                    f"<p>XML dashboard groups: {xml_g} | Azure dashboard groups: "
                    f"{model_h.get('azure_output_count', 0)} | Matched: {matched_g}</p>"
                    f"<p>At raw event level XML contains many maintenance/duplicate/superseded "
                    f"transactions. At Chandra-like dashboard aggregation level, Azure and XML "
                    f"match on <strong>{matched_g} of {xml_g}</strong> groups; Azure has "
                    f"<strong>{'no' if az_only_g == 0 else az_only_g}</strong> extra groups. "
                    f"Remaining mismatch is <strong>{xml_only_g}</strong> XML-only aggregated groups.</p>"
                    "<p>See <code>outputs/comparison/final_business_result.html</code></p>"
                )
            else:
                extra += (
                    f"<p><strong>Final business accuracy (lifecycle-based):</strong> "
                    f"{winner.overall_accuracy:.1f}%</p>"
                )
            extra += (
                "<h3>A) Raw event comparison (diagnostic only)</h3>"
                f"<p>Raw event match rate: {comparison.get('raw_event_match_rate', raw_rates.get('record_match_rate', 0)):.1f}%</p>"
                f"<p>XML raw rows: {comparison.get('xml_raw_rows', 0):,} | "
                f"Azure raw rows: {comparison.get('azure_raw_rows', 0):,}</p>"
                "<h3>B) Lifecycle snapshot comparison (diagnostic only)</h3>"
                f"<p>Lifecycle snapshot match rate: "
                f"{comparison.get('lifecycle_snapshot_match_rate', lifecycle_rates.get('lifecycle_snapshot_match_rate', 0)):.1f}%</p>"
                f"<p>Status match rate: {lifecycle_rates.get('status_match_rate', 0):.1f}%</p>"
                f"<p>XML lifecycle snapshot rows: {comparison.get('xml_lifecycle_snapshot_rows', 0):,} | "
                f"Azure lifecycle snapshot rows: {comparison.get('azure_lifecycle_snapshot_rows', 0):,}</p>"
                f"<p>XML lifecycle not in Azure: {comparison.get('xml_not_in_azure_count', 0):,}</p>"
                f"<p>Azure not in XML: {comparison.get('azure_not_in_xml_count', 0):,}</p>"
            )
            if comparison.get("event_explanation"):
                extra += f"<p><em>{comparison['event_explanation']}</em></p>"
            if comparison.get("relationship_valid") or lifecycle_rates.get("relationship_valid"):
                extra += "<p><strong>Relationship:</strong> VALID</p>"
            extra += f"<p><strong>Weakest aggregate level:</strong> {weak}</p>"
    else:
        extra = f"<p><strong>Cannot calculate:</strong> {comparison.get('cannot_calculate_reason', 'unknown')}</p>"

    html_path = out / "final_result.html"
    safe_write_html_report(
        html_path,
        title=f"Final Result — issuer {issuer}",
        summary_df=summary,
        detail_df=strategy_scores,
        extra_html=extra,
        export_errors=export_errors,
    )
    paths["final_result_html"] = str(html_path)
    return paths


def export_final_comparison(
    comparison: dict[str, Any],
    *,
    issuer: str = "",
    export_errors: ExportErrors | None = None,
) -> dict[str, str]:
    out = comparison_output_dir()
    winner: ComparisonResult | None = comparison.get("winner")
    results: list[ComparisonResult] = comparison.get("results", [])

    strategy_scores = _strategy_scores_df(results)
    detail_rows: list[dict] = []
    for r in results:
        detail_rows.extend(r.detail_rows)
    comparison_details = pd.DataFrame(detail_rows)

    if winner:
        best = pd.DataFrame([{
            "selected_table": winner.source_table,
            "selected_strategy": winner.strategy_id,
            "selected_date_column": winner.date_column,
            "selected_status_column": winner.status_column,
            "selected_join_key": winner.join_key,
            "selected_status_mapping": str(winner.status_mapping),
            "confidence_score": winner.overall_accuracy,
            "overall_accuracy": winner.overall_accuracy,
            "historical_window": comparison.get("historical_window", ""),
            **winner.scores,
        }])
    else:
        best = pd.DataFrame()

    summary = best.copy()
    if not summary.empty:
        summary["issuer"] = issuer

    paths: dict[str, str] = {}
    paths["comparison_summary"] = str(out / "comparison_summary.xlsx")
    paths["comparison_details"] = str(out / "comparison_details.xlsx")
    paths["strategy_scores"] = str(out / "strategy_scores.xlsx")
    paths["best_strategy"] = str(out / "best_strategy.xlsx")

    write_excel_report(
        Path(paths["comparison_summary"]),
        {"summary": summary, "strategy_scores": strategy_scores},
        export_errors=export_errors,
    )
    write_excel_report(
        Path(paths["comparison_details"]),
        {"details": comparison_details},
        export_errors=export_errors,
    )
    write_excel_report(
        Path(paths["strategy_scores"]),
        {"strategy_scores": strategy_scores},
        export_errors=export_errors,
    )
    write_excel_report(
        Path(paths["best_strategy"]),
        {"best_strategy": best},
        export_errors=export_errors,
    )

    sqlite_path = out / "comparison.sqlite"
    try:
        with sqlite3.connect(sqlite_path) as conn:
            if not strategy_scores.empty:
                safe_write_sqlite(conn, "strategy_scores", strategy_scores, export_errors=export_errors)
            if not comparison_details.empty:
                safe_write_sqlite(conn, "comparison_details", comparison_details, export_errors=export_errors)
            if not best.empty:
                safe_write_sqlite(conn, "best_strategy", best, export_errors=export_errors)
    except Exception as exc:
        if export_errors:
            export_errors.record(f"comparison.sqlite failed: {exc}")

    paths["comparison_sqlite"] = str(sqlite_path)
    write_csv_fallback("strategy_scores", strategy_scores, export_errors=export_errors)
    write_csv_fallback("comparison_details", comparison_details, export_errors=export_errors)

    paths["comparison_dashboard"] = str(_write_comparison_dashboard(out, strategy_scores, winner))
    paths["strategy_dashboard"] = str(_write_strategy_dashboard(out, strategy_scores))
    paths["business_dashboard"] = str(_write_business_dashboard(out, winner))

    paths.update(export_final_result(comparison, issuer=issuer, export_errors=export_errors))
    logger.info("Final comparison exported to %s", out)
    return paths


def _write_comparison_dashboard(out: Path, scores: pd.DataFrame, winner: ComparisonResult | None) -> Path:
    path = out / "comparison_dashboard.html"
    try:
        fig = make_subplots(rows=1, cols=2, subplot_titles=("Strategy Accuracy", "Winner Scores"))
        if not scores.empty:
            fig.add_trace(
                go.Bar(x=scores["source_table"] + " / " + scores["strategy_id"], y=scores["overall_accuracy"]),
                row=1, col=1,
            )
        if winner:
            fig.add_trace(
                go.Bar(x=list(winner.scores.keys()), y=list(winner.scores.values()), name="Winner"),
                row=1, col=2,
            )
        fig.update_layout(title="Final Comparison Dashboard", template="plotly_dark", height=500)
        fig.write_html(str(path), include_plotlyjs="cdn")
    except Exception as exc:
        logger.warning("comparison_dashboard.html failed: %s", exc)
        path.write_text(f"<html><body><p>Dashboard failed: {exc}</p></body></html>")
    return path


def _write_strategy_dashboard(out: Path, scores: pd.DataFrame) -> Path:
    path = out / "strategy_dashboard.html"
    try:
        fig = go.Figure()
        if not scores.empty:
            for sid in scores["strategy_id"].unique():
                sub = scores[scores["strategy_id"] == sid]
                fig.add_trace(go.Scatter(
                    x=sub["source_table"], y=sub["overall_accuracy"],
                    mode="markers+lines", name=f"Strategy {sid}",
                ))
        fig.update_layout(title="Strategy Scores by Table", template="plotly_dark", height=450)
        fig.write_html(str(path), include_plotlyjs="cdn")
    except Exception as exc:
        path.write_text(f"<html><body><p>Dashboard failed: {exc}</p></body></html>")
    return path


def _write_business_dashboard(out: Path, winner: ComparisonResult | None) -> Path:
    path = out / "business_dashboard.html"
    try:
        fig = go.Figure()
        if winner:
            fig.add_trace(go.Bar(
                x=list(winner.scores.keys()),
                y=list(winner.scores.values()),
                text=[f"{v:.1f}%" for v in winner.scores.values()],
                textposition="auto",
            ))
        fig.update_layout(
            title=f"Business Accuracy — {winner.source_table if winner else 'n/a'}",
            template="plotly_dark", height=450,
        )
        fig.write_html(str(path), include_plotlyjs="cdn")
    except Exception as exc:
        path.write_text(f"<html><body><p>Dashboard failed: {exc}</p></body></html>")
    return path
