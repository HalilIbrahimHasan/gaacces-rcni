"""
Model H export orchestration — always runs per issuer regardless of match rate.

Wiring only; Model H aggregation logic lives in reconciliation_analysis.
"""

from __future__ import annotations

import traceback
from pathlib import Path
from typing import Any

import pandas as pd

from azure_reconciliation.fixed_azure_candidate import FIXED_DATE_COL, build_fixed_profile
from azure_reconciliation.partition_discovery import Partition
from azure_reconciliation.reconciliation_analysis import (
    finalize_model_h_business_output,
)
from azure_reconciliation.safe_export import ExportErrors, safe_write_csv, safe_write_html_report
from config.config import settings
from utils.logger import get_logger

logger = get_logger(__name__)

MODEL_H_DETAIL_COLUMNS = [
    "issuer", "year", "month", "insurance_type", "status",
    "xml_enrollment_count", "xml_enrollee_count", "xml_subscriber_count",
    "azure_enrollment_count", "azure_enrollee_count", "azure_subscriber_count",
    "match_status", "count_match_pct",
]

MODEL_H_XML_ONLY_COLUMNS = [
    "issuer", "year", "month", "insurance_type", "status",
    "xml_enrollment_count", "xml_enrollee_count", "xml_subscriber_count",
    "azure_enrollment_count", "azure_enrollee_count", "azure_subscriber_count",
    "difference_reason",
]

REQUIRED_OUTPUTS = [
    "comparison/{issuer}_final_business_result.html",
    "comparison/final_business_result.html",
    "debug/{issuer}_model_h_xml_vs_azure_detail.csv",
    "debug/{issuer}_model_h_xml_not_in_azure.csv",
    "debug/{issuer}_model_h_count_column_audit.csv",
    "debug/{issuer}_final_executive_summary.md",
]


def _dbg() -> Path:
    d = settings.outputs_path / "debug"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _cmp() -> Path:
    d = settings.outputs_path / "comparison"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _empty_detail_df() -> pd.DataFrame:
    return pd.DataFrame(columns=MODEL_H_DETAIL_COLUMNS)


def _empty_xml_only_df() -> pd.DataFrame:
    return pd.DataFrame(columns=MODEL_H_XML_ONLY_COLUMNS)


def _empty_count_audit_df() -> pd.DataFrame:
    return pd.DataFrame(columns=[
        "source", "count_type", "candidate_columns", "columns_with_data",
        "primary_column", "rows_with_id", "rows_total", "nonempty_rate_pct",
    ])


def resolve_canonical_for_model_h(
    comparison: dict[str, Any],
    xml_raw: pd.DataFrame,
    partitions: list[Partition],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Resolve canonical XML/Azure frames from comparison or build from raw."""
    from azure_reconciliation.lifecycle_snapshot_comparison import (
        build_enriched_canonical_azure,
        build_enriched_canonical_xml,
    )

    lifecycle_result = comparison.get("lifecycle_result") or {}
    xml_canonical = lifecycle_result.get("xml_canonical", pd.DataFrame())
    az_canonical = lifecycle_result.get("azure_canonical", pd.DataFrame())

    record_stats = comparison.get("record_stats") or {}
    join_mapping = record_stats.get("join_mapping")
    if join_mapping is None and isinstance(record_stats, dict):
        join_mapping = comparison.get("join_mapping")

    if not isinstance(xml_canonical, pd.DataFrame) or xml_canonical.empty:
        if not xml_raw.empty:
            xml_canonical = build_enriched_canonical_xml(
                xml_raw, join_mapping, partitions=partitions,
            )
        else:
            xml_canonical = pd.DataFrame()

    if not isinstance(az_canonical, pd.DataFrame) or az_canonical.empty:
        table_df = comparison.get("azure_raw", pd.DataFrame())
        if isinstance(table_df, pd.DataFrame) and not table_df.empty:
            profile = build_fixed_profile(list(table_df.columns))
            az_canonical = build_enriched_canonical_azure(
                table_df,
                profile,
                join_mapping,
                date_col=FIXED_DATE_COL,
                partitions=partitions,
            )
        else:
            az_canonical = pd.DataFrame()

    return xml_canonical, az_canonical


def _write_executive_summary_empty(path: Path, *, issuer: str, xml_raw_rows: int, note: str) -> None:
    lines = [
        "# Final Executive Summary",
        "",
        f"**Issuer:** {issuer}",
        f"**Primary business model:** Model H (Chandra-like dashboard aggregation)",
        "",
        "## Primary conclusion",
        "",
        note,
        "",
        f"XML raw rows: {xml_raw_rows:,}",
        "",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    logger.info("MODEL H EXPORT WRITTEN: %s", path)


def _write_empty_model_h_outputs(
    *,
    issuer: str,
    xml_raw_rows: int,
    error: str | None = None,
) -> dict[str, Any]:
    """Write placeholder outputs when Model H has zero groups or export failed."""
    dbg = _dbg()
    cmp_dir = _cmp()
    note = "No Model H groups were generated for this issuer."
    if error:
        note = f"{note} Export error: {error}"

    detail_out = _empty_detail_df()
    xml_only_df = _empty_xml_only_df()
    audit_df = _empty_count_audit_df()

    paths: dict[str, str] = {}
    file_map = {
        f"{issuer}_model_h_xml_vs_azure_detail.csv": detail_out,
        "model_h_xml_vs_azure_detail.csv": detail_out,
        f"{issuer}_model_h_xml_not_in_azure.csv": xml_only_df,
        "model_h_xml_not_in_azure.csv": xml_only_df,
        f"{issuer}_model_h_count_column_audit.csv": audit_df,
        "model_h_count_column_audit.csv": audit_df,
    }
    for name, df in file_map.items():
        p = dbg / name
        safe_write_csv(p, df, table_name=name.replace(".csv", ""), drop_duplicate_value_columns=False)
        paths[name] = str(p)
        logger.info("MODEL H EXPORT WRITTEN: %s", p)

    exec_issuer = dbg / f"{issuer}_final_executive_summary.md"
    exec_alias = dbg / "final_executive_summary.md"
    for p in (exec_issuer, exec_alias):
        _write_executive_summary_empty(p, issuer=issuer, xml_raw_rows=xml_raw_rows, note=note)
    paths["final_executive_summary"] = str(exec_alias)
    paths[f"{issuer}_final_executive_summary"] = str(exec_issuer)

    summary_df = pd.DataFrame([{
        "issuer": issuer,
        "model_id": "H",
        "model_name": "Chandra-like dashboard aggregation",
        "xml_dashboard_groups": 0,
        "azure_dashboard_groups": 0,
        "matched_groups": 0,
        "xml_not_in_azure_groups": 0,
        "azure_not_in_xml_groups": 0,
        "group_match_rate": 0.0,
        "status_match_rate": 0.0,
        "note": note,
    }])
    extra = f"<p><strong>{note}</strong></p>"
    html_issuer = cmp_dir / f"{issuer}_final_business_result.html"
    html_alias = cmp_dir / "final_business_result.html"
    for html_path in (html_issuer, html_alias):
        safe_write_html_report(
            html_path,
            title=f"Final Business Result (Model H) — issuer {issuer}",
            summary_df=summary_df,
            detail_df=pd.DataFrame(),
            extra_html=extra,
        )
        paths[str(html_path.name)] = str(html_path)
        logger.info("MODEL H EXPORT WRITTEN: %s", html_path)

    return {
        "model_id": "H",
        "xml_output_count": 0,
        "azure_output_count": 0,
        "match_count": 0,
        "xml_not_in_azure": 0,
        "azure_not_in_xml": 0,
        "group_match_rate": 0.0,
        "status_match_rate": 0.0,
        "count_accuracy_on_matched": 0.0,
        "overall_business_accuracy": 0.0,
        "relationship_valid": False,
        "paths": paths,
        "detail_df": detail_out,
        "xml_only_df": xml_only_df,
        "count_audit_df": audit_df,
        "empty": True,
    }


def export_model_h_for_issuer(
    *,
    issuer: str,
    comparison: dict[str, Any],
    xml_raw: pd.DataFrame,
    partitions: list[Partition],
    export_errors: ExportErrors | None = None,
    engine: Any | None = None,
) -> dict[str, Any]:
    """Always export Model H outputs for one issuer."""
    logger.info("MODEL H STARTED for issuer %s", issuer)
    dbg = _dbg()
    azure_zero_reason = ""

    try:
        xml_canonical, az_canonical = resolve_canonical_for_model_h(
            comparison, xml_raw, partitions,
        )

        if engine is not None:
            from azure_reconciliation.azure_fetch_diagnostics import write_issuer_diagnostics
            _, azure_zero_reason = write_issuer_diagnostics(
                engine,
                issuer=issuer,
                partitions=partitions,
                canonical_azure=az_canonical,
                xml_raw_rows=len(xml_raw),
            )

        from azure_reconciliation.reconciliation_analysis import _chandra_dashboard
        xml_dash = _chandra_dashboard(xml_canonical, source="xml")
        az_dash = _chandra_dashboard(az_canonical, source="azure")
        logger.info("MODEL H ROWS XML=%s AZURE=%s", len(xml_dash), len(az_dash))

        if xml_dash.empty and az_dash.empty:
            model_h = _write_empty_model_h_outputs(
                issuer=issuer, xml_raw_rows=len(xml_raw),
            )
        else:
            model_h = finalize_model_h_business_output(
                issuer=issuer,
                xml_canonical=xml_canonical,
                az_canonical=az_canonical,
                xml_raw_rows=len(xml_raw),
                write_per_issuer_paths=True,
                azure_zero_reason=azure_zero_reason,
            )
            model_h["azure_zero_reason"] = azure_zero_reason

        comparison["model_h"] = model_h
        comparison["azure_zero_reason"] = azure_zero_reason
        for _k, path in model_h.get("paths", {}).items():
            if str(path).endswith((".csv", ".md", ".html")):
                logger.info("MODEL H EXPORT WRITTEN: %s", path)
        return model_h

    except Exception as exc:
        tb = traceback.format_exc()
        logger.error("MODEL H export failed for issuer %s:\n%s", issuer, tb)
        err_path = dbg / f"{issuer}_model_h_export_error.txt"
        err_path.write_text(tb, encoding="utf-8")
        logger.info("MODEL H EXPORT WRITTEN: %s", err_path)
        if export_errors:
            export_errors.record(f"model_h_{issuer}: {exc}")
        model_h = _write_empty_model_h_outputs(
            issuer=issuer, xml_raw_rows=len(xml_raw), error=str(exc),
        )
        comparison["model_h"] = model_h
        return model_h


def validate_model_h_outputs_for_issuer(issuer: str) -> list[str]:
    """Return list of missing required output paths for an issuer."""
    root = settings.outputs_path
    missing: list[str] = []
    for pattern in REQUIRED_OUTPUTS:
        rel = pattern.format(issuer=issuer)
        path = root / rel
        if not path.exists():
            missing.append(str(path))
    return missing


def write_all_issuers_business_result_html(
    issuer_model_h: list[tuple[str, dict[str, Any]]],
    export_errors: ExportErrors | None = None,
) -> Path | None:
    """Combine per-issuer Model H summaries into one HTML report."""
    if len(issuer_model_h) <= 1:
        return None
    cmp_dir = _cmp()
    rows: list[dict[str, Any]] = []
    for issuer, mh in issuer_model_h:
        rows.append({
            "issuer": issuer,
            "xml_dashboard_groups": mh.get("xml_output_count", 0),
            "azure_dashboard_groups": mh.get("azure_output_count", 0),
            "matched_groups": mh.get("match_count", 0),
            "xml_not_in_azure": mh.get("xml_not_in_azure", 0),
            "azure_not_in_xml": mh.get("azure_not_in_xml", 0),
            "group_match_rate": mh.get("group_match_rate", 0),
            "status_match_rate": mh.get("status_match_rate", 0),
        })
    summary_df = pd.DataFrame(rows)
    html_path = cmp_dir / "final_business_result_all_issuers.html"
    safe_write_html_report(
        html_path,
        title="Final Business Result (Model H) — all issuers",
        summary_df=summary_df,
        export_errors=export_errors,
    )
    logger.info("MODEL H EXPORT WRITTEN: %s", html_path)
    return html_path
