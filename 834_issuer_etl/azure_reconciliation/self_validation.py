"""
Self-validation and final run printout for Azure intelligence.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from utils.logger import get_logger

logger = get_logger(__name__)


def validate_run(stats: dict[str, Any], *, require_azure: bool = False) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []

    def _check(name: str, ok: bool, detail: str = "") -> None:
        checks.append({"check": name, "passed": ok, "detail": detail})
        if not ok:
            logger.warning("Validation failed — %s: %s", name, detail or "failed")

    _check("partitions_discovered", stats.get("partitions", 0) > 0)
    _check("xml_rows_loaded", stats.get("xml_raw_rows", 0) > 0)
    _check("xml_lifecycle_built", stats.get("xml_lifecycle_rows", 0) >= 0)

    if require_azure:
        _check("azure_connected", stats.get("azure_available", False), stats.get("azure_error", ""))
        table = stats.get("final_selected_table") or stats.get("azure_selected_table")
        _check(
            "azure_table_selected",
            bool(table) and str(table).lower() not in ("n/a", "none", ""),
            f"table={table!r}",
        )
        has_rows = stats.get("azure_raw_rows", 0) > 0
        has_final = bool(stats.get("final_selected_table"))
        _check(
            "azure_rows_fetched",
            has_rows or has_final,
            f"azure_raw_rows={stats.get('azure_raw_rows', 0)} final_comparison={has_final}",
        )
        _check(
            "final_comparison_ran",
            bool(stats.get("final_selected_table")) or bool(stats.get("final_comparison_paths")),
            stats.get("final_comparison_skipped", ""),
        )

    if stats.get("excel_output_enabled", True):
        _check(
            "excel_output",
            bool(stats.get("output_excel_path")) or bool(stats.get("final_comparison_paths")),
            "excel may have failed — see outputs/comparison/final_result.*",
        )
    if stats.get("sqlite_output_enabled", True):
        _check(
            "sqlite_output",
            bool(stats.get("output_sqlite_path")) or stats.get("export_error_count", 0) > 0,
            "sqlite may have failed — CSV fallbacks in outputs/csv/",
        )

    passed = sum(1 for c in checks if c["passed"])
    report = {"checks": checks, "passed": passed, "total": len(checks), "all_passed": passed == len(checks)}
    logger.info("Self-validation: %d/%d passed", passed, len(checks))
    return report


def print_final_validation(stats: dict[str, Any]) -> None:
    """Required final validation printout."""
    lines = [
        "=" * 60,
        "AZURE INTELLIGENCE — FINAL VALIDATION",
        "=" * 60,
        f"source_data issuers discovered: {stats.get('source_issuers', stats.get('source_issuers', []))}",
        f"source_data years discovered:   {stats.get('source_years', [])}",
        f"source_data months discovered:  {stats.get('source_months', [])}",
        f"XML raw rows:                   {stats.get('xml_raw_rows', 0)}",
        f"XML lifecycle rows:             {stats.get('xml_lifecycle_rows', 0)}",
        f"Azure selected table:           {stats.get('azure_selected_table', 'n/a')}",
        f"Azure selected strategy:        {stats.get('azure_selected_strategy', 'n/a')}",
        f"Azure confidence score:         {stats.get('azure_confidence_score', 'n/a')}",
        f"Azure raw rows:                 {stats.get('azure_raw_rows', 0)}",
        f"Azure lifecycle rows:           {stats.get('azure_lifecycle_rows', 0)}",
        f"Final selected table:           {stats.get('final_selected_table', 'n/a')}",
        f"Final selected strategy:        {stats.get('final_selected_strategy', 'n/a')}",
        f"Final overall accuracy:         {stats.get('final_overall_accuracy', 'n/a')}",
        f"Final comparison outputs:       {stats.get('final_comparison_paths', 'n/a')}",
        f"event-level match count:        {stats.get('event_level_match_count', 0)}",
        f"lifecycle-level match count:    {stats.get('lifecycle_level_match_count', 0)}",
        f"status difference count:        {stats.get('status_difference_count', 0)}",
        f"XML not found in Azure count:   {stats.get('xml_not_in_azure_count', 0)}",
        f"Azure not found in XML count:   {stats.get('azure_not_in_xml_count', 0)}",
        f"output Excel path:              {stats.get('output_excel_path', 'n/a')}",
        f"output SQLite path:             {stats.get('output_sqlite_path', 'n/a')}",
        f"output dashboard path:          {stats.get('output_dashboard_path', 'n/a')}",
        f"issuer reports index:           {stats.get('issuer_reports_index', 'n/a')}",
        f"final comparison paths:         {stats.get('final_comparison_paths', 'n/a')}",
        f"export error count:             {stats.get('export_error_count', 0)}",
        f"debug paths:                    {stats.get('debug_paths', 'n/a')}",
        f"execution seconds:              {stats.get('execution_seconds', 'n/a')}",
        "=" * 60,
    ]
    for line in lines:
        print(line)
        logger.info(line)


def investigate_zero_azure_rows(
    partitions: list,
    azure_frames: list[pd.DataFrame],
    fetch_plans: list[dict[str, Any]],
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for i, part in enumerate(partitions):
        frame = azure_frames[i] if i < len(azure_frames) else pd.DataFrame()
        plan = fetch_plans[i] if i < len(fetch_plans) else {}
        if frame.empty:
            rows.append({
                "partition": part.label(),
                "azure_table": plan.get("table", ""),
                "strategy_id": plan.get("strategy_id", ""),
                "diagnosis": "Zero rows — discovery engine will try next candidate",
            })
    return pd.DataFrame(rows)
