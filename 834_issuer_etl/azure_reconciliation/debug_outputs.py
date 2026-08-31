"""Debug outputs written on every intelligence / final comparison run."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from config.config import ENV_FILE, azure_startup_diagnostics, settings
from azure_reconciliation.safe_export import ExportErrors, safe_write_csv
from utils.logger import get_logger

logger = get_logger(__name__)


def debug_dir() -> Path:
    d = settings.outputs_path / "debug"
    d.mkdir(parents=True, exist_ok=True)
    return d


def write_run_config(stats: dict[str, Any]) -> Path:
    diag = azure_startup_diagnostics()
    lines = [
        f"ENABLE_AZURE={settings.azure_enabled}",
        f"env_file={ENV_FILE}",
        f"env_file_exists={ENV_FILE.is_file()}",
        f"SERVER_present={diag.get('server_present')}",
        f"DATABASE_present={diag.get('database_present')}",
        f"USERNAME_present={diag.get('username_present')}",
        f"DRIVER_present={diag.get('driver_present')}",
        f"DRIVER_value={diag.get('driver_value')}",
        f"issuer_filter={settings.issuer_filter}",
        f"year_filter={settings.year_filter}",
        f"month_filter={settings.month_filter}",
        f"skip_azure={stats.get('skip_azure')}",
        f"azure_requested={stats.get('azure_requested')}",
        f"FAST_MODE={settings.fast_mode}",
        f"ENABLE_FULL_DISCOVERY={settings.enable_full_discovery}",
        f"USE_FIXED_AZURE_CANDIDATE={settings.use_fixed_azure_candidate}",
    ]
    path = debug_dir() / "run_config.txt"
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def write_source_data_coverage(coverage_df: pd.DataFrame) -> Path:
    path = debug_dir() / "source_data_coverage.csv"
    safe_write_csv(path, coverage_df, table_name="source_data_coverage")
    return path


def write_azure_selected_candidate(row: dict[str, Any]) -> Path:
    path = debug_dir() / "azure_selected_candidate.csv"
    safe_write_csv(path, pd.DataFrame([row]), table_name="azure_selected_candidate")
    return path


def write_strategy_scores(scores_df: pd.DataFrame) -> Path:
    path = debug_dir() / "strategy_scores.csv"
    safe_write_csv(path, scores_df, table_name="strategy_scores")
    return path


def write_final_validation(stats: dict[str, Any], validation: dict[str, Any]) -> Path:
    lines = [
        "FINAL VALIDATION",
        "=" * 40,
    ]
    for key in sorted(stats.keys()):
        if key == "validation":
            continue
        lines.append(f"{key}: {stats[key]}")
    lines.append("")
    lines.append("VALIDATION CHECKS")
    for check in validation.get("checks", []):
        mark = "PASS" if check.get("passed") else "FAIL"
        lines.append(f"  [{mark}] {check.get('check')}: {check.get('detail', '')}")
    lines.append(f"all_passed={validation.get('all_passed')}")
    path = debug_dir() / "final_validation.txt"
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def write_all_debug_outputs(
    *,
    stats: dict[str, Any],
    coverage_df: pd.DataFrame,
    export_errors: ExportErrors,
    strategy_scores: pd.DataFrame | None = None,
    validation: dict[str, Any] | None = None,
) -> dict[str, str]:
    paths: dict[str, str] = {}
    paths["run_config"] = str(write_run_config(stats))
    if not coverage_df.empty:
        paths["source_data_coverage"] = str(write_source_data_coverage(coverage_df))
    if stats.get("azure_selected_table"):
        paths["azure_selected_candidate"] = str(write_azure_selected_candidate({
            "table": stats.get("azure_selected_table"),
            "strategy": stats.get("azure_selected_strategy"),
            "confidence": stats.get("azure_confidence_score"),
            "final_table": stats.get("final_selected_table"),
            "final_strategy": stats.get("final_selected_strategy"),
            "final_accuracy": stats.get("final_overall_accuracy"),
        }))
    if strategy_scores is not None and not strategy_scores.empty:
        paths["strategy_scores"] = str(write_strategy_scores(strategy_scores))
    paths["export_errors"] = str(export_errors.write_debug_file())
    if validation:
        paths["final_validation"] = str(write_final_validation(stats, validation))
    return paths
