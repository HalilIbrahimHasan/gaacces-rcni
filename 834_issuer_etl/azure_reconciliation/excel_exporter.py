"""Excel exports for Azure vs XML reconciliation."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from azure_reconciliation.safe_export import ExportErrors, safe_write_excel
from utils.logger import get_logger

logger = get_logger(__name__)


def write_excel_report(
    output_path: Path,
    sheets: dict[str, pd.DataFrame],
    *,
    export_errors: ExportErrors | None = None,
) -> Path | None:
    ok = safe_write_excel(output_path, sheets, export_errors=export_errors)
    if ok:
        return output_path
    return None


def build_comparison_workbook(
    *,
    final_summary: pd.DataFrame,
    status_summary: pd.DataFrame,
    issuer_month: pd.DataFrame,
    xml_summary: pd.DataFrame,
    azure_summary: pd.DataFrame,
    match_sample: pd.DataFrame,
    status_diff: pd.DataFrame,
    xml_not_in_azure: pd.DataFrame,
    azure_not_in_xml: pd.DataFrame,
    detailed: pd.DataFrame,
    lifecycle_summary: pd.DataFrame | None = None,
    column_mapping: pd.DataFrame | None = None,
    azure_discovery: pd.DataFrame | None = None,
) -> dict[str, pd.DataFrame]:
    sheets = {
        "Final_Result_Summary": final_summary,
        "Status_Result_Summary": status_summary,
        "Issuer_Month_Result": issuer_month,
        "XML_Summary": xml_summary,
        "Azure_Summary": azure_summary,
        "Match_Sample": match_sample,
        "Status_Diff": status_diff,
        "XML_Not_In_Azure": xml_not_in_azure,
        "Azure_Not_In_XML": azure_not_in_xml,
        "Detailed_Comparison": detailed,
    }
    if lifecycle_summary is not None and not lifecycle_summary.empty:
        sheets["Lifecycle_Summary"] = lifecycle_summary
    if column_mapping is not None and not column_mapping.empty:
        sheets["Column_Mapping"] = column_mapping
    if azure_discovery is not None and not azure_discovery.empty:
        sheets["Azure_Discovery"] = azure_discovery
    return sheets
