"""
Multi-level comparison engine for XML vs Azure.

Levels:
1. event — XML events vs Azure events (same month)
2. lifecycle — XML lifecycle vs Azure lifecycle
3. xml_lifecycle_vs_azure_snapshot — XML lifecycle vs Azure final snapshot
4. azure_lifecycle_vs_azure_snapshot — Azure lifecycle vs Azure final snapshot
"""

from __future__ import annotations

import pandas as pd

from azure_reconciliation.column_mapper import ColumnMappingResult
from azure_reconciliation.reconciler import compare_snapshots
from utils.logger import get_logger

logger = get_logger(__name__)

COMPARISON_LEVELS = (
    "event",
    "lifecycle",
    "xml_lifecycle_vs_azure_snapshot",
    "azure_lifecycle_vs_azure_snapshot",
)


def compare_at_level(
    left: pd.DataFrame,
    right: pd.DataFrame,
    mapping: ColumnMappingResult,
    *,
    partition_label: str,
    level: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    detail, summary = compare_snapshots(left, right, mapping, partition_label=partition_label)
    if not detail.empty:
        detail["comparison_level"] = level
    if not summary.empty:
        summary["comparison_level"] = level
    return detail, summary


def aggregate_level_stats(summaries: list[pd.DataFrame]) -> pd.DataFrame:
    if not summaries:
        return pd.DataFrame()
    combined = pd.concat(summaries, ignore_index=True)
    if "comparison_level" not in combined.columns:
        return combined
    return (
        combined.groupby("comparison_level", dropna=False)
        .agg(
            partitions=("partition", "count"),
            matched_keys=("matched_keys", "sum"),
            status_differences=("status_differences", "sum"),
            xml_not_in_azure=("xml_not_in_azure", "sum"),
            azure_not_in_xml=("azure_not_in_xml", "sum"),
        )
        .reset_index()
    )
