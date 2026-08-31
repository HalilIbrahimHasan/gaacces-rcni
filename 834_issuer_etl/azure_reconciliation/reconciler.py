"""
Compare XML lifecycle snapshots against Azure enrollment snapshots.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from azure_reconciliation.column_mapper import ColumnMappingResult, rename_to_canonical
from azure_reconciliation.status_mapper import normalize_insurance_type, normalize_status
from utils.logger import get_logger

logger = get_logger(__name__)


def _col_series(df: pd.DataFrame, *names: str) -> pd.Series:
    for name in names:
        if name in df.columns:
            return df[name]
    return pd.Series([""] * len(df), index=df.index)


def _join_key_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    out = df.copy()
    out["join_issuer"] = _col_series(out, "issuer", "issuer_id").astype(str).str.strip()
    out["join_enrollment"] = _col_series(out, "enrollment_id", "policy_id").astype(str).str.strip()
    out["join_enrollee"] = _col_series(out, "enrollee_id", "member_id").astype(str).str.strip()
    out["join_insurance"] = _col_series(out, "insurance_type", "insurance_type_code").apply(
        normalize_insurance_type
    )
    out["_join_key"] = (
        out["join_issuer"] + "|" + out["join_enrollment"] + "|"
        + out["join_enrollee"] + "|" + out["join_insurance"]
    )
    return out


def _azure_status(row: pd.Series) -> str:
    for col in (
        "enrollment_status",
        "enrollee_status_description",
        "enrollment_status_description",
        "enrollee_status",
    ):
        if col in row.index and pd.notna(row.get(col)):
            return normalize_status(str(row.get(col)))
    return "UNKNOWN"


def compare_snapshots(
    xml_snapshot: pd.DataFrame,
    azure_snapshot: pd.DataFrame,
    mapping: ColumnMappingResult,
    *,
    partition_label: str = "",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Return (comparison_detail, comparison_summary) DataFrames.
    """
    if xml_snapshot.empty and azure_snapshot.empty:
        return pd.DataFrame(), pd.DataFrame()

    xml = rename_to_canonical(xml_snapshot, mapping, "xml") if not xml_snapshot.empty else pd.DataFrame()
    az = rename_to_canonical(azure_snapshot, mapping, "azure") if not azure_snapshot.empty else pd.DataFrame()

    if xml.empty and az.empty:
        return pd.DataFrame(), pd.DataFrame()

    if "canonical_status" not in xml.columns:
        xml["canonical_status"] = xml.apply(
            lambda r: normalize_status(str(r.get("enrollment_status") or "")),
            axis=1,
        )
    if "canonical_status" not in az.columns:
        az["canonical_status"] = az.apply(_azure_status, axis=1)

    xml = _join_key_frame(xml)
    az = _join_key_frame(az)

    if az.empty:
        merged = xml.copy()
        merged["match_type"] = "XML_ONLY"
        merged["status_match"] = None
        merged["_merge"] = "left_only"
    elif xml.empty:
        merged = az.copy()
        merged["match_type"] = "AZURE_ONLY"
        merged["status_match"] = None
        merged["_merge"] = "right_only"
    else:
        merged = xml.merge(
            az,
            on="_join_key",
            how="outer",
            suffixes=("_xml", "_azure"),
            indicator=True,
        )
        merged["match_type"] = merged["_merge"].map({
            "both": "MATCHED",
            "left_only": "XML_ONLY",
            "right_only": "AZURE_ONLY",
        })
    if "status_match" not in merged.columns:
        merged["status_match"] = merged.apply(
            lambda r: (
                r.get("canonical_status_xml") == r.get("canonical_status_azure")
                if r.get("match_type") == "MATCHED"
                else None
            ),
            axis=1,
        )
    else:
        merged["status_match"] = merged.apply(
            lambda r: (
                r.get("canonical_status_xml") == r.get("canonical_status_azure")
                if r.get("match_type") == "MATCHED"
                else None
            ),
            axis=1,
        )
    merged["partition"] = partition_label

    detail_cols = [
        c for c in merged.columns
        if c in (
            "partition", "match_type", "status_match", "_join_key",
            "join_issuer_xml", "join_enrollment_xml", "join_enrollee_xml", "join_insurance_xml",
            "join_issuer_azure", "join_enrollment_azure", "join_enrollee_azure", "join_insurance_azure",
            "canonical_status_xml", "canonical_status_azure",
            "benefit_effective_date_xml", "benefit_effective_date_azure",
            "benefit_end_date_xml", "benefit_end_date_azure",
            "coverage_year_xml", "coverage_year_azure", "snapshot_month_xml",
        )
    ]
    detail = merged[detail_cols] if detail_cols else merged.copy()

    total = len(merged)
    matched = int((merged["match_type"] == "MATCHED").sum())
    status_diff = int(
        merged.loc[merged["match_type"] == "MATCHED", "status_match"].eq(False).sum()
    )
    xml_only = int((merged["match_type"] == "XML_ONLY").sum())
    azure_only = int((merged["match_type"] == "AZURE_ONLY").sum())

    summary = pd.DataFrame([{
        "partition": partition_label,
        "total_keys": total,
        "matched_keys": matched,
        "status_differences": status_diff,
        "xml_not_in_azure": xml_only,
        "azure_not_in_xml": azure_only,
        "match_rate_pct": round(100.0 * matched / total, 2) if total else 0.0,
    }])

    logger.info(
        "Comparison %s: matched=%d status_diff=%d xml_only=%d azure_only=%d",
        partition_label, matched, status_diff, xml_only, azure_only,
    )
    return detail, summary


def issuer_month_summary(all_summaries: pd.DataFrame) -> pd.DataFrame:
    if all_summaries.empty:
        return all_summaries
    return (
        all_summaries.groupby(["partition"], dropna=False)
        .agg(
            total_keys=("total_keys", "sum"),
            matched_keys=("matched_keys", "sum"),
            status_differences=("status_differences", "sum"),
            xml_not_in_azure=("xml_not_in_azure", "sum"),
            azure_not_in_xml=("azure_not_in_xml", "sum"),
        )
        .reset_index()
    )
