"""
Business month assignment trace — read-only investigation.

Explains why apply_business_month_basis assigns a different business month
than the source_data folder month for each affected record.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from azure_reconciliation.lifecycle_snapshot_comparison import build_enriched_canonical_xml
from azure_reconciliation.month_reassignment_investigation import _merge_source_metadata
from azure_reconciliation.partition_discovery import discover_partitions
from azure_reconciliation.safe_export import safe_write_excel
from azure_reconciliation.xml_business_reports import (
    MONTH_BASIS_PRIORITY,
    apply_business_month_basis,
)
from azure_reconciliation.xml_loader import load_xml_rows
from config.config import settings
from utils.logger import get_logger

logger = get_logger(__name__)

BASIS_LABELS = {
    "file_event_year_month": "File Event Date",
    "member_maint_year_month": "Member Maintenance Effective Date",
    "benefit_effective_year_month": "Benefit Effective Date",
    "coverage_year_month": "Coverage Year / Source Folder",
}

TRACE_COLUMNS = [
    "issuer",
    "policy_id",
    "member_id",
    "source_year",
    "source_month",
    "business_year",
    "business_month",
    "coverage_year_month",
    "benefit_effective_year_month",
    "member_maint_year_month",
    "file_event_year_month",
    "month_basis_used",
    "selected_month_source",
    "why_selected",
    "why_other_candidates_not_selected",
]


def _debug_dir() -> Path:
    d = settings.outputs_path / "debug"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _zmonth(m: Any) -> str:
    return str(m).strip().zfill(2)


def _parse_ym(ym: str) -> tuple[str, str]:
    s = str(ym or "").strip()
    if "-" in s:
        y, m = s.split("-", 1)
        return y, _zmonth(m)
    return "", ""


def _simulate_month_basis(row: pd.Series) -> tuple[str, str, str]:
    """Mirror apply_business_month_basis selection without modifying data."""
    for col, label in MONTH_BASIS_PRIORITY:
        ym = str(row.get(col, "") or "").strip()
        if ym and "-" in ym:
            y, m = _parse_ym(ym)
            return col, label, f"{y}-{m}"
    y = str(row.get("source_year", row.get("year", "")) or "")
    m = _zmonth(str(row.get("source_month", row.get("month", "")) or ""))
    return "coverage_year_month", BASIS_LABELS["coverage_year_month"], f"{y}-{m}"


def _why_not_selected(
    row: pd.Series,
    *,
    chosen_col: str,
    chosen_label: str,
) -> str:
    notes: list[str] = []
    for col, label in MONTH_BASIS_PRIORITY:
        if col == chosen_col:
            continue
        ym = str(row.get(col, "") or "").strip()
        if not ym or "-" not in ym:
            notes.append(f"{label}: not available (empty or invalid year-month)")
        else:
            notes.append(
                f"{label} ({ym}): not selected — {chosen_label} has higher priority in "
                "apply_business_month_basis (file event → maint → benefit → coverage)"
            )
    return " | ".join(notes)


def _why_selected(chosen_col: str, chosen_label: str, chosen_ym: str, row: pd.Series) -> str:
    if chosen_col == "file_event_year_month":
        dt = row.get("file_event_date") or row.get("event_date") or ""
        return f"{chosen_label} year-month {chosen_ym} from date {dt} (priority 1)"
    if chosen_col == "member_maint_year_month":
        return (
            f"{chosen_label} year-month {chosen_ym} from "
            f"{row.get('member_maint_effective_date', '')} (priority 2)"
        )
    if chosen_col == "benefit_effective_year_month":
        return (
            f"{chosen_label} year-month {chosen_ym} from "
            f"{row.get('benefit_effective_date', '')} (priority 3)"
        )
    return (
        f"{chosen_label} year-month {chosen_ym} — default when no higher-priority "
        "date year-month is present (priority 4)"
    )


def build_assignment_trace_rows(
    xml_raw: pd.DataFrame,
    partitions: list,
    *,
    issuer: str,
    year: str,
) -> pd.DataFrame:
    """Build trace rows for records where source folder month != business month."""
    if xml_raw.empty:
        return pd.DataFrame(columns=TRACE_COLUMNS)

    enriched = build_enriched_canonical_xml(xml_raw, None, partitions=partitions)
    business, _ = apply_business_month_basis(enriched.copy())
    work = _merge_source_metadata(business, xml_raw)
    work["source_year"] = work.get("source_year", work.get("year", "")).astype(str)
    work["source_month"] = work.get("source_month", "").astype(str).map(_zmonth)
    work["business_year"] = work["year"].astype(str)
    work["business_month"] = work["month"].astype(str).map(_zmonth)

    work = work[work["source_year"].astype(str) == str(year)].copy()
    moved = work[work["source_month"] != work["business_month"]].copy()

    if moved.empty:
        return pd.DataFrame(columns=TRACE_COLUMNS)

    rows: list[dict[str, Any]] = []
    for _, row in moved.iterrows():
        chosen_col, chosen_label, chosen_ym = _simulate_month_basis(row)
        by, bm = _parse_ym(chosen_ym)
        rows.append({
            "issuer": issuer,
            "policy_id": row.get("policy_id", ""),
            "member_id": row.get("member_id", ""),
            "source_year": row.get("source_year", ""),
            "source_month": row.get("source_month", ""),
            "business_year": row.get("business_year", by),
            "business_month": row.get("business_month", bm),
            "coverage_year_month": row.get("coverage_year_month", ""),
            "benefit_effective_year_month": row.get("benefit_effective_year_month", ""),
            "member_maint_year_month": row.get("member_maint_year_month", ""),
            "file_event_year_month": row.get("file_event_year_month", ""),
            "month_basis_used": row.get("month_basis_used", chosen_col),
            "selected_month_source": chosen_label,
            "why_selected": _why_selected(chosen_col, chosen_label, chosen_ym, row),
            "why_other_candidates_not_selected": _why_not_selected(
                row, chosen_col=chosen_col, chosen_label=chosen_label,
            ),
        })
    return pd.DataFrame(rows)[TRACE_COLUMNS].reset_index(drop=True)


def run_business_month_assignment_trace(
    *,
    issuer: str,
    year: str,
    parse_source: bool = False,
) -> Path:
    """Write outputs/debug/business_month_assignment_trace.xlsx."""
    partitions = discover_partitions(issuer_filter=issuer, year_filter=year)
    xml_raw = load_xml_rows(
        prefer_staging=not parse_source,
        issuer_filter=issuer,
        year_filter=year,
    )
    if xml_raw.empty:
        raise RuntimeError(f"No XML rows for {issuer}/{year}")

    trace = build_assignment_trace_rows(xml_raw, partitions, issuer=issuer, year=year)

    out_path = _debug_dir() / "business_month_assignment_trace.xlsx"
    safe_write_excel(
        out_path,
        {
            "Assignment_Trace": trace,
            "README": pd.DataFrame({
                "note": [
                    "Records where source_data folder month != business month after apply_business_month_basis.",
                    "Priority: file_event_year_month → member_maint → benefit_effective → coverage_year_month.",
                    "Read-only investigation — does not change production logic.",
                ],
            }),
        },
        drop_duplicate_value_columns=False,
    )
    logger.info("Wrote business month assignment trace → %s (%d rows)", out_path, len(trace))
    return out_path
