"""
Azure lifecycle engine — chronological event replay for Azure event tables.

Mirrors XML lifecycle_engine pattern: normalize → join key → sort → replay → snapshot.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pandas as pd

from azure_reconciliation.column_mapper import ColumnMappingResult, rename_to_canonical
from azure_reconciliation.partition_discovery import Partition
from azure_reconciliation.status_mapper import normalize_insurance_type, normalize_status
from utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class AzureLifecycleState:
    issuer: str
    enrollment_id: str
    enrollee_id: str
    insurance_type: str
    canonical_status: str = "UNKNOWN"
    benefit_effective_date: str | None = None
    benefit_end_date: str | None = None
    event_date: str | None = None
    last_event_year: str | None = None
    last_event_month: str | None = None
    event_count: int = 0
    source_table: str = ""


def _member_key(row: pd.Series) -> tuple[str, str, str, str]:
    issuer = str(row.get("issuer") or row.get("issuer_id") or "")
    enrollment = str(row.get("enrollment_id") or row.get("policy_id") or "")
    enrollee = str(row.get("enrollee_id") or row.get("member_id") or "")
    ins = normalize_insurance_type(row.get("insurance_type") or row.get("insurance_type_code"))
    return issuer, enrollment, enrollee, ins


def _status_from_row(row: pd.Series) -> str:
    for col in (
        "enrollment_status", "enrollee_status_description", "enrollment_status_description",
        "enrollee_status", "action_code", "actionCode", "status",
    ):
        if col in row.index and pd.notna(row.get(col)):
            s = normalize_status(str(row.get(col)))
            if s != "UNKNOWN":
                return s
    return "UNKNOWN"


def _event_ym(row: pd.Series, date_col: str | None) -> int:
    for col in (date_col, "member_maint_effective_date", "event_date", "year"):
        if col and col in row.index and pd.notna(row.get(col)):
            if col == "year":
                y = int(str(row.get("year", 0)))
                m = int(str(row.get("month", 0) or 0))
                return y * 100 + m
            try:
                dt = pd.to_datetime(row[col], errors="coerce")
                if pd.notna(dt):
                    return int(dt.year) * 100 + int(dt.month)
            except Exception:
                pass
    y = int(str(row.get("year") or row.get("coverage_year") or 0))
    m = int(str(row.get("month") or row.get("snapshot_month") or 0))
    return y * 100 + m


def normalize_azure_events(
    raw_df: pd.DataFrame,
    mapping: ColumnMappingResult,
    *,
    date_col: str | None = None,
    source_table: str = "",
) -> pd.DataFrame:
    if raw_df.empty:
        return pd.DataFrame()
    work = rename_to_canonical(raw_df, mapping, "azure").copy()
    if date_col and date_col in raw_df.columns:
        work["event_date"] = raw_df[date_col]
    work["canonical_status"] = work.apply(_status_from_row, axis=1)
    if "insurance_type" in work.columns:
        work["insurance_type"] = work["insurance_type"].apply(normalize_insurance_type)
    work["source_table"] = source_table
    if "year" not in work.columns and "event_date" in work.columns:
        dt = pd.to_datetime(work["event_date"], errors="coerce")
        work["year"] = dt.dt.year.astype("Int64")
        work["month"] = dt.dt.month.astype("Int64").astype(str).str.zfill(2)
    return work


def replay_azure_lifecycle(
    events_df: pd.DataFrame,
    target_partition: Partition,
    *,
    date_col: str | None = None,
) -> pd.DataFrame:
    """Replay Azure events chronologically up to target partition month."""
    if events_df.empty:
        return pd.DataFrame()

    work = events_df.copy()
    target_ym = int(target_partition.year) * 100 + int(target_partition.month)
    work["_ym"] = work.apply(lambda r: _event_ym(r, date_col), axis=1)
    issuer_series = work["issuer"] if "issuer" in work.columns else work.get("issuer_id", pd.Series([""] * len(work)))
    work = work[(work["_ym"] <= target_ym) & (issuer_series.astype(str) == str(target_partition.issuer))].copy()
    if work.empty:
        return pd.DataFrame()

    sort_cols = ["_ym"]
    for col in ("event_date", "member_maint_effective_date"):
        if col in work.columns:
            sort_cols.append(col)
    work = work.sort_values(sort_cols, ascending=True, na_position="last", kind="mergesort")

    states: dict[tuple[str, str, str, str], AzureLifecycleState] = {}
    for _, row in work.iterrows():
        key = _member_key(row)
        if not key[0] or not key[1] or not key[2]:
            continue
        st = states.get(key) or AzureLifecycleState(
            issuer=key[0], enrollment_id=key[1], enrollee_id=key[2], insurance_type=key[3],
            source_table=str(row.get("source_table", "")),
        )
        st.canonical_status = _status_from_row(row)
        st.benefit_effective_date = row.get("benefit_effective_date")
        st.benefit_end_date = row.get("benefit_end_date")
        st.event_date = str(row.get("event_date") or "")
        st.last_event_year = str(target_partition.year)
        st.last_event_month = str(target_partition.month).zfill(2)
        st.event_count += 1
        states[key] = st

    rows = [{
        "issuer": s.issuer,
        "enrollment_id": s.enrollment_id,
        "enrollee_id": s.enrollee_id,
        "insurance_type": s.insurance_type,
        "canonical_status": s.canonical_status,
        "benefit_effective_date": s.benefit_effective_date,
        "benefit_end_date": s.benefit_end_date,
        "coverage_year": target_partition.year,
        "snapshot_month": target_partition.month,
        "event_count": s.event_count,
        "last_event_year": s.last_event_year,
        "last_event_month": s.last_event_month,
        "source_table": s.source_table,
    } for s in states.values()]
    result = pd.DataFrame(rows)
    logger.info("Azure lifecycle %s: %d members from %d events", target_partition.label(), len(result), len(work))
    return result


def build_all_azure_lifecycle_snapshots(
    events_df: pd.DataFrame,
    partitions: list[Partition],
    *,
    date_col: str | None = None,
) -> pd.DataFrame:
    frames = [
        replay_azure_lifecycle(events_df, p, date_col=date_col)
        for p in partitions
    ]
    frames = [f for f in frames if not f.empty]
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def azure_final_snapshot(
    lifecycle_df: pd.DataFrame,
    target_partition: Partition,
) -> pd.DataFrame:
    """Latest Azure lifecycle state at target partition (final snapshot view)."""
    if lifecycle_df.empty:
        return pd.DataFrame()
    mask = (
        (lifecycle_df["coverage_year"].astype(str) == str(target_partition.year))
        & (lifecycle_df["snapshot_month"].astype(str).str.zfill(2) == str(target_partition.month).zfill(2))
        & (lifecycle_df["issuer"].astype(str) == str(target_partition.issuer))
    )
    return lifecycle_df.loc[mask].copy()
