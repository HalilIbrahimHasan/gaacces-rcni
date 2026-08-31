"""
Chronological lifecycle replay for XML enrollment events.

Replays all maintenance events in partition order to derive comparison state
at each target month — latest event alone is NOT sufficient.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pandas as pd

from azure_reconciliation.partition_discovery import Partition
from azure_reconciliation.status_mapper import normalize_insurance_type, normalize_status
from utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class LifecycleState:
    issuer: str
    enrollment_id: str
    enrollee_id: str
    insurance_type: str
    canonical_status: str = "UNKNOWN"
    benefit_effective_date: str | None = None
    benefit_end_date: str | None = None
    member_maint_effective_date: str | None = None
    last_event_month: str | None = None
    last_event_year: str | None = None
    event_count: int = 0
    source_files: list[str] = field(default_factory=list)


def _event_sort_key(row: pd.Series) -> tuple:
    year = int(str(row.get("year") or row.get("source_year") or 0))
    month = int(str(row.get("month") or row.get("source_month") or 0))
    maint = str(row.get("member_maint_effective_date") or "")
    req_ts = str(row.get("request_submit_timestamp") or "")
    src = str(row.get("file_name") or row.get("source_file") or "")
    return (year, month, maint, req_ts, src)


def _member_key(row: pd.Series) -> tuple[str, str, str, str]:
    issuer = str(row.get("issuer") or row.get("issuer_id") or "")
    enrollment = str(row.get("policy_id") or row.get("enrollment_id") or "")
    enrollee = str(row.get("member_id") or row.get("enrollee_id") or "")
    ins = normalize_insurance_type(
        row.get("insurance_type_code") or row.get("insurance_type")
    )
    return issuer, enrollment, enrollee, ins


def _status_from_row(row: pd.Series) -> str:
    for col in (
        "additional_maint_reason_code",
        "coverage_status",
        "action_code_description",
        "transaction_classification",
        "enrollee_event_type_code",
    ):
        val = row.get(col)
        if val:
            status = normalize_status(str(val))
            if status != "UNKNOWN":
                return status
    return "UNKNOWN"


def replay_lifecycle(
    events_df: pd.DataFrame,
    target_partition: Partition,
) -> pd.DataFrame:
    """
    Replay all events up to and including target_partition month.

    Returns one row per (issuer, enrollment_id, enrollee_id, insurance_type)
    representing lifecycle state at end of target month.
    """
    if events_df.empty:
        return pd.DataFrame()

    work = events_df.copy()
    target_key = (int(target_partition.year), int(target_partition.month))

    # Filter events chronologically up to target month
    work["_year_int"] = work["year"].astype(str).str.replace(r"\D", "", regex=True)
    work["_month_int"] = work["month"].astype(str).str.zfill(2)
    work["_ym"] = work["_year_int"].astype(int) * 100 + work["_month_int"].astype(int)
    target_ym = int(target_partition.year) * 100 + int(target_partition.month)
    work = work[work["_ym"] <= target_ym].copy()

    issuer_mask = work["issuer"].astype(str) == str(target_partition.issuer)
    work = work[issuer_mask].copy()

    if work.empty:
        return pd.DataFrame()

    sort_cols = ["_year_int", "_month_int"]
    for col in ("member_maint_effective_date", "request_submit_timestamp", "file_name"):
        if col in work.columns:
            sort_cols.append(col)
    work = work.sort_values(by=sort_cols, ascending=True, na_position="last", kind="mergesort")

    states: dict[tuple[str, str, str, str], LifecycleState] = {}

    for _, row in work.iterrows():
        key = _member_key(row)
        if not key[0] or not key[1] or not key[2]:
            continue

        st = states.get(key) or LifecycleState(
            issuer=key[0],
            enrollment_id=key[1],
            enrollee_id=key[2],
            insurance_type=key[3],
        )
        st.canonical_status = _status_from_row(row)
        st.benefit_effective_date = row.get("benefit_effective_date")
        st.benefit_end_date = row.get("benefit_end_date")
        st.member_maint_effective_date = row.get("member_maint_effective_date")
        st.last_event_year = str(row.get("year"))
        st.last_event_month = str(row.get("month")).zfill(2)
        st.event_count += 1
        fn = str(row.get("file_name") or row.get("source_file") or "")
        if fn and fn not in st.source_files:
            st.source_files.append(fn)
        states[key] = st

    rows: list[dict[str, Any]] = []
    for st in states.values():
        rows.append({
            "issuer": st.issuer,
            "enrollment_id": st.enrollment_id,
            "enrollee_id": st.enrollee_id,
            "insurance_type": st.insurance_type,
            "canonical_status": st.canonical_status,
            "benefit_effective_date": st.benefit_effective_date,
            "benefit_end_date": st.benefit_end_date,
            "member_maint_effective_date": st.member_maint_effective_date,
            "coverage_year": target_partition.year,
            "snapshot_month": target_partition.month,
            "event_count": st.event_count,
            "last_event_year": st.last_event_year,
            "last_event_month": st.last_event_month,
            "source_files": "|".join(st.source_files[:5]),
        })

    result = pd.DataFrame(rows)
    logger.info(
        "Lifecycle snapshot %s: %d member(s) from %d event(s)",
        target_partition.label(),
        len(result),
        len(work),
    )
    return result


def build_all_lifecycle_snapshots(
    events_df: pd.DataFrame,
    partitions: list[Partition],
) -> pd.DataFrame:
    """Build lifecycle snapshots for every discovered partition."""
    frames: list[pd.DataFrame] = []
    for part in partitions:
        snap = replay_lifecycle(events_df, part)
        if not snap.empty:
            frames.append(snap)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)
