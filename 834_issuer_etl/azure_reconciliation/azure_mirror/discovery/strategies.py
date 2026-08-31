"""Azure logic strategy runners (A–E) for discovery comparison."""

from __future__ import annotations

from typing import Any

import pandas as pd

from azure_reconciliation.azure_mirror.discovery.table_inspector import TableProfile
from azure_reconciliation.azure_mirror.query import filter_by_active_coverage
from azure_reconciliation.partition_discovery import Partition
from azure_reconciliation.status_mapper import normalize_insurance_type, normalize_status
from utils.logger import get_logger

logger = get_logger(__name__)

STRATEGY_META: dict[str, tuple[str, str, str]] = {
    "A": ("Active Coverage Snapshot", "snapshot", "benefit_effective_date / benefit_end_date"),
    "B": ("Enrollment Status Snapshot", "snapshot", "enrollment_status / enrollee_status"),
    "C": ("Event Date Logic", "event", "event/update/confirmation dates"),
    "D": ("834 Inbound Logic", "event", "834_Inbound_test action/event fields"),
    "E": ("CarrierInvoice Logic", "financial", "invoice_year / invoice_month"),
}


def _nunique(df: pd.DataFrame, col: str | None) -> int:
    if df.empty or not col or col not in df.columns:
        return 0
    return int(df[col].nunique(dropna=True))


def _status_series(df: pd.DataFrame, profile: TableProfile) -> pd.Series:
    if profile.status_col and profile.status_col in df.columns:
        return df[profile.status_col].astype(str).map(normalize_status)
    if profile.action_col and profile.action_col in df.columns:
        return df[profile.action_col].astype(str).map(normalize_status)
    return pd.Series(["UNKNOWN"] * len(df), index=df.index)


def _insurance_series(df: pd.DataFrame, profile: TableProfile) -> pd.Series:
    if profile.insurance_type_col and profile.insurance_type_col in df.columns:
        return df[profile.insurance_type_col].map(normalize_insurance_type)
    return pd.Series(["HEALTH"] * len(df), index=df.index)


def _subscriber_count(df: pd.DataFrame, profile: TableProfile) -> int | None:
    if profile.subscriber_col and profile.subscriber_col in df.columns:
        s = df[profile.subscriber_col].astype(str).str.upper()
        if (s == "Y").any() or s.str.contains("SUBSCR", na=False).any():
            return int(((s == "Y") | s.str.contains("SUBSCR", na=False)).sum())
    return None


def _build_summary_rows(
    *,
    strategy_id: str,
    issuer: str,
    year: str,
    month: str,
    df: pd.DataFrame,
    profile: TableProfile,
    source_date_column: str,
    source_status_column: str,
    logic_type: str,
    notes: str = "",
) -> list[dict[str, Any]]:
    name = STRATEGY_META[strategy_id][0]
    if df.empty:
        return [{
            "strategy_id": strategy_id,
            "strategy_name": name,
            "issuer": issuer,
            "year": year,
            "month": str(month).zfill(2),
            "insurance_type": "(all)",
            "status": "(all)",
            "raw_rows": 0,
            "enrollment_count": 0,
            "enrollee_count": 0,
            "subscriber_count": None,
            "source_table": profile.full_name,
            "source_date_column": source_date_column,
            "source_status_column": source_status_column,
            "logic_type": logic_type,
            "notes": notes,
        }]

    tmp = df.copy()
    tmp["_ins"] = _insurance_series(df, profile).values
    tmp["_status"] = _status_series(df, profile).values
    rows: list[dict[str, Any]] = []
    for (ins_type, st), grp in tmp.groupby(["_ins", "_status"], dropna=False):
        rows.append({
            "strategy_id": strategy_id,
            "strategy_name": name,
            "issuer": issuer,
            "year": year,
            "month": str(month).zfill(2),
            "insurance_type": ins_type,
            "status": st,
            "raw_rows": len(grp),
            "enrollment_count": _nunique(grp, profile.policy_col),
            "enrollee_count": _nunique(grp, profile.member_col),
            "subscriber_count": _subscriber_count(grp, profile),
            "source_table": profile.full_name,
            "source_date_column": source_date_column,
            "source_status_column": source_status_column,
            "logic_type": logic_type,
            "notes": notes,
        })
    return rows


def _to_df(rows: list[dict[str, Any]]) -> pd.DataFrame:
    return pd.DataFrame(rows) if rows else pd.DataFrame()


def run_strategy_a(
    df: pd.DataFrame, profile: TableProfile, partitions: list[Partition]
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for part in partitions:
        filtered, _ = filter_by_active_coverage(df, year=part.year, month=part.month)
        rows.extend(_build_summary_rows(
            strategy_id="A", issuer=part.issuer, year=part.year, month=part.month,
            df=filtered, profile=profile,
            source_date_column="benefit_effective_date / benefit_end_date",
            source_status_column=profile.status_col or "",
            logic_type=STRATEGY_META["A"][1],
            notes="Active population — counts may repeat across months",
        ))
    out = _to_df(rows)
    logger.info("Strategy A summary rows: %d", len(out))
    return out


def run_strategy_b(
    df: pd.DataFrame, profile: TableProfile, partitions: list[Partition]
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for part in partitions:
        rows.extend(_build_summary_rows(
            strategy_id="B", issuer=part.issuer, year=part.year, month=part.month,
            df=df, profile=profile,
            source_date_column="(none — snapshot)",
            source_status_column=profile.status_col or "",
            logic_type=STRATEGY_META["B"][1],
            notes="Full issuer/year snapshot repeated per source_data month",
        ))
    return _to_df(rows)


def run_strategy_c(
    df: pd.DataFrame, profile: TableProfile, partitions: list[Partition]
) -> pd.DataFrame:
    date_cols = list(profile.event_date_cols)
    if profile.maint_date_col and profile.maint_date_col not in date_cols:
        date_cols.insert(0, profile.maint_date_col)
    if not date_cols:
        return pd.DataFrame()

    best_rows: list[dict[str, Any]] = []
    best_col = ""
    best_score = -1

    for date_col in date_cols:
        if date_col not in df.columns:
            continue
        dates = pd.to_datetime(df[date_col], errors="coerce")
        rows: list[dict[str, Any]] = []
        for part in partitions:
            m = int(str(part.month).lstrip("0") or "0") or int(part.month)
            mask = dates.dt.month == m
            rows.extend(_build_summary_rows(
                strategy_id="C", issuer=part.issuer, year=part.year, month=part.month,
                df=df.loc[mask], profile=profile,
                source_date_column=date_col,
                source_status_column=profile.status_col or profile.action_col or "",
                logic_type=STRATEGY_META["C"][1],
                notes=f"MONTH({date_col}) = partition month",
            ))
        score = sum(r["enrollee_count"] for r in rows)
        if score > best_score:
            best_score = score
            best_rows = rows
            best_col = date_col

    out = _to_df(best_rows)
    if not out.empty:
        out["source_date_column"] = best_col
    logger.info("Strategy C best_date_col=%s rows=%d", best_col, len(out))
    return out


def run_strategy_d(
    df: pd.DataFrame, profile: TableProfile, partitions: list[Partition]
) -> pd.DataFrame:
    if "834_Inbound" not in profile.table:
        return pd.DataFrame()

    date_col = profile.maint_date_col or profile.file_date_col
    if not date_col or date_col not in df.columns:
        return pd.DataFrame()

    status_profile = profile
    if profile.action_col:
        status_profile = TableProfile(
            schema=profile.schema, table=profile.table, columns=profile.columns,
            issuer_col=profile.issuer_col, year_col=profile.year_col,
            policy_col=profile.policy_col, member_col=profile.member_col,
            insurance_type_col=profile.insurance_type_col,
            status_col=profile.action_col, action_col=profile.action_col,
        )

    dates = pd.to_datetime(df[date_col], errors="coerce")
    rows: list[dict[str, Any]] = []
    for part in partitions:
        m = int(str(part.month).lstrip("0") or "0") or int(part.month)
        rows.extend(_build_summary_rows(
            strategy_id="D", issuer=part.issuer, year=part.year, month=part.month,
            df=df.loc[dates.dt.month == m], profile=status_profile,
            source_date_column=date_col,
            source_status_column=profile.action_col or profile.status_col or "",
            logic_type=STRATEGY_META["D"][1],
            notes="834 inbound — actionCode/event fields; closest to XML transactions",
        ))
    return _to_df(rows)


def run_strategy_e(
    df: pd.DataFrame, profile: TableProfile, partitions: list[Partition]
) -> pd.DataFrame:
    if "CarrierInvoice" not in profile.table:
        return pd.DataFrame()

    if not profile.month_col or profile.month_col not in df.columns:
        return pd.DataFrame()

    rows: list[dict[str, Any]] = []
    col = df[profile.month_col]
    for part in partitions:
        m_int = int(str(part.month).lstrip("0") or "0") or int(part.month)
        if pd.api.types.is_numeric_dtype(col):
            mask = col.astype(int) == m_int
        else:
            mask = col.astype(str).str.zfill(2) == str(part.month).zfill(2)
        rows.extend(_build_summary_rows(
            strategy_id="E", issuer=part.issuer, year=part.year, month=part.month,
            df=df.loc[mask], profile=profile,
            source_date_column=profile.month_col,
            source_status_column=profile.status_col or "enrollment_event",
            logic_type=STRATEGY_META["E"][1],
            notes="CarrierInvoice invoice_month grouping",
        ))
    return _to_df(rows)


def run_applicable_strategies(
    df: pd.DataFrame,
    profile: TableProfile,
    partitions: list[Partition],
) -> dict[str, pd.DataFrame]:
    out: dict[str, pd.DataFrame] = {}
    if profile.benefit_effective_col and profile.benefit_end_col:
        out["A"] = run_strategy_a(df, profile, partitions)
    out["B"] = run_strategy_b(df, profile, partitions)
    if profile.event_date_cols or profile.maint_date_col:
        out["C"] = run_strategy_c(df, profile, partitions)
    if "834_Inbound" in profile.table:
        out["D"] = run_strategy_d(df, profile, partitions)
    if "CarrierInvoice" in profile.table:
        out["E"] = run_strategy_e(df, profile, partitions)
    return out
