"""
Three-month rolling window business rule validation — read-only investigation.

Compares multiple interpretations of Chandra's possible 3-month enrollment window:
  - forward-only (current + next 2 months)
  - enrollment-start anchored (start month + 2 following)
  - current-month centered (previous + current + next)
  - special enrollment cancellation (cancel/term within 3 months of effective date)

Does NOT change any pipeline logic, counts, or reports.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from azure_reconciliation.chandra_business_format import (
    STATUS_ID_MAP,
    gaa_load_date,
    insurance_type_display,
)
from azure_reconciliation.dashboard_difference_analysis import (
    _build_latest_state_keys,
    _build_reference_status_map,
    _classify_extra_row,
    _current_counts_by_display,
    _enrollment_detail_rows,
    _enrollment_id_series,
    _expected_from_reference,
    _identify_extra_enrollment_ids,
    _to_display_status,
)
from azure_reconciliation.lifecycle_snapshot_comparison import _sort_chronological
from azure_reconciliation.partition_discovery import Partition, discover_partitions
from azure_reconciliation.record_comparison import LIFECYCLE_PRIMARY_JOIN, join_key_series
from azure_reconciliation.safe_export import safe_write_excel, safe_write_html_report
from azure_reconciliation.status_mapper import normalize_insurance_type
from azure_reconciliation.xml_business_reports import process_issuer_xml_business
from azure_reconciliation.xml_loader import load_xml_rows
from config.config import settings
from utils.logger import get_logger

logger = get_logger(__name__)

DISPLAY_STATUSES = ("CONFIRM", "CANCEL", "TERM")
CANCEL_STATUSES = frozenset({"CANCEL"})
TERM_STATUSES = frozenset({"TERM"})

FORWARD_COLUMNS = [
    "Issuer", "Enrollment_ID", "Policy_ID", "Member_ID", "Insurance_Type",
    "Current_Month", "Current_Status", "Next_Month_Status", "Following_Month_Status",
    "Cancelled_Within_3_Months", "Terminated_Within_3_Months",
    "Potential_Three_Month_Cleanup", "Reason", "Source_XML_File",
]

ANCHORED_COLUMNS = [
    "Issuer", "Enrollment_ID", "Policy_ID", "Member_ID", "Insurance_Type",
    "Analysis_Month", "Analysis_Month_Status",
    "Enrollment_Start_Month", "Window_Month_1", "Window_Month_2", "Window_Month_3",
    "Analysis_Month_In_Window", "Cancel_Or_Term_Within_Window",
    "Cancel_Or_Term_Month", "Cancel_Or_Term_Status",
    "Potential_Enrollment_Anchored_Cleanup", "Reason", "Source_XML_File",
]

CENTERED_COLUMNS = [
    "Issuer", "Enrollment_ID", "Policy_ID", "Member_ID", "Insurance_Type",
    "Previous_Month", "Current_Month", "Next_Month",
    "Previous_Month_Status", "Current_Month_Status", "Next_Month_Status",
    "Cancelled_In_Centered_Window", "Terminated_In_Centered_Window",
    "Potential_Centered_Window_Cleanup", "Reason", "Source_XML_File",
]

SPECIAL_CANCEL_COLUMNS = [
    "Issuer", "Enrollment_ID", "Policy_ID", "Member_ID", "Insurance_Type",
    "Enrollment_Start_Month", "Enrollment_Effective_Date",
    "Cancel_Or_Term_Month", "Cancel_Or_Term_Status",
    "Months_From_Enrollment_Start", "Within_3_Month_Window",
    "Analysis_Month_Status", "Special_Enrollment_Cancel_Candidate",
    "Reason", "Source_XML_File",
]

UNEXPLAINED_COLUMNS = [
    "Issuer", "Enrollment_ID", "Policy_ID", "Member_ID", "Insurance_Type",
    "Current_Month", "Current_Status", "Gap_Status", "Unexplained_Category",
    "Detail", "Source_XML_File",
]

RULE_DEFINITIONS = (
    ("Forward_Three_Month", "Current month + next 2 months (original forward-only)"),
    ("Enrollment_Anchored_Window", "Enrollment-start month + following 2 months"),
    ("Current_Month_Centered", "Previous month + current month + next month"),
    ("Special_Enrollment_Cancel", "Cancel/term within 3 months of enrollment/effective date"),
)

# Debug-only defaults when no CLI/reference — not used in production aggregation.
_DEBUG_DEFAULT_EXPECTED: dict[tuple[str, str, str], dict[str, int]] = {
    ("15105", "2026", "01"): {
        "CONFIRM": 1240,
        "CANCEL": 47,
        "TERM": 217,
    },
}


def _resolve_expected_counts(
    issuer: str,
    year: str,
    month: str,
    *,
    expected_counts: dict[str, int] | None = None,
    cli_confirm: int | None = None,
    cli_cancel: int | None = None,
    cli_term: int | None = None,
) -> dict[str, int] | None:
    zm = _zmonth(month)
    if expected_counts:
        return expected_counts

    reference = _expected_from_reference(issuer, year, zm)
    debug_default = _DEBUG_DEFAULT_EXPECTED.get((str(issuer), str(year), zm))

    if cli_confirm is not None or cli_cancel is not None or cli_term is not None:
        base = reference or debug_default or {}
        return {
            "CONFIRM": cli_confirm if cli_confirm is not None else int(base.get("CONFIRM", 0)),
            "CANCEL": cli_cancel if cli_cancel is not None else int(base.get("CANCEL", 0)),
            "TERM": cli_term if cli_term is not None else int(base.get("TERM", 0)),
        }

    if reference:
        return reference
    return debug_default


def _debug_dir() -> Path:
    d = settings.outputs_path / "debug"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _zmonth(m: str) -> str:
    return str(m).strip().zfill(2)


def _ym_key(year: str, month: str) -> str:
    return f"{year}-{_zmonth(month)}"


def _parse_ym(date_str: str) -> tuple[str, str] | None:
    s = str(date_str or "").strip()
    if len(s) < 7:
        return None
    parts = s[:10].replace("/", "-").split("-")
    if len(parts) >= 2 and parts[0].isdigit() and parts[1].isdigit():
        return parts[0], _zmonth(parts[1])
    return None


def _shift_month(year: str, month: str, offset: int) -> tuple[str, str]:
    y, m = int(year), int(_zmonth(month))
    m += offset
    while m > 12:
        m -= 12
        y += 1
    while m < 1:
        m += 12
        y -= 1
    return str(y), _zmonth(str(m))


def _window_from_start(start_year: str, start_month: str) -> list[tuple[str, str]]:
    y, m = start_year, _zmonth(start_month)
    return [
        (y, m),
        _shift_month(y, m, 1),
        _shift_month(y, m, 2),
    ]


def _month_index(year: str, month: str) -> int:
    return int(year) * 12 + int(_zmonth(month))


def _months_between(start: tuple[str, str], end: tuple[str, str]) -> int:
    return _month_index(end[0], end[1]) - _month_index(start[0], start[1])


def _source_file(row: pd.Series) -> str:
    for col in ("source_file", "file_name", "raw_xml_path"):
        if col in row.index and str(row.get(col, "") or "").strip():
            return str(row[col]).strip()
    return ""


def _entity_key_series(df: pd.DataFrame) -> pd.Series:
    keys = [k for k in LIFECYCLE_PRIMARY_JOIN if k in df.columns]
    if not keys:
        return pd.Series([""] * len(df), index=df.index, dtype=str)
    return join_key_series(df, keys)


def _build_entity_month_index(lifecycle_input: pd.DataFrame) -> pd.DataFrame:
    """Latest display status per primary lifecycle key per business month."""
    if lifecycle_input.empty:
        return pd.DataFrame()

    work = lifecycle_input.copy()
    work["_display_status"] = work.get("status", "UNKNOWN").astype(str).map(_to_display_status)
    work["_entity_key"] = _entity_key_series(work)
    work["_enrollment_id"] = _enrollment_id_series(work)
    work["year"] = work["year"].astype(str)
    work["month"] = work["month"].astype(str).map(_zmonth)

    group_cols = ["_entity_key", "year", "month"]
    sorted_work = _sort_chronological(work)
    latest = sorted_work.groupby(group_cols, dropna=False, as_index=False).last()

    pick_cols = [
        "issuer", "policy_id", "member_id", "insurance_type",
        "_enrollment_id", "_display_status", "year", "month", "_entity_key",
        "benefit_effective_date", "member_maint_effective_date",
    ]
    for col in ("source_file", "file_name", "raw_xml_path"):
        if col in latest.columns:
            pick_cols.append(col)

    return latest[[c for c in pick_cols if c in latest.columns]].rename(
        columns={"_display_status": "status", "_enrollment_id": "enrollment_id"},
    )


def _build_enrollment_start_map(
    entity_index: pd.DataFrame,
    canonical: pd.DataFrame,
) -> dict[str, dict[str, Any]]:
    """
    Per entity_key: enrollment start month from earliest CONFIRM business month
    or earliest benefit/maint effective date (read-only diagnostic).
    """
    starts: dict[str, dict[str, Any]] = {}

    if not entity_index.empty:
        for ek, grp in entity_index.groupby("_entity_key", dropna=False):
            ek = str(ek)
            confirm = grp[grp["status"].astype(str) == "CONFIRM"].copy()
            if not confirm.empty:
                confirm = confirm.sort_values(["year", "month"])
                row = confirm.iloc[0]
                starts[ek] = {
                    "year": str(row["year"]),
                    "month": _zmonth(str(row["month"])),
                    "source": "earliest_confirm_business_month",
                    "effective_date": "",
                }
            else:
                grp = grp.sort_values(["year", "month"])
                row = grp.iloc[0]
                starts[ek] = {
                    "year": str(row["year"]),
                    "month": _zmonth(str(row["month"])),
                    "source": "earliest_entity_month",
                    "effective_date": "",
                }

    if not canonical.empty:
        canon = canonical.copy()
        canon["_entity_key"] = _entity_key_series(canon)
        for ek, grp in canon.groupby("_entity_key", dropna=False):
            ek = str(ek)
            dates: list[tuple[str, str, str]] = []
            for _, row in grp.iterrows():
                for col in ("benefit_effective_date", "member_maint_effective_date"):
                    parsed = _parse_ym(str(row.get(col, "") or ""))
                    if parsed:
                        dates.append((parsed[0], parsed[1], str(row.get(col, ""))))
            if not dates:
                continue
            dates.sort(key=lambda x: _month_index(x[0], x[1]))
            dy, dm, raw = dates[0]
            existing = starts.get(ek)
            if existing is None or _month_index(dy, dm) < _month_index(existing["year"], existing["month"]):
                starts[ek] = {
                    "year": dy,
                    "month": dm,
                    "source": "benefit_or_maint_effective_date",
                    "effective_date": raw,
                }
            elif existing and not existing.get("effective_date"):
                existing["effective_date"] = raw

    return starts


def _status_lookup(
    index: pd.DataFrame,
    entity_key: str,
    year: str,
    month: str,
) -> str:
    if index.empty:
        return ""
    match = index[
        (index["_entity_key"].astype(str) == entity_key)
        & (index["year"].astype(str) == str(year))
        & (index["month"].astype(str).map(_zmonth) == _zmonth(month))
    ]
    if match.empty:
        return ""
    return str(match.iloc[-1]["status"])


def _cancel_term_in_months(
    index: pd.DataFrame,
    entity_key: str,
    months: list[tuple[str, str]],
) -> tuple[bool, str, str]:
    """Return (found, month_ym, status)."""
    for y, m in months:
        st = _status_lookup(index, entity_key, y, m)
        if st in CANCEL_STATUSES:
            return True, _ym_key(y, m), "CANCEL"
        if st in TERM_STATUSES:
            return True, _ym_key(y, m), "TERM"
    return False, "", ""


def _entity_row_fields(row: pd.Series, issuer: str) -> dict[str, str]:
    return {
        "Issuer": str(row.get("issuer", issuer)),
        "Enrollment_ID": str(row.get("enrollment_id", row.get("policy_id", ""))),
        "Policy_ID": str(row.get("policy_id", "")),
        "Member_ID": str(row.get("member_id", "")),
        "Insurance_Type": str(row.get("insurance_type", "")).upper(),
        "Source_XML_File": _source_file(row),
        "_entity_key": str(row["_entity_key"]),
    }


def _build_forward_window_rows(
    entity_index: pd.DataFrame,
    *,
    issuer: str,
    year: str,
    month: str,
) -> pd.DataFrame:
    zm = _zmonth(month)
    current = entity_index[
        (entity_index.get("issuer", pd.Series(dtype=str)).astype(str) == str(issuer))
        & (entity_index["year"].astype(str) == str(year))
        & (entity_index["month"].astype(str).map(_zmonth) == zm)
    ].copy()
    if current.empty:
        return pd.DataFrame(columns=FORWARD_COLUMNS)

    ny, nm = _shift_month(year, zm, 1)
    fy, fm = _shift_month(year, zm, 2)
    rows: list[dict[str, Any]] = []
    for _, row in current.iterrows():
        ek = str(row["_entity_key"])
        cur_status = str(row.get("status", ""))
        next_status = _status_lookup(entity_index, ek, ny, nm)
        follow_status = _status_lookup(entity_index, ek, fy, fm)
        cancelled = next_status in CANCEL_STATUSES or follow_status in CANCEL_STATUSES
        terminated = next_status in TERM_STATUSES or follow_status in TERM_STATUSES
        potential = cur_status == "CONFIRM" and (cancelled or terminated)
        reasons: list[str] = []
        if potential:
            reasons.append(
                "CONFIRM in current month; CANCEL/TERM within next 2 months (forward-only window)"
            )
        rows.append({
            **_entity_row_fields(row, issuer),
            "Current_Month": f"{year}-{zm}",
            "Current_Status": cur_status,
            "Next_Month_Status": next_status or "(no record)",
            "Following_Month_Status": follow_status or "(no record)",
            "Cancelled_Within_3_Months": cancelled,
            "Terminated_Within_3_Months": terminated,
            "Potential_Three_Month_Cleanup": potential,
            "Reason": "; ".join(reasons),
        })
    return pd.DataFrame(rows)


def _build_enrollment_anchored_rows(
    entity_index: pd.DataFrame,
    enrollment_starts: dict[str, dict[str, Any]],
    *,
    issuer: str,
    year: str,
    month: str,
) -> pd.DataFrame:
    zm = _zmonth(month)
    analysis_ym = (year, zm)
    current = entity_index[
        (entity_index.get("issuer", pd.Series(dtype=str)).astype(str) == str(issuer))
        & (entity_index["year"].astype(str) == str(year))
        & (entity_index["month"].astype(str).map(_zmonth) == zm)
    ].copy()
    if current.empty:
        return pd.DataFrame(columns=ANCHORED_COLUMNS)

    rows: list[dict[str, Any]] = []
    for _, row in current.iterrows():
        ek = str(row["_entity_key"])
        cur_status = str(row.get("status", ""))
        start = enrollment_starts.get(ek, {})
        sy, sm = str(start.get("year", year)), _zmonth(str(start.get("month", zm)))
        window = _window_from_start(sy, sm)
        w_keys = [_ym_key(y, m) for y, m in window]
        in_window = analysis_ym in window
        found, cot_month, cot_status = _cancel_term_in_months(entity_index, ek, window)

        potential = (
            in_window
            and cur_status == "CONFIRM"
            and found
        )
        reasons: list[str] = []
        if potential:
            reasons.append(
                f"CONFIRM in {year}-{zm}; enrollment started {sy}-{sm}; "
                f"{cot_status} in window ({', '.join(w_keys)})"
            )
        elif in_window and found:
            reasons.append(
                f"Analysis month in enrollment window; {cot_status} at {cot_month}"
            )

        rows.append({
            **_entity_row_fields(row, issuer),
            "Analysis_Month": f"{year}-{zm}",
            "Analysis_Month_Status": cur_status,
            "Enrollment_Start_Month": _ym_key(sy, sm),
            "Window_Month_1": w_keys[0],
            "Window_Month_2": w_keys[1],
            "Window_Month_3": w_keys[2],
            "Analysis_Month_In_Window": in_window,
            "Cancel_Or_Term_Within_Window": found,
            "Cancel_Or_Term_Month": cot_month,
            "Cancel_Or_Term_Status": cot_status,
            "Potential_Enrollment_Anchored_Cleanup": potential,
            "Reason": "; ".join(reasons),
        })
    return pd.DataFrame(rows)


def _build_centered_window_rows(
    entity_index: pd.DataFrame,
    *,
    issuer: str,
    year: str,
    month: str,
) -> pd.DataFrame:
    zm = _zmonth(month)
    current = entity_index[
        (entity_index.get("issuer", pd.Series(dtype=str)).astype(str) == str(issuer))
        & (entity_index["year"].astype(str) == str(year))
        & (entity_index["month"].astype(str).map(_zmonth) == zm)
    ].copy()
    if current.empty:
        return pd.DataFrame(columns=CENTERED_COLUMNS)

    py, pm = _shift_month(year, zm, -1)
    ny, nm = _shift_month(year, zm, 1)
    rows: list[dict[str, Any]] = []
    for _, row in current.iterrows():
        ek = str(row["_entity_key"])
        cur_status = str(row.get("status", ""))
        prev_status = _status_lookup(entity_index, ek, py, pm)
        next_status = _status_lookup(entity_index, ek, ny, nm)
        cancelled = prev_status in CANCEL_STATUSES or next_status in CANCEL_STATUSES
        terminated = prev_status in TERM_STATUSES or next_status in TERM_STATUSES
        potential = cur_status == "CONFIRM" and (cancelled or terminated)
        reasons: list[str] = []
        if potential:
            parts = []
            if prev_status in CANCEL_STATUSES | TERM_STATUSES:
                parts.append(f"prior month {py}-{pm}={prev_status}")
            if next_status in CANCEL_STATUSES | TERM_STATUSES:
                parts.append(f"next month {ny}-{nm}={next_status}")
            reasons.append(
                f"CONFIRM in {year}-{zm}; bidirectional window: " + "; ".join(parts)
            )

        rows.append({
            **_entity_row_fields(row, issuer),
            "Previous_Month": f"{py}-{pm}",
            "Current_Month": f"{year}-{zm}",
            "Next_Month": f"{ny}-{nm}",
            "Previous_Month_Status": prev_status or "(no record)",
            "Current_Month_Status": cur_status,
            "Next_Month_Status": next_status or "(no record)",
            "Cancelled_In_Centered_Window": cancelled,
            "Terminated_In_Centered_Window": terminated,
            "Potential_Centered_Window_Cleanup": potential,
            "Reason": "; ".join(reasons),
        })
    return pd.DataFrame(rows)


def _build_special_cancel_candidates(
    entity_index: pd.DataFrame,
    enrollment_starts: dict[str, dict[str, Any]],
    *,
    issuer: str,
    year: str,
    month: str,
) -> pd.DataFrame:
    zm = _zmonth(month)
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()

    for ek, grp in entity_index.groupby("_entity_key", dropna=False):
        ek = str(ek)
        start = enrollment_starts.get(ek)
        if not start:
            continue
        sy, sm = str(start["year"]), _zmonth(str(start["month"]))
        window = _window_from_start(sy, sm)
        found, cot_month, cot_status = _cancel_term_in_months(entity_index, ek, window)
        if not found:
            continue

        cot_y, cot_m = cot_month.split("-", 1) if cot_month else ("", "")
        months_from = _months_between((sy, sm), (cot_y, cot_m)) if cot_month else 0
        within = 0 <= months_from <= 2

        analysis_row = grp[
            (grp.get("issuer", pd.Series(dtype=str)).astype(str) == str(issuer))
            & (grp["year"].astype(str) == str(year))
            & (grp["month"].astype(str).map(_zmonth) == zm)
        ]
        analysis_status = str(analysis_row.iloc[-1]["status"]) if not analysis_row.empty else ""
        rep = analysis_row.iloc[-1] if not analysis_row.empty else grp.iloc[-1]

        candidate = within and (
            analysis_status == "CONFIRM"
            or (analysis_status in CANCEL_STATUSES | TERM_STATUSES and cot_status == analysis_status)
        )
        key = f"{ek}|{cot_month}"
        if key in seen:
            continue
        seen.add(key)

        eff = str(start.get("effective_date", "") or "")
        if not eff:
            for col in ("benefit_effective_date", "member_maint_effective_date"):
                if col in rep.index and str(rep.get(col, "") or "").strip():
                    eff = str(rep[col]).strip()
                    break

        rows.append({
            **_entity_row_fields(rep, issuer),
            "Enrollment_Start_Month": _ym_key(sy, sm),
            "Enrollment_Effective_Date": eff,
            "Cancel_Or_Term_Month": cot_month,
            "Cancel_Or_Term_Status": cot_status,
            "Months_From_Enrollment_Start": months_from,
            "Within_3_Month_Window": within,
            "Analysis_Month_Status": analysis_status or "(not in analysis month)",
            "Special_Enrollment_Cancel_Candidate": candidate,
            "Reason": (
                f"{cot_status} at {cot_month} is {months_from} month(s) after enrollment start {sy}-{sm}"
                if within else f"{cot_status} outside 3-month window from enrollment start"
            ),
        })

    return pd.DataFrame(rows) if rows else pd.DataFrame(columns=SPECIAL_CANCEL_COLUMNS)


def _rule_flagged_by_status(
    rule_name: str,
    *,
    forward_df: pd.DataFrame,
    anchored_df: pd.DataFrame,
    centered_df: pd.DataFrame,
    special_df: pd.DataFrame,
    analysis_status_col: str,
) -> dict[str, set[str]]:
    """Enrollment IDs flagged as cleanup candidates per display status."""
    out: dict[str, set[str]] = {s: set() for s in DISPLAY_STATUSES}

    if rule_name == "Forward_Three_Month" and not forward_df.empty:
        flagged = forward_df[forward_df["Potential_Three_Month_Cleanup"]]
        for st in DISPLAY_STATUSES:
            out[st] = set(
                flagged.loc[flagged["Current_Status"] == st, "Enrollment_ID"].astype(str)
            ) - {""}

    elif rule_name == "Enrollment_Anchored_Window" and not anchored_df.empty:
        flagged = anchored_df[anchored_df["Potential_Enrollment_Anchored_Cleanup"]]
        for st in DISPLAY_STATUSES:
            out[st] = set(
                flagged.loc[flagged["Analysis_Month_Status"] == st, "Enrollment_ID"].astype(str)
            ) - {""}

    elif rule_name == "Current_Month_Centered" and not centered_df.empty:
        flagged = centered_df[centered_df["Potential_Centered_Window_Cleanup"]]
        for st in DISPLAY_STATUSES:
            out[st] = set(
                flagged.loc[flagged["Current_Month_Status"] == st, "Enrollment_ID"].astype(str)
            ) - {""}

    elif rule_name == "Special_Enrollment_Cancel" and not special_df.empty:
        flagged = special_df[special_df["Special_Enrollment_Cancel_Candidate"]]
        for st in DISPLAY_STATUSES:
            if st == "CONFIRM":
                out[st] = set(
                    flagged.loc[
                        flagged["Analysis_Month_Status"] == "CONFIRM", "Enrollment_ID",
                    ].astype(str)
                ) - {""}
            else:
                out[st] = set(
                    flagged.loc[
                        flagged["Cancel_Or_Term_Status"] == st, "Enrollment_ID",
                    ].astype(str)
                ) - {""}

    return out


def _confidence_level(pct: float) -> str:
    if pct >= 80:
        return "High"
    if pct >= 50:
        return "Medium"
    return "Low"


def _build_rule_comparison_summary(
    *,
    gap_extra_ids: dict[str, set[str]],
    gaps: dict[str, int],
    forward_df: pd.DataFrame,
    anchored_df: pd.DataFrame,
    centered_df: pd.DataFrame,
    special_df: pd.DataFrame,
) -> pd.DataFrame:
    total_gap = sum(max(gaps.get(s, 0), 0) for s in DISPLAY_STATUSES)
    rows: list[dict[str, Any]] = []

    for rule_name, interpretation in RULE_DEFINITIONS:
        flagged = _rule_flagged_by_status(
            rule_name,
            forward_df=forward_df,
            anchored_df=anchored_df,
            centered_df=centered_df,
            special_df=special_df,
            analysis_status_col="",
        )
        explained_by_status: dict[str, int] = {}
        affected_by_status: dict[str, int] = {}
        for st in DISPLAY_STATUSES:
            affected_by_status[st] = len(flagged[st])
            explained_by_status[st] = len(gap_extra_ids[st] & flagged[st])

        total_explained = sum(explained_by_status.values())
        pct = round(100.0 * total_explained / total_gap, 1) if total_gap > 0 else 0.0

        rows.append({
            "Rule_Name": rule_name,
            "Interpretation": interpretation,
            "Affected_CONFIRM_Records": affected_by_status["CONFIRM"],
            "Affected_CANCEL_Records": affected_by_status["CANCEL"],
            "Affected_TERM_Records": affected_by_status["TERM"],
            "Explained_CONFIRM_Gap": explained_by_status["CONFIRM"],
            "Explained_CANCEL_Gap": explained_by_status["CANCEL"],
            "Explained_TERM_Gap": explained_by_status["TERM"],
            "Explained_Difference_Count": total_explained,
            "Total_Positive_Gap": total_gap,
            "Pct_Gap_Explained": pct,
            "Confidence_Level": _confidence_level(pct) if total_gap > 0 else "N/A",
            "Notes": (
                f"Explains {explained_by_status['CONFIRM']}/{max(gaps.get('CONFIRM', 0), 0)} CONFIRM, "
                f"{explained_by_status['CANCEL']}/{max(gaps.get('CANCEL', 0), 0)} CANCEL, "
                f"{explained_by_status['TERM']}/{max(gaps.get('TERM', 0), 0)} TERM gap enrollments"
            ),
        })

    summary = pd.DataFrame(rows)
    if not summary.empty:
        summary = summary.sort_values(
            ["Pct_Gap_Explained", "Explained_Difference_Count"],
            ascending=[False, False],
        ).reset_index(drop=True)
    return summary


def _collect_gap_extra_ids(
    detail: pd.DataFrame,
    *,
    expected: dict[str, int],
    actual_counts: dict[str, int],
    reference_status: dict[str, str],
) -> tuple[dict[str, set[str]], dict[str, int]]:
    gap_extra_ids: dict[str, set[str]] = {}
    gaps: dict[str, int] = {}
    for st in DISPLAY_STATUSES:
        exp = int(expected.get(st, 0))
        act = int(actual_counts.get(st, 0))
        gaps[st] = act - exp
        gap_extra_ids[st] = _identify_extra_enrollment_ids(
            detail,
            display_status=st,
            expected_count=exp,
            reference_status=reference_status,
        )
    return gap_extra_ids, gaps


def _classify_unexplained(
    row: pd.Series,
    *,
    detail_row: pd.Series | None,
    business_month: str,
    is_rule_candidate: bool,
) -> tuple[str, str]:
    cat_detail = ""
    if is_rule_candidate:
        return "Business rule candidate", str(row.get("Reason", ""))
    if detail_row is not None and not detail_row.empty:
        bucket = _classify_extra_row(detail_row, business_month=business_month)
        mapping = {
            "duplicate transaction": ("Duplicate transaction", "Flagged as duplicate in cleanup diagnostics"),
            "maintenance-only": ("Maintenance-only", "Maintenance-only transaction retained in Model H input"),
            "superseded event": ("Superseded event", "Superseded by later transaction in same month"),
            "status transition issue": ("Latest state issue", "Lifecycle snapshot status differs from Model H input"),
            "latest state missing": ("Latest state issue", "No matching lifecycle snapshot for enrollment"),
            "older transaction kept instead of latest": ("Latest state issue", "Not the latest state row for entity/month"),
            "month assignment issue": ("Month assignment", "Business month basis differs from source folder month"),
            "effective date issue": ("Month assignment", "Benefit vs maintenance effective dates span months"),
            "unknown": ("Unknown", "No single dominant classification"),
        }
        cat, cat_detail = mapping.get(bucket, ("Unknown", bucket))
        return cat, cat_detail
    return "Unknown", "No supporting diagnostic row"


def _enrollee_counts_from_business_monthly(
    business_monthly: pd.DataFrame,
    *,
    issuer: str,
    year: str,
    month: str,
    insurance_type: str,
) -> dict[str, int]:
    counts: dict[str, int] = {s: 0 for s in DISPLAY_STATUSES}
    if business_monthly.empty:
        return counts
    zm = _zmonth(month)
    scoped = business_monthly[
        (business_monthly.get("issuer", pd.Series(dtype=str)).astype(str) == str(issuer))
        & (business_monthly["year"].astype(str) == str(year))
        & (business_monthly["month"].astype(str).map(_zmonth) == zm)
    ].copy()
    if scoped.empty:
        return counts
    if "insurance_type" in scoped.columns:
        scoped = scoped[
            scoped["insurance_type"].astype(str).map(normalize_insurance_type) == insurance_type
        ]
    for _, row in scoped.iterrows():
        disp = _to_display_status(str(row.get("status", "")))
        if disp in counts and "Enrollee_Count" in row.index:
            counts[disp] = int(row.get("Enrollee_Count", 0))
    return counts


def _expected_enrollee_counts(issuer: str, year: str, month: str) -> dict[str, int]:
    from reporting.enrollment_comparison import CHANDRA_REFERENCE_15105

    zm = _zmonth(month)
    load_date = gaa_load_date(year, zm)
    counts: dict[str, int] = {s: 0 for s in DISPLAY_STATUSES}
    for row in CHANDRA_REFERENCE_15105:
        if (
            str(row.get("GAA_HIOS_ID")) == str(issuer)
            and str(row.get("GAA_Load_Date")) == load_date
            and str(row.get("Insurance_Type", "")).lower() == "health"
        ):
            st = str(row.get("enrolleeStatus", "")).upper()
            if st in counts:
                counts[st] = int(row.get("Enrollee_Count", 0))
    return counts if any(counts.values()) else {}


def _build_monthly_summary(
    *,
    issuer: str,
    year: str,
    month: str,
    insurance_type: str,
    status_summaries: dict[str, dict[str, Any]],
    best_rule: dict[str, Any],
    business_monthly: pd.DataFrame,
) -> pd.DataFrame:
    zm = _zmonth(month)
    best_name = str(best_rule.get("Rule_Name", ""))
    best_conf = str(best_rule.get("Confidence_Level", "N/A"))
    enrollee_actual = _enrollee_counts_from_business_monthly(
        business_monthly, issuer=issuer, year=year, month=zm, insurance_type=insurance_type,
    )
    enrollee_expected = _expected_enrollee_counts(issuer, year, zm)

    rows: list[dict[str, Any]] = []
    for st in DISPLAY_STATUSES:
        s = status_summaries.get(st, {})
        row: dict[str, Any] = {
            "Coverage_Year": str(year),
            "GAA_HIOS_ID": str(issuer),
            "GAA_Load_Date": gaa_load_date(year, zm),
            "Insurance_Type": insurance_type_display(insurance_type),
            "status_Id": STATUS_ID_MAP.get(st, 0),
            "enrolleeStatus": st,
            "Current_XML_Count": int(s.get("actual", 0)),
            "Chandra_Expected_Count": int(s.get("expected", 0)),
            "Difference": int(s.get("delta", 0)),
            "Best_Rule_Explained_Count": int(s.get("best_rule_explained", 0)),
            "Remaining_Difference": int(s.get("remaining_unexplained", 0)),
            "Best_Rule_Name": best_name,
            "Confidence_Level": best_conf,
        }
        if enrollee_actual.get(st, 0) or enrollee_expected.get(st, 0):
            row["Enrollee_Count"] = enrollee_actual.get(st, 0)
            row["Chandra_Expected_Enrollee_Count"] = enrollee_expected.get(st, 0)
        rows.append(row)
    return pd.DataFrame(rows)


def _write_monthly_summary_outputs(
    monthly_df: pd.DataFrame,
    *,
    issuer: str,
    year: str,
    month: str,
) -> tuple[Path, Path]:
    xlsx_path = _debug_dir() / "three_month_monthly_summary.xlsx"
    html_path = _debug_dir() / "three_month_monthly_summary.html"
    safe_write_excel(xlsx_path, {"Monthly_Summary": monthly_df}, drop_duplicate_value_columns=False)
    safe_write_html_report(
        html_path,
        title=f"Three-Month Monthly Summary — {issuer} / {year} / {month}",
        summary_df=monthly_df,
    )
    return xlsx_path, html_path


def _best_rule_verdict(rule_comparison: pd.DataFrame, *, has_reference: bool, total_gap: int) -> str:
    if not has_reference:
        return "Chandra reference not configured — rule comparison requires expected counts."
    if total_gap <= 0:
        return "No positive dashboard gap — rule comparison not testable for this month."
    if rule_comparison.empty:
        return "No rule candidates evaluated."
    best = rule_comparison.iloc[0]
    pct = float(best.get("Pct_Gap_Explained", 0))
    name = str(best.get("Rule_Name", ""))
    if pct >= 80:
        return (
            f"'{name}' is likely part of Chandra's dashboard calculation "
            f"({pct:.1f}% of total gap explained)."
        )
    return (
        f"No single rule explains ≥80% of the gap. Best candidate: '{name}' "
        f"at {pct:.1f}% — hypothesis NOT sufficient alone."
    )


def run_three_month_business_rule_validation(
    *,
    issuer: str = "15105",
    year: str = "2026",
    month: str = "01",
    insurance_type: str = "HEALTH",
    parse_source: bool = False,
    expected_counts: dict[str, int] | None = None,
    cli_confirm: int | None = None,
    cli_cancel: int | None = None,
    cli_term: int | None = None,
) -> dict[str, Any]:
    """Read-only multi-rule three-month window investigation vs Chandra gaps."""
    settings.ensure_dirs()
    zm = _zmonth(month)
    expected = _resolve_expected_counts(
        issuer, year, zm,
        expected_counts=expected_counts,
        cli_confirm=cli_confirm,
        cli_cancel=cli_cancel,
        cli_term=cli_term,
    )
    has_reference = expected is not None
    if not expected:
        expected = {s: 0 for s in DISPLAY_STATUSES}
        logger.warning(
            "No Chandra reference for %s/%s/%s — windows still generated; gap comparison unavailable",
            issuer, year, zm,
        )

    partitions = discover_partitions(settings.source_data_path, issuer_filter=issuer)
    xml_raw = load_xml_rows(prefer_staging=not parse_source, issuer_filter=issuer)
    if xml_raw.empty:
        raise RuntimeError(f"No XML rows for issuer {issuer}")
    if not partitions:
        partitions = [Partition(issuer=issuer, year=year, month=zm)]

    result = process_issuer_xml_business(issuer, xml_raw, partitions)

    entity_index = _build_entity_month_index(result.lifecycle_input)
    if not entity_index.empty and "insurance_type" in entity_index.columns:
        entity_index = entity_index[
            entity_index["insurance_type"].astype(str).map(normalize_insurance_type) == insurance_type
        ].copy()

    enrollment_starts = _build_enrollment_start_map(entity_index, result.canonical)

    forward_df = _build_forward_window_rows(entity_index, issuer=issuer, year=year, month=zm)
    anchored_df = _build_enrollment_anchored_rows(
        entity_index, enrollment_starts, issuer=issuer, year=year, month=zm,
    )
    centered_df = _build_centered_window_rows(entity_index, issuer=issuer, year=year, month=zm)
    special_df = _build_special_cancel_candidates(
        entity_index, enrollment_starts, issuer=issuer, year=year, month=zm,
    )

    reference_status = _build_reference_status_map(
        result.lifecycle_snapshots, issuer=issuer, year=year, month=zm,
    )
    latest_state_keys = _build_latest_state_keys(
        result.canonical, issuer=issuer, year=year, month=zm,
    )
    detail = _enrollment_detail_rows(
        result.lifecycle_input,
        result.canonical,
        issuer=issuer,
        year=year,
        month=zm,
        insurance_type=insurance_type,
        duplicate_df=result.duplicate_df,
        maintenance_df=result.maintenance_df,
        superseded_df=result.superseded_df,
        reference_status=reference_status,
        latest_state_keys=latest_state_keys,
    )

    actual_counts = _current_counts_by_display(
        result.lifecycle_input,
        issuer=issuer, year=year, month=zm, insurance_type=insurance_type,
    )
    gap_extra_ids, gaps = _collect_gap_extra_ids(
        detail, expected=expected, actual_counts=actual_counts, reference_status=reference_status,
    )

    rule_comparison = _build_rule_comparison_summary(
        gap_extra_ids=gap_extra_ids,
        gaps=gaps,
        forward_df=forward_df,
        anchored_df=anchored_df,
        centered_df=centered_df,
        special_df=special_df,
    )

    best_rule = (
        rule_comparison.iloc[0].to_dict() if not rule_comparison.empty else {}
    )
    total_gap = sum(max(gaps.get(s, 0), 0) for s in DISPLAY_STATUSES)

    # Unexplained: gap enrollments not explained by best rule
    best_rule_name = str(best_rule.get("Rule_Name", "Forward_Three_Month"))
    best_flagged = _rule_flagged_by_status(
        best_rule_name,
        forward_df=forward_df,
        anchored_df=anchored_df,
        centered_df=centered_df,
        special_df=special_df,
        analysis_status_col="",
    )
    all_explained: set[str] = set()
    for st in DISPLAY_STATUSES:
        all_explained |= gap_extra_ids[st] & best_flagged[st]

    unexplained_frames: list[dict[str, Any]] = []
    all_bucket_counts: dict[str, int] = {}
    status_summaries: dict[str, dict[str, Any]] = {}

    for display_status in DISPLAY_STATUSES:
        exp = int(expected.get(display_status, 0))
        act = int(actual_counts.get(display_status, 0))
        delta = gaps[display_status]
        extra_ids = gap_extra_ids[display_status]
        explained_ids = extra_ids & best_flagged[display_status]
        remaining_ids = extra_ids - explained_ids

        status_summaries[display_status] = {
            "expected": exp,
            "actual": act,
            "delta": delta,
            "best_rule_explained": len(explained_ids),
            "remaining_unexplained": len(remaining_ids) if delta > 0 else 0,
            "pct_explained": round(100.0 * len(explained_ids) / delta, 1) if delta > 0 else 100.0,
        }

        if delta <= 0:
            continue

        for eid in sorted(remaining_ids):
            det = detail[detail["enrollment_id"].astype(str) == eid]
            det_row = det.iloc[0] if not det.empty else None
            is_candidate = eid in all_explained
            cat, cat_detail = _classify_unexplained(
                pd.Series(dtype=object),
                detail_row=det_row,
                business_month=zm,
                is_rule_candidate=is_candidate,
            )
            all_bucket_counts[cat] = all_bucket_counts.get(cat, 0) + 1
            unexplained_frames.append({
                "Issuer": issuer,
                "Enrollment_ID": eid,
                "Policy_ID": str(det_row["policy_id"]) if det_row is not None else "",
                "Member_ID": str(det_row["member_id"]) if det_row is not None else "",
                "Insurance_Type": insurance_type,
                "Current_Month": f"{year}-{zm}",
                "Current_Status": display_status,
                "Gap_Status": display_status,
                "Unexplained_Category": cat,
                "Detail": cat_detail,
                "Source_XML_File": str(det_row["source_file"]) if det_row is not None else "",
            })

    unexplained_df = pd.DataFrame(unexplained_frames)
    if not unexplained_df.empty:
        unexplained_df = unexplained_df[UNEXPLAINED_COLUMNS]

    xlsx_path = _debug_dir() / "three_month_business_rule_validation.xlsx"
    safe_write_excel(
        xlsx_path,
        {
            "Rule_Comparison_Summary": rule_comparison,
            "Enrollment_Anchored_Window": anchored_df[[c for c in ANCHORED_COLUMNS if c in anchored_df.columns]],
            "Current_Month_Centered_Window": centered_df[[c for c in CENTERED_COLUMNS if c in centered_df.columns]],
            "Special_Enrollment_Cancel_Candidates": special_df[[c for c in SPECIAL_CANCEL_COLUMNS if c in special_df.columns]],
            "Forward_Three_Month_Window": forward_df[[c for c in FORWARD_COLUMNS if c in forward_df.columns]],
            "Unexplained_Gap_Records": unexplained_df,
            "Gap_Summary_By_Status": pd.DataFrame([
                {"Status": st, **status_summaries.get(st, {})}
                for st in DISPLAY_STATUSES
            ]),
        },
        drop_duplicate_value_columns=False,
    )

    md_path = _write_summary_md(
        issuer=issuer, year=year, month=zm,
        status_summaries=status_summaries,
        rule_comparison=rule_comparison,
        bucket_counts=all_bucket_counts,
        has_reference=has_reference,
        total_gap=total_gap,
        best_rule=best_rule,
    )

    monthly_df = _build_monthly_summary(
        issuer=issuer,
        year=year,
        month=zm,
        insurance_type=insurance_type,
        status_summaries=status_summaries,
        best_rule=best_rule,
        business_monthly=result.business_monthly,
    )
    monthly_xlsx, monthly_html = _write_monthly_summary_outputs(
        monthly_df, issuer=issuer, year=year, month=zm,
    )

    logger.info("Wrote three-month business rule validation → %s", xlsx_path)
    logger.info("Wrote three-month business rule summary → %s", md_path)
    logger.info("Wrote three-month monthly summary → %s, %s", monthly_xlsx, monthly_html)

    return {
        "xlsx": str(xlsx_path),
        "summary_md": str(md_path),
        "monthly_xlsx": str(monthly_xlsx),
        "monthly_html": str(monthly_html),
        "status_summaries": status_summaries,
        "rule_comparison": rule_comparison,
        "best_rule": best_rule,
        "monthly_summary": monthly_df,
        "hypothesis_verdict": _best_rule_verdict(rule_comparison, has_reference=has_reference, total_gap=total_gap),
    }


def _write_summary_md(
    *,
    issuer: str,
    year: str,
    month: str,
    status_summaries: dict[str, dict[str, Any]],
    rule_comparison: pd.DataFrame,
    bucket_counts: dict[str, int],
    has_reference: bool,
    total_gap: int,
    best_rule: dict[str, Any],
) -> Path:
    md_path = _debug_dir() / "three_month_business_rule_summary.md"
    generated = datetime.now(timezone.utc).isoformat(timespec="seconds")
    lines = [
        "# Three-Month Business Rule Validation",
        "",
        f"**Issuer / month:** {issuer} / {year} / {month}",
        f"**Generated:** {generated}",
        "",
        "Read-only investigation — no records removed, no logic changed.",
        "",
    ]
    if not has_reference:
        lines.append(
            "> No Chandra reference counts configured — gap comparison is informational only.",
        )
        lines.append("")

    lines.extend(["## Dashboard gap by status", ""])
    for st in DISPLAY_STATUSES:
        s = status_summaries.get(st, {})
        lines.extend([
            f"### {st}",
            "",
            f"- Expected (Chandra): {s.get('expected', 0)}",
            f"- Current dashboard: {s.get('actual', 0)}",
            f"- Remaining difference: {s.get('delta', 0):+d}",
            f"- Explained by best rule: {s.get('best_rule_explained', 0)}",
            f"- Remaining unexplained: {s.get('remaining_unexplained', 0)}",
            "",
        ])

    lines.extend([
        "## Rule comparison (which interpretation fits Chandra best?)",
        "",
        _best_rule_verdict(rule_comparison, has_reference=has_reference, total_gap=total_gap),
        "",
    ])
    if not rule_comparison.empty:
        lines.append("| Rule | Explained | % Gap | Confidence |")
        lines.append("|------|-----------|-------|------------|")
        for _, row in rule_comparison.iterrows():
            lines.append(
                f"| {row['Rule_Name']} | {row['Explained_Difference_Count']}/{row['Total_Positive_Gap']} "
                f"| {row['Pct_Gap_Explained']}% | {row['Confidence_Level']} |"
            )
        lines.append("")

    if best_rule:
        lines.extend([
            f"**Best candidate:** {best_rule.get('Rule_Name', '')}",
            "",
            f"- {best_rule.get('Notes', '')}",
            "",
        ])

    lines.extend(["## Unexplained record categories (after best rule)", ""])
    if bucket_counts:
        for cat, count in sorted(bucket_counts.items(), key=lambda x: (-x[1], x[0])):
            lines.append(f"- {cat}: {count}")
    else:
        lines.append("- (none)")
    lines.extend([
        "",
        "## Interpretations tested",
        "",
        "1. **Forward-only** — current month + next 2 months",
        "2. **Enrollment-anchored** — enrollment start month + 2 following months (bidirectional within window)",
        "3. **Current-month centered** — previous + current + next month",
        "4. **Special enrollment cancel** — cancel/term within 3 months of enrollment/effective date",
        "",
        "> This analysis does NOT implement any rule. Production pipeline unchanged.",
        "",
    ])
    md_path.write_text("\n".join(lines), encoding="utf-8")
    return md_path
