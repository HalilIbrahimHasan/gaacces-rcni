"""
Read-only reason-field business investigation.

Profiles underlying transaction/reason/type columns (NOT high-level status
action codes like 021/024) and simulates excluding specific reason values
against current business output. Does NOT modify production logic.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from itertools import combinations
from pathlib import Path
from typing import Any

import pandas as pd

from azure_reconciliation.dashboard_difference_analysis import (
    _current_counts_by_display,
    _enrollment_id_series,
    _filter_business_month,
    _to_display_status,
)
from azure_reconciliation.partition_discovery import Partition, discover_partitions
from azure_reconciliation.reconciliation_analysis import MAINT_ACTION_PREFIXES
from azure_reconciliation.safe_export import safe_write_excel
from azure_reconciliation.status_mapper import normalize_insurance_type, normalize_status
from azure_reconciliation.three_month_business_rule_validation import _resolve_expected_counts
from azure_reconciliation.xml_business_reports import process_issuer_xml_business
from azure_reconciliation.xml_loader import load_xml_rows
from config.config import settings
from utils.logger import get_logger

logger = get_logger(__name__)

DISPLAY_STATUSES = ("CONFIRM", "CANCEL", "TERM")

_FIELD_PATTERNS = re.compile(
    r"reason|reason_code|event_reason|maintenance_reason|maint_reason|"
    r"change_reason|action_reason|action_description|transaction_reason|"
    r"update_reason|change_type|maintenance_type|event_type|transaction_type|"
    r"qualifier|classification|ins\b|dtp\b|hd\b|ref\b",
    re.IGNORECASE,
)

# High-level status/outcome columns — not valid reason filters for this investigation.
_EXCLUDED_COLUMNS = frozenset({
    "maintenance_type_code",
    "action_code",
    "enrollment_action_code",
    "enrollee_event_type_code",
    "normalized_status",
    "status",
    "coverage_status",
    "additional_maint_reason_code",
    "canonical_status",
    "month_basis_used",
    "insurance_type",
    "insurance_type_code",
})

# Status/action outcome values — excluding these alone is not meaningful.
_STATUS_OUTCOME_VALUES = frozenset({
    *MAINT_ACTION_PREFIXES,
    "021", "024", "001", "030", "032", "33", "34", "XN",
    "CONFIRM", "CANCEL", "TERM", "TERMINATED", "CANCELLED", "CANCELED",
    "ENROLLED", "CONFIRMED", "CONFIRMATION", "CANCELLATION", "TERMINATION",
    "ACTIVE", "EFFECTUATED", "REINSTATE", "UNKNOWN",
    "1", "2", "3",
})


def _debug_dir() -> Path:
    d = settings.outputs_path / "debug"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _zmonth(m: str) -> str:
    return str(m).strip().zfill(2)


def _member_id_series(df: pd.DataFrame) -> pd.Series:
    col = "member_id" if "member_id" in df.columns else None
    if col:
        return df[col].astype(str)
    return pd.Series([""] * len(df), index=df.index, dtype=str)


def _display_status(row: pd.Series) -> str:
    for col in (
        "additional_maint_reason_code",
        "coverage_status",
        "action_code_description",
        "normalized_status",
        "status",
        "transaction_classification",
    ):
        if col in row.index:
            val = str(row.get(col, "") or "").strip()
            if val:
                return _to_display_status(val)
    return "UNKNOWN"


def _clean_value(val: Any) -> str:
    s = str(val or "").strip()
    if s.lower() in ("", "nan", "none", "null"):
        return ""
    return s


def _is_candidate_column(name: str) -> bool:
    low = name.lower()
    if low in _EXCLUDED_COLUMNS:
        return False
    if low.endswith("_id") or low in ("issuer", "year", "month", "file_name", "raw_xml_path"):
        return False
    return bool(_FIELD_PATTERNS.search(low))


def _values_are_status_outcomes(values: set[str]) -> bool:
    if not values:
        return True
    normalized = set()
    for v in values:
        vu = v.upper()
        normalized.add(vu)
        normalized.add(vu[:3])
        normalized.add(normalize_status(vu))
        normalized.add(_to_display_status(vu))
    outcome_hits = sum(
        1 for v in normalized
        if v in _STATUS_OUTCOME_VALUES or v in DISPLAY_STATUSES
    )
    return outcome_hits >= max(1, int(0.8 * len(values)))


def _discover_columns(xml_raw: pd.DataFrame, canonical: pd.DataFrame) -> list[str]:
    cols: set[str] = set()
    for df in (xml_raw, canonical):
        if not df.empty:
            cols.update(df.columns.astype(str))
    return sorted(c for c in cols if _is_candidate_column(c))


def _scoped_month_df(
    df: pd.DataFrame,
    *,
    issuer: str,
    year: str,
    month: str,
) -> pd.DataFrame:
    if df.empty:
        return df
    work = df.copy()
    if "year" in work.columns and "month" in work.columns:
        return _filter_business_month(work, issuer=issuer, year=year, month=month)
    work["year"] = work.get("year", "").astype(str)
    work["month"] = work.get("month", "").astype(str).map(_zmonth)
    mask = work.get("issuer", pd.Series(dtype=str)).astype(str) == str(issuer)
    mask &= work["year"].astype(str) == str(year)
    mask &= work["month"].astype(str).map(_zmonth) == _zmonth(month)
    return work[mask].copy()


def _combined_frames(xml_raw: pd.DataFrame, canonical: pd.DataFrame) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for label, df in (("xml_raw", xml_raw), ("canonical", canonical)):
        if df.empty:
            continue
        w = df.copy()
        w["_source"] = label
        frames.append(w)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def build_reason_field_discovery(
    xml_raw: pd.DataFrame,
    canonical: pd.DataFrame,
) -> pd.DataFrame:
    """Investigation 1 — profile candidate reason/type columns."""
    combined = _combined_frames(xml_raw, canonical)
    candidate_cols = _discover_columns(xml_raw, canonical)
    if not candidate_cols or combined.empty:
        return pd.DataFrame()

    rows: list[dict[str, Any]] = []
    for col in candidate_cols:
        if col not in combined.columns:
            continue
        series = combined[col].map(_clean_value)
        non_null = series[series != ""]
        if non_null.empty:
            continue
        distinct = sorted(non_null.unique().tolist())
        distinct_set = set(distinct)
        status_flags = combined[series != ""].copy()
        status_flags["_status"] = status_flags.apply(_display_status, axis=1)

        recommended = (
            col not in _EXCLUDED_COLUMNS
            and not _values_are_status_outcomes(distinct_set)
            and len(distinct_set) > 1
        )

        samples = distinct[:8]
        rows.append({
            "column_name": col,
            "non_null_count": int(len(non_null)),
            "distinct_count": len(distinct_set),
            "sample_values": "; ".join(samples),
            "appears_in_confirm": int((status_flags["_status"] == "CONFIRM").sum()),
            "appears_in_cancel": int((status_flags["_status"] == "CANCEL").sum()),
            "appears_in_term": int((status_flags["_status"] == "TERM").sum()),
            "recommended_for_testing": "yes" if recommended else "no",
        })

    out = pd.DataFrame(rows)
    if out.empty:
        return out
    return out.sort_values(
        ["recommended_for_testing", "non_null_count"],
        ascending=[False, False],
    ).reset_index(drop=True)


def build_reason_value_frequency(
    xml_raw: pd.DataFrame,
    canonical: pd.DataFrame,
    discovery: pd.DataFrame,
) -> pd.DataFrame:
    """Investigation 2 — frequency per recommended column/value."""
    if discovery.empty:
        return pd.DataFrame()

    recommended = discovery[
        discovery["recommended_for_testing"].astype(str).str.lower() == "yes"
    ]["column_name"].astype(str).tolist()
    combined = _combined_frames(xml_raw, canonical)
    if combined.empty or not recommended:
        return pd.DataFrame()

    combined["_status"] = combined.apply(_display_status, axis=1)
    combined["_enrollment_id"] = _enrollment_id_series(combined)
    combined["_member_id"] = _member_id_series(combined)

    rows: list[dict[str, Any]] = []
    for col in recommended:
        if col not in combined.columns:
            continue
        work = combined.copy()
        work["_val"] = work[col].map(_clean_value)
        work = work[work["_val"] != ""]
        if work.empty:
            continue
        if _values_are_status_outcomes(set(work["_val"].unique())):
            continue
        total = len(work)
        for val, grp in work.groupby("_val", dropna=False):
            if _clean_value(val) in _STATUS_OUTCOME_VALUES:
                continue
            status_counts = grp["_status"].value_counts().to_dict()
            rows.append({
                "column_name": col,
                "reason_value": str(val),
                "total_records": len(grp),
                "distinct_enrollments": int(grp["_enrollment_id"].replace("", pd.NA).dropna().nunique()),
                "distinct_enrollees": int(grp["_member_id"].replace("", pd.NA).dropna().nunique()),
                "CONFIRM_count": int(status_counts.get("CONFIRM", 0)),
                "CANCEL_count": int(status_counts.get("CANCEL", 0)),
                "TERM_count": int(status_counts.get("TERM", 0)),
                "percent_of_total": round(100.0 * len(grp) / total, 2),
            })

    out = pd.DataFrame(rows)
    if out.empty:
        return out
    return out.sort_values("total_records", ascending=False).reset_index(drop=True)


def _build_enrollment_month_status(
    lifecycle_input: pd.DataFrame,
    *,
    issuer: str,
    year: str,
    month: str,
    insurance_type: str,
) -> pd.DataFrame:
    scoped = _filter_business_month(
        lifecycle_input, issuer=issuer, year=year, month=month, insurance_type=insurance_type,
    )
    if scoped.empty:
        return pd.DataFrame(columns=["enrollment_id", "display_status"])
    work = scoped.copy()
    work["_enrollment_id"] = _enrollment_id_series(work)
    work["_display_status"] = work.get("status", "UNKNOWN").astype(str).map(_to_display_status)
    return (
        work.groupby("_enrollment_id", dropna=False)["_display_status"]
        .last()
        .reset_index()
        .rename(columns={"_enrollment_id": "enrollment_id", "_display_status": "display_status"})
    )


def _build_enrollment_reason_tags(
    xml_raw: pd.DataFrame,
    canonical: pd.DataFrame,
    *,
    issuer: str,
    year: str,
    month: str,
    recommended_cols: list[str],
) -> dict[str, set[tuple[str, str]]]:
    """Enrollment → set of (column_name, reason_value) in business month scope."""
    tags: dict[str, set[tuple[str, str]]] = {}
    for df in (_scoped_month_df(canonical, issuer=issuer, year=year, month=month),
               _scoped_month_df(xml_raw, issuer=issuer, year=year, month=month)):
        if df.empty:
            continue
        for _, row in df.iterrows():
            eid = str(_enrollment_id_series(pd.DataFrame([row])).iloc[0]).strip()
            if not eid:
                continue
            for col in recommended_cols:
                if col not in row.index:
                    continue
                val = _clean_value(row[col])
                if not val or val in _STATUS_OUTCOME_VALUES:
                    continue
                if _values_are_status_outcomes({val}):
                    continue
                tags.setdefault(eid, set()).add((col, val))
    return tags


def _simulate_counts(
    enrollment_status: pd.DataFrame,
    reason_tags: dict[str, set[tuple[str, str]]],
    exclude: set[tuple[str, str]],
) -> dict[str, int]:
    counts = {s: 0 for s in DISPLAY_STATUSES}
    if enrollment_status.empty:
        return counts
    for _, row in enrollment_status.iterrows():
        eid = str(row["enrollment_id"]).strip()
        if not eid:
            continue
        tagged = reason_tags.get(eid, set())
        if tagged & exclude:
            continue
        st = str(row["display_status"])
        if st in counts:
            counts[st] += 1
    return counts


def _total_gap(counts: dict[str, int], expected: dict[str, int]) -> int:
    return sum(abs(int(counts.get(s, 0)) - int(expected.get(s, 0))) for s in DISPLAY_STATUSES)


def _confidence(score: float, *, maintenance_overlap: float, over_removed: bool) -> str:
    if over_removed or score <= 0:
        return "Low"
    if score >= 8 and maintenance_overlap >= 0.3:
        return "High"
    if score >= 4:
        return "Medium"
    return "Low"


def build_reason_rule_impact(
    *,
    enrollment_status: pd.DataFrame,
    reason_tags: dict[str, set[tuple[str, str]]],
    current_counts: dict[str, int],
    expected: dict[str, int],
    frequency_df: pd.DataFrame,
    maintenance_enrollment_ids: set[str],
) -> pd.DataFrame:
    """Investigation 3 — simulate excluding one reason value at a time."""
    if frequency_df.empty:
        return pd.DataFrame()

    confirm_before = int(current_counts.get("CONFIRM", 0)) - int(expected.get("CONFIRM", 0))
    rows: list[dict[str, Any]] = []

    for _, freq in frequency_df.iterrows():
        col = str(freq["column_name"])
        val = str(freq["reason_value"])
        if val in _STATUS_OUTCOME_VALUES or _values_are_status_outcomes({val}):
            continue

        excl = {(col, val)}
        sim = _simulate_counts(enrollment_status, reason_tags, excl)
        sim_confirm = int(sim.get("CONFIRM", 0))
        sim_cancel = int(sim.get("CANCEL", 0))
        sim_term = int(sim.get("TERM", 0))

        gap_after = sim_confirm - int(expected.get("CONFIRM", 0))
        gap_reduced = confirm_before - gap_after
        over_removed = sim_confirm < int(expected.get("CONFIRM", 0))

        affected_enrollments = sum(
            1 for eid, tags in reason_tags.items()
            if (col, val) in tags
        )
        affected_records = int(freq.get("total_records", 0))

        maint_overlap = 0.0
        if affected_enrollments:
            maint_hits = sum(
                1 for eid, tags in reason_tags.items()
                if (col, val) in tags and eid in maintenance_enrollment_ids
            )
            maint_overlap = maint_hits / affected_enrollments

        score = _total_gap(current_counts, expected) - _total_gap(sim, expected)
        if over_removed:
            score -= abs(int(expected.get("CONFIRM", 0)) - sim_confirm) * 2

        rows.append({
            "column_name": col,
            "reason_value": val,
            "current_confirm": int(current_counts.get("CONFIRM", 0)),
            "simulated_confirm": sim_confirm,
            "chandra_expected_confirm": int(expected.get("CONFIRM", 0)),
            "confirm_gap_before": confirm_before,
            "confirm_gap_after": gap_after,
            "confirm_gap_reduced": gap_reduced,
            "current_cancel": int(current_counts.get("CANCEL", 0)),
            "simulated_cancel": sim_cancel,
            "current_term": int(current_counts.get("TERM", 0)),
            "simulated_term": sim_term,
            "affected_records": affected_records,
            "affected_enrollments": affected_enrollments,
            "total_gap_score_improvement": round(score, 2),
            "maintenance_only_overlap_pct": round(100.0 * maint_overlap, 1),
            "confidence": _confidence(score, maintenance_overlap=maint_overlap, over_removed=over_removed),
        })

    out = pd.DataFrame(rows)
    if out.empty:
        return out
    return out.sort_values(
        ["total_gap_score_improvement", "confirm_gap_reduced"],
        ascending=[False, False],
    ).reset_index(drop=True)


def build_reason_combination_impact(
    *,
    enrollment_status: pd.DataFrame,
    reason_tags: dict[str, set[tuple[str, str]]],
    rule_impact: pd.DataFrame,
    current_counts: dict[str, int],
    expected: dict[str, int],
    top_n: int = 15,
) -> pd.DataFrame:
    """Investigation 4 — pairs/triples from top reason values without over-removal."""
    if rule_impact.empty:
        return pd.DataFrame()

    candidates = rule_impact[
        (rule_impact["confirm_gap_reduced"] > 0)
        & (rule_impact["simulated_confirm"] >= rule_impact["chandra_expected_confirm"] * 0.9)
        & (rule_impact["simulated_confirm"] > 0)
    ].head(top_n)

    if candidates.empty:
        candidates = rule_impact.head(min(top_n, len(rule_impact)))

    pairs: list[tuple[str, set[tuple[str, str]]]] = []
    keys = [
        (str(r["column_name"]), str(r["reason_value"]))
        for _, r in candidates.iterrows()
    ]

    for r in (1, 2, 3):
        for combo in combinations(keys, r):
            label = " + ".join(f"{c}={v}" for c, v in combo)
            pairs.append((label, set(combo)))

    confirm_before = int(current_counts.get("CONFIRM", 0)) - int(expected.get("CONFIRM", 0))
    rows: list[dict[str, Any]] = []
    for label, excl in pairs:
        sim = _simulate_counts(enrollment_status, reason_tags, excl)
        sim_confirm = int(sim.get("CONFIRM", 0))
        gap_after = sim_confirm - int(expected.get("CONFIRM", 0))
        over_removed = sim_confirm < int(expected.get("CONFIRM", 0))
        score = _total_gap(current_counts, expected) - _total_gap(sim, expected)
        if over_removed:
            score -= abs(int(expected.get("CONFIRM", 0)) - sim_confirm) * 2

        affected = sum(1 for tags in reason_tags.values() if tags & excl)
        rows.append({
            "Combination": label,
            "Combination_Size": len(excl),
            "Simulated_CONFIRM": sim_confirm,
            "Simulated_CANCEL": int(sim.get("CANCEL", 0)),
            "Simulated_TERM": int(sim.get("TERM", 0)),
            "Confirm_Gap_Before": confirm_before,
            "Confirm_Gap_After": gap_after,
            "Confirm_Gap_Reduced": confirm_before - gap_after,
            "Total_Gap_Score_Improvement": round(score, 2),
            "Affected_Enrollments": affected,
            "Over_Removed": over_removed,
        })

    out = pd.DataFrame(rows)
    if out.empty:
        return out
    return out.sort_values(
        ["Total_Gap_Score_Improvement", "Confirm_Gap_Reduced"],
        ascending=[False, False],
    ).reset_index(drop=True)


def _maintenance_enrollment_ids(maintenance_df: pd.DataFrame) -> set[str]:
    if maintenance_df.empty:
        return set()
    return set(_enrollment_id_series(maintenance_df).astype(str).str.strip()) - {""}


def _write_reason_summary(
    *,
    issuer: str,
    year: str,
    month: str,
    discovery: pd.DataFrame,
    rule_impact: pd.DataFrame,
    combination_df: pd.DataFrame,
    current_counts: dict[str, int],
    expected: dict[str, int],
) -> Path:
    md_path = _debug_dir() / "reason_investigation_summary.md"
    generated = datetime.now(timezone.utc).isoformat(timespec="seconds")
    zm = _zmonth(month)

    best = rule_impact.iloc[0] if not rule_impact.empty else None
    best_combo = combination_df.iloc[0] if not combination_df.empty else None
    recommended = discovery[
        discovery["recommended_for_testing"].astype(str).str.lower() == "yes"
    ] if not discovery.empty else pd.DataFrame()

    lines = [
        "# Reason Field Business Investigation",
        "",
        f"**Issuer / month:** {issuer} / {year} / {zm}",
        f"**Generated:** {generated}",
        "",
        "Read-only simulation — production pipeline unchanged.",
        "",
        "## Why the previous 021/024 maintenance test was invalid",
        "",
        "Codes **021** (Confirmed/Effectuated) and **024** (Terminated) are **high-level "
        "enrollment status/action outcome codes**, not granular transaction reasons. "
        "Excluding 021 removes essentially all CONFIRM enrollments (1292 → 0), which "
        "proves nothing about Chandra's hidden business rules. This investigation tests "
        "**underlying reason/type fields** only (e.g. `enrollee_event_reason_code`, "
        "`transaction_classification`, event qualifiers) and explicitly excludes "
        "status-outcome columns and values.",
        "",
        "## Current gap vs Chandra",
        "",
        f"- CONFIRM: {current_counts.get('CONFIRM', 0)} vs {expected.get('CONFIRM', 0)} "
        f"(gap {int(current_counts.get('CONFIRM', 0)) - int(expected.get('CONFIRM', 0)):+d})",
        f"- CANCEL: {current_counts.get('CANCEL', 0)} vs {expected.get('CANCEL', 0)}",
        f"- TERM: {current_counts.get('TERM', 0)} vs {expected.get('TERM', 0)}",
        "",
        "## Reason fields found",
        "",
    ]

    if not recommended.empty:
        for _, row in recommended.iterrows():
            lines.append(
                f"- `{row['column_name']}` — {row['non_null_count']} values, "
                f"{row['distinct_count']} distinct; samples: {row.get('sample_values', '')}"
            )
    else:
        lines.append("- (no recommended reason columns discovered)")

    lines.extend(["", "## Best individual reason value (by total-gap score)", ""])
    if best is not None:
        lines.extend([
            f"- **Field:** `{best['column_name']}` = `{best['reason_value']}`",
            f"- **Simulated CONFIRM:** {best['simulated_confirm']} (target {expected.get('CONFIRM', 0)})",
            f"- **CONFIRM gap:** {best['confirm_gap_before']} → {best['confirm_gap_after']}",
            f"- **Simulated CANCEL/TERM:** {best['simulated_cancel']} / {best['simulated_term']}",
            f"- **Score improvement:** {best.get('total_gap_score_improvement', 0)}",
            f"- **Confidence:** {best.get('confidence', 'Low')}",
            "",
        ])
    else:
        lines.append("- (no rule impact rows)")

    lines.extend(["## Best combination", ""])
    if best_combo is not None:
        lines.extend([
            f"- **Combination:** {best_combo['Combination']}",
            f"- **Simulated CONFIRM/CANCEL/TERM:** "
            f"{best_combo['Simulated_CONFIRM']} / {best_combo['Simulated_CANCEL']} / "
            f"{best_combo['Simulated_TERM']}",
            f"- **Score improvement:** {best_combo.get('Total_Gap_Score_Improvement', 0)}",
            "",
        ])

    total_gap_before = _total_gap(current_counts, expected)
    if best_combo is not None:
        sim_best = {
            "CONFIRM": int(best_combo["Simulated_CONFIRM"]),
            "CANCEL": int(best_combo["Simulated_CANCEL"]),
            "TERM": int(best_combo["Simulated_TERM"]),
        }
        total_gap_after = _total_gap(sim_best, expected)
    elif best is not None:
        sim_best = {
            "CONFIRM": int(best["simulated_confirm"]),
            "CANCEL": int(best["simulated_cancel"]),
            "TERM": int(best["simulated_term"]),
        }
        total_gap_after = _total_gap(sim_best, expected)
    else:
        total_gap_after = total_gap_before

    pct = round(100.0 * (total_gap_before - total_gap_after) / total_gap_before, 1) if total_gap_before else 0
    confidence = "Low"
    if pct >= 50 and total_gap_after <= 10:
        confidence = "High"
    elif pct >= 25:
        confidence = "Medium"

    lines.extend([
        "## Does any reason-based rule explain the remaining difference?",
        "",
    ])
    if total_gap_after < total_gap_before:
        lines.append(
            f"Reason-value exclusion simulations improve total absolute gap from "
            f"**{total_gap_before}** to **{total_gap_after}** ({pct:.1f}% improvement)."
        )
    else:
        lines.append("No reason-value exclusion materially improved all three status gaps.")

    lines.extend([
        "",
        f"**Confidence level:** {confidence}",
        "",
        "## Recommendation",
        "",
    ])

    if best is not None and float(best.get("total_gap_score_improvement", 0)) > 2:
        lines.append(
            f"Investigate **`{best['column_name']}` = `{best['reason_value']}`** further — "
            f"it moves CONFIRM toward {expected.get('CONFIRM', 0)} without collapsing counts. "
            f"Cross-check affected enrollments against maintenance-only diagnostics."
        )
    else:
        lines.append(
            "No single reason value strongly explains the Chandra gap. Continue with "
            "month-basis, lifecycle latest-state, or composite business rules."
        )

    lines.extend([
        "",
        "> This analysis does NOT implement any rule. Production pipeline unchanged.",
        "",
    ])
    md_path.write_text("\n".join(lines), encoding="utf-8")
    return md_path


def run_reason_investigation(
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
    """Run all reason-field investigations (read-only)."""
    settings.ensure_dirs()
    zm = _zmonth(month)
    expected = _resolve_expected_counts(
        issuer, year, zm,
        expected_counts=expected_counts,
        cli_confirm=cli_confirm,
        cli_cancel=cli_cancel,
        cli_term=cli_term,
    ) or {s: 0 for s in DISPLAY_STATUSES}

    partitions = discover_partitions(settings.source_data_path, issuer_filter=issuer)
    xml_raw = load_xml_rows(prefer_staging=not parse_source, issuer_filter=issuer)
    if xml_raw.empty:
        raise RuntimeError(f"No XML rows for issuer {issuer}")
    if not partitions:
        partitions = [Partition(issuer=issuer, year=year, month=zm)]

    biz = process_issuer_xml_business(issuer, xml_raw, partitions)

    discovery = build_reason_field_discovery(xml_raw, biz.canonical)
    frequency = build_reason_value_frequency(xml_raw, biz.canonical, discovery)

    recommended_cols = (
        discovery[discovery["recommended_for_testing"].astype(str).str.lower() == "yes"]["column_name"]
        .astype(str).tolist()
        if not discovery.empty else []
    )

    enrollment_status = _build_enrollment_month_status(
        biz.lifecycle_input,
        issuer=issuer, year=year, month=zm, insurance_type=insurance_type,
    )
    reason_tags = _build_enrollment_reason_tags(
        xml_raw, biz.canonical,
        issuer=issuer, year=year, month=zm,
        recommended_cols=recommended_cols,
    )
    current_counts = _current_counts_by_display(
        biz.lifecycle_input,
        issuer=issuer, year=year, month=zm, insurance_type=insurance_type,
    )
    maint_ids = _maintenance_enrollment_ids(biz.maintenance_df)

    rule_impact = build_reason_rule_impact(
        enrollment_status=enrollment_status,
        reason_tags=reason_tags,
        current_counts=current_counts,
        expected=expected,
        frequency_df=frequency,
        maintenance_enrollment_ids=maint_ids,
    )
    combination_df = build_reason_combination_impact(
        enrollment_status=enrollment_status,
        reason_tags=reason_tags,
        rule_impact=rule_impact,
        current_counts=current_counts,
        expected=expected,
    )

    paths = {
        "discovery": _debug_dir() / "reason_field_discovery.xlsx",
        "frequency": _debug_dir() / "reason_value_frequency.xlsx",
        "rule_impact": _debug_dir() / "reason_rule_impact.xlsx",
        "combination": _debug_dir() / "reason_combination_impact.xlsx",
    }
    safe_write_excel(paths["discovery"], {"Field_Discovery": discovery}, drop_duplicate_value_columns=False)
    safe_write_excel(paths["frequency"], {"Value_Frequency": frequency}, drop_duplicate_value_columns=False)
    safe_write_excel(paths["rule_impact"], {"Rule_Impact": rule_impact}, drop_duplicate_value_columns=False)
    safe_write_excel(paths["combination"], {"Combination_Impact": combination_df}, drop_duplicate_value_columns=False)

    summary_path = _write_reason_summary(
        issuer=issuer, year=year, month=zm,
        discovery=discovery,
        rule_impact=rule_impact,
        combination_df=combination_df,
        current_counts=current_counts,
        expected=expected,
    )

    logger.info("Wrote reason investigation outputs → %s", _debug_dir())

    return {
        "discovery_xlsx": str(paths["discovery"]),
        "frequency_xlsx": str(paths["frequency"]),
        "rule_impact_xlsx": str(paths["rule_impact"]),
        "combination_xlsx": str(paths["combination"]),
        "summary_md": str(summary_path),
        "current_counts": current_counts,
        "expected_counts": expected,
        "recommended_fields": recommended_cols,
        "best_rule": rule_impact.iloc[0].to_dict() if not rule_impact.empty else {},
        "best_combination": combination_df.iloc[0].to_dict() if not combination_df.empty else {},
    }
