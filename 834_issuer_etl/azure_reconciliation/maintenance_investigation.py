"""
Read-only maintenance code business investigation.

Discovers maintenance/action codes from XML and simulates excluding them
one-at-a-time (and in small combinations) against current business output.
Does NOT modify parser, canonical, lifecycle, Model H, or reports.
"""

from __future__ import annotations

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
from azure_reconciliation.safe_export import safe_write_excel
from azure_reconciliation.status_mapper import normalize_insurance_type
from azure_reconciliation.three_month_business_rule_validation import _resolve_expected_counts
from azure_reconciliation.xml_business_reports import process_issuer_xml_business
from azure_reconciliation.xml_loader import load_xml_rows
from config.config import settings
from utils.logger import get_logger

logger = get_logger(__name__)

DISPLAY_STATUSES = ("CONFIRM", "CANCEL", "TERM")

_MAINT_CODE_COLS = (
    "maintenance_type_code",
    "action_code",
    "enrollment_action_code",
    "enrollee_event_type_code",
)

_DESC_COLS = (
    "action_code_description",
    "coverage_status",
    "additional_maint_reason_code",
    "enrollee_event_reason_code",
)

_REASON_GROUP_COLS = (
    "maintenance_type_code",
    "additional_maint_reason_code",
    "enrollee_event_reason_code",
    "action_code_description",
)


def _debug_dir() -> Path:
    d = settings.outputs_path / "debug"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _zmonth(m: str) -> str:
    return str(m).strip().zfill(2)


def _maintenance_code(row: pd.Series) -> str:
    for col in _MAINT_CODE_COLS:
        if col in row.index:
            val = str(row.get(col, "") or "").strip()
            if val and val.lower() not in ("nan", "none", ""):
                return val
    return ""


def _maintenance_description(row: pd.Series) -> str:
    for col in _DESC_COLS:
        if col in row.index:
            val = str(row.get(col, "") or "").strip()
            if val and val.lower() not in ("nan", "none", ""):
                return val
    return ""


def _display_status(row: pd.Series) -> str:
    for col in (
        "additional_maint_reason_code",
        "coverage_status",
        "action_code_description",
        "normalized_status",
        "status",
    ):
        if col in row.index:
            val = str(row.get(col, "") or "").strip()
            if val:
                return _to_display_status(val)
    return "UNKNOWN"


def _code_description_map(df: pd.DataFrame) -> dict[str, str]:
    if df.empty:
        return {}
    work = df.copy()
    work["_code"] = work.apply(_maintenance_code, axis=1)
    work["_desc"] = work.apply(_maintenance_description, axis=1)
    work = work[(work["_code"] != "") & (work["_desc"] != "")]
    if work.empty:
        return {}
    return (
        work.groupby("_code")["_desc"]
        .agg(lambda s: s.mode().iloc[0] if not s.mode().empty else s.iloc[0])
        .to_dict()
    )


def build_maintenance_frequency(xml_raw: pd.DataFrame) -> pd.DataFrame:
    """Investigation 1 — maintenance code frequency across all XML rows."""
    if xml_raw.empty:
        return pd.DataFrame()

    work = xml_raw.copy()
    work["_code"] = work.apply(_maintenance_code, axis=1)
    work["_desc"] = work.apply(_maintenance_description, axis=1)
    work["_status"] = work.apply(_display_status, axis=1)
    work["_enrollment_id"] = _enrollment_id_series(work)
    work["_member_id"] = work.get("member_id", pd.Series([""] * len(work))).astype(str)

    work = work[work["_code"] != ""].copy()
    if work.empty:
        return pd.DataFrame()

    total = len(work)
    rows: list[dict[str, Any]] = []
    for code, grp in work.groupby("_code", dropna=False):
        desc = grp["_desc"].mode().iloc[0] if not grp["_desc"].mode().empty else ""
        status_counts = grp["_status"].value_counts().to_dict()
        rows.append({
            "Maintenance_Code": str(code),
            "Maintenance_Description": str(desc),
            "Total_Records": len(grp),
            "Distinct_Enrollments": int(grp["_enrollment_id"].replace("", pd.NA).dropna().nunique()),
            "Distinct_Enrollees": int(grp["_member_id"].replace("", pd.NA).dropna().nunique()),
            "CONFIRM": int(status_counts.get("CONFIRM", 0)),
            "CANCEL": int(status_counts.get("CANCEL", 0)),
            "TERM": int(status_counts.get("TERM", 0)),
            "Pct_Of_Total": round(100.0 * len(grp) / total, 2),
        })

    out = pd.DataFrame(rows)
    return out.sort_values("Total_Records", ascending=False).reset_index(drop=True)


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


def _build_enrollment_code_tags(
    canonical: pd.DataFrame,
    xml_raw: pd.DataFrame,
    *,
    issuer: str,
    year: str,
    month: str,
) -> dict[str, set[str]]:
    """Enrollment → maintenance codes on canonical/xml rows in business month scope."""
    tags: dict[str, set[str]] = {}
    canon = _filter_business_month(canonical, issuer=issuer, year=year, month=month)
    for _, row in canon.iterrows():
        code = _maintenance_code(row)
        if not code:
            continue
        eid = str(_enrollment_id_series(pd.DataFrame([row])).iloc[0]).strip()
        if not eid:
            continue
        tags.setdefault(eid, set()).add(code)

    raw = xml_raw.copy()
    if not raw.empty:
        raw["year"] = raw.get("year", "").astype(str)
        raw["month"] = raw.get("month", "").astype(str).map(_zmonth)
        raw = raw[
            (raw.get("issuer", pd.Series(dtype=str)).astype(str) == str(issuer))
            & (raw["year"].astype(str) == str(year))
            & (raw["month"].astype(str).map(_zmonth) == _zmonth(month))
        ]
        for _, row in raw.iterrows():
            code = _maintenance_code(row)
            if not code:
                continue
            eid = str(_enrollment_id_series(pd.DataFrame([row])).iloc[0]).strip()
            if not eid:
                continue
            tags.setdefault(eid, set()).add(code)
    return tags


def _simulate_status_counts(
    enrollment_status: pd.DataFrame,
    code_tags: dict[str, set[str]],
    exclude_codes: set[str],
) -> dict[str, int]:
    counts = {s: 0 for s in DISPLAY_STATUSES}
    if enrollment_status.empty:
        return counts
    for _, row in enrollment_status.iterrows():
        eid = str(row["enrollment_id"]).strip()
        if not eid:
            continue
        tagged = code_tags.get(eid, set())
        if tagged & exclude_codes:
            continue
        st = str(row["display_status"])
        if st in counts:
            counts[st] += 1
    return counts


def _affected_enrollment_count(
    enrollment_status: pd.DataFrame,
    code_tags: dict[str, set[str]],
    exclude_codes: set[str],
) -> int:
    if enrollment_status.empty:
        return 0
    n = 0
    for _, row in enrollment_status.iterrows():
        eid = str(row["enrollment_id"]).strip()
        if eid and (code_tags.get(eid, set()) & exclude_codes):
            n += 1
    return n


def build_maintenance_rule_impact(
    *,
    enrollment_status: pd.DataFrame,
    code_tags: dict[str, set[str]],
    current_counts: dict[str, int],
    expected_confirm: int,
    descriptions: dict[str, str],
    frequency_df: pd.DataFrame,
) -> pd.DataFrame:
    """Investigation 2 — simulate excluding one maintenance code at a time."""
    codes = sorted({c for tags in code_tags.values() for c in tags})
    if not codes and not frequency_df.empty:
        codes = frequency_df["Maintenance_Code"].astype(str).tolist()

    current_confirm = int(current_counts.get("CONFIRM", 0))
    rows: list[dict[str, Any]] = []

    for code in codes:
        excl = {code}
        sim = _simulate_status_counts(enrollment_status, code_tags, excl)
        sim_confirm = int(sim.get("CONFIRM", 0))
        gap_remaining = sim_confirm - expected_confirm
        gap_reduced = current_confirm - sim_confirm
        affected = _affected_enrollment_count(enrollment_status, code_tags, excl)

        rows.append({
            "Maintenance_Code": code,
            "Maintenance_Description": descriptions.get(code, ""),
            "Current_CONFIRM": current_confirm,
            "Simulated_CONFIRM": sim_confirm,
            "Gap_Remaining": gap_remaining,
            "Gap_Reduced": gap_reduced,
            "Current_CANCEL": int(current_counts.get("CANCEL", 0)),
            "Simulated_CANCEL": int(sim.get("CANCEL", 0)),
            "Current_TERM": int(current_counts.get("TERM", 0)),
            "Simulated_TERM": int(sim.get("TERM", 0)),
            "Records_Affected": affected,
            "Impact_Score": gap_reduced,
        })

    if not rows:
        return pd.DataFrame()

    out = pd.DataFrame(rows)
    out = out.sort_values(["Gap_Reduced", "Impact_Score"], ascending=False).reset_index(drop=True)
    out["Rank"] = range(1, len(out) + 1)
    return out


def build_maintenance_combination_impact(
    *,
    enrollment_status: pd.DataFrame,
    code_tags: dict[str, set[str]],
    rule_impact: pd.DataFrame,
    expected_confirm: int,
    top_n: int = 10,
) -> pd.DataFrame:
    """Investigation 3 — singles + pairs + triples from top maintenance codes."""
    if rule_impact.empty:
        return pd.DataFrame()

    top_codes = (
        rule_impact.head(top_n)["Maintenance_Code"].astype(str).tolist()
    )
    current_confirm = int(
        enrollment_status[enrollment_status["display_status"] == "CONFIRM"].shape[0]
        if not enrollment_status.empty else 0
    )

    combos: list[tuple[str, set[str]]] = []
    for r in (1, 2, 3):
        for combo in combinations(top_codes, r):
            label = "+".join(combo)
            combos.append((label, set(combo)))

    rows: list[dict[str, Any]] = []
    for label, excl in combos:
        sim = _simulate_status_counts(enrollment_status, code_tags, excl)
        sim_confirm = int(sim.get("CONFIRM", 0))
        gap_reduced = current_confirm - sim_confirm
        rows.append({
            "Combination": label,
            "Combination_Size": len(excl),
            "Simulated_CONFIRM": sim_confirm,
            "Gap_Remaining": sim_confirm - expected_confirm,
            "Gap_Reduced": gap_reduced,
            "Records_Affected": _affected_enrollment_count(enrollment_status, code_tags, excl),
            "Impact_Score": gap_reduced,
        })

    out = pd.DataFrame(rows)
    out = out.sort_values(["Gap_Reduced", "Impact_Score"], ascending=False).reset_index(drop=True)
    out["Rank"] = range(1, len(out) + 1)
    return out


def build_maintenance_reason_analysis(
    xml_raw: pd.DataFrame,
    canonical: pd.DataFrame,
) -> pd.DataFrame:
    """Investigation 4 — group by maintenance code + reason/type fields."""
    frames = []
    for name, df in (("xml_raw", xml_raw), ("canonical", canonical)):
        if df.empty:
            continue
        work = df.copy()
        work["_source"] = name
        work["_code"] = work.apply(_maintenance_code, axis=1)
        work["_status"] = work.apply(_display_status, axis=1)
        work["_enrollment_id"] = _enrollment_id_series(work)
        frames.append(work)

    if not frames:
        return pd.DataFrame()

    work = pd.concat(frames, ignore_index=True)
    work = work[work["_code"] != ""].copy()
    if work.empty:
        return pd.DataFrame()

    group_cols = ["_code"]
    for col in _REASON_GROUP_COLS:
        if col in work.columns:
            group_cols.append(col)

    rows: list[dict[str, Any]] = []
    for key, grp in work.groupby(group_cols, dropna=False):
        if not isinstance(key, tuple):
            key = (key,)
        row: dict[str, Any] = {
            "Maintenance_Code": str(key[0]),
            "Total_Records": len(grp),
            "Distinct_Enrollments": int(grp["_enrollment_id"].replace("", pd.NA).dropna().nunique()),
            "CONFIRM": int((grp["_status"] == "CONFIRM").sum()),
            "CANCEL": int((grp["_status"] == "CANCEL").sum()),
            "TERM": int((grp["_status"] == "TERM").sum()),
            "Source": ",".join(sorted(grp["_source"].astype(str).unique())),
        }
        for i, col in enumerate(_REASON_GROUP_COLS):
            if col in group_cols:
                idx = group_cols.index(col)
                row[col] = str(key[idx]) if idx < len(key) else ""
        rows.append(row)

    out = pd.DataFrame(rows)
    return out.sort_values("Total_Records", ascending=False).reset_index(drop=True)


def _write_investigation_summary(
    *,
    issuer: str,
    year: str,
    month: str,
    frequency_df: pd.DataFrame,
    rule_impact: pd.DataFrame,
    combination_df: pd.DataFrame,
    current_counts: dict[str, int],
    expected: dict[str, int],
) -> Path:
    md_path = _debug_dir() / "maintenance_investigation_summary.md"
    generated = datetime.now(timezone.utc).isoformat(timespec="seconds")
    zm = _zmonth(month)

    confirm_gap = int(current_counts.get("CONFIRM", 0)) - int(expected.get("CONFIRM", 0))
    best_single = rule_impact.iloc[0] if not rule_impact.empty else None
    best_combo = combination_df.iloc[0] if not combination_df.empty else None

    lines = [
        "# Maintenance Code Business Investigation",
        "",
        f"**Issuer / month:** {issuer} / {year} / {zm}",
        f"**Generated:** {generated}",
        "",
        "Read-only simulation — production pipeline unchanged.",
        "",
        "## Current dashboard gap vs Chandra",
        "",
        f"- CONFIRM: {current_counts.get('CONFIRM', 0)} vs {expected.get('CONFIRM', 0)} (gap {confirm_gap:+d})",
        f"- CANCEL: {current_counts.get('CANCEL', 0)} vs {expected.get('CANCEL', 0)}",
        f"- TERM: {current_counts.get('TERM', 0)} vs {expected.get('TERM', 0)}",
        "",
        "## 1. Top maintenance codes by frequency",
        "",
    ]

    if not frequency_df.empty:
        for _, row in frequency_df.head(10).iterrows():
            lines.append(
                f"- `{row['Maintenance_Code']}` — {row['Total_Records']} records "
                f"({row.get('Pct_Of_Total', 0)}%): {row.get('Maintenance_Description', '')}"
            )
    else:
        lines.append("- (no maintenance codes found)")

    lines.extend(["", "## 2. Individual code with largest gap reduction", ""])
    if best_single is not None:
        lines.extend([
            f"- **Code:** `{best_single['Maintenance_Code']}`",
            f"- **Description:** {best_single.get('Maintenance_Description', '')}",
            f"- **Simulated CONFIRM:** {best_single['Simulated_CONFIRM']}",
            f"- **Gap reduced:** {best_single['Gap_Reduced']} (remaining {best_single['Gap_Remaining']})",
            f"- **Records affected:** {best_single['Records_Affected']}",
            "",
        ])
    else:
        lines.append("- (no impact rows)")

    lines.extend(["## 3. Best combination (top 10 codes, sizes 1–3)", ""])
    if best_combo is not None:
        lines.extend([
            f"- **Combination:** `{best_combo['Combination']}`",
            f"- **Simulated CONFIRM:** {best_combo['Simulated_CONFIRM']}",
            f"- **Gap reduced:** {best_combo['Gap_Reduced']} (remaining {best_combo['Gap_Remaining']})",
            f"- **Records affected:** {best_combo['Records_Affected']}",
            "",
        ])
    else:
        lines.append("- (no combination rows)")

    best_gap_reduced = int(best_combo["Gap_Reduced"]) if best_combo is not None else 0
    remaining = confirm_gap - best_gap_reduced
    pct = round(100.0 * best_gap_reduced / confirm_gap, 1) if confirm_gap > 0 else 0.0

    if pct >= 80:
        confidence = "High"
    elif pct >= 50:
        confidence = "Medium"
    else:
        confidence = "Low"

    lines.extend([
        "## 4. Do maintenance transactions explain Chandra's dashboard?",
        "",
    ])
    if best_gap_reduced > 0 and confirm_gap > 0:
        lines.append(
            f"Excluding maintenance code(s) in simulation reduces the CONFIRM gap by "
            f"**{best_gap_reduced}** of **{confirm_gap}** ({pct:.1f}%)."
        )
    else:
        lines.append("No maintenance exclusion simulation materially reduced the CONFIRM gap.")

    lines.extend([
        "",
        f"**Confidence level:** {confidence}",
        "",
        "## 5. Recommendation",
        "",
    ])

    if best_combo is not None and best_gap_reduced >= 4:
        codes = str(best_combo["Combination"])
        lines.append(
            f"The current evidence suggests that excluding maintenance code(s) "
            f"**{codes}** reduces the CONFIRM gap from **{confirm_gap}** to "
            f"**{int(best_combo['Gap_Remaining'])}** records "
            f"({best_gap_reduced} removed in simulation), making this the strongest "
            f"maintenance-related business-rule candidate discovered so far."
        )
    elif best_single is not None and int(best_single.get("Gap_Reduced", 0)) > 0:
        lines.append(
            f"Maintenance code `{best_single['Maintenance_Code']}` shows the largest "
            f"individual impact ({best_single['Gap_Reduced']} CONFIRM reduction) but "
            f"does not fully explain the remaining gap."
        )
    else:
        lines.append(
            "Maintenance code exclusion simulations do not strongly explain the "
            "remaining Chandra dashboard gap. Investigate other business rules."
        )

    lines.extend([
        "",
        "> This analysis does NOT implement any rule. Production pipeline unchanged.",
        "",
    ])
    md_path.write_text("\n".join(lines), encoding="utf-8")
    return md_path


def run_maintenance_investigation(
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
    """Run all five maintenance investigations (read-only)."""
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

    frequency_df = build_maintenance_frequency(xml_raw)
    descriptions = _code_description_map(xml_raw)
    if not descriptions and not biz.canonical.empty:
        descriptions = _code_description_map(biz.canonical)

    enrollment_status = _build_enrollment_month_status(
        biz.lifecycle_input,
        issuer=issuer, year=year, month=zm, insurance_type=insurance_type,
    )
    code_tags = _build_enrollment_code_tags(
        biz.canonical, xml_raw, issuer=issuer, year=year, month=zm,
    )
    current_counts = _current_counts_by_display(
        biz.lifecycle_input,
        issuer=issuer, year=year, month=zm, insurance_type=insurance_type,
    )

    rule_impact = build_maintenance_rule_impact(
        enrollment_status=enrollment_status,
        code_tags=code_tags,
        current_counts=current_counts,
        expected_confirm=int(expected.get("CONFIRM", 0)),
        descriptions=descriptions,
        frequency_df=frequency_df,
    )
    combination_df = build_maintenance_combination_impact(
        enrollment_status=enrollment_status,
        code_tags=code_tags,
        rule_impact=rule_impact,
        expected_confirm=int(expected.get("CONFIRM", 0)),
    )
    reason_df = build_maintenance_reason_analysis(xml_raw, biz.canonical)

    freq_path = _debug_dir() / "maintenance_frequency.xlsx"
    impact_path = _debug_dir() / "maintenance_rule_impact.xlsx"
    combo_path = _debug_dir() / "maintenance_combination_impact.xlsx"
    reason_path = _debug_dir() / "maintenance_reason_analysis.xlsx"

    safe_write_excel(freq_path, {"Maintenance_Frequency": frequency_df}, drop_duplicate_value_columns=False)
    safe_write_excel(impact_path, {"Rule_Impact": rule_impact}, drop_duplicate_value_columns=False)
    safe_write_excel(combo_path, {"Combination_Impact": combination_df}, drop_duplicate_value_columns=False)
    safe_write_excel(reason_path, {"Reason_Analysis": reason_df}, drop_duplicate_value_columns=False)

    summary_path = _write_investigation_summary(
        issuer=issuer, year=year, month=zm,
        frequency_df=frequency_df,
        rule_impact=rule_impact,
        combination_df=combination_df,
        current_counts=current_counts,
        expected=expected,
    )

    logger.info("Wrote maintenance investigation outputs → %s", _debug_dir())

    return {
        "frequency_xlsx": str(freq_path),
        "rule_impact_xlsx": str(impact_path),
        "combination_xlsx": str(combo_path),
        "reason_xlsx": str(reason_path),
        "summary_md": str(summary_path),
        "current_counts": current_counts,
        "expected_counts": expected,
        "top_code": rule_impact.iloc[0].to_dict() if not rule_impact.empty else {},
        "best_combination": combination_df.iloc[0].to_dict() if not combination_df.empty else {},
    }
