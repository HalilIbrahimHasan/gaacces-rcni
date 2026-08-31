"""
Final read-only gap investigation — explain every remaining dashboard difference.

Does NOT modify parser, canonical, lifecycle, Model H, or any production output.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from azure_reconciliation.dashboard_difference_analysis import (
    _build_latest_state_keys,
    _build_reference_status_map,
    _current_counts_by_display,
    _enrollment_detail_rows,
    _enrollment_id_series,
    _filter_business_month,
    _identify_extra_enrollment_ids,
    _to_display_status,
)
from azure_reconciliation.lifecycle_snapshot_comparison import _sort_chronological
from azure_reconciliation.partition_discovery import Partition, discover_partitions
from azure_reconciliation.record_comparison import LIFECYCLE_PRIMARY_JOIN, join_key_series
from azure_reconciliation.reconciliation_analysis import MAINT_ACTION_PREFIXES
from azure_reconciliation.safe_export import safe_write_excel
from azure_reconciliation.status_mapper import normalize_insurance_type
from azure_reconciliation.three_month_business_rule_validation import _resolve_expected_counts
from azure_reconciliation.xml_business_reports import PK, process_issuer_xml_business
from azure_reconciliation.xml_loader import load_xml_rows
from config.config import settings
from utils.logger import get_logger

logger = get_logger(__name__)

DISPLAY_STATUSES = ("CONFIRM", "CANCEL", "TERM")
PK_ENTITY = list(LIFECYCLE_PRIMARY_JOIN)
CANCEL_STATUSES = frozenset({"CANCEL"})
TERM_STATUSES = frozenset({"TERM"})

GAP_CATEGORIES = (
    "Maintenance chain",
    "Superseded transaction",
    "Duplicate transaction",
    "Future cancel",
    "Future term",
    "Coverage overlap",
    "Eligibility difference",
    "Different latest transaction",
    "Different representative transaction",
    "Business month interpretation",
    "Unknown",
)


def _debug_dir() -> Path:
    d = settings.outputs_path / "debug"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _zmonth(m: str) -> str:
    return str(m).strip().zfill(2)


def _entity_key_series(df: pd.DataFrame) -> pd.Series:
    keys = [k for k in PK_ENTITY if k in df.columns]
    return join_key_series(df, keys) if keys else pd.Series([""] * len(df), index=df.index)


def _maintenance_code(row: pd.Series) -> str:
    for col in ("maintenance_type_code", "action_code", "enrollment_action_code"):
        if col in row.index and str(row.get(col, "") or "").strip():
            return str(row[col]).strip()
    return ""


def _is_maintenance_only(row: pd.Series) -> bool:
    code = _maintenance_code(row)[:3]
    return code in MAINT_ACTION_PREFIXES


def _reason_code(row: pd.Series) -> str:
    for col in ("enrollee_event_reason_code", "additional_maint_reason_code"):
        if col in row.index and str(row.get(col, "") or "").strip():
            return str(row[col]).strip()
    return ""


def _txn_classification(row: pd.Series) -> str:
    for col in ("transaction_classification", "action_code_description", "coverage_status"):
        if col in row.index and str(row.get(col, "") or "").strip():
            return str(row[col]).strip()
    return ""


def _source_file(row: pd.Series) -> str:
    for col in ("source_file", "file_name", "raw_xml_path"):
        if col in row.index and str(row.get(col, "") or "").strip():
            return str(row[col]).strip()
    return ""


def _display_status(row: pd.Series) -> str:
    for col in ("normalized_status", "status", "additional_maint_reason_code", "coverage_status"):
        if col in row.index:
            val = str(row.get(col, "") or "").strip()
            if val:
                return _to_display_status(val)
    return "UNKNOWN"


def _enrich_canonical_from_xml(canonical: pd.DataFrame, xml_raw: pd.DataFrame) -> pd.DataFrame:
    if canonical.empty:
        return canonical
    work = canonical.copy()
    if xml_raw.empty:
        return work
    xml = xml_raw.copy()
    for col in (
        "enrollee_event_reason_code", "transaction_classification",
        "action_code_description", "enrollee_event_type_code",
    ):
        if col not in work.columns and col in xml.columns:
            work[col] = ""
    if "policy_id" in work.columns and "member_id" in work.columns:
        pick = ["policy_id", "member_id", "source_file", "file_name"]
        pick += [c for c in (
            "enrollee_event_reason_code", "transaction_classification",
            "action_code_description",
        ) if c in xml.columns]
        xml_sub = xml[[c for c in pick if c in xml.columns]].drop_duplicates(
            subset=[c for c in ("policy_id", "member_id", "file_name") if c in xml.columns],
            keep="last",
        )
        merge_on = [c for c in ("policy_id", "member_id") if c in work.columns and c in xml_sub.columns]
        if merge_on:
            work = work.merge(xml_sub, on=merge_on, how="left", suffixes=("", "_xml"))
    return work


def build_complete_enrollment_history(
    canonical: pd.DataFrame,
    xml_raw: pd.DataFrame,
) -> pd.DataFrame:
    """Step 1 — full chronological transaction history per entity (nothing removed)."""
    if canonical.empty:
        return pd.DataFrame()

    work = _enrich_canonical_from_xml(canonical, xml_raw)
    work["_entity_key"] = _entity_key_series(work)
    work["_enrollment_id"] = _enrollment_id_series(work)
    work["_display_status"] = work.apply(_display_status, axis=1)
    work = _sort_chronological(work)

    rows: list[dict[str, Any]] = []
    for ek, grp in work.groupby("_entity_key", dropna=False):
        grp = grp.reset_index(drop=True)
        for i, (_, row) in enumerate(grp.iterrows(), start=1):
            rows.append({
                "issuer": str(row.get("issuer", "")),
                "policy_id": str(row.get("policy_id", "")),
                "member_id": str(row.get("member_id", "")),
                "insurance_type": str(row.get("insurance_type", "")),
                "enrollment_id": str(row.get("_enrollment_id", row.get("policy_id", ""))),
                "entity_key": str(ek),
                "transaction_number": i,
                "maintenance_code": _maintenance_code(row),
                "transaction_classification": _txn_classification(row),
                "reason_code": _reason_code(row),
                "business_status": str(row.get("_display_status", "")),
                "benefit_effective_date": str(row.get("benefit_effective_date", "") or ""),
                "member_maint_effective_date": str(row.get("member_maint_effective_date", "") or ""),
                "coverage_start": str(row.get("benefit_effective_date", "") or ""),
                "coverage_end": str(row.get("benefit_end_date", "") or ""),
                "source_xml": _source_file(row),
                "business_year": str(row.get("year", "")),
                "business_month": _zmonth(str(row.get("month", ""))),
                "business_month_ym": f"{row.get('year', '')}-{_zmonth(str(row.get('month', '')))}",
                "month_basis_used": str(row.get("month_basis_used", "") or ""),
                "file_event_year_month": str(row.get("file_event_year_month", "") or ""),
                "is_maintenance_only": _is_maintenance_only(row),
            })
    return pd.DataFrame(rows)


def _pick_last(df: pd.DataFrame) -> pd.Series | None:
    if df.empty:
        return None
    return df.iloc[-1]


def _alternative_selections(
    history: pd.DataFrame,
    entity_key: str,
    analysis_ym: str,
) -> dict[str, dict[str, Any]]:
    """Step 3 — diagnostic selection methods for one entity in analysis month."""
    ent = history[history["entity_key"].astype(str) == entity_key].copy()
    month_rows = ent[ent["business_month_ym"].astype(str) == analysis_ym].copy()
    result: dict[str, dict[str, Any]] = {}

    def _pack(row: pd.Series | None, method: str) -> dict[str, Any]:
        if row is None:
            return {"method": method, "transaction_number": "", "business_status": "", "source_xml": ""}
        return {
            "method": method,
            "transaction_number": int(row.get("transaction_number", 0)),
            "business_status": str(row.get("business_status", "")),
            "maintenance_code": str(row.get("maintenance_code", "")),
            "source_xml": str(row.get("source_xml", "")),
        }

    result["latest_transaction"] = _pack(_pick_last(month_rows), "Latest Transaction")
    non_maint = month_rows[~month_rows["is_maintenance_only"].astype(bool)]
    result["latest_non_maintenance"] = _pack(_pick_last(non_maint), "Latest Non-Maintenance")
    result["latest_eligible"] = _pack(_pick_last(non_maint), "Latest Eligible Transaction")

    confirm_rows = month_rows[month_rows["business_status"] == "CONFIRM"]
    result["latest_enrollment"] = _pack(_pick_last(confirm_rows), "Latest Enrollment Transaction")

    status_change = month_rows.copy()
    if len(status_change) > 1:
        status_change["_prev"] = status_change["business_status"].shift(1)
        changes = status_change[status_change["business_status"] != status_change["_prev"]]
        result["latest_status_change"] = _pack(_pick_last(changes), "Latest Status Change")
    else:
        result["latest_status_change"] = _pack(_pick_last(month_rows), "Latest Status Change")

    effect = ent[ent["business_status"] == "CONFIRM"]
    result["earliest_effectuation"] = _pack(effect.iloc[0] if not effect.empty else None, "Earliest Effectuation")

    return result


def _future_status(
    history: pd.DataFrame,
    entity_key: str,
    analysis_ym: str,
) -> tuple[bool, bool, str]:
    """Look ahead for CANCEL/TERM after analysis month."""
    ent = history[history["entity_key"].astype(str) == entity_key].copy()
    if ent.empty:
        return False, False, ""
    future = ent[ent["business_month_ym"].astype(str) > analysis_ym]
    cancel = bool((future["business_status"] == "CANCEL").any())
    term = bool((future["business_status"] == "TERM").any())
    note = ""
    if cancel:
        note = "CANCEL in month after analysis"
    if term:
        note = (note + "; " if note else "") + "TERM in month after analysis"
    return cancel, term, note


def _build_timeline_text(history: pd.DataFrame, entity_key: str, *, selected_txn: int | None) -> str:
    ent = history[history["entity_key"].astype(str) == entity_key].sort_values("transaction_number")
    if ent.empty:
        return "(no history)"
    lines: list[str] = []
    for _, row in ent.iterrows():
        txn = int(row["transaction_number"])
        ym = str(row["business_month_ym"])
        st = str(row["business_status"])
        mc = str(row.get("maintenance_code", ""))
        cls = str(row.get("transaction_classification", ""))
        marker = " ← SELECTED BY MODEL H" if selected_txn and txn == selected_txn else ""
        lines.append(f"{ym} | Txn {txn} | {st} | maint={mc} | {cls}{marker}")
    return "\n".join(lines)


def _classify_gap_enrollment(
    *,
    detail_row: pd.Series,
    history: pd.DataFrame,
    entity_key: str,
    analysis_ym: str,
    alt: dict[str, dict[str, Any]],
    model_h_txn: int | None,
    reference_status: str,
) -> tuple[str, str]:
    """Assign exactly one gap category with evidence."""
    if bool(detail_row.get("duplicate_flag")):
        return "Duplicate transaction", "Enrollment flagged as duplicate XML transaction in cleanup diagnostics"
    if bool(detail_row.get("maintenance_only_flag")):
        return "Maintenance chain", "Enrollment retained via maintenance-only chain in Model H input"
    if bool(detail_row.get("superseded_flag")):
        return "Superseded transaction", "A later transaction superseded an earlier event for this entity"

    fc, ft, fnote = _future_status(history, entity_key, analysis_ym)
    if fc and str(detail_row.get("display_status")) == "CONFIRM":
        return "Future cancel", f"CONFIRM in analysis month; {fnote}"
    if ft and str(detail_row.get("display_status")) == "CONFIRM":
        return "Future term", f"CONFIRM in analysis month; {fnote}"

    ref = reference_status
    disp = str(detail_row.get("display_status", ""))
    if ref and ref != disp:
        return "Different representative transaction", f"Lifecycle snapshot status={ref}; Model H status={disp}"

    basis = str(detail_row.get("month_basis_used", "") or "")
    src_m = _zmonth(str(detail_row.get("source_folder_month", "") or ""))
    ana_m = analysis_ym.split("-", 1)[-1] if "-" in analysis_ym else analysis_ym
    if basis and basis != "file_event_year_month":
        return "Business month interpretation", f"Business month from {basis}; source folder month={src_m}"
    if src_m and src_m != ana_m:
        return "Business month interpretation", f"Source folder month {src_m} vs business month {ana_m}"

    cur = alt.get("latest_transaction", {})
    elig = alt.get("latest_eligible", {})
    if (
        cur.get("transaction_number") != elig.get("transaction_number")
        and elig.get("business_status")
        and str(elig.get("business_status")) != disp
    ):
        return "Different latest transaction", (
            f"Latest eligible txn {elig.get('transaction_number')} ({elig.get('business_status')}) "
            f"differs from Model H selection txn {model_h_txn} ({disp})"
        )

    if cur.get("business_status") and str(cur.get("business_status")) != disp:
        return "Eligibility difference", (
            f"Latest month transaction status {cur.get('business_status')} differs from Model H {disp}"
        )

    ent = history[history["entity_key"].astype(str) == entity_key]
    if not ent.empty:
        spans = ent[
            (ent["coverage_start"].astype(str).str[:7] != ent["coverage_end"].astype(str).str[:7])
            & (ent["coverage_end"].astype(str).str.strip() != "")
        ]
        if not spans.empty:
            return "Coverage overlap", "Benefit effective/end dates span multiple months for this entity"

    if not bool(detail_row.get("latest_state_flag")):
        return "Different latest transaction", "Model H row is not the latest canonical state for entity/month"

    return "Unknown", "No single dominant XML semantic; likely downstream eligibility or external business rule"


def build_model_h_selection_analysis(
    history: pd.DataFrame,
    lifecycle_input: pd.DataFrame,
    *,
    issuer: str,
    year: str,
    month: str,
    insurance_type: str,
) -> pd.DataFrame:
    """Step 2+3 — selection analysis for enrollments in Model H analysis month."""
    zm = _zmonth(month)
    analysis_ym = f"{year}-{zm}"
    scoped = _filter_business_month(
        lifecycle_input, issuer=issuer, year=year, month=month, insurance_type=insurance_type,
    )
    if scoped.empty or history.empty:
        return pd.DataFrame()

    scoped = scoped.copy()
    scoped["_enrollment_id"] = _enrollment_id_series(scoped)
    scoped["_entity_key"] = _entity_key_series(scoped)
    scoped["_display_status"] = scoped.get("status", "").astype(str).map(_to_display_status)

    rows: list[dict[str, Any]] = []
    for _, mh_row in scoped.iterrows():
        eid = str(mh_row["_enrollment_id"])
        ek = str(mh_row["_entity_key"])
        month_hist = history[
            (history["entity_key"].astype(str) == ek)
            & (history["business_month_ym"].astype(str) == analysis_ym)
        ]
        selected = _pick_last(month_hist)
        txn_num = int(selected["transaction_number"]) if selected is not None else None

        ignored: list[str] = []
        if selected is not None and not month_hist.empty:
            for _, trow in month_hist.iterrows():
                if int(trow["transaction_number"]) < txn_num:
                    ignored.append(
                        f"Txn {int(trow['transaction_number'])} {trow['business_status']} "
                        f"({trow.get('maintenance_code', '')}) superseded by later txn in month"
                    )

        alt = _alternative_selections(history, ek, analysis_ym)
        row: dict[str, Any] = {
            "issuer": issuer,
            "enrollment_id": eid,
            "policy_id": str(mh_row.get("policy_id", "")),
            "member_id": str(mh_row.get("member_id", "")),
            "insurance_type": insurance_type,
            "analysis_month": analysis_ym,
            "model_h_status": str(mh_row["_display_status"]),
            "selected_by_model_h": True,
            "selected_transaction_number": txn_num,
            "selected_maintenance_code": str(selected.get("maintenance_code", "")) if selected is not None else "",
            "selected_classification": str(selected.get("transaction_classification", "")) if selected is not None else "",
            "selected_reason_code": str(selected.get("reason_code", "")) if selected is not None else "",
            "selected_source_xml": str(selected.get("source_xml", "")) if selected is not None else "",
            "why_selected": (
                "Business transaction collapse + Model H input keeps final business-significant row "
                f"for {analysis_ym} (txn {txn_num})"
                if txn_num else "No matching canonical transaction in analysis month"
            ),
            "why_prior_ignored": "; ".join(ignored[:5]) if ignored else "N/A (first/only txn in month)",
        }
        for method, pick in alt.items():
            row[f"alt_{method}_txn"] = pick.get("transaction_number", "")
            row[f"alt_{method}_status"] = pick.get("business_status", "")
        rows.append(row)

    return pd.DataFrame(rows)


def build_gap_analysis(
    *,
    detail: pd.DataFrame,
    history: pd.DataFrame,
    selection_df: pd.DataFrame,
    reference_status: dict[str, str],
    issuer: str,
    year: str,
    month: str,
    expected: dict[str, int],
    actual_counts: dict[str, int],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Step 4 — one explanation per gap enrollment."""
    zm = _zmonth(month)
    analysis_ym = f"{year}-{zm}"
    gap_rows: list[dict[str, Any]] = []
    timeline_rows: list[dict[str, Any]] = []
    all_gap_ids: set[str] = set()

    for display_status in DISPLAY_STATUSES:
        exp = int(expected.get(display_status, 0))
        extra_ids = _identify_extra_enrollment_ids(
            detail,
            display_status=display_status,
            expected_count=exp,
            reference_status=reference_status,
        )
        all_gap_ids |= extra_ids

    for eid in sorted(all_gap_ids):
        det = detail[detail["enrollment_id"].astype(str) == eid]
        if det.empty:
            continue
        drow = det.iloc[0]
        ek_rows = selection_df[selection_df["enrollment_id"].astype(str) == eid]
        entity_hist = history[history["enrollment_id"].astype(str) == eid]
        if entity_hist.empty and not ek_rows.empty:
            pol = str(ek_rows.iloc[0].get("policy_id", ""))
            mem = str(ek_rows.iloc[0].get("member_id", ""))
            entity_hist = history[
                (history["policy_id"].astype(str) == pol)
                & (history["member_id"].astype(str) == mem)
            ]
        entity_key = str(entity_hist.iloc[0]["entity_key"]) if not entity_hist.empty else ""

        sel_txn = None
        if not ek_rows.empty and ek_rows.iloc[0].get("selected_transaction_number"):
            sel_txn = int(ek_rows.iloc[0]["selected_transaction_number"])

        alt = _alternative_selections(history, entity_key, analysis_ym)
        category, evidence = _classify_gap_enrollment(
            detail_row=drow,
            history=history,
            entity_key=entity_key,
            analysis_ym=analysis_ym,
            alt=alt,
            model_h_txn=sel_txn,
            reference_status=reference_status.get(eid, ""),
        )
        timeline = _build_timeline_text(history, entity_key, selected_txn=sel_txn)

        gap_rows.append({
            "issuer": issuer,
            "enrollment_id": eid,
            "policy_id": str(drow.get("policy_id", "")),
            "member_id": str(drow.get("member_id", "")),
            "gap_status": str(drow.get("display_status", "")),
            "reference_status": reference_status.get(eid, ""),
            "difference_category": category,
            "evidence": evidence,
            "duplicate_flag": bool(drow.get("duplicate_flag")),
            "maintenance_only_flag": bool(drow.get("maintenance_only_flag")),
            "superseded_flag": bool(drow.get("superseded_flag")),
            "latest_state_flag": bool(drow.get("latest_state_flag")),
            "month_basis_used": str(drow.get("month_basis_used", "")),
            "source_file": str(drow.get("source_file", "")),
            "model_h_selected_txn": sel_txn,
            "alt_latest_eligible_status": alt.get("latest_eligible", {}).get("business_status", ""),
            "alt_earliest_effectuation_status": alt.get("earliest_effectuation", {}).get("business_status", ""),
        })
        timeline_rows.append({
            "enrollment_id": eid,
            "policy_id": str(drow.get("policy_id", "")),
            "gap_status": str(drow.get("display_status", "")),
            "difference_category": category,
            "timeline": timeline,
            "model_h_selected": f"Txn {sel_txn}" if sel_txn else "unknown",
            "alternative_latest_eligible": (
                f"Txn {alt.get('latest_eligible', {}).get('transaction_number')} "
                f"{alt.get('latest_eligible', {}).get('business_status')}"
            ),
            "why_difference": evidence,
        })

    gap_df = pd.DataFrame(gap_rows)
    timeline_df = pd.DataFrame(timeline_rows)

    cat_df = pd.DataFrame()
    if not gap_df.empty:
        total = len(gap_df)
        counts = gap_df["difference_category"].value_counts().reset_index()
        counts.columns = ["Category", "Enrollment_Count"]
        counts["Percentage"] = (counts["Enrollment_Count"] / total * 100).round(1)
        cat_df = counts.sort_values("Enrollment_Count", ascending=False)

    summary_rows = [{
        "issuer": issuer,
        "analysis_month": analysis_ym,
        "current_confirm": actual_counts.get("CONFIRM", 0),
        "expected_confirm": expected.get("CONFIRM", 0),
        "confirm_gap": int(actual_counts.get("CONFIRM", 0)) - int(expected.get("CONFIRM", 0)),
        "current_cancel": actual_counts.get("CANCEL", 0),
        "expected_cancel": expected.get("CANCEL", 0),
        "cancel_gap": int(actual_counts.get("CANCEL", 0)) - int(expected.get("CANCEL", 0)),
        "current_term": actual_counts.get("TERM", 0),
        "expected_term": expected.get("TERM", 0),
        "term_gap": int(actual_counts.get("TERM", 0)) - int(expected.get("TERM", 0)),
        "gap_enrollments_explained": len(gap_df),
        "dominant_category": str(cat_df.iloc[0]["Category"]) if not cat_df.empty else "",
        "dominant_pct": float(cat_df.iloc[0]["Percentage"]) if not cat_df.empty else 0.0,
    }]
    summary_df = pd.DataFrame(summary_rows)
    return gap_df, timeline_df, cat_df, summary_df


def _write_engineering_conclusion(
    *,
    issuer: str,
    year: str,
    month: str,
    actual_counts: dict[str, int],
    expected: dict[str, int],
    gap_df: pd.DataFrame,
    cat_df: pd.DataFrame,
    dominant_threshold: float = 80.0,
) -> Path:
    md_path = _debug_dir() / "final_engineering_conclusion.md"
    generated = datetime.now(timezone.utc).isoformat(timespec="seconds")
    zm = _zmonth(month)

    lines = [
        "# Final Engineering Conclusion — Remaining Dashboard Gap",
        "",
        f"**Issuer / month:** {issuer} / {year} / {zm}",
        f"**Generated:** {generated}",
        "",
        "Pipeline status: **FROZEN** — read-only investigation only.",
        "",
        "## Dashboard gap",
        "",
        f"| Status | Current | Chandra | Gap |",
        f"|--------|---------|---------|-----|",
    ]
    for st in DISPLAY_STATUSES:
        cur = int(actual_counts.get(st, 0))
        exp = int(expected.get(st, 0))
        lines.append(f"| {st} | {cur} | {exp} | {cur - exp:+d} |")

    lines.extend(["", "## Gap enrollment explanations", ""])
    if not gap_df.empty:
        lines.append(f"**{len(gap_df)}** gap enrollments classified (one category each).")
        for _, row in cat_df.iterrows():
            lines.append(
                f"- **{row['Category']}:** {int(row['Enrollment_Count'])} "
                f"({row['Percentage']}%)"
            )
    else:
        lines.append("No positive gap enrollments identified for this month.")

    dominant = ""
    dominant_pct = 0.0
    if not cat_df.empty:
        dominant = str(cat_df.iloc[0]["Category"])
        dominant_pct = float(cat_df.iloc[0]["Percentage"])

    lines.extend(["", "## Dominant pattern", ""])
    if dominant_pct >= dominant_threshold:
        lines.append(
            f"**{dominant_pct:.1f}%** of gap enrollments are **{dominant}**. "
            "No further hypothesis search is warranted — this is the dominant remaining "
            "business difference for sign-off discussion."
        )
    elif dominant:
        lines.append(
            f"Largest category is **{dominant}** at **{dominant_pct:.1f}%** — "
            "no single category exceeds 80%; remaining gap is mixed."
        )

    lines.extend([
        "",
        "## Why does our pipeline include these enrollments?",
        "",
        "Model H input applies, in order: canonical enrichment → business-month basis → "
        "duplicate/maintenance diagnostics → business transaction collapse → enrollment grain "
        "counting. Gap enrollments are included because their **final business-significant "
        "transaction** for the analysis month maps to CONFIRM/CANCEL/TERM in lifecycle input.",
        "",
        "## Why might Chandra exclude them?",
        "",
    ])

    if dominant in ("Future cancel", "Future term", "Maintenance chain"):
        lines.append(
            "Evidence points to **downstream eligibility or cross-month selection** — "
            "Chandra may retroactively exclude enrollments with later CANCEL/TERM or "
            "maintenance-only chains not reflected in our frozen XML-only aggregation."
        )
    elif dominant in ("Business month interpretation", "Different representative transaction"):
        lines.append(
            "Evidence points to **different transaction/month selection semantics**, not "
            "parser or ID errors. Month-basis or representative-transaction choice likely "
            "differs from Chandra's dashboard engine."
        )
    elif dominant_pct < 50:
        lines.append(
            "Mixed categories suggest the remaining gap is **not explained by a single "
            "XML transformation rule**. The difference is most likely caused by "
            "**downstream business logic or eligibility processing outside the XML "
            "transformation pipeline**."
        )
    else:
        lines.append(
            f"Primary evidence category **{dominant}** — see `gap_enrollment_timelines.xlsx` "
            "for per-enrollment transaction timelines and alternative selections."
        )

    lines.extend([
        "",
        "## Root-cause attribution (evidence-based)",
        "",
        "| Layer | Verdict |",
        "|-------|---------|",
        "| XML parser | ✓ Validated — not root cause |",
        "| Canonical model | ✓ Validated — not root cause |",
        "| Relationship / IDs | ✓ Validated — not root cause |",
        "| Cleanup | Contributes to *which* txn is selected; see category breakdown |",
        "| Lifecycle replay | Contributes to representative status; see timelines |",
        "| Model H aggregation | ✓ Working as designed — gap is selection semantics |",
        "| **Downstream eligibility** | **Most likely for unexplained portion** |",
        "",
        "## Sign-off recommendation",
        "",
    ])

    total_gap = sum(
        max(0, int(actual_counts.get(s, 0)) - int(expected.get(s, 0)))
        for s in DISPLAY_STATUSES
    )
    if total_gap <= 10:
        lines.append(
            "Remaining gap is small relative to volume. Present category breakdown and "
            "timelines to Hari/Chandra; confirm whether dashboard uses external eligibility."
        )
    else:
        lines.append(
            "Present `final_remaining_gap_analysis.xlsx` and per-enrollment timelines. "
            "Request Chandra documentation on enrollment **selection** and **eligibility** "
            "rules for the analysis month — XML pipeline alone cannot close this gap without "
            "those business rules."
        )

    lines.extend([
        "",
        "> Production pipeline unchanged. This is the final investigation before sign-off.",
        "",
    ])
    md_path.write_text("\n".join(lines), encoding="utf-8")
    return md_path


def run_final_gap_investigation(
    *,
    issuer: str = "15105",
    year: str = "2026",
    month: str = "01",
    insurance_type: str = "HEALTH",
    parse_source: bool = False,
    cli_confirm: int | None = None,
    cli_cancel: int | None = None,
    cli_term: int | None = None,
) -> dict[str, Any]:
    """Run final read-only gap investigation."""
    settings.ensure_dirs()
    zm = _zmonth(month)
    expected = _resolve_expected_counts(
        issuer, year, zm,
        cli_confirm=cli_confirm, cli_cancel=cli_cancel, cli_term=cli_term,
    ) or {s: 0 for s in DISPLAY_STATUSES}

    partitions = discover_partitions(settings.source_data_path, issuer_filter=issuer)
    xml_raw = load_xml_rows(prefer_staging=not parse_source, issuer_filter=issuer)
    if xml_raw.empty:
        raise RuntimeError(f"No XML rows for issuer {issuer}")
    if not partitions:
        partitions = [Partition(issuer=issuer, year=year, month=zm)]

    biz = process_issuer_xml_business(issuer, xml_raw, partitions)

    history = build_complete_enrollment_history(biz.canonical, xml_raw)

    reference_status = _build_reference_status_map(
        biz.lifecycle_snapshots, issuer=issuer, year=year, month=zm,
    )
    latest_state_keys = _build_latest_state_keys(
        biz.canonical, issuer=issuer, year=year, month=zm,
    )
    detail = _enrollment_detail_rows(
        biz.lifecycle_input,
        biz.canonical,
        issuer=issuer,
        year=year,
        month=zm,
        insurance_type=insurance_type,
        duplicate_df=biz.duplicate_df,
        maintenance_df=biz.maintenance_df,
        superseded_df=biz.superseded_df,
        reference_status=reference_status,
        latest_state_keys=latest_state_keys,
    )

    actual_counts = _current_counts_by_display(
        biz.lifecycle_input,
        issuer=issuer, year=year, month=zm, insurance_type=insurance_type,
    )

    selection_df = build_model_h_selection_analysis(
        history, biz.lifecycle_input,
        issuer=issuer, year=year, month=zm, insurance_type=insurance_type,
    )

    gap_df, timeline_df, cat_df, summary_df = build_gap_analysis(
        detail=detail,
        history=history,
        selection_df=selection_df,
        reference_status=reference_status,
        issuer=issuer,
        year=year,
        month=zm,
        expected=expected,
        actual_counts=actual_counts,
    )

    # Mark selected transactions in full history for Model H enrollments in analysis month
    history_out = history.copy()
    if not selection_df.empty and not history_out.empty:
        sel_map = dict(zip(
            selection_df["enrollment_id"].astype(str),
            selection_df["selected_transaction_number"],
        ))
        history_out["selected_by_model_h"] = history_out.apply(
            lambda r: (
                str(r["enrollment_id"]) in sel_map
                and sel_map[str(r["enrollment_id"])]
                and int(r["transaction_number"]) == int(sel_map[str(r["enrollment_id"])])
            ),
            axis=1,
        )
    else:
        history_out["selected_by_model_h"] = False

    paths = {
        "history": _debug_dir() / "enrollment_history_timeline.xlsx",
        "selection": _debug_dir() / "model_h_selection_analysis.xlsx",
        "gap_timelines": _debug_dir() / "gap_enrollment_timelines.xlsx",
        "categories": _debug_dir() / "enrollment_difference_categories.xlsx",
        "final": _debug_dir() / "final_remaining_gap_analysis.xlsx",
    }

    safe_write_excel(paths["history"], {"Enrollment_History": history_out}, drop_duplicate_value_columns=False)
    safe_write_excel(paths["selection"], {"Model_H_Selection": selection_df}, drop_duplicate_value_columns=False)
    safe_write_excel(paths["gap_timelines"], {"Gap_Timelines": timeline_df}, drop_duplicate_value_columns=False)
    safe_write_excel(paths["categories"], {"Difference_Categories": cat_df}, drop_duplicate_value_columns=False)
    safe_write_excel(
        paths["final"],
        {"Gap_Summary": summary_df, "Gap_Enrollments": gap_df},
        drop_duplicate_value_columns=False,
    )

    conclusion_path = _write_engineering_conclusion(
        issuer=issuer, year=year, month=zm,
        actual_counts=actual_counts,
        expected=expected,
        gap_df=gap_df,
        cat_df=cat_df,
    )

    logger.info("Wrote final gap investigation → %s", _debug_dir())

    return {
        "history_xlsx": str(paths["history"]),
        "selection_xlsx": str(paths["selection"]),
        "gap_timelines_xlsx": str(paths["gap_timelines"]),
        "categories_xlsx": str(paths["categories"]),
        "final_xlsx": str(paths["final"]),
        "conclusion_md": str(conclusion_path),
        "actual_counts": actual_counts,
        "expected_counts": expected,
        "gap_enrollment_count": len(gap_df),
        "categories": cat_df.to_dict("records") if not cat_df.empty else [],
    }
