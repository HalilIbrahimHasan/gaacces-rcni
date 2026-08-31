"""
Pre-Model-H business transaction collapse for maintenance-only chains.

Collapses intermediate maintenance-only events per business key/month so they
do not inflate Model H enrollment counts. Diagnostics only for suppressed rows.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from azure_reconciliation.chandra_business_format import STATUS_TO_CHANDRA_DISPLAY
from azure_reconciliation.df_utils import find_col
from azure_reconciliation.reconciliation_analysis import MAINT_ACTION_PREFIXES
from azure_reconciliation.safe_export import safe_write_excel
from azure_reconciliation.status_mapper import normalize_status
from config.config import settings
from utils.logger import get_logger

logger = get_logger(__name__)

COLLAPSE_SAFETY_THRESHOLD = 0.10

BUSINESS_KEY_COLS = ["issuer", "policy_id", "member_id", "insurance_type", "year", "month"]

REASON_VALUES = (
    "collapsed_noop_maintenance",
    "collapsed_intermediate_maintenance",
    "collapsed_duplicate_maintenance",
    "kept_final_business_state",
    "no_collapse_needed",
)

AUDIT_COLUMNS = [
    "issuer",
    "policy_id",
    "member_id",
    "insurance_type",
    "year",
    "month",
    "group_original_row_count",
    "status_before",
    "status_after",
    "maintenance_code",
    "benefit_effective_date",
    "benefit_end_date",
    "source_file",
    "kept_for_model_h",
    "collapsed_event_count",
    "collapsed_maintenance_only_count",
    "reason",
]


@dataclass
class CollapseResult:
    collapsed: pd.DataFrame = field(default_factory=pd.DataFrame)
    audit: pd.DataFrame = field(default_factory=pd.DataFrame)
    summary: dict[str, Any] = field(default_factory=dict)
    applied: bool = False
    before_model_h_input: pd.DataFrame = field(default_factory=pd.DataFrame)
    warning: str = ""


def _debug_dir() -> Path:
    d = settings.outputs_path / "debug"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _zmonth(m: str) -> str:
    return str(m).strip().zfill(2)


def _maintenance_code(row: pd.Series) -> str:
    for col in ("maintenance_type_code", "action_code", "enrollment_action_code"):
        if col in row.index:
            val = str(row.get(col, "") or "").strip()
            if val:
                return val
    return ""


def _is_maintenance_only(row: pd.Series) -> bool:
    code = _maintenance_code(row)
    if not code:
        return False
    return code[:3] in MAINT_ACTION_PREFIXES


def _source_file(row: pd.Series) -> str:
    for col in ("source_file", "file_name", "raw_xml_path"):
        if col in row.index:
            val = str(row.get(col, "") or "").strip()
            if val:
                return val
    return ""


def _file_event_date(row: pd.Series) -> str:
    for col in ("file_event_date", "event_date"):
        if col in row.index:
            return str(row.get(col, "") or "").strip()
    return ""


def _status_value(row: pd.Series) -> str:
    for col in ("normalized_status", "status", "canonical_status"):
        if col in row.index:
            return normalize_status(str(row.get(col, "")))
    return "UNKNOWN"


def _business_sort(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    work = df.copy()
    work["_sort_maint"] = work.apply(
        lambda r: str(r.get("member_maint_effective_date", "") or ""), axis=1,
    )
    work["_sort_benefit"] = work.apply(
        lambda r: str(r.get("benefit_effective_date", "") or ""), axis=1,
    )
    work["_sort_file_evt"] = work.apply(_file_event_date, axis=1)
    work["_sort_source"] = work.apply(_source_file, axis=1)
    return work.sort_values(
        ["_sort_maint", "_sort_benefit", "_sort_file_evt", "_sort_source"],
        ascending=True,
        na_position="last",
        kind="mergesort",
    ).drop(columns=["_sort_maint", "_sort_benefit", "_sort_file_evt", "_sort_source"])


def _resolve_business_keys(df: pd.DataFrame) -> list[str]:
    keys: list[str] = []
    if "issuer" in df.columns:
        keys.append("issuer")
    pol = find_col(df, "policy_id", "enrollment_id")
    if pol:
        keys.append(pol)
    mem = find_col(df, "member_id", "enrollee_id")
    if mem:
        keys.append(mem)
    if "insurance_type" in df.columns:
        keys.append("insurance_type")
    if "year" in df.columns:
        keys.append("year")
    if "month" in df.columns:
        keys.append("month")
    return keys


def _normalize_group_cols(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy()
    if "policy_id" not in work.columns:
        pol = find_col(work, "enrollment_id")
        if pol:
            work["policy_id"] = work[pol]
    if "member_id" not in work.columns:
        mem = find_col(work, "enrollee_id")
        if mem:
            work["member_id"] = work[mem]
    if "month" in work.columns:
        work["month"] = work["month"].astype(str).map(_zmonth)
    return work


def _state_unchanged(prev: pd.Series, curr: pd.Series) -> bool:
    if _status_value(prev) != _status_value(curr):
        return False
    bef_p = str(prev.get("benefit_effective_date", "") or "").strip()
    bef_c = str(curr.get("benefit_effective_date", "") or "").strip()
    end_p = str(prev.get("benefit_end_date", "") or "").strip()
    end_c = str(curr.get("benefit_end_date", "") or "").strip()
    return bef_p == bef_c and end_p == end_c


def _classify_suppressed(
    row: pd.Series,
    *,
    idx: int,
    keep_idx: int,
    rows: list[pd.Series],
    maint_only_indices: list[int],
) -> str:
    if not _is_maintenance_only(row):
        return "collapsed_intermediate_maintenance"
    if idx > 0 and _state_unchanged(rows[idx - 1], row):
        return "collapsed_noop_maintenance"
    if len(maint_only_indices) > 1 and idx != keep_idx:
        dup_siblings = [
            j for j in maint_only_indices
            if j != keep_idx and _status_value(rows[j]) == _status_value(row)
        ]
        if len(dup_siblings) >= 1 and idx in dup_siblings:
            return "collapsed_duplicate_maintenance"
    return "collapsed_intermediate_maintenance"


def collapse_business_transactions(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Collapse maintenance-only chains per business key/month.

    Returns (rows_for_model_h, full_audit).
    """
    if df.empty:
        return df, pd.DataFrame(columns=AUDIT_COLUMNS)

    work = _normalize_group_cols(df)
    group_cols = _resolve_business_keys(work)
    if len(group_cols) < 5:
        return work, pd.DataFrame(columns=AUDIT_COLUMNS)

    kept_rows: list[dict[str, Any]] = []
    audit_rows: list[dict[str, Any]] = []

    for _, grp in work.groupby(group_cols, dropna=False, sort=False):
        rows = [_business_sort(grp).iloc[i] for i in range(len(grp))]
        n = len(rows)
        maint_indices = [i for i, r in enumerate(rows) if _is_maintenance_only(r)]
        biz_indices = [i for i, r in enumerate(rows) if not _is_maintenance_only(r)]

        if n == 1:
            keep_idx = 0
            reason = "no_collapse_needed"
        elif biz_indices:
            keep_idx = biz_indices[-1]
            reason = "kept_final_business_state"
        else:
            keep_idx = n - 1
            reason = "kept_final_business_state"

        status_after = _status_value(rows[keep_idx])
        maint_count = len(maint_indices)

        for i, row in enumerate(rows):
            if i == keep_idx:
                row_reason = reason if n > 1 else "no_collapse_needed"
                kept = row.to_dict()
                kept["collapsed_event_count"] = n
                kept["collapsed_maintenance_only_count"] = maint_count
                kept_rows.append(kept)
            else:
                row_reason = _classify_suppressed(
                    row, idx=i, keep_idx=keep_idx, rows=rows, maint_only_indices=maint_indices,
                )

            audit_rows.append({
                "issuer": str(row.get("issuer", "")),
                "policy_id": str(row.get("policy_id", "") or row.get("enrollment_id", "")),
                "member_id": str(row.get("member_id", "") or row.get("enrollee_id", "")),
                "insurance_type": str(row.get("insurance_type", "")),
                "year": str(row.get("year", "")),
                "month": _zmonth(str(row.get("month", ""))),
                "group_original_row_count": n,
                "status_before": _status_value(row),
                "status_after": status_after,
                "maintenance_code": _maintenance_code(row),
                "benefit_effective_date": str(row.get("benefit_effective_date", "") or ""),
                "benefit_end_date": str(row.get("benefit_end_date", "") or ""),
                "source_file": _source_file(row),
                "kept_for_model_h": i == keep_idx,
                "collapsed_event_count": n,
                "collapsed_maintenance_only_count": maint_count,
                "reason": row_reason if i != keep_idx else (reason if n > 1 else "no_collapse_needed"),
            })

    collapsed = pd.DataFrame(kept_rows) if kept_rows else pd.DataFrame(columns=work.columns)
    audit = pd.DataFrame(audit_rows)
    return collapsed, audit


def _build_summary(
    original: pd.DataFrame,
    collapsed: pd.DataFrame,
    audit: pd.DataFrame,
    *,
    applied: bool,
    warning: str = "",
) -> dict[str, Any]:
    original_n = len(original)
    collapsed_n = len(collapsed)
    removed = max(original_n - collapsed_n, 0)
    pol_col = find_col(original, "policy_id", "enrollment_id") or "policy_id"
    affected = 0
    if not audit.empty and pol_col:
        suppressed = audit[~audit["kept_for_model_h"]]
        if not suppressed.empty and "policy_id" in suppressed.columns:
            affected = int(suppressed["policy_id"].astype(str).str.strip().replace("", pd.NA).dropna().nunique())

    maint_collapsed = 0
    if not audit.empty:
        maint_collapsed = int(
            audit[~audit["kept_for_model_h"]]["reason"]
            .astype(str)
            .str.contains("maintenance", case=False)
            .sum()
        )

    reason_counts: dict[str, int] = {}
    if not audit.empty:
        reason_counts = (
            audit[~audit["kept_for_model_h"]]["reason"].value_counts().to_dict()
        )

    return {
        "original_row_count": original_n,
        "collapsed_row_count": collapsed_n,
        "rows_removed_from_model_h": removed,
        "affected_enrollments": affected,
        "maintenance_only_collapsed_count": maint_collapsed,
        "applied": applied,
        "warning": warning,
        "reason_breakdown": reason_counts,
        "pct_removed": round(100.0 * removed / max(original_n, 1), 2),
    }


def apply_business_transaction_collapse(
    df: pd.DataFrame,
    *,
    fallback_latest_state_fn: Any | None = None,
) -> CollapseResult:
    """
    Apply collapse with 10% safety guard on Model H enrollment footprint.

    If enrollment count impact exceeds threshold, returns un-collapsed fallback input.
    """
    if df.empty:
        return CollapseResult(collapsed=df, applied=False)

    before_input = fallback_latest_state_fn(df) if fallback_latest_state_fn else df.copy()

    collapsed, audit = collapse_business_transactions(df)
    summary = _build_summary(df, collapsed, audit, applied=True)

    from azure_reconciliation.reconciliation_analysis import _chandra_dashboard

    before_mh = _chandra_dashboard(before_input, source="xml")
    after_mh = _chandra_dashboard(collapsed, source="xml")
    before_e = int(before_mh["enrollment_count"].sum()) if not before_mh.empty else 0
    after_e = int(after_mh["enrollment_count"].sum()) if not after_mh.empty else 0
    enroll_delta_pct = abs(before_e - after_e) / max(before_e, 1)

    if enroll_delta_pct > COLLAPSE_SAFETY_THRESHOLD:
        warning = (
            f"Collapse would change enrollment footprint by {before_e} → {after_e} "
            f"({enroll_delta_pct:.1%}) — exceeds {COLLAPSE_SAFETY_THRESHOLD:.0%} "
            "safety threshold; not applied."
        )
        logger.warning(warning)
        summary = _build_summary(df, before_input, audit, applied=False, warning=warning)
        return CollapseResult(
            collapsed=before_input,
            audit=audit,
            summary=summary,
            applied=False,
            before_model_h_input=before_input,
            warning=warning,
        )

    logger.info(
        "Business transaction collapse applied: %d → %d rows; enrollment footprint %d → %d",
        len(df), len(collapsed), before_e, after_e,
    )
    return CollapseResult(
        collapsed=collapsed,
        audit=audit,
        summary=summary,
        applied=True,
        before_model_h_input=before_input,
    )


def _display_status_counts(
    model_h: pd.DataFrame,
    *,
    issuer: str | None = None,
    year: str | None = None,
    month: str | None = None,
) -> dict[str, int]:
    if model_h.empty:
        return {"CONFIRM": 0, "CANCEL": 0, "TERM": 0}
    work = model_h.copy()
    if issuer and "issuer" in work.columns:
        work = work[work["issuer"].astype(str) == str(issuer)]
    if year and "year" in work.columns:
        work = work[work["year"].astype(str) == str(year)]
    if month and "month" in work.columns:
        work = work[work["month"].astype(str).map(_zmonth) == _zmonth(month)]
    work["_display"] = work["status"].astype(str).map(
        lambda s: STATUS_TO_CHANDRA_DISPLAY.get(normalize_status(s), str(s).upper()),
    )
    counts: dict[str, int] = {}
    for disp in ("CONFIRM", "CANCEL", "TERM"):
        sub = work[work["_display"] == disp]
        counts[disp] = int(sub["enrollment_count"].sum()) if "enrollment_count" in sub.columns else 0
    return counts


def write_collapse_summary_md(summary: dict[str, Any], path: Path, *, title: str = "") -> None:
    generated = datetime.now(timezone.utc).isoformat(timespec="seconds")
    lines = [
        f"# {title or 'Business Transaction Collapse Summary'}",
        "",
        f"**Generated:** {generated}",
        f"**Applied:** {summary.get('applied', False)}",
        "",
    ]
    if summary.get("warning"):
        lines.extend([f"**Warning:** {summary['warning']}", ""])

    lines.extend([
        f"- Original row count: {summary.get('original_row_count', 0)}",
        f"- Collapsed row count: {summary.get('collapsed_row_count', 0)}",
        f"- Rows removed from Model H input: {summary.get('rows_removed_from_model_h', 0)}",
        f"- Percent removed: {summary.get('pct_removed', 0)}%",
        f"- Affected enrollments: {summary.get('affected_enrollments', 0)}",
        f"- Maintenance-only collapsed: {summary.get('maintenance_only_collapsed_count', 0)}",
        "",
        "## Reason breakdown (suppressed rows)",
        "",
    ])
    breakdown = summary.get("reason_breakdown") or {}
    if breakdown:
        for reason, count in sorted(breakdown.items(), key=lambda x: (-x[1], x[0])):
            lines.append(f"- {reason}: {count}")
    else:
        lines.append("- (none)")
    lines.append("")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_before_after_md(
    before_counts: dict[str, int],
    after_counts: dict[str, int],
    path: Path,
    *,
    issuer: str,
    year: str,
    month: str,
) -> None:
    generated = datetime.now(timezone.utc).isoformat(timespec="seconds")
    lines = [
        f"# Model H Before/After Collapse — {issuer} / {year} / {_zmonth(month)}",
        "",
        f"**Generated:** {generated}",
        "",
        "| Status | Before | After | Delta |",
        "|--------|--------|-------|-------|",
    ]
    for status in ("CONFIRM", "CANCEL", "TERM"):
        b = before_counts.get(status, 0)
        a = after_counts.get(status, 0)
        lines.append(f"| {status} | {b} | {a} | {a - b} |")
    lines.append("")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_collapse_audits(
    result: CollapseResult,
    *,
    issuer: str,
    partitions: list[Any] | None = None,
) -> dict[str, str]:
    """Write global and per-month collapse diagnostics."""
    paths: dict[str, str] = {}
    debug = _debug_dir()

    xlsx_global = debug / "business_transaction_collapse_audit.xlsx"
    safe_write_excel(
        xlsx_global,
        {
            "collapse_audit": result.audit,
            "summary": pd.DataFrame([result.summary]),
        },
        drop_duplicate_value_columns=False,
    )
    paths["audit_xlsx"] = str(xlsx_global)

    md_global = debug / "business_transaction_collapse_summary.md"
    write_collapse_summary_md(result.summary, md_global)
    paths["summary_md"] = str(md_global)

    if partitions:
        assets_root = settings.assets_path
        for part in partitions:
            ym_audit = result.audit
            if not ym_audit.empty and "year" in ym_audit.columns and "month" in ym_audit.columns:
                ym_audit = ym_audit[
                    (ym_audit["issuer"].astype(str) == str(part.issuer))
                    & (ym_audit["year"].astype(str) == str(part.year))
                    & (ym_audit["month"].astype(str).map(_zmonth) == _zmonth(part.month))
                ]
            month_diag = (
                assets_root / str(part.issuer) / str(part.year) / _zmonth(part.month)
                / "reports" / "diagnostics"
            )
            month_diag.mkdir(parents=True, exist_ok=True)
            month_xlsx = month_diag / "business_transaction_collapse_audit.xlsx"
            safe_write_excel(
                month_xlsx,
                {"collapse_audit": ym_audit},
                drop_duplicate_value_columns=False,
            )
            month_md = month_diag / "business_transaction_collapse_summary.md"
            part_summary = {**result.summary, "partition": part.label()}
            write_collapse_summary_md(
                part_summary,
                month_md,
                title=f"Business Transaction Collapse — {part.label()}",
            )
            paths[f"assets_{part.label()}_xlsx"] = str(month_xlsx)
            paths[f"assets_{part.label()}_md"] = str(month_md)

    if result.applied and not result.before_model_h_input.empty and not result.collapsed.empty:
        from azure_reconciliation.reconciliation_analysis import _chandra_dashboard

        y, m = "2026", "01"
        iss = issuer
        if partitions:
            iss = str(partitions[0].issuer)
            y = str(partitions[0].year)
            m = _zmonth(partitions[0].month)
            for p in partitions:
                if p.year == "2026" and _zmonth(p.month) == "01":
                    iss, y, m = str(p.issuer), str(p.year), _zmonth(p.month)
                    break

        before_mh = _chandra_dashboard(result.before_model_h_input, source="xml")
        after_mh = _chandra_dashboard(result.collapsed, source="xml")
        before_counts = _display_status_counts(before_mh, issuer=iss, year=y, month=m)
        after_counts = _display_status_counts(after_mh, issuer=iss, year=y, month=m)
        if any(after_counts.get(s, 0) != before_counts.get(s, 0) for s in ("CONFIRM", "CANCEL", "TERM")):
            ba_path = debug / f"model_h_before_after_collapse_{iss}_{y}_{m}.md"
            write_before_after_md(
                before_counts, after_counts, ba_path,
                issuer=iss, year=y, month=m,
            )
            paths["before_after_md"] = str(ba_path)

    return paths
