"""
Dashboard difference analysis — classify remaining Chandra vs current enrollment gaps.

Read-only diagnostics. Does not modify parser, canonical, cleanup, lifecycle, Model H, or reports.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from azure_reconciliation.chandra_business_format import STATUS_TO_CHANDRA_DISPLAY
from azure_reconciliation.df_utils import find_col, normalize_id_series
from azure_reconciliation.lifecycle_snapshot_comparison import _sort_chronological
from azure_reconciliation.partition_discovery import Partition, discover_partitions
from azure_reconciliation.record_comparison import LIFECYCLE_PRIMARY_JOIN
from azure_reconciliation.reconciliation_analysis import (
    XML_ENROLLMENT_ID_COLS,
    _coalesce_id_series,
)
from azure_reconciliation.safe_export import safe_write_excel
from azure_reconciliation.status_mapper import normalize_insurance_type, normalize_status
from azure_reconciliation.xml_business_reports import (
    PK,
    process_issuer_xml_business,
)
from azure_reconciliation.xml_loader import load_xml_rows
from config.config import settings
from reporting.enrollment_comparison import CHANDRA_REFERENCE_15105
from utils.logger import get_logger

logger = get_logger(__name__)

DISPLAY_STATUSES = ("CONFIRM", "CANCEL", "TERM")

# Business-approved expected enrollment counts (Chandra screenshots / reference).
EXPECTED_ENROLLMENT_COUNTS: dict[tuple[str, str, str, str], dict[str, int]] = {
    ("15105", "2026", "01", "HEALTH"): {
        "CONFIRM": 1240,
        "CANCEL": 47,
        "TERM": 217,
    },
}

OUTPUT_COLUMNS = [
    "issuer",
    "policy_id",
    "member_id",
    "maintenance_code",
    "status",
    "benefit_effective_date",
    "member_maintenance_effective_date",
    "source_file",
    "duplicate_flag",
    "maintenance_only_flag",
    "superseded_flag",
    "latest_state_flag",
    "difference_bucket",
    "display_status",
    "reference_display_status",
    "enrollment_id",
]


def _debug_dir() -> Path:
    d = settings.outputs_path / "debug"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _zmonth(m: str) -> str:
    return str(m).strip().zfill(2)


def _to_display_status(raw: str) -> str:
    key = str(raw or "").strip().upper()
    if key in STATUS_TO_CHANDRA_DISPLAY:
        return STATUS_TO_CHANDRA_DISPLAY[key]
    normalized = normalize_status(key)
    return STATUS_TO_CHANDRA_DISPLAY.get(normalized, STATUS_TO_CHANDRA_DISPLAY.get(key, "UNKNOWN"))


def _enrollment_id_series(df: pd.DataFrame) -> pd.Series:
    return _coalesce_id_series(df, XML_ENROLLMENT_ID_COLS)


def _ids_from_df(df: pd.DataFrame) -> set[str]:
    if df.empty:
        return set()
    ids = _enrollment_id_series(df).astype(str).str.strip()
    return set(ids[ids != ""])


def _filter_business_month(
    df: pd.DataFrame,
    *,
    issuer: str,
    year: str,
    month: str,
    insurance_type: str = "HEALTH",
) -> pd.DataFrame:
    if df.empty:
        return df
    mask = df.get("issuer", pd.Series(dtype=str)).astype(str) == str(issuer)
    if "year" in df.columns:
        mask &= df["year"].astype(str) == str(year)
    if "month" in df.columns:
        mask &= df["month"].astype(str).map(_zmonth) == _zmonth(month)
    if "insurance_type" in df.columns:
        mask &= df["insurance_type"].astype(str).map(normalize_insurance_type) == insurance_type
    return df[mask].copy()


def _maintenance_code(row: pd.Series) -> str:
    for col in ("maintenance_type_code", "action_code", "enrollment_action_code"):
        if col in row.index and str(row.get(col, "") or "").strip():
            return str(row[col]).strip()
    return ""


def _source_file(row: pd.Series) -> str:
    for col in ("source_file", "file_name", "raw_xml_path"):
        if col in row.index and str(row.get(col, "") or "").strip():
            return str(row[col]).strip()
    return ""


def _build_reference_status_map(
    lifecycle_snapshots: pd.DataFrame,
    *,
    issuer: str,
    year: str,
    month: str,
) -> dict[str, str]:
    """Reference enrollment → display status from partition lifecycle snapshot."""
    if lifecycle_snapshots.empty:
        return {}
    snap = lifecycle_snapshots.copy()
    mask = snap.get("issuer", pd.Series(dtype=str)).astype(str) == str(issuer)
    if "coverage_year" in snap.columns:
        mask &= snap["coverage_year"].astype(str) == str(year)
    if "snapshot_month" in snap.columns:
        mask &= snap["snapshot_month"].astype(str).map(_zmonth) == _zmonth(month)
    snap = snap[mask].copy()
    if snap.empty:
        return {}
    if "enrollment_id" in snap.columns and "policy_id" not in snap.columns:
        snap["policy_id"] = snap["enrollment_id"]
    snap["_enrollment_id"] = _enrollment_id_series(snap)
    snap["_display_status"] = snap.get("canonical_status", "UNKNOWN").astype(str).map(_to_display_status)
    out: dict[str, str] = {}
    for _, row in snap.iterrows():
        eid = str(row["_enrollment_id"]).strip()
        if not eid:
            continue
        out[eid] = str(row["_display_status"])
    return out


def _build_latest_state_keys(
    canonical: pd.DataFrame,
    *,
    issuer: str,
    year: str,
    month: str,
) -> set[str]:
    scoped = _filter_business_month(canonical, issuer=issuer, year=year, month=month)
    if scoped.empty:
        return set()
    sorted_work = _sort_chronological(scoped)
    final = sorted_work.groupby(
        [k for k in PK if k in sorted_work.columns],
        dropna=False,
        as_index=False,
    ).last()
    keys: set[str] = set()
    for _, row in final.iterrows():
        eid = str(_enrollment_id_series(pd.DataFrame([row])).iloc[0]).strip()
        if eid:
            keys.add(eid)
    return keys


def _enrollment_detail_rows(
    lifecycle_input: pd.DataFrame,
    canonical: pd.DataFrame,
    *,
    issuer: str,
    year: str,
    month: str,
    insurance_type: str,
    duplicate_df: pd.DataFrame,
    maintenance_df: pd.DataFrame,
    superseded_df: pd.DataFrame,
    reference_status: dict[str, str],
    latest_state_keys: set[str],
) -> pd.DataFrame:
    """One representative row per enrollment in Model H input for the business month."""
    scoped = _filter_business_month(
        lifecycle_input, issuer=issuer, year=year, month=month, insurance_type=insurance_type,
    )
    if scoped.empty:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    dup_ids = _ids_from_df(duplicate_df)
    maint_ids = _ids_from_df(maintenance_df)
    sup_ids = _ids_from_df(superseded_df)

    scoped = scoped.copy()
    scoped["_enrollment_id"] = _enrollment_id_series(scoped)
    scoped["_display_status"] = scoped.get("status", "UNKNOWN").astype(str).map(_to_display_status)

    canon_scoped = _filter_business_month(
        canonical, issuer=issuer, year=year, month=month, insurance_type=insurance_type,
    )
    canon_scoped = canon_scoped.copy()
    if not canon_scoped.empty:
        canon_scoped["_enrollment_id"] = _enrollment_id_series(canon_scoped)

    rows: list[dict[str, Any]] = []
    for eid, grp in scoped.groupby("_enrollment_id", dropna=False):
        eid = str(eid).strip()
        if not eid:
            continue
        row = grp.iloc[-1]
        canon_rows = canon_scoped[canon_scoped["_enrollment_id"].astype(str) == eid] if not canon_scoped.empty else pd.DataFrame()
        canon_row = canon_rows.iloc[-1] if not canon_rows.empty else row

        policy = str(row.get("policy_id") or row.get("enrollment_id") or eid)
        member = str(row.get("member_id") or row.get("enrollee_id") or "")

        rows.append({
            "issuer": issuer,
            "policy_id": policy,
            "member_id": member,
            "maintenance_code": _maintenance_code(canon_row),
            "status": str(row.get("status") or canon_row.get("status") or ""),
            "benefit_effective_date": str(
                row.get("benefit_effective_date") or canon_row.get("benefit_effective_date") or ""
            ),
            "member_maintenance_effective_date": str(
                row.get("member_maint_effective_date")
                or canon_row.get("member_maint_effective_date")
                or ""
            ),
            "source_file": _source_file(canon_row) or _source_file(row),
            "duplicate_flag": eid in dup_ids,
            "maintenance_only_flag": eid in maint_ids,
            "superseded_flag": eid in sup_ids,
            "latest_state_flag": eid in latest_state_keys,
            "difference_bucket": "",
            "display_status": str(grp["_display_status"].iloc[0]),
            "reference_display_status": reference_status.get(eid, ""),
            "enrollment_id": eid,
            "month_basis_used": str(canon_row.get("month_basis_used", "") if not canon_rows.empty else ""),
            "source_folder_month": str(canon_row.get("month", "") if not canon_rows.empty else row.get("month", "")),
            "source_folder_year": str(canon_row.get("year", "") if not canon_rows.empty else row.get("year", "")),
        })

    return pd.DataFrame(rows)


def _classify_extra_row(row: pd.Series, *, business_month: str) -> str:
    """Classify with explicit business month for folder comparison."""
    ref = str(row.get("reference_display_status") or "").strip()
    disp = str(row.get("display_status") or "").strip()
    if bool(row.get("duplicate_flag")):
        return "duplicate transaction"
    if bool(row.get("maintenance_only_flag")):
        return "maintenance-only"
    if bool(row.get("superseded_flag")):
        return "superseded event"
    if ref and ref != disp:
        return "status transition issue"
    if not ref:
        return "latest state missing"
    basis = str(row.get("month_basis_used") or "")
    if basis and basis not in ("file_event_year_month",):
        return "month assignment issue"
    src_m = _zmonth(str(row.get("source_folder_month") or ""))
    if src_m and src_m != _zmonth(business_month):
        return "month assignment issue"
    if not bool(row.get("latest_state_flag")):
        return "older transaction kept instead of latest"
    bef = str(row.get("benefit_effective_date") or "").strip()
    mem = str(row.get("member_maintenance_effective_date") or "").strip()
    if bef and mem and bef[:7] != mem[:7]:
        return "effective date issue"
    return "unknown"


def _current_counts_by_display(
    lifecycle_input: pd.DataFrame,
    *,
    issuer: str,
    year: str,
    month: str,
    insurance_type: str,
) -> dict[str, int]:
    scoped = _filter_business_month(
        lifecycle_input, issuer=issuer, year=year, month=month, insurance_type=insurance_type,
    )
    if scoped.empty:
        return {s: 0 for s in DISPLAY_STATUSES}
    scoped = scoped.copy()
    scoped["_enrollment_id"] = _enrollment_id_series(scoped)
    scoped["_display_status"] = scoped.get("status", "UNKNOWN").astype(str).map(_to_display_status)
    counts: dict[str, int] = {}
    for status in DISPLAY_STATUSES:
        sub = scoped[scoped["_display_status"] == status]
        counts[status] = int(sub["_enrollment_id"].astype(str).str.strip().replace("", pd.NA).dropna().nunique())
    return counts


def _identify_extra_enrollment_ids(
    detail: pd.DataFrame,
    *,
    display_status: str,
    expected_count: int,
    reference_status: dict[str, str],
) -> set[str]:
    """Enrollments in current dashboard status that explain actual − expected gap."""
    current_rows = detail[detail["display_status"] == display_status].copy()
    current_ids = set(current_rows["enrollment_id"].astype(str).str.strip()) - {""}
    actual = len(current_ids)
    delta = actual - expected_count
    if delta <= 0:
        return set()

    reference_ids = {eid for eid, st in reference_status.items() if st == display_status}
    extras = current_ids - reference_ids

    if len(extras) < delta:
        for eid in current_ids:
            ref_st = reference_status.get(eid, "")
            if ref_st and ref_st != display_status:
                extras.add(eid)

    if len(extras) < delta:
        flagged = current_rows[
            current_rows["duplicate_flag"]
            | current_rows["maintenance_only_flag"]
            | current_rows["superseded_flag"]
            | ~current_rows["latest_state_flag"]
        ]
        extras |= set(flagged["enrollment_id"].astype(str).str.strip()) - {""}

    if len(extras) > delta:
        scored: list[tuple[int, str]] = []
        for eid in extras:
            row = current_rows[current_rows["enrollment_id"] == eid].iloc[0]
            score = sum([
                10 if row.get("duplicate_flag") else 0,
                8 if row.get("maintenance_only_flag") else 0,
                7 if row.get("superseded_flag") else 0,
                5 if not row.get("latest_state_flag") else 0,
                4 if reference_status.get(eid, "") != display_status else 0,
                3 if not reference_status.get(eid) else 0,
            ])
            scored.append((score, eid))
        scored.sort(reverse=True)
        extras = {eid for _, eid in scored[:delta]}

    return extras


def _sheet_for_status(
    detail: pd.DataFrame,
    extra_ids: set[str],
    *,
    business_month: str,
) -> pd.DataFrame:
    if not extra_ids:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)
    rows = detail[detail["enrollment_id"].astype(str).isin(extra_ids)].copy()
    rows["difference_bucket"] = rows.apply(
        lambda r: _classify_extra_row(r, business_month=business_month), axis=1,
    )
    out = rows[[c for c in OUTPUT_COLUMNS if c in rows.columns]]
    return out.sort_values(["difference_bucket", "enrollment_id"]).reset_index(drop=True)


def _bucket_counts(extra_df: pd.DataFrame) -> dict[str, int]:
    if extra_df.empty or "difference_bucket" not in extra_df.columns:
        return {}
    return extra_df["difference_bucket"].value_counts().to_dict()


def _expected_from_reference(
    issuer: str,
    year: str,
    month: str,
) -> dict[str, int] | None:
    key = (issuer, year, _zmonth(month), "HEALTH")
    if key in EXPECTED_ENROLLMENT_COUNTS:
        return EXPECTED_ENROLLMENT_COUNTS[key]
    load_date = f"{int(month)}/1/{int(year)}"
    counts = {s: 0 for s in DISPLAY_STATUSES}
    for row in CHANDRA_REFERENCE_15105:
        if (
            str(row.get("GAA_HIOS_ID")) == issuer
            and str(row.get("GAA_Load_Date")) == load_date
            and str(row.get("Insurance_Type", "")).lower() == "health"
        ):
            st = str(row.get("enrolleeStatus", "")).upper()
            if st in counts:
                counts[st] = int(row.get("Enrollment_Count", 0))
    if any(counts.values()):
        return counts
    return None


def run_dashboard_difference_analysis(
    *,
    issuer: str = "15105",
    year: str = "2026",
    month: str = "01",
    insurance_type: str = "HEALTH",
    parse_source: bool = False,
    expected_counts: dict[str, int] | None = None,
) -> dict[str, Any]:
    """
    Classify enrollments explaining current vs Chandra dashboard enrollment gaps.
    Does not change any business logic.
    """
    settings.ensure_dirs()
    zm = _zmonth(month)
    expected = expected_counts or _expected_from_reference(issuer, year, zm)
    if not expected:
        raise RuntimeError(f"No expected counts configured for {issuer}/{year}/{zm}")

    partitions = discover_partitions(settings.source_data_path, issuer_filter=issuer)
    xml_raw = load_xml_rows(prefer_staging=not parse_source, issuer_filter=issuer)
    if xml_raw.empty:
        raise RuntimeError(f"No XML rows for issuer {issuer}")

    if not partitions:
        partitions = [Partition(issuer=issuer, year=year, month=zm)]

    result = process_issuer_xml_business(issuer, xml_raw, partitions)

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
        result.lifecycle_input, issuer=issuer, year=year, month=zm, insurance_type=insurance_type,
    )

    sheets: dict[str, pd.DataFrame] = {}
    summary_sections: list[str] = []

    for display_status in DISPLAY_STATUSES:
        exp = int(expected.get(display_status, 0))
        act = int(actual_counts.get(display_status, 0))
        delta = act - exp
        extra_ids = _identify_extra_enrollment_ids(
            detail,
            display_status=display_status,
            expected_count=exp,
            reference_status=reference_status,
        )
        sheet_df = _sheet_for_status(detail, extra_ids, business_month=zm)
        sheet_name = f"{display_status.title()}_Extra"
        if display_status == "CONFIRM":
            sheet_name = "Confirm_Extra"
        elif display_status == "CANCEL":
            sheet_name = "Cancel_Extra"
        elif display_status == "TERM":
            sheet_name = "Term_Extra"
        sheets[sheet_name] = sheet_df

        buckets = _bucket_counts(sheet_df)
        lines = [
            f"## {display_status}",
            "",
            f"Expected: {exp}",
            f"Actual: {act}",
            "",
            f"Difference: {delta}",
            "",
            "Breakdown",
            "",
        ]
        if buckets:
            for bucket, count in sorted(buckets.items(), key=lambda x: (-x[1], x[0])):
                lines.append(f"{count} {bucket}")
        else:
            lines.append("(no extra enrollments identified)" if delta <= 0 else f"{len(extra_ids)} extra enrollments (see workbook)")
        lines.append("")
        summary_sections.extend(lines)

    xlsx_path = _debug_dir() / "dashboard_difference_analysis.xlsx"
    safe_write_excel(xlsx_path, sheets, drop_duplicate_value_columns=False)

    md_path = _debug_dir() / "dashboard_difference_summary.md"
    generated = datetime.now(timezone.utc).isoformat(timespec="seconds")
    md_lines = [
        "# Dashboard Difference Summary",
        "",
        f"**Issuer / month:** {issuer} / {year} / {zm}",
        f"**Generated:** {generated}",
        f"**Insurance type:** {insurance_type}",
        "",
        "Read-only classification — no records removed.",
        "",
    ] + summary_sections
    md_path.write_text("\n".join(md_lines) + "\n", encoding="utf-8")
    logger.info("Wrote dashboard difference analysis → %s", xlsx_path)
    logger.info("Wrote dashboard difference summary → %s", md_path)

    return {
        "xlsx": str(xlsx_path),
        "summary_md": str(md_path),
        "actual_counts": actual_counts,
        "expected_counts": expected,
        "sheets": {k: len(v) for k, v in sheets.items()},
    }
