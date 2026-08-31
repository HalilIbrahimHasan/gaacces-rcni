"""
Record-level XML vs Azure comparison — ID overlap diagnostics and auto join selection.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from azure_reconciliation.azure_mirror.discovery.table_inspector import TableProfile
from azure_reconciliation.df_utils import (
    col_series as _col_series,
    find_col,
    normalize_id,
    normalize_id_series,
    zmonth as _zmonth,
)
from azure_reconciliation.safe_export import safe_write_csv
from azure_reconciliation.status_mapper import normalize_insurance_type, normalize_status
from config.config import settings
from utils.logger import get_logger

logger = get_logger(__name__)

PRIMARY_JOIN = ["issuer", "policy_id", "member_id", "insurance_type", "year", "month"]
LIFECYCLE_PRIMARY_JOIN = ["issuer", "policy_id", "member_id", "insurance_type"]

XML_ID_CANDIDATES = [
    "policy_id",
    "member_id",
    "subscriber_id",
    "exchg_assigned_policy_id",
    "exchg_subscriber_identifier",
    "exchg_indiv_identifier",
    "health_coverage_policy_no",
    "healthCoveragePolicyID",
    "exchg_assigned_enrollee_id",
    "household_or_employee_case_id",
    "issuer_subscriber_identifier",
    "issuer_indiv_identifier",
]

AZURE_ID_CANDIDATES = [
    "exchgAssignedPolicyID",
    "exchgSubscriberIdentifier",
    "exchgIndivIdentifier",
    "healthCoveragePolicyID",
    "memberSSN",
]

# Required pairwise comparisons (left XML logical name -> right Azure logical name)
REQUIRED_OVERLAP_PAIRS: list[tuple[list[str], list[str]]] = [
    (["policy_id", "exchg_assigned_policy_id"], ["exchgAssignedPolicyID"]),
    (["policy_id", "exchg_assigned_policy_id"], ["healthCoveragePolicyID"]),
    (["health_coverage_policy_no"], ["healthCoveragePolicyID"]),
    (["health_coverage_policy_no"], ["exchgAssignedPolicyID"]),
    (["member_id", "exchg_indiv_identifier", "issuer_indiv_identifier"], ["exchgIndivIdentifier"]),
    (["member_id", "exchg_indiv_identifier"], ["exchgSubscriberIdentifier"]),
    (["subscriber_id", "exchg_subscriber_identifier", "issuer_subscriber_identifier"], ["exchgSubscriberIdentifier"]),
    (["subscriber_id", "exchg_subscriber_identifier"], ["exchgIndivIdentifier"]),
]

DIAGNOSTIC_JOINS: list[tuple[str, list[str]]] = [
    ("issuer_policy_member_insurance", ["issuer", "policy_id", "member_id", "insurance_type"]),
    ("issuer_policy_subscriber_insurance", ["issuer", "policy_id", "subscriber_id", "insurance_type"]),
    ("issuer_member_insurance", ["issuer", "member_id", "insurance_type"]),
    ("issuer_subscriber_insurance", ["issuer", "subscriber_id", "insurance_type"]),
]

JOIN_EXCLUDE_FROM_SELECTION = frozenset({"memberSSN"})


@dataclass
class JoinMapping:
    xml_policy_col: str
    azure_policy_col: str
    xml_member_col: str
    azure_member_col: str
    overlap_score: float = 0.0
    full_join_match_count: int = 0
    reliable: bool = False

    def label(self) -> str:
        return (
            f"issuer + {self.xml_policy_col}->{self.azure_policy_col} + "
            f"{self.xml_member_col}->{self.azure_member_col} + insurance_type + year + month"
        )


def join_key_series(df: pd.DataFrame, keys: list[str]) -> pd.Series:
    if df is None or df.empty:
        return pd.Series(dtype=str)
    parts: list[pd.Series] = []
    for key in keys:
        if key in df.columns:
            parts.append(normalize_id_series(df[key]))
        else:
            parts.append(pd.Series([""] * len(df), index=df.index, dtype=str))
    if not parts:
        return pd.Series([""] * len(df), index=df.index, dtype=str)
    out = parts[0]
    for part in parts[1:]:
        out = out + "|" + part
    return out


def _resolve_id_columns(df: pd.DataFrame, candidates: list[str]) -> dict[str, str]:
    """Map logical name -> actual column name present in df."""
    out: dict[str, str] = {}
    for cand in candidates:
        col = find_col(df, cand)
        if col and col not in out.values():
            out[cand] = col
    return out


def _id_value_set(df: pd.DataFrame, col: str) -> set[str]:
    if col not in df.columns or df.empty:
        return set()
    vals = normalize_id_series(df[col])
    return set(vals[vals != ""].unique())


def _overlap_row(
    xml_df: pd.DataFrame,
    az_df: pd.DataFrame,
    left_col: str,
    right_col: str,
) -> dict[str, Any]:
    xl = _id_value_set(xml_df, left_col)
    al = _id_value_set(az_df, right_col)
    overlap = xl & al
    rate = len(overlap) / max(len(xl), len(al), 1)
    return {
        "left_column": left_col,
        "right_column": right_col,
        "xml_distinct": len(xl),
        "azure_distinct": len(al),
        "overlap_count": len(overlap),
        "overlap_rate": round(rate, 4),
        "sample_matches": "|".join(sorted(overlap)[:5]),
        "sample_xml_only": "|".join(sorted(xl - al)[:5]),
        "sample_azure_only": "|".join(sorted(al - xl)[:5]),
    }


def build_id_overlap_matrix(xml_raw: pd.DataFrame, table_df: pd.DataFrame) -> pd.DataFrame:
    """Compare every XML ID candidate vs every Azure ID candidate."""
    rows: list[dict[str, Any]] = []
    xml_cols = _resolve_id_columns(xml_raw, XML_ID_CANDIDATES)
    az_cols = _resolve_id_columns(table_df, AZURE_ID_CANDIDATES)

    seen: set[tuple[str, str]] = set()

    def _add(left_names: list[str], right_names: list[str]) -> None:
        for ln in left_names:
            lc = xml_cols.get(ln) or find_col(xml_raw, ln)
            if not lc:
                continue
            for rn in right_names:
                rc = az_cols.get(rn) or find_col(table_df, rn)
                if not rc:
                    continue
                key = (lc, rc)
                if key in seen:
                    continue
                seen.add(key)
                rows.append(_overlap_row(xml_raw, table_df, lc, rc))

    for left_names, right_names in REQUIRED_OVERLAP_PAIRS:
        _add(left_names, right_names)

    for lc in xml_cols.values():
        for rc in az_cols.values():
            _add([lc], [rc])

    if not rows:
        return pd.DataFrame(columns=[
            "left_column", "right_column", "xml_distinct", "azure_distinct",
            "overlap_count", "overlap_rate", "sample_matches", "sample_xml_only", "sample_azure_only",
        ])
    return pd.DataFrame(rows).sort_values(
        ["overlap_count", "overlap_rate"], ascending=False,
    ).reset_index(drop=True)


def _id_frequency(df: pd.DataFrame, col: str, source: str) -> pd.DataFrame:
    if col not in df.columns or df.empty:
        return pd.DataFrame()
    vals = normalize_id_series(df[col])
    vc = vals[vals != ""].value_counts().head(100).reset_index()
    vc.columns = ["id_value", "row_count"]
    vc.insert(0, "source", source)
    vc.insert(1, "column", col)
    return vc


def _insurance_series_xml(work: pd.DataFrame) -> pd.Series:
    for col_name in (
        "insurance_type_code", "insurance_type", "insurance_type_description",
        "planCoverageDescription", "PlanCoverageDescription",
    ):
        col = find_col(work, col_name)
        if col and col in work.columns:
            raw = work[col].astype(str).str.strip()
            if ((raw != "") & (raw.str.upper() != "NAN")).any():
                return raw.map(normalize_insurance_type)
    return pd.Series([""] * len(work), index=work.index)


def _insurance_series_azure(work: pd.DataFrame, profile: TableProfile) -> pd.Series:
    candidates: list[str] = []
    if profile.insurance_type_col:
        candidates.append(profile.insurance_type_col)
    candidates.extend([
        "Insurance_Type", "planCoverageDescription", "PlanCoverageDescription",
        "insurance_type", "insurance_type_code", "product_type",
    ])
    seen: set[str] = set()
    for col_name in candidates:
        if col_name in seen:
            continue
        col = find_col(work, col_name)
        if not col or col not in work.columns:
            continue
        seen.add(col)
        raw = work[col].astype(str).str.strip()
        if ((raw != "") & (raw.str.upper() != "NAN") & (raw.str.upper() != "NONE")).any():
            return work[col].astype(str).map(normalize_insurance_type)
    return pd.Series([""] * len(work), index=work.index)


def _month_series_xml(work: pd.DataFrame) -> pd.Series:
    return normalize_id_series(_col_series(work, "month", "source_month", "snapshot_month")).map(_zmonth)


def _month_series_azure(work: pd.DataFrame, profile: TableProfile, date_col: str) -> pd.Series:
    dc = find_col(work, date_col) or profile.file_date_col or "GAA_834_File_Date"
    dates = pd.to_datetime(_col_series(work, dc) if dc else pd.Series([None] * len(work)), errors="coerce")
    month_from_date = dates.dt.month.apply(lambda x: _zmonth(str(int(x))) if pd.notna(x) else "")
    fallback = _month_series_xml(work)
    return month_from_date.where(month_from_date != "", fallback)


def build_canonical_xml_records(
    xml_raw: pd.DataFrame,
    join_mapping: JoinMapping | None = None,
) -> pd.DataFrame:
    if xml_raw.empty:
        return pd.DataFrame()

    work = xml_raw.copy()
    rec: dict[str, pd.Series] = {
        "issuer": normalize_id_series(_col_series(work, "issuer", "issuer_id")),
        "year": normalize_id_series(_col_series(work, "year", "source_year", "coverage_year")),
        "month": _month_series_xml(work),
        "insurance_type": _insurance_series_xml(work),
    }

    for logical in XML_ID_CANDIDATES:
        col = find_col(work, logical)
        if col:
            rec[logical] = normalize_id_series(work[col])

    # Legacy aliases exposed separately
    for logical, aliases in (
        ("policy_id", ["policy_id"]),
        ("member_id", ["member_id"]),
        ("subscriber_id", ["subscriber_id"]),
    ):
        if logical not in rec:
            rec[logical] = normalize_id_series(_col_series(work, logical, *aliases))

    jm = join_mapping
    if jm:
        xp = find_col(work, jm.xml_policy_col) or jm.xml_policy_col
        xm = find_col(work, jm.xml_member_col) or jm.xml_member_col
        rec["policy_id"] = normalize_id_series(work[xp]) if xp in work.columns else pd.Series([""] * len(work), index=work.index)
        rec["member_id"] = normalize_id_series(work[xm]) if xm in work.columns else pd.Series([""] * len(work), index=work.index)
        rec["join_policy_source"] = pd.Series([xp] * len(work), index=work.index)
        rec["join_member_source"] = pd.Series([xm] * len(work), index=work.index)

    rec["normalized_status"] = _col_series(
        work, "additional_maint_reason_code", "coverage_status", "canonical_status",
    ).astype(str).map(normalize_status)
    rec["member_maint_effective_date"] = _date_ymd(
        _col_series(work, "member_maint_effective_date", "event_date")
    )
    rec["benefit_effective_date"] = _date_ymd(
        _col_series(work, "benefit_effective_date", "benefit_effective_begin_date")
    )
    rec["benefit_end_date"] = _date_ymd(
        _col_series(work, "benefit_end_date", "benefit_effective_end_date")
    )
    rec["event_date"] = rec["member_maint_effective_date"]
    rec["action_code"] = _col_series(
        work, "maintenance_type_code", "enrollment_action_code", "event_type_code",
    ).astype(str)
    rec["source_file"] = _col_series(work, "file_name", "raw_xml_path", "source_file").astype(str)
    rec["source_table"] = pd.Series(["xml"] * len(work), index=work.index)

    out = pd.DataFrame(rec)
    out["_record_key"] = join_key_series(out, PRIMARY_JOIN)
    return out


def build_canonical_azure_records(
    table_df: pd.DataFrame,
    profile: TableProfile,
    *,
    date_col: str = "GAA_834_File_Date",
    join_mapping: JoinMapping | None = None,
) -> pd.DataFrame:
    if table_df.empty:
        return pd.DataFrame()

    work = table_df.copy()
    dc = find_col(work, date_col) or profile.file_date_col or "GAA_834_File_Date"
    dates = pd.to_datetime(_col_series(work, dc) if dc else pd.Series([None] * len(work)), errors="coerce")

    rec: dict[str, pd.Series] = {
        "issuer": normalize_id_series(_col_series(work, profile.issuer_col or "GAA_HIOS_ID")),
        "year": normalize_id_series(_col_series(work, profile.year_col or "Coverage_Year")),
        "month": _month_series_azure(work, profile, dc or ""),
        "insurance_type": _insurance_series_azure(work, profile),
    }

    for logical in AZURE_ID_CANDIDATES:
        col = find_col(work, logical)
        if col:
            rec[logical] = normalize_id_series(work[col])

    sf = find_col(work, "subscriberFlag", "subscriber_flag")
    if sf:
        rec["subscriberFlag"] = work[sf].astype(str)

    jm = join_mapping
    if jm:
        ap = find_col(work, jm.azure_policy_col) or jm.azure_policy_col
        am = find_col(work, jm.azure_member_col) or jm.azure_member_col
        rec["policy_id"] = normalize_id_series(work[ap]) if ap in work.columns else pd.Series([""] * len(work), index=work.index)
        rec["member_id"] = normalize_id_series(work[am]) if am in work.columns else pd.Series([""] * len(work), index=work.index)
        rec["join_policy_source"] = pd.Series([ap] * len(work), index=work.index)
        rec["join_member_source"] = pd.Series([am] * len(work), index=work.index)

    st_col = profile.status_col or find_col(work, "enrolleeStatus") or "enrolleeStatus"
    act_col = profile.action_col or find_col(work, "actionCode") or "actionCode"
    rec["subscriber_id"] = normalize_id_series(
        _col_series(work, profile.subscriber_col or "exchgSubscriberIdentifier")
    )
    rec["normalized_status"] = _col_series(work, st_col).astype(str).map(normalize_status)
    rec["file_event_date"] = _date_ymd(dates)
    rec["member_maint_effective_date"] = _date_ymd(
        _col_series(work, "memberMaintEffectiveDate", "member_maint_effective_date")
    )
    rec["benefit_effective_date"] = _date_ymd(
        _col_series(work, "benefitEffectiveBeginDate", "benefit_effective_date")
    )
    rec["benefit_end_date"] = _date_ymd(
        _col_series(work, "benefitEffectiveEndDate", "benefit_end_date")
    )
    rec["event_date"] = rec["file_event_date"]
    rec["action_code"] = _col_series(work, act_col).astype(str)
    rec["source_table"] = pd.Series([profile.full_name] * len(work), index=work.index)

    out = pd.DataFrame(rec)
    out["_record_key"] = join_key_series(out, PRIMARY_JOIN)
    return out


def _score_full_join(
    xml_raw: pd.DataFrame,
    table_df: pd.DataFrame,
    profile: TableProfile,
    *,
    xml_policy_col: str,
    azure_policy_col: str,
    xml_member_col: str,
    azure_member_col: str,
    date_col: str,
) -> int:
    jm = JoinMapping(xml_policy_col, azure_policy_col, xml_member_col, azure_member_col)
    xml_rec = build_canonical_xml_records(xml_raw, jm)
    az_rec = build_canonical_azure_records(table_df, profile, date_col=date_col, join_mapping=jm)
    if xml_rec.empty or az_rec.empty:
        return 0
    merged = xml_rec.merge(az_rec, on=PRIMARY_JOIN, how="inner")
    return len(merged)


def select_best_join_mapping(
    xml_raw: pd.DataFrame,
    table_df: pd.DataFrame,
    profile: TableProfile,
    overlap_matrix: pd.DataFrame,
    *,
    date_col: str = "GAA_834_File_Date",
) -> JoinMapping:
    """Pick policy+member column pair with highest full-record join overlap."""
    default = JoinMapping(
        xml_policy_col="policy_id",
        azure_policy_col="exchgAssignedPolicyID",
        xml_member_col="member_id",
        azure_member_col="exchgIndivIdentifier",
    )

    if overlap_matrix.empty:
        return default

    policy_pairs: list[tuple[str, str, float]] = []
    member_pairs: list[tuple[str, str, float]] = []

    xml_policy_like = {"policy_id", "exchg_assigned_policy_id", "health_coverage_policy_no", "healthCoveragePolicyID"}
    xml_member_like = {"member_id", "exchg_indiv_identifier", "subscriber_id", "exchg_subscriber_identifier", "exchg_assigned_enrollee_id"}
    az_policy_like = {"exchgAssignedPolicyID", "healthCoveragePolicyID"}
    az_member_like = {"exchgIndivIdentifier", "exchgSubscriberIdentifier"}

    for _, row in overlap_matrix.iterrows():
        lc, rc = str(row["left_column"]), str(row["right_column"])
        rate = float(row["overlap_count"])
        if rc in JOIN_EXCLUDE_FROM_SELECTION:
            continue
        if any(p in lc.lower() for p in ("policy", "assigned")) or lc in xml_policy_like:
            if rc in az_policy_like or "policy" in rc.lower():
                policy_pairs.append((lc, rc, rate))
        if any(m in lc.lower() for m in ("member", "indiv", "subscriber", "enrollee")) or lc in xml_member_like:
            if rc in az_member_like:
                member_pairs.append((lc, rc, rate))

    if not policy_pairs:
        policy_pairs = [(r["left_column"], r["right_column"], r["overlap_count"]) for _, r in overlap_matrix.head(5).iterrows()]
    if not member_pairs:
        member_pairs = [(r["left_column"], r["right_column"], r["overlap_count"]) for _, r in overlap_matrix.head(5).iterrows()]

    policy_pairs = sorted(set(policy_pairs), key=lambda x: x[2], reverse=True)[:6]
    member_pairs = sorted(set(member_pairs), key=lambda x: x[2], reverse=True)[:6]

    best = default
    best_count = -1
    best_rate = 0.0

    for xp, ap, pr in policy_pairs:
        for xm, am, mr in member_pairs:
            if am in JOIN_EXCLUDE_FROM_SELECTION:
                continue
            count = _score_full_join(
                xml_raw, table_df, profile,
                xml_policy_col=xp, azure_policy_col=ap,
                xml_member_col=xm, azure_member_col=am,
                date_col=date_col,
            )
            pair_rate = float(pr) + float(mr)
            if count > best_count or (count == best_count and pair_rate > best_rate):
                best_count = count
                best_rate = pair_rate
                best = JoinMapping(
                    xml_policy_col=xp,
                    azure_policy_col=ap,
                    xml_member_col=xm,
                    azure_member_col=am,
                    overlap_score=pair_rate,
                    full_join_match_count=count,
                    reliable=count > 0,
                )

    logger.info(
        "Auto join selected: %s (full matches=%d)",
        best.label(), best.full_join_match_count,
    )
    return best


def _date_ymd(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce").dt.strftime("%Y-%m-%d").fillna("")


def _pct(n: float, d: float) -> float:
    return round(100.0 * n / d, 2) if d else 0.0


def _compute_record_rates(
    matched: pd.DataFrame,
    *,
    xml_total: int,
    az_total: int,
    status_diff_count: int,
) -> dict[str, float | bool]:
    """Record-level rates — dates compared only on matching business fields."""
    mc = len(matched)
    xml_only = max(xml_total - mc, 0)
    az_only = max(az_total - mc, 0)
    denom = max(xml_total, 1)

    rates: dict[str, float | bool] = {
        "record_match_rate": _pct(mc, denom),
        "status_match_rate": _pct(mc - status_diff_count, mc) if mc else 0.0,
        "date_exact_match_rate": 0.0,
        "date_month_match_rate": 100.0 if mc else 0.0,
        "effective_date_match_rate": 0.0,
        "coverage_effective_match_rate": 0.0,
        "benefit_end_match_rate": 0.0,
        "maint_effective_match_rate": 0.0,
        "file_event_month_match_rate": 0.0,
        "relationship_valid": False,
    }

    if mc == 0:
        return rates

    # Month alignment on matched rows — use join month or file-event month columns
    if "year" in matched.columns and "month" in matched.columns:
        xml_ym = matched["year"].astype(str) + "-" + matched["month"].astype(str).str.zfill(2)
    elif "file_event_year_month_xml" in matched.columns:
        xml_ym = matched["file_event_year_month_xml"].astype(str)
    elif "file_event_year_month" in matched.columns:
        xml_ym = matched["file_event_year_month"].astype(str)
    else:
        xml_ym = pd.Series([""] * mc, index=matched.index)

    az_ym_col = "file_event_date_az" if "file_event_date_az" in matched.columns else None
    if az_ym_col:
        az_dt = pd.to_datetime(matched[az_ym_col], errors="coerce")
        az_ym = az_dt.dt.year.astype(str) + "-" + az_dt.dt.month.astype(str).str.zfill(2)
    elif "file_event_year_month_az" in matched.columns:
        az_ym = matched["file_event_year_month_az"].astype(str)
    elif "file_event_year_month" in matched.columns:
        az_ym = matched["file_event_year_month"].astype(str)
    else:
        az_ym = xml_ym

    if xml_ym.str.strip().any() and az_ym.str.strip().any():
        rates["file_event_month_match_rate"] = _pct(int((xml_ym == az_ym).sum()), mc)
    else:
        rates["file_event_month_match_rate"] = 100.0

    rates["date_month_match_rate"] = rates["file_event_month_match_rate"]

    # Same business-field date comparisons only
    pairs = [
        ("benefit_effective_date", "benefit_effective_date", "effective_date_match_rate"),
        ("benefit_effective_date", "benefit_effective_date", "coverage_effective_match_rate"),
        ("benefit_end_date", "benefit_end_date", "benefit_end_match_rate"),
        ("member_maint_effective_date", "member_maint_effective_date", "maint_effective_match_rate"),
    ]
    maint_matches = 0
    for xml_suffix, az_suffix, rate_key in pairs:
        xc = f"{xml_suffix}_xml" if f"{xml_suffix}_xml" in matched.columns else xml_suffix
        ac = f"{az_suffix}_az" if f"{az_suffix}_az" in matched.columns else az_suffix
        if xc in matched.columns and ac in matched.columns:
            xm = _date_ymd(matched[xc])
            am = _date_ymd(matched[ac])
            both = (xm != "") & (am != "")
            if both.any():
                n_match = int((xm[both] == am[both]).sum())
                rates[rate_key] = _pct(n_match, int(both.sum()))
                if rate_key == "maint_effective_match_rate":
                    maint_matches = n_match

    rates["date_exact_match_rate"] = rates["maint_effective_match_rate"]

    rr = float(rates["record_match_rate"])
    sr = float(rates["status_match_rate"])
    rates["relationship_valid"] = (
        rr >= settings.relationship_min_record_match_rate
        and sr >= settings.relationship_min_status_match_rate
    )

    rates["xml_not_in_azure_remaining"] = xml_only
    rates["azure_not_in_xml_remaining"] = az_only
    return rates


def _comparable_date_diff(matched: pd.DataFrame) -> pd.DataFrame:
    """Rows where same business-field dates differ (not file date vs maint date)."""
    if matched.empty:
        return matched
    work = matched.copy()
    flags = pd.Series(False, index=work.index)
    for xml_suffix, az_suffix in (
        ("benefit_effective_date", "benefit_effective_date"),
        ("benefit_end_date", "benefit_end_date"),
        ("member_maint_effective_date", "member_maint_effective_date"),
    ):
        xc = f"{xml_suffix}_xml" if f"{xml_suffix}_xml" in work.columns else xml_suffix
        ac = f"{az_suffix}_az" if f"{az_suffix}_az" in work.columns else az_suffix
        if xc in work.columns and ac in work.columns:
            xm = _date_ymd(work[xc])
            am = _date_ymd(work[ac])
            both = (xm != "") & (am != "")
            flags = flags | (both & (xm != am))
    return work[flags].copy()


def _cross_field_date_note(matched: pd.DataFrame) -> pd.DataFrame:
    """Informational: XML maint/event date vs Azure file date (different business fields)."""
    if matched.empty:
        return matched
    xc = "member_maint_effective_date_xml" if "member_maint_effective_date_xml" in matched.columns else "event_date_xml"
    ac = "file_event_date_az" if "file_event_date_az" in matched.columns else "event_date_az"
    if xc not in matched.columns or ac not in matched.columns:
        return pd.DataFrame()
    xm = _date_ymd(matched[xc])
    am = _date_ymd(matched[ac])
    both = (xm != "") & (am != "")
    return matched[both & (xm != am)].copy()


def learn_status_mapping_from_matches(matched: pd.DataFrame) -> dict[str, str]:
    if matched.empty:
        return {}
    sx = "normalized_status_xml" if "normalized_status_xml" in matched.columns else "normalized_status_x"
    sa = "normalized_status_az" if "normalized_status_az" in matched.columns else "normalized_status_y"
    if sx not in matched.columns or sa not in matched.columns:
        return {}
    mapping: dict[str, str] = {}
    for xml_st, grp in matched.groupby(sx):
        if not str(xml_st).strip():
            continue
        counts = grp[sa].value_counts()
        if counts.empty:
            continue
        mapping[str(xml_st).upper()] = normalize_status(str(counts.index[0]))
    return mapping


def compare_records(
    xml_records: pd.DataFrame,
    az_records: pd.DataFrame,
    *,
    join_mapping: JoinMapping | None = None,
    join_keys: list[str] | None = None,
) -> dict[str, Any]:
    keys = join_keys or PRIMARY_JOIN
    empty_stats: dict[str, Any] = {
        "match_count": 0,
        "xml_not_in_azure_count": 0,
        "azure_not_in_xml_count": 0,
        "status_diff_count": 0,
        "date_diff_count": 0,
        "comparable_date_diff_count": 0,
        "cross_field_date_diff_count": 0,
        "matched_records": pd.DataFrame(),
        "xml_not_in_azure": pd.DataFrame(),
        "azure_not_in_xml": pd.DataFrame(),
        "status_diff": pd.DataFrame(),
        "date_diff": pd.DataFrame(),
        "comparable_date_diff": pd.DataFrame(),
        "cross_field_date_diff": pd.DataFrame(),
        "summary_rows": [],
        "join_mapping": join_mapping,
        "join_keys": keys,
        "status_mapping_reliable": False,
        "status_mapping": {},
        "rates": {},
    }
    if xml_records.empty and az_records.empty:
        return empty_stats

    merged = xml_records.merge(
        az_records, on=keys, how="outer", suffixes=("_xml", "_az"), indicator=True,
    )
    matched = merged[merged["_merge"] == "both"].copy()
    xml_only = merged[merged["_merge"] == "left_only"].copy()
    az_only = merged[merged["_merge"] == "right_only"].copy()

    status_diff = pd.DataFrame()
    comparable_date_diff = pd.DataFrame()
    cross_field_date_diff = pd.DataFrame()
    status_mapping: dict[str, str] = {}
    reliable = len(matched) > 0

    if reliable:
        sx_col = "normalized_status_xml" if "normalized_status_xml" in matched.columns else "normalized_status_x"
        sa_col = "normalized_status_az" if "normalized_status_az" in matched.columns else "normalized_status_y"
        if sx_col in matched.columns and sa_col in matched.columns:
            status_diff = matched[matched[sx_col].astype(str) != matched[sa_col].astype(str)].copy()
            status_mapping = learn_status_mapping_from_matches(matched)
        comparable_date_diff = _comparable_date_diff(matched)
        cross_field_date_diff = _cross_field_date_note(matched)

    rates = _compute_record_rates(
        matched,
        xml_total=len(xml_records),
        az_total=len(az_records),
        status_diff_count=len(status_diff),
    )

    join_label = join_mapping.label() if join_mapping else "+".join(keys)
    summary_rows = [{
        "join_type": "primary_auto" if join_mapping else "primary",
        "join_keys": join_label if join_mapping else "+".join(keys),
        "match_count": len(matched),
        "xml_not_in_azure": len(xml_only),
        "azure_not_in_xml": len(az_only),
        "status_diff": len(status_diff),
        "date_diff": len(comparable_date_diff),
        "cross_field_date_diff": len(cross_field_date_diff),
        "record_match_rate": rates.get("record_match_rate", 0),
        "status_match_rate": rates.get("status_match_rate", 0),
        "effective_date_match_rate": rates.get("effective_date_match_rate", 0),
        "file_event_month_match_rate": rates.get("file_event_month_match_rate", 0),
        "relationship_valid": rates.get("relationship_valid", False),
    }]

    for label, keys in DIAGNOSTIC_JOINS:
        if xml_records.empty or az_records.empty:
            continue
        xk = join_key_series(xml_records, keys)
        ak = join_key_series(az_records, keys)
        summary_rows.append({
            "join_type": label,
            "join_keys": "+".join(keys),
            "match_count": len(set(xk) & set(ak)),
            "xml_not_in_azure": len(set(xk) - set(ak)),
            "azure_not_in_xml": len(set(ak) - set(xk)),
            "status_diff": "",
            "date_diff": "",
        })

    return {
        "match_count": len(matched),
        "xml_not_in_azure_count": len(xml_only),
        "azure_not_in_xml_count": len(az_only),
        "status_diff_count": len(status_diff),
        "date_diff_count": len(comparable_date_diff),
        "comparable_date_diff_count": len(comparable_date_diff),
        "cross_field_date_diff_count": len(cross_field_date_diff),
        "matched_records": matched,
        "xml_not_in_azure": xml_only,
        "azure_not_in_xml": az_only,
        "status_diff": status_diff,
        "date_diff": comparable_date_diff,
        "comparable_date_diff": comparable_date_diff,
        "cross_field_date_diff": cross_field_date_diff,
        "summary_rows": summary_rows,
        "join_mapping": join_mapping,
        "join_keys": keys,
        "status_mapping_reliable": reliable,
        "status_mapping": status_mapping if reliable else {},
        "rates": rates,
    }


def run_record_comparison(
    xml_raw: pd.DataFrame,
    table_df: pd.DataFrame,
    profile: TableProfile,
    *,
    date_col: str = "GAA_834_File_Date",
) -> dict[str, Any]:
    """
    Full record pipeline: ID overlap audit -> auto join -> compare -> debug exports.
    """
    overlap_matrix = build_id_overlap_matrix(xml_raw, table_df)
    join_mapping = select_best_join_mapping(
        xml_raw, table_df, profile, overlap_matrix, date_col=date_col,
    )

    xml_records = build_canonical_xml_records(xml_raw, join_mapping)
    az_records = build_canonical_azure_records(
        table_df, profile, date_col=date_col, join_mapping=join_mapping,
    )
    record_stats = compare_records(xml_records, az_records, join_mapping=join_mapping)
    debug_paths = export_record_debug_csvs(
        record_stats,
        xml_raw=xml_raw,
        table_df=table_df,
        xml_records=xml_records,
        az_records=az_records,
        overlap_matrix=overlap_matrix,
        join_mapping=join_mapping,
    )
    record_stats["debug_paths"] = debug_paths
    record_stats["id_overlap_matrix"] = overlap_matrix
    return record_stats


def export_record_debug_csvs(
    record_stats: dict[str, Any],
    *,
    xml_raw: pd.DataFrame | None = None,
    table_df: pd.DataFrame | None = None,
    xml_records: pd.DataFrame | None = None,
    az_records: pd.DataFrame | None = None,
    overlap_matrix: pd.DataFrame | None = None,
    join_mapping: JoinMapping | None = None,
) -> dict[str, str]:
    dbg = settings.outputs_path / "debug"
    dbg.mkdir(parents=True, exist_ok=True)
    paths: dict[str, str] = {}

    if overlap_matrix is not None and not overlap_matrix.empty:
        p = dbg / "id_overlap_matrix.csv"
        safe_write_csv(p, overlap_matrix, table_name="id_overlap_matrix")
        paths["id_overlap_matrix"] = str(p)

    summary = pd.DataFrame(record_stats.get("summary_rows", []))
    paths["record_match_summary"] = str(dbg / "record_match_summary.csv")
    safe_write_csv(Path(paths["record_match_summary"]), summary, table_name="record_match_summary")

    if xml_records is not None and not xml_records.empty:
        p = dbg / "canonical_xml_sample.csv"
        safe_write_csv(p, xml_records.head(5000), table_name="canonical_xml_sample")
        paths["canonical_xml_sample"] = str(p)

    if az_records is not None and not az_records.empty:
        p = dbg / "canonical_azure_sample.csv"
        safe_write_csv(p, az_records.head(5000), table_name="canonical_azure_sample")
        paths["canonical_azure_sample"] = str(p)

    if join_mapping and xml_records is not None and not xml_records.empty:
        sample = xml_records.head(200).copy()
        sample["join_mapping"] = join_mapping.label()
        p = dbg / "primary_join_key_sample.csv"
        safe_write_csv(p, sample, table_name="primary_join_key_sample")
        paths["primary_join_key_sample"] = str(p)

    if xml_raw is not None and not xml_raw.empty:
        freq_parts = []
        for col in _resolve_id_columns(xml_raw, XML_ID_CANDIDATES).values():
            f = _id_frequency(xml_raw, col, "xml")
            if not f.empty:
                freq_parts.append(f)
        if freq_parts:
            p = dbg / "xml_id_frequency.csv"
            safe_write_csv(p, pd.concat(freq_parts, ignore_index=True), table_name="xml_id_frequency")
            paths["xml_id_frequency"] = str(p)

    if table_df is not None and not table_df.empty:
        freq_parts = []
        for col in _resolve_id_columns(table_df, AZURE_ID_CANDIDATES).values():
            f = _id_frequency(table_df, col, "azure")
            if not f.empty:
                freq_parts.append(f)
        if freq_parts:
            p = dbg / "azure_id_frequency.csv"
            safe_write_csv(p, pd.concat(freq_parts, ignore_index=True), table_name="azure_id_frequency")
            paths["azure_id_frequency"] = str(p)

    for key, fname in [
        ("matched_records", "matched_records.csv"),
        ("xml_not_in_azure", "xml_not_in_azure.csv"),
        ("azure_not_in_xml", "azure_not_in_xml.csv"),
        ("status_diff", "status_diff.csv"),
        ("date_diff", "date_diff.csv"),
    ]:
        df = record_stats.get(key, pd.DataFrame())
        if isinstance(df, pd.DataFrame) and not df.empty:
            p = dbg / fname
            safe_write_csv(p, df.head(100_000), table_name=fname.replace(".csv", ""))
            paths[key] = str(p)

    for key, fname in [
        ("status_diff", "status_diff_sample.csv"),
        ("date_diff", "date_diff_sample.csv"),
        ("xml_not_in_azure", "xml_not_in_azure_sample.csv"),
        ("azure_not_in_xml", "azure_not_in_xml_sample.csv"),
    ]:
        df = record_stats.get(key, pd.DataFrame())
        if isinstance(df, pd.DataFrame) and not df.empty:
            p = dbg / fname
            safe_write_csv(p, df.head(500), table_name=fname.replace(".csv", ""))
            paths[fname.replace(".csv", "")] = str(p)
    return paths
