"""
Lifecycle snapshot comparison — diagnostic member/policy-level XML vs Azure match.

Collapses transaction events to member/policy grain, compares across multiple
month-basis join strategies, and auto-selects the best match rate.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from azure_reconciliation.azure_mirror.discovery.table_inspector import TableProfile
from azure_reconciliation.partition_discovery import Partition
from azure_reconciliation.record_comparison import (
    JoinMapping,
    LIFECYCLE_PRIMARY_JOIN,
    _date_ymd,
    build_canonical_azure_records,
    build_canonical_xml_records,
    compare_records,
    join_key_series,
)
from azure_reconciliation.safe_export import safe_write_csv, safe_write_html_report
from config.config import settings
from utils.logger import get_logger

logger = get_logger(__name__)

XML_EVENT_EXPLANATION = (
    "XML contains multiple transaction events per member/policy; "
    "lifecycle snapshot comparison is the correct business-level comparison."
)

MONTH_BASIS_COMPARISONS: list[tuple[str, list[str]]] = [
    ("join_without_month", list(LIFECYCLE_PRIMARY_JOIN)),
    ("join_file_event_month", [*LIFECYCLE_PRIMARY_JOIN, "file_event_year_month"]),
    ("join_benefit_effective_month", [*LIFECYCLE_PRIMARY_JOIN, "benefit_effective_year_month"]),
    ("join_member_maint_month", [*LIFECYCLE_PRIMARY_JOIN, "member_maint_year_month"]),
]


def _ym_from_date(series: pd.Series) -> pd.Series:
    dt = pd.to_datetime(series, errors="coerce")
    return dt.dt.strftime("%Y-%m").fillna("")


def _ym_from_year_month(year: pd.Series, month: pd.Series) -> pd.Series:
    return year.astype(str).str.strip() + "-" + month.astype(str).str.strip().str.zfill(2)


def enrich_month_bases(df: pd.DataFrame) -> pd.DataFrame:
    """Add separate year-month columns for each business date basis."""
    if df.empty:
        return df
    out = df.copy()
    out["coverage_year_month"] = _ym_from_year_month(out["year"], out["month"])
    file_src = out["file_event_date"] if "file_event_date" in out.columns else out.get("event_date", pd.Series([""] * len(out)))
    out["file_event_year_month"] = _ym_from_date(file_src)
    empty_file = out["file_event_year_month"] == ""
    if empty_file.any():
        out.loc[empty_file, "file_event_year_month"] = out.loc[empty_file, "coverage_year_month"]
    out["benefit_effective_year_month"] = _ym_from_date(out["benefit_effective_date"])
    empty_benefit = out["benefit_effective_year_month"] == ""
    if empty_benefit.any():
        out.loc[empty_benefit, "benefit_effective_year_month"] = out.loc[empty_benefit, "coverage_year_month"]
    out["member_maint_year_month"] = _ym_from_date(out["member_maint_effective_date"])
    empty_maint = out["member_maint_year_month"] == ""
    if empty_maint.any():
        out.loc[empty_maint, "member_maint_year_month"] = out.loc[empty_maint, "coverage_year_month"]
    return out


def _sort_chronological(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    sort_cols: list[str] = []
    for col in (
        "member_maint_effective_date",
        "file_event_date",
        "event_date",
        "source_file",
        "year",
        "month",
    ):
        if col in df.columns:
            sort_cols.append(col)
    if not sort_cols:
        return df
    return df.sort_values(sort_cols, ascending=True, na_position="last", kind="mergesort")


def collapse_to_snapshot(records: pd.DataFrame, group_keys: list[str]) -> pd.DataFrame:
    """Latest business state per group key after chronological sort."""
    if records.empty:
        return records
    work = _sort_chronological(records)
    valid_keys = [k for k in group_keys if k in work.columns]
    if not valid_keys:
        return work
    out = work.groupby(valid_keys, dropna=False, as_index=False).last()
    if "event_count" not in out.columns:
        counts = work.groupby(valid_keys, dropna=False).size().reset_index(name="event_count")
        out = out.merge(counts, on=valid_keys, how="left")
    return out


def _filter_partitions(records: pd.DataFrame, partitions: list[Partition] | None) -> pd.DataFrame:
    if records.empty or not partitions:
        return records
    issuer_set = {str(p.issuer) for p in partitions}
    ym_allowed = {f"{p.year}-{str(p.month).zfill(2)}" for p in partitions}
    work = records[records["issuer"].astype(str).isin(issuer_set)].copy()
    if work.empty:
        return work
    in_coverage = work["coverage_year_month"].isin(ym_allowed)
    in_file = work["file_event_year_month"].isin(ym_allowed)
    in_benefit = work["benefit_effective_year_month"].isin(ym_allowed)
    in_maint = work["member_maint_year_month"].isin(ym_allowed)
    return work[in_coverage | in_file | in_benefit | in_maint].copy()


def build_enriched_canonical_xml(
    xml_raw: pd.DataFrame,
    join_mapping: JoinMapping | None,
    *,
    partitions: list[Partition] | None = None,
) -> pd.DataFrame:
    canonical = enrich_month_bases(build_canonical_xml_records(xml_raw, join_mapping))
    canonical = _filter_partitions(canonical, partitions)
    if not canonical.empty:
        canonical["snapshot_source"] = "xml"
    return canonical


def build_enriched_canonical_azure(
    table_df: pd.DataFrame,
    profile: TableProfile,
    join_mapping: JoinMapping | None,
    *,
    date_col: str = "GAA_834_File_Date",
    partitions: list[Partition] | None = None,
) -> pd.DataFrame:
    canonical = enrich_month_bases(
        build_canonical_azure_records(
            table_df, profile, date_col=date_col, join_mapping=join_mapping,
        )
    )
    canonical = _filter_partitions(canonical, partitions)
    if not canonical.empty:
        canonical["snapshot_source"] = "azure"
    return canonical


def _lifecycle_rates(
    stats: dict[str, Any],
    *,
    xml_snapshot_total: int,
    az_snapshot_total: int,
    raw_event_match_rate: float | None = None,
) -> dict[str, float | bool]:
    rates = dict(stats.get("rates") or {})
    mc = int(stats.get("match_count", 0))
    xml_denom = max(xml_snapshot_total, 1)
    rates["lifecycle_snapshot_match_rate"] = round(100.0 * mc / xml_denom, 2)
    rates["match_rate"] = rates["lifecycle_snapshot_match_rate"]
    rates["azure_snapshot_coverage_rate"] = round(100.0 * mc / max(az_snapshot_total, 1), 2)
    if raw_event_match_rate is not None:
        rates["raw_event_match_rate"] = raw_event_match_rate
    rates["record_match_rate"] = rates["lifecycle_snapshot_match_rate"]
    rr = float(rates.get("lifecycle_snapshot_match_rate", 0))
    sr = float(rates.get("status_match_rate", 0))
    rates["relationship_valid"] = (
        rr >= settings.relationship_min_record_match_rate
        and sr >= settings.relationship_min_status_match_rate
    )
    rates["xml_not_in_azure_remaining"] = int(stats.get("xml_not_in_azure_count", 0))
    rates["azure_not_in_xml_remaining"] = int(stats.get("azure_not_in_xml_count", 0))
    return rates


def run_month_basis_comparisons(
    xml_canonical: pd.DataFrame,
    az_canonical: pd.DataFrame,
    *,
    join_mapping: JoinMapping | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any], str, list[str]]:
    """Run A/B/C/D comparisons; return rows, best stats, best label, best join keys."""
    comparison_rows: list[dict[str, Any]] = []
    best_stats: dict[str, Any] | None = None
    best_label = ""
    best_keys: list[str] = list(LIFECYCLE_PRIMARY_JOIN)
    best_score = (-1, -1.0)

    for label, join_keys in MONTH_BASIS_COMPARISONS:
        xml_snap = collapse_to_snapshot(xml_canonical, join_keys)
        az_snap = collapse_to_snapshot(az_canonical, join_keys)
        stats = compare_records(xml_snap, az_snap, join_mapping=join_mapping, join_keys=join_keys)
        rates = stats.get("rates") or {}
        row = {
            "comparison_type": label,
            "join_key": "+".join(join_keys),
            "match_count": stats.get("match_count", 0),
            "xml_not_in_azure": stats.get("xml_not_in_azure_count", 0),
            "azure_not_in_xml": stats.get("azure_not_in_xml_count", 0),
            "status_diff": stats.get("status_diff_count", 0),
            "match_rate": rates.get("record_match_rate", 0),
            "status_match_rate": rates.get("status_match_rate", 0),
            "effective_date_match_rate": rates.get("effective_date_match_rate", 0),
            "xml_snapshot_rows": len(xml_snap),
            "azure_snapshot_rows": len(az_snap),
        }
        comparison_rows.append(row)
        score = (int(stats.get("match_count", 0)), float(rates.get("status_match_rate", 0)))
        if score > best_score:
            best_score = score
            best_stats = {**stats, "xml_snap": xml_snap, "az_snap": az_snap, "join_keys": join_keys}
            best_label = label
            best_keys = join_keys

    if best_stats is None:
        empty = compare_records(pd.DataFrame(), pd.DataFrame(), join_keys=LIFECYCLE_PRIMARY_JOIN)
        return comparison_rows, empty, "", LIFECYCLE_PRIMARY_JOIN
    return comparison_rows, best_stats, best_label, best_keys


def compute_month_basis_diff(
    xml_canonical: pd.DataFrame,
    az_canonical: pd.DataFrame,
    *,
    lifecycle_stats: dict[str, Any],
) -> pd.DataFrame:
    """Members on both sides where month basis columns differ."""
    if xml_canonical.empty or az_canonical.empty:
        return pd.DataFrame()

    xml_pk = collapse_to_snapshot(xml_canonical, LIFECYCLE_PRIMARY_JOIN)
    az_pk = collapse_to_snapshot(az_canonical, LIFECYCLE_PRIMARY_JOIN)
    az_pk_keys = set(join_key_series(az_pk, LIFECYCLE_PRIMARY_JOIN))

    parts: list[pd.DataFrame] = []

    # Members in xml_not_in_azure that exist on Azure at member grain
    xml_only = lifecycle_stats.get("xml_not_in_azure", pd.DataFrame())
    if isinstance(xml_only, pd.DataFrame) and not xml_only.empty:
        work = xml_only.copy()
        for c in LIFECYCLE_PRIMARY_JOIN:
            src = c if c in work.columns else (f"{c}_xml" if f"{c}_xml" in work.columns else None)
            if src:
                work[c] = work[src]
        work["_pk"] = join_key_series(work, LIFECYCLE_PRIMARY_JOIN)
        reclassified = work[work["_pk"].isin(az_pk_keys)].copy()
        if not reclassified.empty:
            reclassified["diff_reason"] = "MONTH_BASIS_DIFF"
            parts.append(reclassified)

    # Inner join pairs with differing month columns
    inner = xml_pk.merge(az_pk, on=LIFECYCLE_PRIMARY_JOIN, suffixes=("_xml", "_az"), how="inner")
    if not inner.empty:
        flags = pd.Series(False, index=inner.index)
        for col in (
            "coverage_year_month", "file_event_year_month",
            "benefit_effective_year_month", "member_maint_year_month",
        ):
            xc, ac = f"{col}_xml", f"{col}_az"
            if xc in inner.columns and ac in inner.columns:
                flags = flags | (inner[xc].astype(str) != inner[ac].astype(str))
        month_diff = inner[flags].copy()
        if not month_diff.empty:
            month_diff["diff_reason"] = "MONTH_BASIS_DIFF"
            parts.append(month_diff)

    if not parts:
        return pd.DataFrame()
    combined = pd.concat(parts, ignore_index=True)
    return combined.drop_duplicates(
        subset=[c for c in LIFECYCLE_PRIMARY_JOIN if c in combined.columns],
        keep="first",
    )


def apply_month_basis_reclassification(
    lifecycle_stats: dict[str, Any],
    month_basis_diff: pd.DataFrame,
) -> dict[str, Any]:
    """Move MONTH_BASIS_DIFF rows out of xml_not_in_azure count."""
    if month_basis_diff.empty:
        lifecycle_stats["month_basis_diff_count"] = 0
        return lifecycle_stats
    mbd_count = len(month_basis_diff)
    lifecycle_stats["month_basis_diff"] = month_basis_diff
    lifecycle_stats["month_basis_diff_count"] = mbd_count
    lifecycle_stats["xml_not_in_azure_count"] = max(
        0, int(lifecycle_stats.get("xml_not_in_azure_count", 0)) - mbd_count,
    )
    xml_only = lifecycle_stats.get("xml_not_in_azure", pd.DataFrame())
    if isinstance(xml_only, pd.DataFrame) and not xml_only.empty:
        work = xml_only.copy()
        for c in LIFECYCLE_PRIMARY_JOIN:
            xc = c if c in work.columns else (f"{c}_xml" if f"{c}_xml" in work.columns else None)
            if xc and xc in work.columns:
                work[c] = work[xc]
        mbd_keys = set(join_key_series(month_basis_diff, LIFECYCLE_PRIMARY_JOIN))
        pk = join_key_series(work, LIFECYCLE_PRIMARY_JOIN)
        lifecycle_stats["xml_not_in_azure"] = work[~pk.isin(mbd_keys)].copy()
    return lifecycle_stats


def run_lifecycle_snapshot_comparison(
    xml_raw: pd.DataFrame,
    table_df: pd.DataFrame,
    profile: TableProfile,
    *,
    issuer: str = "",
    join_mapping: JoinMapping | None = None,
    date_col: str = "GAA_834_File_Date",
    partitions: list[Partition] | None = None,
    raw_event_rates: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build snapshots, compare month bases, auto-select best, export debug artifacts."""
    xml_canonical = build_enriched_canonical_xml(xml_raw, join_mapping, partitions=partitions)
    az_canonical = build_enriched_canonical_azure(
        table_df, profile, join_mapping, date_col=date_col, partitions=partitions,
    )

    comparison_rows, best_stats, best_label, best_keys = run_month_basis_comparisons(
        xml_canonical, az_canonical, join_mapping=join_mapping,
    )

    xml_snap = best_stats.get("xml_snap", pd.DataFrame())
    az_snap = best_stats.get("az_snap", pd.DataFrame())

    month_basis_diff = compute_month_basis_diff(
        xml_canonical, az_canonical,
        lifecycle_stats=best_stats,
    )
    lifecycle_stats = {k: v for k, v in best_stats.items() if k not in ("xml_snap", "az_snap", "join_keys")}
    lifecycle_stats = apply_month_basis_reclassification(lifecycle_stats, month_basis_diff)

    raw_rate = float(raw_event_rates.get("record_match_rate", 0)) if raw_event_rates else None
    lifecycle_stats["rates"] = _lifecycle_rates(
        lifecycle_stats,
        xml_snapshot_total=len(xml_snap),
        az_snapshot_total=len(az_snap),
        raw_event_match_rate=raw_rate,
    )
    lifecycle_stats["rates"]["best_month_basis"] = best_label
    for row in comparison_rows:
        lifecycle_stats["rates"][f"match_rate_{row['comparison_type']}"] = row["match_rate"]

    # Latest state (no month) for final business match
    xml_latest = collapse_to_snapshot(xml_canonical, LIFECYCLE_PRIMARY_JOIN)
    az_latest = collapse_to_snapshot(az_canonical, LIFECYCLE_PRIMARY_JOIN)
    latest_stats = compare_records(
        xml_latest, az_latest, join_mapping=join_mapping, join_keys=LIFECYCLE_PRIMARY_JOIN,
    )
    latest_rates = _lifecycle_rates(
        latest_stats,
        xml_snapshot_total=len(xml_latest),
        az_snapshot_total=len(az_latest),
    )
    latest_rates["final_business_match_rate"] = latest_rates.get("lifecycle_snapshot_match_rate", 0)
    latest_stats["rates"] = latest_rates

    debug_paths = export_lifecycle_debug_csvs(
        xml_snap=xml_snap,
        az_snap=az_snap,
        lifecycle_stats=lifecycle_stats,
        latest_stats=latest_stats,
        latest_rates=latest_rates,
        comparison_rows=comparison_rows,
        month_basis_diff=month_basis_diff,
        best_month_basis=best_label,
        best_join_keys=best_keys,
        xml_raw_rows=len(xml_raw),
        az_raw_rows=len(table_df),
    )

    from azure_reconciliation.reconciliation_analysis import run_reconciliation_analysis
    analysis_result = run_reconciliation_analysis(
        issuer=issuer,
        xml_raw=xml_raw,
        table_df=table_df,
        xml_canonical=xml_canonical,
        az_canonical=az_canonical,
        xml_snap=xml_snap,
        az_snap=az_snap,
        lifecycle_stats=lifecycle_stats,
        lifecycle_rates=lifecycle_stats.get("rates", {}),
        join_mapping=join_mapping,
        best_join_keys=best_keys,
        best_month_basis=best_label,
    )
    analysis_paths = analysis_result.get("paths", analysis_result if isinstance(analysis_result, dict) else {})
    model_h = analysis_result.get("model_h", {}) if isinstance(analysis_result, dict) else {}
    debug_paths.update(analysis_paths if isinstance(analysis_paths, dict) else {})

    return {
        "xml_processed_snapshot": xml_snap,
        "azure_processed_snapshot": az_snap,
        "xml_canonical": xml_canonical,
        "azure_canonical": az_canonical,
        "xml_latest_snapshot": xml_latest,
        "azure_latest_snapshot": az_latest,
        "lifecycle_stats": lifecycle_stats,
        "latest_stats": latest_stats,
        "lifecycle_rates": lifecycle_stats["rates"],
        "latest_rates": latest_rates,
        "month_basis_comparison": comparison_rows,
        "best_month_basis": best_label,
        "best_join_keys": best_keys,
        "month_basis_diff_count": lifecycle_stats.get("month_basis_diff_count", 0),
        "month_basis_diff": month_basis_diff,
        "xml_snapshot_rows": len(xml_snap),
        "azure_snapshot_rows": len(az_snap),
        "xml_latest_rows": len(xml_latest),
        "azure_latest_rows": len(az_latest),
        "match_count": lifecycle_stats.get("match_count", 0),
        "xml_not_in_azure_count": lifecycle_stats.get("xml_not_in_azure_count", 0),
        "azure_not_in_xml_count": lifecycle_stats.get("azure_not_in_xml_count", 0),
        "status_diff_count": lifecycle_stats.get("status_diff_count", 0),
        "status_mapping": lifecycle_stats.get("status_mapping", {}),
        "status_mapping_reliable": lifecycle_stats.get("status_mapping_reliable", False),
        "debug_paths": debug_paths,
        "analysis_paths": analysis_paths,
        "model_h": model_h,
        "event_explanation": XML_EVENT_EXPLANATION
        if len(xml_raw) > len(table_df) * 1.5 else "",
    }


def export_lifecycle_debug_csvs(
    *,
    xml_snap: pd.DataFrame,
    az_snap: pd.DataFrame,
    lifecycle_stats: dict[str, Any],
    latest_stats: dict[str, Any],
    latest_rates: dict[str, Any],
    comparison_rows: list[dict[str, Any]],
    month_basis_diff: pd.DataFrame,
    best_month_basis: str,
    best_join_keys: list[str],
    xml_raw_rows: int = 0,
    az_raw_rows: int = 0,
) -> dict[str, str]:
    dbg = settings.outputs_path / "debug"
    cmp_dir = settings.outputs_path / "comparison"
    dbg.mkdir(parents=True, exist_ok=True)
    cmp_dir.mkdir(parents=True, exist_ok=True)
    paths: dict[str, str] = {}

    for df, path, name in [
        (xml_snap, dbg / "xml_lifecycle_snapshot.csv", "xml_lifecycle_snapshot"),
        (az_snap, dbg / "azure_lifecycle_snapshot.csv", "azure_lifecycle_snapshot"),
    ]:
        if isinstance(df, pd.DataFrame) and not df.empty:
            safe_write_csv(path, df, table_name=name)
            paths[name] = str(path)

    p_mbc = dbg / "month_basis_comparison.csv"
    safe_write_csv(p_mbc, pd.DataFrame(comparison_rows), table_name="month_basis_comparison")
    paths["month_basis_comparison"] = str(p_mbc)

    if isinstance(month_basis_diff, pd.DataFrame) and not month_basis_diff.empty:
        p_mbd = dbg / "month_basis_diff.csv"
        safe_write_csv(p_mbd, month_basis_diff.head(100_000), table_name="month_basis_diff")
        paths["month_basis_diff"] = str(p_mbd)

    rates = lifecycle_stats.get("rates") or {}
    summary_rows = [{
        "comparison_type": "lifecycle_snapshot_best",
        "join_keys": "+".join(best_join_keys),
        "best_month_basis": best_month_basis,
        "xml_raw_rows": xml_raw_rows,
        "azure_raw_rows": az_raw_rows,
        "xml_snapshot_rows": len(xml_snap),
        "azure_snapshot_rows": len(az_snap),
        "match_count": lifecycle_stats.get("match_count", 0),
        "xml_not_in_azure": lifecycle_stats.get("xml_not_in_azure_count", 0),
        "azure_not_in_xml": lifecycle_stats.get("azure_not_in_xml_count", 0),
        "month_basis_diff": lifecycle_stats.get("month_basis_diff_count", 0),
        "status_diff": lifecycle_stats.get("status_diff_count", 0),
        **{k: v for k, v in rates.items() if isinstance(v, (int, float, bool, str))},
    }, {
        "comparison_type": "latest_state",
        "join_keys": "+".join(LIFECYCLE_PRIMARY_JOIN),
        "match_count": latest_stats.get("match_count", 0),
        "xml_not_in_azure": latest_stats.get("xml_not_in_azure_count", 0),
        "azure_not_in_xml": latest_stats.get("azure_not_in_xml_count", 0),
        "status_diff": latest_stats.get("status_diff_count", 0),
        **{k: v for k, v in latest_rates.items() if isinstance(v, (int, float, bool))},
    }]
    p_summary = dbg / "lifecycle_match_summary.csv"
    safe_write_csv(p_summary, pd.DataFrame(summary_rows), table_name="lifecycle_match_summary")
    paths["lifecycle_match_summary"] = str(p_summary)

    for key, fname in [
        ("xml_not_in_azure", "lifecycle_xml_not_in_azure.csv"),
        ("azure_not_in_xml", "lifecycle_azure_not_in_xml.csv"),
        ("status_diff", "lifecycle_status_diff.csv"),
    ]:
        df = lifecycle_stats.get(key, pd.DataFrame())
        if isinstance(df, pd.DataFrame) and not df.empty:
            p = dbg / fname
            safe_write_csv(p, df.head(100_000), table_name=fname.replace(".csv", ""))
            paths[key] = str(p)

    html_path = cmp_dir / "final_lifecycle_result.html"
    summary_df = pd.DataFrame([{
        "best_month_basis": best_month_basis,
        "xml_raw_rows": xml_raw_rows,
        "azure_raw_rows": az_raw_rows,
        "xml_lifecycle_snapshot_rows": len(xml_snap),
        "azure_lifecycle_snapshot_rows": len(az_snap),
        "raw_event_match_rate": rates.get("raw_event_match_rate"),
        "lifecycle_snapshot_match_rate": rates.get("lifecycle_snapshot_match_rate"),
        "match_rate_without_month": rates.get("match_rate_join_without_month"),
        "match_rate_file_event_month": rates.get("match_rate_join_file_event_month"),
        "match_rate_benefit_month": rates.get("match_rate_join_benefit_effective_month"),
        "match_rate_maint_month": rates.get("match_rate_join_member_maint_month"),
        "status_match_rate": rates.get("status_match_rate"),
        "effective_date_match_rate": rates.get("effective_date_match_rate"),
        "month_basis_diff_count": lifecycle_stats.get("month_basis_diff_count", 0),
        "lifecycle_xml_not_in_azure": lifecycle_stats.get("xml_not_in_azure_count", 0),
        "lifecycle_azure_not_in_xml": lifecycle_stats.get("azure_not_in_xml_count", 0),
        "final_business_match_rate": latest_rates.get("final_business_match_rate"),
    }])
    extra = f"<p><em>{XML_EVENT_EXPLANATION}</em></p>" if xml_raw_rows > az_raw_rows * 1.5 else ""
    extra += (
        f"<p><strong>Best month basis selected:</strong> {best_month_basis}</p>"
        f"<p><strong>Join key:</strong> {'+'.join(best_join_keys)}</p>"
        "<h3>Month basis match rates</h3>"
        "<ul>"
        f"<li>Without month: {rates.get('match_rate_join_without_month', 'n/a')}%</li>"
        f"<li>File/event month: {rates.get('match_rate_join_file_event_month', 'n/a')}%</li>"
        f"<li>Benefit effective month: {rates.get('match_rate_join_benefit_effective_month', 'n/a')}%</li>"
        f"<li>Member maintenance month: {rates.get('match_rate_join_member_maint_month', 'n/a')}%</li>"
        "</ul>"
        f"<p><strong>MONTH_BASIS_DIFF count:</strong> {lifecycle_stats.get('month_basis_diff_count', 0):,}</p>"
        "<h3>A) Raw event comparison (diagnostic)</h3>"
        f"<p>Raw event match rate: {rates.get('raw_event_match_rate', 'n/a')}%</p>"
        "<h3>B) Lifecycle snapshot comparison (diagnostic only)</h3>"
        "<p><em>Primary business result is Model H — see outputs/comparison/final_business_result.html</em></p>"
        f"<p>Lifecycle snapshot match rate: {rates.get('lifecycle_snapshot_match_rate', 0):.2f}%</p>"
        f"<p>Status match rate: {rates.get('status_match_rate', 0):.2f}%</p>"
        f"<p>XML lifecycle not in Azure (after reclassification): "
        f"{lifecycle_stats.get('xml_not_in_azure_count', 0):,}</p>"
        f"<p>Azure not in XML: {lifecycle_stats.get('azure_not_in_xml_count', 0):,}</p>"
    )
    safe_write_html_report(
        html_path,
        title="Final Lifecycle Result",
        summary_df=summary_df,
        detail_df=pd.DataFrame(comparison_rows),
        extra_html=extra,
    )
    paths["final_lifecycle_result_html"] = str(html_path)
    return paths
