"""
Partitioned XML / Azure / Comparison reports under outputs/partitioned_reports/.

Dynamically generates reports for every issuer/year/month discovered in source_data.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from azure_reconciliation.partition_discovery import Partition
from azure_reconciliation.reconciliation_analysis import (
    build_model_h_xml_not_in_azure,
    score_model_h,
    _chandra_dashboard,
)
from azure_reconciliation.safe_export import (
    ExportErrors,
    safe_write_csv,
    safe_write_excel,
    safe_write_html_report,
)
from azure_reconciliation.status_mapper import normalize_status
from config.config import settings
from utils.logger import get_logger

logger = get_logger(__name__)

SUMMARY_COLS = [
    "issuer", "year", "month", "insurance_type", "status",
    "xml_enrollment_count", "xml_enrollee_count", "xml_subscriber_count",
    "azure_enrollment_count", "azure_enrollee_count", "azure_subscriber_count",
    "enrollment_diff", "enrollee_diff", "subscriber_diff",
    "match_status", "count_match_pct", "reason_bucket",
]

DETAIL_COLS = [
    "issuer", "year", "month", "insurance_type", "status",
    "policy_id", "member_id", "subscriber_id",
    "event_date", "benefit_effective_date", "benefit_end_date",
    "source_file", "source_table",
    "match_status", "difference_reason",
]


def partitioned_reports_root() -> Path:
    d = settings.outputs_path / "partitioned_reports"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _zmonth(m: str) -> str:
    return str(m).strip().zfill(2)


def _col(df: pd.DataFrame, name: str) -> pd.Series:
    if name in df.columns:
        return df[name].astype(str)
    return pd.Series([""] * len(df), index=df.index)


def _status_series(df: pd.DataFrame) -> pd.Series:
    for c in ("normalized_status", "canonical_status", "status"):
        if c in df.columns:
            return df[c].astype(str).map(normalize_status)
    return pd.Series(["UNKNOWN"] * len(df), index=df.index)


def _filter_year(df: pd.DataFrame, year: str) -> pd.DataFrame:
    if df.empty or "year" not in df.columns:
        return df
    return df[df["year"].astype(str) == str(year)].copy()


def _filter_xml_side_summary(summary: pd.DataFrame) -> pd.DataFrame:
    """XML partition summaries — groups with XML enrollment counts only."""
    if summary.empty:
        return summary
    work = summary.copy()
    work["xml_enrollment_count"] = pd.to_numeric(
        work.get("xml_enrollment_count", 0), errors="coerce",
    ).fillna(0).astype(int)
    return work[work["xml_enrollment_count"] > 0].reset_index(drop=True)


def _filter_azure_side_summary(summary: pd.DataFrame) -> pd.DataFrame:
    """Azure partition summaries — groups with Azure enrollment counts only."""
    if summary.empty:
        return summary
    work = summary.copy()
    work["azure_enrollment_count"] = pd.to_numeric(
        work.get("azure_enrollment_count", 0), errors="coerce",
    ).fillna(0).astype(int)
    return work[work["azure_enrollment_count"] > 0].reset_index(drop=True)


def _comparison_side_summary(summary: pd.DataFrame) -> pd.DataFrame:
    """Comparison summaries include all Model H groups."""
    return summary.copy() if not summary.empty else summary


def _filter_year_month(df: pd.DataFrame, year: str, month: str) -> pd.DataFrame:
    work = _filter_year(df, year)
    if work.empty or "month" not in work.columns:
        return work
    return work[work["month"].astype(str).str.zfill(2) == _zmonth(month)].copy()


def _enrich_model_h_summary(detail: pd.DataFrame, xml_only: pd.DataFrame | None = None) -> pd.DataFrame:
    """Add diff columns and reason_bucket to Model H detail."""
    if detail.empty:
        return pd.DataFrame(columns=SUMMARY_COLS)
    work = detail.copy()
    rename = {
        "enrollment_count_xml": "xml_enrollment_count",
        "enrollee_count_xml": "xml_enrollee_count",
        "subscriber_count_xml": "xml_subscriber_count",
        "enrollment_count_az": "azure_enrollment_count",
        "enrollee_count_az": "azure_enrollee_count",
        "subscriber_count_az": "azure_subscriber_count",
    }
    for old, new in rename.items():
        if old in work.columns and new not in work.columns:
            work = work.rename(columns={old: new})
    for col in (
        "xml_enrollment_count", "xml_enrollee_count", "xml_subscriber_count",
        "azure_enrollment_count", "azure_enrollee_count", "azure_subscriber_count",
    ):
        if col not in work.columns:
            work[col] = 0
        work[col] = pd.to_numeric(work[col], errors="coerce").fillna(0).astype(int)
    work["enrollment_diff"] = work["xml_enrollment_count"] - work["azure_enrollment_count"]
    work["enrollee_diff"] = work["xml_enrollee_count"] - work["azure_enrollee_count"]
    work["subscriber_diff"] = work["xml_subscriber_count"] - work["azure_subscriber_count"]
    work["reason_bucket"] = ""
    if xml_only is not None and not xml_only.empty and "difference_reason" in xml_only.columns:
        reason_cols = ["issuer", "year", "month", "insurance_type", "status"]
        reasons = xml_only[reason_cols + ["difference_reason"]].drop_duplicates()
        work = work.merge(reasons, on=reason_cols, how="left", suffixes=("", "_r"))
        if "difference_reason" in work.columns:
            work["reason_bucket"] = work["difference_reason"].fillna(work.get("reason_bucket", ""))
            work = work.drop(columns=["difference_reason"], errors="ignore")
    for c in SUMMARY_COLS:
        if c not in work.columns:
            work[c] = ""
    return work[SUMMARY_COLS].sort_values(
        ["issuer", "year", "month", "insurance_type", "status"],
    ).reset_index(drop=True)


def _model_h_summary_from_canonical(
    xml_canonical: pd.DataFrame,
    az_canonical: pd.DataFrame,
) -> pd.DataFrame:
    """Build Model H summary when detail_df is not in memory."""
    xml_dash = _chandra_dashboard(xml_canonical, source="xml")
    az_dash = _chandra_dashboard(az_canonical, source="azure")
    h = score_model_h(xml_dash, az_dash)
    xml_only = build_model_h_xml_not_in_azure(h["detail"], az_dash)
    detail = h["detail"]
    if not detail.empty:
        detail = detail.rename(columns={
            "enrollment_count_xml": "xml_enrollment_count",
            "enrollee_count_xml": "xml_enrollee_count",
            "subscriber_count_xml": "xml_subscriber_count",
            "enrollment_count_az": "azure_enrollment_count",
            "enrollee_count_az": "azure_enrollee_count",
            "subscriber_count_az": "azure_subscriber_count",
        })
    return _enrich_model_h_summary(detail, xml_only)


def _canonical_detail_records(df: pd.DataFrame, *, side: str) -> pd.DataFrame:
    """Record-level canonical detail for XML or Azure partition exports."""
    if df.empty:
        return pd.DataFrame(columns=DETAIL_COLS)
    work = df.copy()
    out = pd.DataFrame({
        "issuer": _col(work, "issuer"),
        "year": _col(work, "year"),
        "month": _col(work, "month").str.zfill(2),
        "insurance_type": _col(work, "insurance_type"),
        "status": _status_series(work),
        "policy_id": _col(work, "policy_id"),
        "member_id": _col(work, "member_id"),
        "subscriber_id": _col(work, "subscriber_id"),
        "event_date": (
            _col(work, "event_date")
            if "event_date" in work.columns
            else _col(work, "file_event_date")
        ),
        "benefit_effective_date": _col(work, "benefit_effective_date").str[:10],
        "benefit_end_date": _col(work, "benefit_end_date").str[:10],
        "source_file": _col(work, "source_file") if "source_file" in work.columns else "",
        "source_table": _col(work, "source_table"),
        "match_status": side.upper(),
        "difference_reason": "",
    })
    return out[DETAIL_COLS]


def _comparison_detail_records(
    xml_canonical: pd.DataFrame,
    az_canonical: pd.DataFrame,
) -> pd.DataFrame:
    """Record-level comparison detail via outer join on grain keys."""
    keys = ["issuer", "year", "month", "insurance_type", "policy_id", "member_id"]
    xml_d = _canonical_detail_records(xml_canonical, side="xml")
    az_d = _canonical_detail_records(az_canonical, side="azure")
    if xml_d.empty and az_d.empty:
        return pd.DataFrame(columns=DETAIL_COLS)
    for k in keys:
        if k in xml_d.columns:
            xml_d[k] = xml_d[k].astype(str)
        if k in az_d.columns:
            az_d[k] = az_d[k].astype(str)
    merged = xml_d.merge(az_d, on=keys, how="outer", suffixes=("_xml", "_az"), indicator=True)
    rows: list[dict[str, Any]] = []
    for _, row in merged.iterrows():
        side = row.get("_merge", "")
        if side == "both":
            match_status = "MATCHED"
            diff_reason = ""
        elif side == "left_only":
            match_status = "XML_ONLY"
            diff_reason = "XML_RECORD_NOT_IN_AZURE"
        else:
            match_status = "AZURE_ONLY"
            diff_reason = "AZURE_RECORD_NOT_IN_XML"

        def _pick(field: str) -> str:
            xv, av = row.get(f"{field}_xml", ""), row.get(f"{field}_az", "")
            xs, as_ = str(xv) if pd.notna(xv) else "", str(av) if pd.notna(av) else ""
            return xs if xs.strip() else as_

        rows.append({
            "issuer": _pick("issuer") or row.get("issuer", ""),
            "year": _pick("year") or row.get("year", ""),
            "month": _zmonth(_pick("month") or row.get("month", "")),
            "insurance_type": _pick("insurance_type") or row.get("insurance_type", ""),
            "status": _pick("status") or row.get("status", ""),
            "policy_id": _pick("policy_id") or row.get("policy_id", ""),
            "member_id": _pick("member_id") or row.get("member_id", ""),
            "subscriber_id": _pick("subscriber_id") or row.get("subscriber_id", ""),
            "event_date": _pick("event_date"),
            "benefit_effective_date": _pick("benefit_effective_date"),
            "benefit_end_date": _pick("benefit_end_date"),
            "source_file": _pick("source_file"),
            "source_table": _pick("source_table"),
            "match_status": match_status,
            "difference_reason": diff_reason,
        })
    return pd.DataFrame(rows, columns=DETAIL_COLS)


def _write_partition_bundle(
    base: Path,
    *,
    prefix: str,
    summary_df: pd.DataFrame,
    detail_df: pd.DataFrame,
    export_errors: ExportErrors | None = None,
) -> dict[str, str]:
    """Write summary HTML/XLSX + detail CSV for one partition scope."""
    base.mkdir(parents=True, exist_ok=True)
    paths: dict[str, str] = {}
    summary_name = f"{prefix}_summary"
    detail_name = f"{prefix}_detail"

    csv_path = base / f"{detail_name}.csv"
    safe_write_csv(
        csv_path, detail_df, table_name=detail_name,
        drop_duplicate_value_columns=False, export_errors=export_errors,
    )
    paths["detail_csv"] = str(csv_path)

    xlsx_path = base / f"{summary_name}.xlsx"
    safe_write_excel(
        xlsx_path, {summary_name: summary_df},
        export_errors=export_errors,
        drop_duplicate_value_columns=False,
    )
    paths["summary_xlsx"] = str(xlsx_path)

    html_path = base / f"{summary_name}.html"
    safe_write_html_report(
        html_path,
        title=f"{prefix.replace('_', ' ').title()}",
        summary_df=summary_df,
        detail_df=detail_df.head(500) if not detail_df.empty else pd.DataFrame(),
        export_errors=export_errors,
    )
    paths["summary_html"] = str(html_path)
    return paths


def _generate_side_partitions(
    issuer_dir: Path,
    side: str,
    *,
    summary_all: pd.DataFrame,
    detail_all: pd.DataFrame,
    partitions: list[Partition],
    export_errors: ExportErrors | None = None,
) -> list[str]:
    """Generate xml/ or azure/ partition tree; return HTML link fragments."""
    links: list[str] = []
    side_root = issuer_dir / side
    prefix = side

    all_paths = _write_partition_bundle(
        side_root / "all_years",
        prefix=f"{prefix}_all_years",
        summary_df=summary_all,
        detail_df=detail_all,
        export_errors=export_errors,
    )
    rel = f"{side}/all_years/{prefix}_all_years_summary.html"
    links.append(f'<li><a href="{issuer_dir.name}/{rel}">{side.upper()} — all years</a></li>')

    years = sorted({p.year for p in partitions})
    for year in years:
        year_summary = _filter_year(summary_all, year)
        year_detail = _filter_year(detail_all, year)
        _write_partition_bundle(
            side_root / year,
            prefix=f"{prefix}_year",
            summary_df=year_summary,
            detail_df=year_detail,
            export_errors=export_errors,
        )
        links.append(
            f'<li><a href="{issuer_dir.name}/{side}/{year}/{prefix}_year_summary.html">'
            f"{side.upper()} — {year}</a></li>"
        )
        for part in [p for p in partitions if p.year == year]:
            mo = _zmonth(part.month)
            month_summary = _filter_year_month(summary_all, part.year, part.month)
            month_detail = _filter_year_month(detail_all, part.year, part.month)
            _write_partition_bundle(
                side_root / year / mo,
                prefix=f"{prefix}_month",
                summary_df=month_summary,
                detail_df=month_detail,
                export_errors=export_errors,
            )
            links.append(
                f'<li><a href="{issuer_dir.name}/{side}/{year}/{mo}/{prefix}_month_summary.html">'
                f"{side.upper()} — {year}-{mo}</a></li>"
            )
    return links


def generate_partitioned_reports_for_issuer(
    *,
    issuer: str,
    partitions: list[Partition],
    xml_canonical: pd.DataFrame,
    az_canonical: pd.DataFrame,
    model_h: dict[str, Any] | None = None,
    azure_zero_reason: str = "",
    export_errors: ExportErrors | None = None,
) -> dict[str, Any]:
    """Generate partitioned reports for one issuer."""
    root = partitioned_reports_root()
    issuer_dir = root / str(issuer)
    issuer_dir.mkdir(parents=True, exist_ok=True)
    issuer_parts = [p for p in partitions if p.issuer == str(issuer)]

    mh = model_h or {}
    summary_all = mh.get("detail_df")
    xml_only = mh.get("xml_only_df")
    zero_reason = azure_zero_reason or mh.get("azure_zero_reason", "")
    if summary_all is None or (isinstance(summary_all, pd.DataFrame) and summary_all.empty):
        xml_dash = _chandra_dashboard(xml_canonical, source="xml")
        az_dash = _chandra_dashboard(az_canonical, source="azure")
        h = score_model_h(xml_dash, az_dash)
        xml_only = build_model_h_xml_not_in_azure(
            h["detail"], az_dash, azure_zero_reason=zero_reason,
        )
        detail = h["detail"]
        if not detail.empty:
            detail = detail.rename(columns={
                "enrollment_count_xml": "xml_enrollment_count",
                "enrollee_count_xml": "xml_enrollee_count",
                "subscriber_count_xml": "xml_subscriber_count",
                "enrollment_count_az": "azure_enrollment_count",
                "enrollee_count_az": "azure_enrollee_count",
                "subscriber_count_az": "azure_subscriber_count",
            })
        summary_all = _enrich_model_h_summary(detail, xml_only)
    else:
        summary_all = _enrich_model_h_summary(summary_all, xml_only)

    xml_summary_all = _filter_xml_side_summary(summary_all)
    azure_summary_all = _filter_azure_side_summary(summary_all)
    comp_summary_all = _comparison_side_summary(summary_all)

    xml_detail_all = _canonical_detail_records(xml_canonical, side="xml")
    az_detail_all = _canonical_detail_records(az_canonical, side="azure")
    comp_detail_all = _comparison_detail_records(xml_canonical, az_canonical)

    issuer_summary = comp_summary_all.copy()
    if not issuer_summary.empty and "issuer" in issuer_summary.columns:
        issuer_summary = issuer_summary[issuer_summary["issuer"].astype(str) == str(issuer)]

    _write_partition_bundle(
        issuer_dir,
        prefix="issuer",
        summary_df=issuer_summary,
        detail_df=comp_detail_all,
        export_errors=export_errors,
    )

    xml_links = _generate_side_partitions(
        issuer_dir, "xml",
        summary_all=xml_summary_all,
        detail_all=xml_detail_all,
        partitions=issuer_parts,
        export_errors=export_errors,
    )
    azure_links = _generate_side_partitions(
        issuer_dir, "azure",
        summary_all=azure_summary_all,
        detail_all=az_detail_all,
        partitions=issuer_parts,
        export_errors=export_errors,
    )
    comp_links = _generate_side_partitions(
        issuer_dir, "comparison",
        summary_all=comp_summary_all,
        detail_all=comp_detail_all,
        partitions=issuer_parts,
        export_errors=export_errors,
    )

    return {
        "issuer": issuer,
        "issuer_dir": str(issuer_dir),
        "summary": summary_all,
        "xml_detail": xml_detail_all,
        "az_detail": az_detail_all,
        "comp_detail": comp_detail_all,
        "xml_only": xml_only if isinstance(xml_only, pd.DataFrame) else pd.DataFrame(),
        "count_audit": mh.get("count_audit_df", pd.DataFrame()),
        "links": {
            "issuer_summary": f"{issuer}/issuer_summary.html",
            "xml": xml_links,
            "azure": azure_links,
            "comparison": comp_links,
        },
    }


def _rel_href(from_root: Path, target: Path) -> str:
    try:
        return target.relative_to(from_root).as_posix()
    except ValueError:
        return target.as_posix()


def _read_optional_csv(path: Path) -> pd.DataFrame:
    if path.exists():
        try:
            return pd.read_csv(path)
        except Exception as exc:
            logger.warning("Could not read %s: %s", path, exc)
    return pd.DataFrame()


def generate_partitioned_reports_index(
    *,
    issuer_contexts: list[dict[str, Any]],
    partitions: list[Partition],
    export_errors: ExportErrors | None = None,
) -> Path:
    """Write outputs/partitioned_reports/index.html and index.xlsx."""
    root = partitioned_reports_root()
    dbg = settings.outputs_path / "debug"
    cmp_dir = settings.outputs_path / "comparison"

    sections: list[str] = []
    all_summaries: list[pd.DataFrame] = []
    all_model_h: list[pd.DataFrame] = []
    all_xml_only: list[pd.DataFrame] = []
    all_count_audit: list[pd.DataFrame] = []

    final_biz = cmp_dir / "final_business_result.html"
    final_exec = dbg / "final_executive_summary.md"
    global_links = [
        f'<li><a href="{_rel_href(root, final_biz)}">Final business result (Model H)</a></li>'
        if final_biz.exists() else "",
        f'<li><a href="{_rel_href(root, final_exec)}">Final executive summary</a></li>'
        if final_exec.exists() else "",
    ]

    for ctx in issuer_contexts:
        issuer = ctx["issuer"]
        links = ctx.get("links", {})
        summary = ctx.get("summary", pd.DataFrame())
        if isinstance(summary, pd.DataFrame) and not summary.empty:
            all_summaries.append(summary)
            all_model_h.append(summary)

        xml_only = ctx.get("xml_only", pd.DataFrame())
        if isinstance(xml_only, pd.DataFrame) and not xml_only.empty:
            all_xml_only.append(xml_only)

        audit = ctx.get("count_audit", pd.DataFrame())
        if isinstance(audit, pd.DataFrame) and not audit.empty:
            all_count_audit.append(audit)

        section = [
            f"<h2>Issuer {issuer}</h2>",
            "<ul>",
            f'<li><a href="{links.get("issuer_summary", f"{issuer}/issuer_summary.html")}">Issuer summary</a></li>',
        ]
        section.extend(links.get("xml", []))
        section.extend(links.get("azure", []))
        section.extend(links.get("comparison", []))
        section.append("</ul>")
        sections.append("\n".join(section))

    index_html = [
        "<!DOCTYPE html><html><head><meta charset='utf-8'>",
        "<title>Partitioned Reports Index</title>",
        "<style>body{font-family:sans-serif;margin:1.5rem}ul{margin:0.25rem 0 1rem 1.5rem}</style>",
        "</head><body>",
        "<h1>Partitioned Reports</h1>",
        "<p>XML-derived, Azure-derived, and comparison reports by issuer, year, and month.</p>",
        "<h2>Global</h2><ul>",
        *[ln for ln in global_links if ln],
        "</ul>",
        *sections,
        "</body></html>",
    ]
    index_path = root / "index.html"
    index_path.write_text("\n".join(index_html), encoding="utf-8")
    logger.info("Wrote partitioned reports index: %s", index_path)

    issuer_summary_df = (
        pd.concat(all_summaries, ignore_index=True) if all_summaries else pd.DataFrame()
    )
    model_h_detail = (
        pd.concat(all_model_h, ignore_index=True) if all_model_h else
        _read_optional_csv(dbg / "model_h_xml_vs_azure_detail.csv")
    )
    xml_not_in_az = (
        pd.concat(all_xml_only, ignore_index=True) if all_xml_only else
        _read_optional_csv(dbg / "model_h_xml_not_in_azure.csv")
    )
    az_not_in_xml = _read_optional_csv(dbg / "azure_not_in_xml_reason_summary.csv")
    count_audit = (
        pd.concat(all_count_audit, ignore_index=True) if all_count_audit else
        _read_optional_csv(dbg / "model_h_count_column_audit.csv")
    )
    month_basis = _read_optional_csv(dbg / "month_basis_comparison.csv")

    exec_rows: list[dict[str, str]] = []
    exec_path = dbg / "final_executive_summary.md"
    if exec_path.exists():
        exec_rows.append({"summary": exec_path.read_text(encoding="utf-8")[:32000]})
    executive_df = pd.DataFrame(exec_rows) if exec_rows else pd.DataFrame()

    audit_txt = dbg / "data_source_audit.txt"
    if audit_txt.exists():
        lines = audit_txt.read_text(encoding="utf-8").strip().splitlines()
        data_source_audit = pd.DataFrame({"line": lines})

    coverage_df = pd.DataFrame([
        {"issuer": p.issuer, "year": p.year, "month": p.month} for p in partitions
    ])

    sheets = {
        "Executive_Summary": executive_df,
        "Issuer_Summary": issuer_summary_df if not issuer_summary_df.empty else coverage_df,
        "Model_H_Detail": model_h_detail,
        "XML_Not_In_Azure": xml_not_in_az,
        "Azure_Not_In_XML": az_not_in_xml,
        "Count_Audit": count_audit,
        "Month_Basis": month_basis,
        "Data_Source_Audit": data_source_audit,
    }
    xlsx_path = root / "index.xlsx"
    safe_write_excel(
        xlsx_path, sheets,
        export_errors=export_errors,
        drop_duplicate_value_columns=False,
    )
    logger.info("Wrote partitioned reports workbook: %s", xlsx_path)
    return index_path


def generate_partitioned_reports(
    *,
    issuer_contexts: list[dict[str, Any]],
    partitions: list[Partition],
    export_errors: ExportErrors | None = None,
) -> Path:
    """Finalize index after per-issuer partitioned reports."""
    return generate_partitioned_reports_index(
        issuer_contexts=issuer_contexts,
        partitions=partitions,
        export_errors=export_errors,
    )
