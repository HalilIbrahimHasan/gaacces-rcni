"""
Fast business reporting — lightweight Excel/CSV summaries without legacy assets pipeline.

Does not call business_review_package, full_data_exports, or legacy assets reports.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from azure_reconciliation.business_ready_exports import (
    build_business_ready_records,
    business_ready_dashboard_summary,
)
from azure_reconciliation.chandra_business_format import (
    CHANDRA_BUSINESS_COLUMNS_CORE,
    chandra_year_rollup,
    to_chandra_business_summary,
)
from azure_reconciliation.partition_discovery import Partition, discover_partitions
from azure_reconciliation.prior_year_benefit_filter import (
    apply_prior_year_benefit_filter,
    resolve_reporting_year,
)
from azure_reconciliation.reconciliation_analysis import _chandra_dashboard
from azure_reconciliation.safe_export import ExportErrors, safe_write_csv, safe_write_excel
from azure_reconciliation.xml_business_reports import PK, process_issuer_xml_business
from azure_reconciliation.xml_loader import load_xml_rows
from config.config import settings
from utils.logger import get_logger

logger = get_logger(__name__)

FAST_BUSINESS_READY_COLUMNS = [
    "issuer", "year", "month", "business_month", "insurance_type", "status_Id",
    "enrolleeStatus", "canonical_enrollment_id", "canonical_enrollee_id",
    "policy_id", "member_id", "benefit_effective_date", "benefit_effective_year",
    "selected_transaction_date", "source_file", "dashboard_group_key",
    "raw_transaction_count", "raw_source_files", "raw_transaction_keys", "selection_reason",
]


def fast_reports_root() -> Path:
    return settings.fast_business_reports_path


def _zmonth(m: str) -> str:
    return str(m).strip().zfill(2)


@dataclass
class ProcessTracker:
    """Loop guard — skip duplicate issuer/year/month processing in one run."""

    processed_keys: set[str] = field(default_factory=set)
    issuer_start_times: dict[str, float] = field(default_factory=dict)

    def key(self, issuer: str, year: str, month: str | None = None) -> str:
        if month:
            return f"{issuer}|{year}|{_zmonth(month)}"
        return f"{issuer}|{year}"

    def claim(self, issuer: str, year: str, month: str | None = None) -> bool:
        k = self.key(issuer, year, month)
        if k in self.processed_keys:
            logger.warning("SKIP duplicate issuer/year/month key: %s", k)
            return False
        self.processed_keys.add(k)
        return True

    def start_issuer(self, issuer: str, year: str) -> None:
        self.issuer_start_times[f"{issuer}|{year}"] = time.monotonic()

    def done_issuer(self, issuer: str, year: str) -> float:
        k = f"{issuer}|{year}"
        start = self.issuer_start_times.get(k, time.monotonic())
        elapsed = time.monotonic() - start
        logger.info("DONE issuer=%s year=%s total_time=%.1fs", issuer, year, elapsed)
        return elapsed


def _attach_source_file(work: pd.DataFrame, canonical: pd.DataFrame) -> pd.DataFrame:
    """Add source_file from canonical when missing on business-ready rows."""
    out = work.copy()
    if "source_file" in out.columns and out["source_file"].astype(str).str.strip().ne("").any():
        return out
    if canonical.empty:
        out["source_file"] = ""
        return out
    merge_keys = [c for c in PK + ["year", "month"] if c in out.columns and c in canonical.columns]
    if not merge_keys:
        out["source_file"] = ""
        return out
    src = canonical[merge_keys + ["source_file"]].drop_duplicates(subset=merge_keys, keep="last")
    out = out.merge(src, on=merge_keys, how="left", suffixes=("", "_canon"))
    if "source_file_canon" in out.columns:
        if "source_file" not in out.columns:
            out["source_file"] = out["source_file_canon"]
        else:
            mask = out["source_file"].astype(str).str.strip().isin(["", "nan", "None"])
            out.loc[mask, "source_file"] = out.loc[mask, "source_file_canon"]
        out = out.drop(columns=["source_file_canon"], errors="ignore")
    if "source_file" not in out.columns:
        out["source_file"] = ""
    return out


def _prepare_fast_business_ready(
    result: Any,
    *,
    issuer: str,
    year: str,
    xml_raw: pd.DataFrame,
    reporting_year: str,
    apply_py_filter: bool,
) -> pd.DataFrame:
    df = build_business_ready_records(result, issuer=issuer, year=year, xml_raw=xml_raw)
    df = _attach_source_file(df, result.canonical)
    cols = [c for c in FAST_BUSINESS_READY_COLUMNS if c in df.columns]
    df = df[cols].copy()
    if apply_py_filter and not df.empty:
        before = len(df)
        df = apply_prior_year_benefit_filter(df, reporting_year=reporting_year)
        logger.info(
            "PY filter business_ready %s/%s: %d → %d (reporting_year=%s)",
            issuer, year, before, len(df), reporting_year,
        )
    return df


def _monthly_detail_counts(business_ready: pd.DataFrame) -> pd.DataFrame:
    if business_ready.empty:
        return pd.DataFrame(columns=[
            "issuer", "year", "month", "insurance_type", "enrolleeStatus",
            "Enrollment_Count", "Enrollee_Count", "business_ready_record_count",
        ])
    rows = []
    group_cols = ["issuer", "year", "month", "insurance_type", "enrolleeStatus"]
    for key, grp in business_ready.groupby(
        [c for c in group_cols if c in business_ready.columns], dropna=False,
    ):
        if isinstance(key, tuple):
            vals = dict(zip([c for c in group_cols if c in business_ready.columns], key))
        else:
            vals = {group_cols[0]: key}
        enroll = grp["canonical_enrollment_id"].astype(str).str.strip()
        enrollee = grp["canonical_enrollee_id"].astype(str).str.strip()
        rows.append({
            **vals,
            "Enrollment_Count": int(enroll[enroll != ""].nunique()),
            "Enrollee_Count": int(enrollee[enrollee != ""].nunique()),
            "business_ready_record_count": len(grp),
        })
    return pd.DataFrame(rows)


def _write_pair(
    base: Path,
    stem: str,
    df: pd.DataFrame,
    *,
    export_errors: ExportErrors | None = None,
) -> None:
    if df.empty:
        return
    if settings.generate_xlsx_reports:
        safe_write_excel(
            base / f"{stem}.xlsx", {stem: df},
            export_errors=export_errors, drop_duplicate_value_columns=False,
        )
    safe_write_csv(
        base / f"{stem}.csv", df,
        export_errors=export_errors, drop_duplicate_value_columns=False,
    )


def _write_plotly_dashboards(
    issuer: str,
    year: str,
    chandra_monthly: pd.DataFrame,
    business_ready: pd.DataFrame,
    out_dir: Path,
) -> list[str]:
    """Optional Plotly HTML dashboards — failures are logged, not raised."""
    written: list[str] = []
    if not settings.generate_plotly_dashboards:
        return written
    try:
        import plotly.graph_objects as go
    except ImportError:
        logger.warning("Plotly not installed — skipping dashboards for %s/%s", issuer, year)
        return written

    out_dir.mkdir(parents=True, exist_ok=True)
    try:
        if not chandra_monthly.empty and "GAA_Load_Date" in chandra_monthly.columns:
            work = chandra_monthly.copy()
            work["_month"] = work["GAA_Load_Date"].astype(str).str.split("/").str[0].str.zfill(2)
            fig = go.Figure()
            for status, grp in work.groupby("enrolleeStatus"):
                monthly = grp.groupby("_month", as_index=False)["Enrollment_Count"].sum()
                fig.add_trace(go.Scatter(
                    x=monthly["_month"], y=monthly["Enrollment_Count"],
                    mode="lines+markers", name=str(status),
                ))
            fig.update_layout(title=f"{issuer}/{year} — Monthly Status Trend", template="plotly_white")
            p = out_dir / "monthly_status_trend.html"
            fig.write_html(str(p), include_plotlyjs="cdn")
            written.append(str(p))

            fig2 = go.Figure()
            for status, grp in work.groupby("enrolleeStatus"):
                monthly = grp.groupby("_month", as_index=False)["Enrollment_Count"].sum()
                fig2.add_trace(go.Bar(x=monthly["_month"], y=monthly["Enrollment_Count"], name=str(status)))
            fig2.update_layout(barmode="stack", title=f"{issuer}/{year} — Status Mix", template="plotly_white")
            p2 = out_dir / "issuer_status_mix.html"
            fig2.write_html(str(p2), include_plotlyjs="cdn")
            written.append(str(p2))

            fig3 = go.Figure()
            agg = work.groupby("_month", as_index=False).agg({
                "Enrollment_Count": "sum", "Enrollee_Count": "sum",
            })
            fig3.add_trace(go.Bar(x=agg["_month"], y=agg["Enrollment_Count"], name="Enrollment_Count"))
            fig3.add_trace(go.Bar(x=agg["_month"], y=agg["Enrollee_Count"], name="Enrollee_Count"))
            fig3.update_layout(barmode="group", title=f"{issuer}/{year} — Enrollment vs Enrollee", template="plotly_white")
            p3 = out_dir / "enrollment_vs_enrollee.html"
            fig3.write_html(str(p3), include_plotlyjs="cdn")
            written.append(str(p3))

        if not business_ready.empty:
            top = business_ready.groupby("month").size().reset_index(name="records").sort_values("records", ascending=False)
            fig4 = go.Figure(data=[go.Bar(x=top["month"].astype(str), y=top["records"])])
            fig4.update_layout(title=f"{issuer}/{year} — Top Months by Volume", template="plotly_white")
            p4 = out_dir / "top_months_by_volume.html"
            fig4.write_html(str(p4), include_plotlyjs="cdn")
            written.append(str(p4))
    except Exception as exc:
        logger.warning("Plotly dashboard failed for %s/%s: %s", issuer, year, exc)
    return written


def process_issuer_year_fast(
    issuer: str,
    year: str,
    partitions: list[Partition],
    *,
    parse_source: bool = False,
    force: bool = False,
    tracker: ProcessTracker | None = None,
    export_errors: ExportErrors | None = None,
) -> dict[str, Any]:
    """Process one issuer/year — business ready + summaries only."""
    tr = tracker or ProcessTracker()
    if not tr.claim(issuer, year):
        return {"issuer": issuer, "year": year, "skipped": True}

    tr.start_issuer(issuer, year)
    out_base = fast_reports_root() / issuer / year
    br_dir = out_base / "business_ready"
    rep_dir = out_base / "reports"
    dash_dir = out_base / "dashboards"
    marker = br_dir / "business_ready_all_months.xlsx"

    if marker.exists() and not force:
        logger.info("SKIP existing fast reports for %s/%s (use --force to regenerate)", issuer, year)
        return {"issuer": issuer, "year": year, "skipped": True, "reason": "exists"}

    br_dir.mkdir(parents=True, exist_ok=True)
    rep_dir.mkdir(parents=True, exist_ok=True)

    ry = resolve_reporting_year(partition_year=year)
    apply_py = settings.filter_prior_year_benefit_effective

    xml_raw = load_xml_rows(
        prefer_staging=not parse_source,
        issuer_filter=issuer,
        year_filter=year,
    )
    if xml_raw.empty:
        logger.warning("No XML rows for %s/%s", issuer, year)
        return {"issuer": issuer, "year": year, "rows": 0}

    parts = [p for p in partitions if p.issuer == issuer and p.year == year]
    result = process_issuer_xml_business(issuer, xml_raw, parts)

    business_ready = _prepare_fast_business_ready(
        result, issuer=issuer, year=year, xml_raw=xml_raw,
        reporting_year=ry, apply_py_filter=False,
    )
    if apply_py and not business_ready.empty:
        br_before = len(business_ready)
        business_ready = apply_prior_year_benefit_filter(business_ready, reporting_year=ry)
        logger.info(
            "PY filter business_ready %s/%s: %d → %d (benefit_effective_date)",
            issuer, year, br_before, len(business_ready),
        )

    lifecycle_for_summary = result.lifecycle_input[
        result.lifecycle_input["year"].astype(str) == str(year)
    ].copy()
    if apply_py and not lifecycle_for_summary.empty:
        li_before = len(lifecycle_for_summary)
        lifecycle_for_summary = apply_prior_year_benefit_filter(lifecycle_for_summary, reporting_year=ry)
        logger.info(
            "PY filter lifecycle_input %s/%s: %d → %d (benefit_effective_date)",
            issuer, year, li_before, len(lifecycle_for_summary),
        )

    model_h = _chandra_dashboard(lifecycle_for_summary, source="xml") if not lifecycle_for_summary.empty else result.business_monthly
    chandra_monthly = to_chandra_business_summary(model_h)
    chandra_yearly = chandra_year_rollup(chandra_monthly)
    br_summary = business_ready_dashboard_summary(business_ready)
    monthly_detail = _monthly_detail_counts(business_ready)

    _write_pair(br_dir, "business_ready_all_months", business_ready, export_errors=export_errors)
    if settings.generate_xlsx_reports:
        safe_write_excel(
            br_dir / "business_ready_summary.xlsx",
            {"Business_Ready_Summary": br_summary},
            export_errors=export_errors,
            drop_duplicate_value_columns=False,
        )
    _write_pair(rep_dir, "enrollment_summary", chandra_monthly, export_errors=export_errors)
    _write_pair(rep_dir, "issuer_year_rollup", chandra_yearly, export_errors=export_errors)

    dashboards = _write_plotly_dashboards(issuer, year, chandra_monthly, business_ready, dash_dir)
    elapsed = tr.done_issuer(issuer, year)

    return {
        "issuer": issuer,
        "year": year,
        "business_ready_rows": len(business_ready),
        "summary_rows": len(chandra_monthly),
        "dashboards": dashboards,
        "elapsed_sec": elapsed,
        "output_dir": str(out_base),
        "chandra_monthly": chandra_monthly,
        "business_ready": business_ready,
        "monthly_detail": monthly_detail,
    }


def _write_year_rollup(
    year: str,
    issuer_results: list[dict[str, Any]],
    *,
    export_errors: ExportErrors | None = None,
) -> Path:
    """Aggregate all issuers for one reporting year."""
    year_dir = fast_reports_root() / year
    dash_dir = year_dir / "dashboards"
    year_dir.mkdir(parents=True, exist_ok=True)

    chandra_parts = [r["chandra_monthly"] for r in issuer_results if not r.get("skipped") and "chandra_monthly" in r]
    br_parts = [r["business_ready"] for r in issuer_results if not r.get("skipped") and "business_ready" in r]

    all_summary = pd.concat(chandra_parts, ignore_index=True) if chandra_parts else pd.DataFrame(columns=CHANDRA_BUSINESS_COLUMNS_CORE)
    all_br = pd.concat(br_parts, ignore_index=True) if br_parts else pd.DataFrame()

    _write_pair(year_dir, "all_issuers_business_summary", all_summary, export_errors=export_errors)
    _write_pair(year_dir, "all_issuers_business_ready_records", all_br, export_errors=export_errors)

    if settings.generate_plotly_dashboards and not all_summary.empty:
        try:
            import plotly.graph_objects as go
            from plotly.subplots import make_subplots

            dash_dir.mkdir(parents=True, exist_ok=True)
            by_issuer = all_summary.groupby("GAA_HIOS_ID", as_index=False).agg({
                "Enrollment_Count": "sum", "Enrollee_Count": "sum",
            })
            fig = make_subplots(rows=2, cols=2, subplot_titles=(
                "Enrollment by Issuer", "Enrollee by Issuer",
                "Status Mix", "Monthly Total Trend",
            ))
            fig.add_trace(go.Bar(x=by_issuer["GAA_HIOS_ID"], y=by_issuer["Enrollment_Count"]), row=1, col=1)
            fig.add_trace(go.Bar(x=by_issuer["GAA_HIOS_ID"], y=by_issuer["Enrollee_Count"]), row=1, col=2)
            status_mix = all_summary.groupby("enrolleeStatus", as_index=False)["Enrollment_Count"].sum()
            fig.add_trace(go.Pie(labels=status_mix["enrolleeStatus"], values=status_mix["Enrollment_Count"]), row=2, col=1)
            if "GAA_Load_Date" in all_summary.columns:
                work = all_summary.copy()
                work["_month"] = work["GAA_Load_Date"].astype(str).str.split("/").str[0].str.zfill(2)
                trend = work.groupby("_month", as_index=False)["Enrollment_Count"].sum()
                fig.add_trace(go.Scatter(x=trend["_month"], y=trend["Enrollment_Count"], mode="lines+markers"), row=2, col=2)
            fig.update_layout(title=f"All Issuers {year}", template="plotly_white", height=800)
            dash_path = dash_dir / "all_issuers_dashboard.html"
            fig.write_html(str(dash_path), include_plotlyjs="cdn")
        except Exception as exc:
            logger.warning("All-issuers dashboard failed: %s", exc)

    return year_dir


def _write_index_html(year: str, issuer_results: list[dict[str, Any]]) -> Path:
    root = fast_reports_root()
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    lines = [
        "<!DOCTYPE html><html><head><meta charset='utf-8'>",
        "<title>Fast Business Reports</title>",
        "<style>body{font-family:system-ui,sans-serif;margin:2rem}ul{line-height:1.8}</style>",
        "</head><body>",
        "<h1>Fast Business Reports</h1>",
        f"<p>Generated: {now}</p>",
        f"<h2>Year {year}</h2>",
        "<ul>",
        f"<li><a href='{year}/all_issuers_business_summary.xlsx'>All issuers summary</a></li>",
        f"<li><a href='{year}/all_issuers_business_ready_records.xlsx'>All issuers business ready</a></li>",
        f"<li><a href='{year}/dashboards/all_issuers_dashboard.html'>All issuers dashboard</a></li>",
        "</ul><h2>Issuers</h2><ul>",
    ]
    for r in sorted(issuer_results, key=lambda x: x.get("issuer", "")):
        if r.get("skipped"):
            continue
        iss = r["issuer"]
        lines.append(
            f"<li><a href='{iss}/{year}/reports/enrollment_summary.xlsx'>{iss}</a> "
            f"({r.get('business_ready_rows', 0)} business-ready rows)</li>"
        )
    lines.extend(["</ul></body></html>"])
    path = root / "index.html"
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


def run_fast_business_reports(
    *,
    issuer_filter: str | None = None,
    year_filter: str | None = None,
    month_filter: str | None = None,
    all_issuers: bool = False,
    parse_source: bool = False,
    force: bool = False,
    export_errors: ExportErrors | None = None,
) -> dict[str, Any]:
    """
    Fast business reporting entry point.

    Does not call business_review_package, full_data_exports, or legacy assets.
    """
    settings.apply_fast_business_report_mode(True)
    settings.ensure_dirs()
    fast_reports_root().mkdir(parents=True, exist_ok=True)

    if all_issuers:
        issuer_filter = None

    partitions = discover_partitions(
        issuer_filter=issuer_filter,
        year_filter=year_filter,
        month_filter=month_filter,
    )
    if not partitions:
        raise RuntimeError("No source_data partitions found for fast reporting")

    pairs = sorted({(p.issuer, p.year) for p in partitions})
    logger.info(
        "FAST_BUSINESS_REPORT_MODE=true — processing %d issuer/year pair(s) for year=%s",
        len(pairs), year_filter or "all",
    )

    tracker = ProcessTracker()
    results: list[dict[str, Any]] = []
    run_start = time.monotonic()

    for issuer, year in pairs:
        for part in [p for p in partitions if p.issuer == issuer and p.year == year]:
            logger.info("START issuer=%s year=%s month=%s", part.issuer, part.year, part.month)

        try:
            res = process_issuer_year_fast(
                issuer, year, partitions,
                parse_source=parse_source,
                force=force,
                tracker=tracker,
                export_errors=export_errors,
            )
            results.append(res)
            if not res.get("skipped"):
                logger.info(
                    "DONE issuer=%s year=%s month=all rows=%d",
                    issuer, year, res.get("business_ready_rows", 0),
                )
        except Exception as exc:
            logger.error("Fast reporting failed for %s/%s: %s", issuer, year, exc)
            if export_errors:
                export_errors.record(f"Fast reporting {issuer}/{year}: {exc}")

    year_for_rollup = year_filter or (pairs[0][1] if pairs else "")
    year_dir = _write_year_rollup(year_for_rollup, results, export_errors=export_errors)
    index_path = _write_index_html(year_for_rollup, results)

    elapsed = time.monotonic() - run_start
    logger.info(
        "Fast business reports complete in %.1fs → %s",
        elapsed, fast_reports_root(),
    )

    return {
        "issuer_years": len(results),
        "results": results,
        "output_root": str(fast_reports_root()),
        "year_rollup_dir": str(year_dir),
        "index_html": str(index_path),
        "elapsed_sec": elapsed,
    }
