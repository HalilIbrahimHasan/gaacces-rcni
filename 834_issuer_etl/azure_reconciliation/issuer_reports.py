"""Per-issuer inspectable reports under outputs/issuer_reports/."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from azure_reconciliation.partition_discovery import Partition
from azure_reconciliation.safe_export import (
    ExportErrors,
    safe_write_csv,
    safe_write_html_report,
)
from config.config import settings
from utils.logger import get_logger

logger = get_logger(__name__)


def issuer_reports_root() -> Path:
    d = settings.outputs_path / "issuer_reports"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _zmonth(m: str) -> str:
    return str(m).strip().zfill(2)


def _summary_stats(df: pd.DataFrame, *, source: str) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame([{"source": source, "rows": 0}])
    rows: list[dict[str, Any]] = [{"metric": "row_count", "value": len(df)}]
    for col in ("issuer", "coverage_year", "snapshot_month", "year", "month", "canonical_status"):
        if col in df.columns:
            vc = df[col].astype(str).value_counts().head(20)
            for k, v in vc.items():
                rows.append({"metric": f"{col}={k}", "value": int(v)})
    out = pd.DataFrame(rows)
    out.insert(0, "source", source)
    return out


def _filter_issuer(df: pd.DataFrame, issuer: str) -> pd.DataFrame:
    if df.empty:
        return df
    if "issuer" in df.columns:
        return df[df["issuer"].astype(str) == str(issuer)].copy()
    if "issuer_id" in df.columns:
        return df[df["issuer_id"].astype(str) == str(issuer)].copy()
    return df


def _filter_year_month(df: pd.DataFrame, year: str, month: str) -> pd.DataFrame:
    if df.empty:
        return df
    work = df.copy()
    yr_col = "coverage_year" if "coverage_year" in work.columns else "year" if "year" in work.columns else None
    mo_col = "snapshot_month" if "snapshot_month" in work.columns else "month" if "month" in work.columns else None
    if yr_col:
        work = work[work[yr_col].astype(str) == str(year)]
    if mo_col:
        work = work[work[mo_col].astype(str).str.zfill(2) == _zmonth(month)]
    return work


def _filter_year(df: pd.DataFrame, year: str) -> pd.DataFrame:
    if df.empty:
        return df
    yr_col = "coverage_year" if "coverage_year" in df.columns else "year" if "year" in df.columns else None
    if not yr_col:
        return df
    return df[df[yr_col].astype(str) == str(year)].copy()


def _write_report_bundle(
    base: Path,
    *,
    prefix: str,
    df: pd.DataFrame,
    export_errors: ExportErrors | None = None,
) -> dict[str, str]:
    base.mkdir(parents=True, exist_ok=True)
    summary = _summary_stats(df, source=prefix)
    paths: dict[str, str] = {}
    csv_name = f"{prefix}.csv"
    paths["csv"] = str(base / csv_name)
    safe_write_csv(base / csv_name, df, table_name=prefix, export_errors=export_errors)
    paths["summary_html"] = str(base / f"{prefix}_summary.html")
    safe_write_html_report(
        base / f"{prefix}_summary.html",
        title=f"{prefix} — summary",
        summary_df=summary,
        export_errors=export_errors,
    )
    paths["detail_html"] = str(base / f"{prefix}_detail_sample.html")
    safe_write_html_report(
        base / f"{prefix}_detail_sample.html",
        title=f"{prefix} — detail sample",
        detail_df=df,
        export_errors=export_errors,
    )
    return paths


def _generate_side_reports(
    issuer_dir: Path,
    side: str,
    df: pd.DataFrame,
    partitions: list[Partition],
    export_errors: ExportErrors | None = None,
) -> list[str]:
    links: list[str] = []
    side_root = issuer_dir / side
    issuer_parts = [p for p in partitions if p.issuer == str(issuer_dir.name)]

    all_paths = _write_report_bundle(
        side_root / "all_months",
        prefix=f"{side}_all_months",
        df=df,
        export_errors=export_errors,
    )
    links.append(f'<li><a href="{side}/all_months/{side}_all_months_summary.html">All months summary</a></li>')

    years = sorted({p.year for p in issuer_parts})
    for year in years:
        year_df = _filter_year(df, year)
        _write_report_bundle(
            side_root / "yearly" / year,
            prefix=f"{side}_year",
            df=year_df,
            export_errors=export_errors,
        )
        links.append(
            f'<li><a href="{side}/yearly/{year}/{side}_year_summary.html">Year {year}</a></li>'
        )
        for part in [p for p in issuer_parts if p.year == year]:
            month_df = _filter_year_month(df, part.year, part.month)
            _write_report_bundle(
                side_root / "monthly" / part.year / _zmonth(part.month),
                prefix=f"{side}_month",
                df=month_df,
                export_errors=export_errors,
            )
            links.append(
                f'<li><a href="{side}/monthly/{part.year}/{_zmonth(part.month)}/{side}_month_summary.html">'
                f'{part.year}-{_zmonth(part.month)}</a></li>'
            )
    return links


def generate_issuer_reports(
    *,
    issuers: list[str],
    partitions: list[Partition],
    xml_raw: pd.DataFrame,
    xml_lifecycle: pd.DataFrame,
    azure_raw: pd.DataFrame,
    azure_lifecycle: pd.DataFrame,
    comparison_detail: pd.DataFrame,
    comparison_summary: pd.DataFrame,
    export_errors: ExportErrors | None = None,
) -> Path:
    """Generate outputs/issuer_reports/<issuer>/... and index.html."""
    root = issuer_reports_root()
    index_sections: list[str] = []

    for issuer in issuers:
        issuer_dir = root / str(issuer)
        issuer_dir.mkdir(parents=True, exist_ok=True)
        issuer_parts = [p for p in partitions if p.issuer == str(issuer)]

        xml_df = _filter_issuer(xml_lifecycle if not xml_lifecycle.empty else xml_raw, issuer)
        az_df = _filter_issuer(azure_lifecycle if not azure_lifecycle.empty else azure_raw, issuer)
        comp_df = comparison_detail.copy()
        if not comp_df.empty and "partition" in comp_df.columns:
            comp_df = comp_df[
                comp_df["partition"].astype(str).str.startswith(f"{issuer}/")
            ]
        comp_sum = comparison_summary.copy()
        if not comp_sum.empty and "partition" in comp_sum.columns:
            comp_sum = comp_sum[
                comp_sum["partition"].astype(str).str.startswith(f"{issuer}/")
            ]

        section = [f"<h2>Issuer {issuer}</h2>", "<h3>XML</h3><ul>"]
        section.extend(_generate_side_reports(issuer_dir, "xml", xml_df, issuer_parts, export_errors))
        section.append("</ul><h3>Azure</h3><ul>")
        section.extend(_generate_side_reports(issuer_dir, "azure", az_df, issuer_parts, export_errors))
        section.append("</ul><h3>Comparison</h3><ul>")
        comp_links = _generate_side_reports(
            issuer_dir, "comparison",
            comp_df if not comp_df.empty else comp_sum,
            issuer_parts, export_errors,
        )
        section.extend(comp_links)
        section.append("</ul>")
        index_sections.append("\n".join(section))

    index_html = [
        "<!DOCTYPE html><html><head><meta charset='utf-8'>",
        "<title>Issuer Reports Index</title>",
        "<style>body{font-family:sans-serif;margin:1.5rem}a{margin-right:1rem}</style>",
        "</head><body>",
        "<h1>Issuer Reports</h1>",
        "<p>XML, Azure, and comparison reports by issuer, year, and month.</p>",
        *index_sections,
        "</body></html>",
    ]
    index_path = root / "index.html"
    index_path.write_text("\n".join(index_html), encoding="utf-8")
    logger.info("Wrote issuer reports index: %s", index_path)
    return index_path
