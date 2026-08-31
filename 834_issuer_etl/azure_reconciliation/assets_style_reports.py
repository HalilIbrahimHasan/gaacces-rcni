"""
Assets-style Model H reports under assets/{issuer}/.../reports/.

Business views use Chandra enrollment summary format; technical fields live under diagnostics/.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from azure_reconciliation.chandra_business_format import (
    chandra_all_years_rollup,
    chandra_year_rollup,
    to_chandra_business_summary,
    write_chandra_business_html,
    write_chandra_business_xlsx,
    write_model_h_month_html,
)
from azure_reconciliation.partition_discovery import Partition
from azure_reconciliation.safe_export import (
    ExportErrors,
    safe_write_csv,
    safe_write_excel,
    safe_write_html_report,
)
from azure_reconciliation.xml_business_reports import IssuerBusinessResult
from config.config import settings
from utils.logger import get_logger

logger = get_logger(__name__)


def assets_reports_root() -> Path:
    d = settings.assets_path
    d.mkdir(parents=True, exist_ok=True)
    return d


def _zmonth(m: str) -> str:
    return str(m).strip().zfill(2)


def _filter_partition_df(
    df: pd.DataFrame,
    part: Partition,
    *,
    kind: str = "canonical",
) -> pd.DataFrame:
    if df.empty:
        return df
    if kind == "lifecycle_snapshot":
        mask = df["issuer"].astype(str) == str(part.issuer)
        if "coverage_year" in df.columns:
            mask &= df["coverage_year"].astype(str) == str(part.year)
        if "snapshot_month" in df.columns:
            mask &= df["snapshot_month"].astype(str).map(_zmonth) == _zmonth(part.month)
        return df[mask].copy()
    if kind == "xml_raw":
        mask = df.get("issuer", pd.Series(dtype=str)).astype(str) == str(part.issuer)
        if "year" in df.columns:
            mask &= df["year"].astype(str) == str(part.year)
        if "month" in df.columns:
            mask &= df["month"].astype(str).map(_zmonth) == _zmonth(part.month)
        return df[mask].copy()
    mask = df.get("issuer", pd.Series(dtype=str)).astype(str) == str(part.issuer)
    if "year" in df.columns:
        mask &= df["year"].astype(str) == str(part.year)
    if "month" in df.columns:
        mask &= df["month"].astype(str).map(_zmonth) == _zmonth(part.month)
    return df[mask].copy()


def _filter_year_df(df: pd.DataFrame, issuer: str, year: str) -> pd.DataFrame:
    if df.empty:
        return df
    mask = df["issuer"].astype(str) == str(issuer)
    if "year" in df.columns:
        mask &= df["year"].astype(str) == str(year)
    return df[mask].copy()


def _partition_cleanup_summary(result: IssuerBusinessResult, part: Partition) -> pd.DataFrame:
    raw = _filter_partition_df(result.xml_raw, part, kind="xml_raw")
    dup = _filter_partition_df(result.duplicate_df, part)
    maint = _filter_partition_df(result.maintenance_df, part)
    sup = _filter_partition_df(result.superseded_df, part)
    canon = _filter_partition_df(result.canonical, part)
    life = _filter_partition_df(result.lifecycle_input, part)
    return pd.DataFrame([{
        "issuer": part.issuer,
        "year": part.year,
        "month": _zmonth(part.month),
        "raw_xml_rows": len(raw),
        "canonical_rows": len(canon),
        "latest_state_records": len(life),
        "duplicate_count": len(dup),
        "maintenance_only_count": len(maint),
        "superseded_count": len(sup),
    }])


def _issuer_cleanup_summary(result: IssuerBusinessResult) -> pd.DataFrame:
    if not result.cleanup_summary.empty:
        return result.cleanup_summary.assign(issuer=result.issuer)
    return pd.DataFrame([{
        "issuer": result.issuer,
        "raw_canonical_rows": len(result.canonical),
        "duplicate_count": len(result.duplicate_df),
        "maintenance_only_count": len(result.maintenance_df),
        "superseded_count": len(result.superseded_df),
        "latest_state_records": len(result.lifecycle_input),
    }])


def _remove_stale_diag_files(diag_dir: Path) -> None:
    for name in ("business_validation.md",):
        path = diag_dir / name
        if path.is_file():
            path.unlink()


def _write_diagnostics_month(
    diag_dir: Path,
    result: IssuerBusinessResult,
    part: Partition,
    *,
    export_errors: ExportErrors | None = None,
) -> None:
    from azure_reconciliation.business_output_validation import (
        build_data_quality_summary,
        lifecycle_snapshot_export_df,
        write_data_quality_diagnostics,
        write_subscriber_audit_copy,
    )

    diag_dir.mkdir(parents=True, exist_ok=True)
    _remove_stale_diag_files(diag_dir)
    part_cleanup = _partition_cleanup_summary(result, part)
    safe_write_html_report(
        diag_dir / "cleanup_summary.html",
        title=f"{part.label()} — cleanup diagnostics",
        summary_df=part_cleanup,
        export_errors=export_errors,
    )
    dq = build_data_quality_summary(result, part)
    write_data_quality_diagnostics(
        diag_dir,
        dq,
        title=f"{part.label()} — data quality (XML-only)",
        export_errors=export_errors,
    )
    safe_write_csv(
        diag_dir / "duplicate_transactions.csv",
        _filter_partition_df(result.duplicate_df, part),
        export_errors=export_errors,
    )
    safe_write_csv(
        diag_dir / "maintenance_only_events.csv",
        _filter_partition_df(result.maintenance_df, part),
        export_errors=export_errors,
    )
    safe_write_csv(
        diag_dir / "superseded_events.csv",
        _filter_partition_df(result.superseded_df, part),
        export_errors=export_errors,
    )
    safe_write_csv(
        diag_dir / "lifecycle_snapshot.csv",
        lifecycle_snapshot_export_df(
            _filter_partition_df(result.lifecycle_snapshots, part, kind="lifecycle_snapshot"),
        ),
        export_errors=export_errors,
    )
    write_subscriber_audit_copy(
        diag_dir / "subscriber_mapping_audit.xlsx",
        result,
        partition=part,
    )


def _write_diagnostics_year(
    diag_dir: Path,
    result: IssuerBusinessResult,
    year: str,
    *,
    export_errors: ExportErrors | None = None,
) -> None:
    from azure_reconciliation.business_output_validation import (
        build_data_quality_summary,
        write_data_quality_diagnostics,
    )

    diag_dir.mkdir(parents=True, exist_ok=True)
    _remove_stale_diag_files(diag_dir)
    year_cleanup = pd.concat(
        [_partition_cleanup_summary(result, p) for p in result.partitions if p.year == year],
        ignore_index=True,
    )
    safe_write_html_report(
        diag_dir / "cleanup_summary.html",
        title=f"Issuer {result.issuer} {year} — cleanup diagnostics",
        summary_df=year_cleanup,
        export_errors=export_errors,
    )
    dq = build_data_quality_summary(result, year=year)
    write_data_quality_diagnostics(
        diag_dir,
        dq,
        title=f"Issuer {result.issuer} {year} — data quality (XML-only)",
        export_errors=export_errors,
    )


def _write_diagnostics_issuer(
    diag_dir: Path,
    result: IssuerBusinessResult,
    *,
    export_errors: ExportErrors | None = None,
) -> None:
    from azure_reconciliation.business_output_validation import (
        build_data_quality_summary,
        write_data_quality_diagnostics,
    )

    diag_dir.mkdir(parents=True, exist_ok=True)
    _remove_stale_diag_files(diag_dir)
    safe_write_html_report(
        diag_dir / "cleanup_summary.html",
        title=f"Issuer {result.issuer} — cleanup diagnostics",
        summary_df=_issuer_cleanup_summary(result),
        export_errors=export_errors,
    )
    dq = build_data_quality_summary(result)
    write_data_quality_diagnostics(
        diag_dir,
        dq,
        title=f"Issuer {result.issuer} — data quality (XML-only)",
        export_errors=export_errors,
    )


_LEGACY_MONTH_REPORT_FILES = (
    "cleanup_summary.html",
    "cleanup_summary.xlsx",
    "duplicate_transactions.csv",
    "maintenance_only_events.csv",
    "superseded_events.csv",
    "lifecycle_snapshot.csv",
    "xml_month_summary.html",
    "xml_month_summary.xlsx",
)

_LEGACY_YEAR_REPORT_FILES = (
    "cleanup_summary.html",
    "cleanup_summary.xlsx",
)

_LEGACY_ISSUER_REPORT_FILES = (
    "cleanup_summary.html",
    "cleanup_summary.xlsx",
)


def _remove_legacy_files(directory: Path, names: tuple[str, ...]) -> None:
    for name in names:
        path = directory / name
        if path.is_file():
            path.unlink()


def export_assets_style_reports(
    results: list[IssuerBusinessResult],
    *,
    export_errors: ExportErrors | None = None,
    assets_root: Path | None = None,
) -> Path:
    """Write Chandra-format business reports under assets/{issuer}/.../reports/."""
    from azure_reconciliation.business_output_validation import (
        build_business_processing_summary,
        business_safe_detail_df,
        processing_summary_html,
    )

    root = assets_root or assets_reports_root()
    root.mkdir(parents=True, exist_ok=True)

    for result in results:
        issuer = result.issuer
        chandra_all_months = to_chandra_business_summary(result.business_monthly)

        issuer_reports = root / issuer / "reports"
        issuer_diag = issuer_reports / "diagnostics"
        _remove_legacy_files(issuer_reports, _LEGACY_ISSUER_REPORT_FILES)
        all_years_business = chandra_all_years_rollup(chandra_all_months)
        write_chandra_business_html(
            issuer_reports / "issuer_all_years_rollup.html",
            all_years_business,
            title=f"Issuer {issuer} — All Years Enrollment Summary",
        )
        write_chandra_business_xlsx(
            issuer_reports / "issuer_all_years_rollup.xlsx",
            all_years_business,
            sheet_name="Enrollment_Summary",
        )
        safe_write_csv(
            issuer_reports / "issuer_all_years_detail.csv",
            business_safe_detail_df(result.lifecycle_input),
            export_errors=export_errors,
        )
        _write_diagnostics_issuer(
            issuer_diag, result, export_errors=export_errors,
        )

        years = sorted({p.year for p in result.partitions})
        for year in years:
            year_reports = root / issuer / str(year) / "reports"
            year_diag = year_reports / "diagnostics"
            _remove_legacy_files(year_reports, _LEGACY_YEAR_REPORT_FILES)
            year_internal = _filter_year_df(result.business_monthly, issuer, year)
            chandra_year = to_chandra_business_summary(year_internal)
            year_business = chandra_year_rollup(chandra_year)

            write_chandra_business_html(
                year_reports / "issuer_year_rollup.html",
                year_business,
                title=f"Issuer {issuer} {year} — Year Enrollment Summary",
            )
            write_chandra_business_xlsx(
                year_reports / "issuer_year_rollup.xlsx",
                year_business,
                sheet_name="Enrollment_Summary",
            )
            safe_write_csv(
                year_reports / "issuer_year_detail.csv",
                business_safe_detail_df(_filter_year_df(result.lifecycle_input, issuer, year)),
                export_errors=export_errors,
            )
            _write_diagnostics_year(
                year_diag, result, year,
                export_errors=export_errors,
            )

            for part in sorted(
                (p for p in result.partitions if p.year == year),
                key=lambda p: p.month,
            ):
                month_reports = root / issuer / str(year) / _zmonth(part.month) / "reports"
                month_diag = month_reports / "diagnostics"
                _remove_legacy_files(month_reports, _LEGACY_MONTH_REPORT_FILES)
                month_internal = year_internal[
                    year_internal["month"].astype(str).map(_zmonth) == _zmonth(part.month)
                ]
                chandra_month = to_chandra_business_summary(month_internal)
                month_proc = processing_summary_html(
                    build_business_processing_summary(result, part),
                )

                write_chandra_business_html(
                    month_reports / "enrollment_summary.html",
                    chandra_month,
                    title=f"{issuer}/{year}/{_zmonth(part.month)} — Enrollment Summary",
                )
                write_chandra_business_xlsx(
                    month_reports / "enrollment_summary.xlsx",
                    chandra_month,
                    sheet_name="Enrollment_Summary",
                )

                write_model_h_month_html(
                    month_reports / "model_h_month_summary.html",
                    business_df=chandra_month,
                    processing_html=month_proc,
                    title=f"{issuer}/{year}/{_zmonth(part.month)} — Model H Month Summary",
                )
                safe_write_excel(
                    month_reports / "model_h_month_summary.xlsx",
                    {
                        "Enrollment_Summary": chandra_month,
                        "Processing_Diagnostics": build_business_processing_summary(result, part),
                    },
                    drop_duplicate_value_columns=False,
                    export_errors=export_errors,
                )
                safe_write_csv(
                    month_reports / "xml_month_detail.csv",
                    business_safe_detail_df(
                        _filter_partition_df(result.lifecycle_input, part),
                    ),
                    export_errors=export_errors,
                )

                _write_diagnostics_month(
                    month_diag, result, part,
                    export_errors=export_errors,
                )

        logger.info("Wrote assets-style Chandra reports for issuer %s", issuer)

    write_assets_report_index(results, assets_root=root)
    return root


def write_assets_report_index(
    results: list[IssuerBusinessResult],
    *,
    assets_root: Path | None = None,
) -> Path:
    """Top-level assets/report_index.html linking business reports and diagnostics."""
    root = assets_root or assets_reports_root()
    generated = datetime.now(timezone.utc).isoformat(timespec="seconds")
    lines = [
        "<!DOCTYPE html><html><head><meta charset='utf-8'>",
        "<title>Assets Report Index</title>",
        "<style>body{font-family:system-ui,sans-serif;margin:2rem}",
        "ul{line-height:1.8} a{text-decoration:none;color:#0645ad}",
        "h2{margin-top:1.5rem}</style>",
        "</head><body>",
        "<h1>Assets Report Index — Chandra Enrollment Summary</h1>",
        f"<p>Generated: {generated}</p>",
        "<p>Centralized reports: "
        "<a href='../outputs/xml_business_reports/index.html'>outputs/xml_business_reports/</a></p>",
    ]

    for result in sorted(results, key=lambda r: r.issuer):
        issuer = result.issuer
        lines.append(f"<h2>Issuer {issuer}</h2><ul>")
        lines.append(
            f"<li><a href='{issuer}/reports/issuer_all_years_rollup.html'>"
            f"All-years enrollment summary</a> "
            f"| <a href='{issuer}/reports/diagnostics/data_quality_summary.html'>diagnostics</a></li>"
        )
        for year in sorted({p.year for p in result.partitions}):
            lines.append(
                f"<li><a href='{issuer}/{year}/reports/issuer_year_rollup.html'>"
                f"Year {year} enrollment summary</a> "
                f"| <a href='{issuer}/{year}/reports/diagnostics/data_quality_summary.html'>diagnostics</a>"
            )
            lines.append("<ul>")
            for part in sorted(
                (p for p in result.partitions if p.year == year),
                key=lambda p: p.month,
            ):
                m = _zmonth(part.month)
                base = f"{issuer}/{year}/{m}/reports"
                lines.append(
                    f"<li><a href='{base}/enrollment_summary.html'>{year}/{m} enrollment</a> "
                    f"| <a href='{base}/model_h_month_summary.html'>Model H</a> "
                    f"| <a href='{base}/diagnostics/data_quality_summary.html'>diagnostics</a></li>"
                )
            lines.append("</ul>")
        lines.append("</ul>")

    lines.append("</body></html>")
    index_path = root / "report_index.html"
    index_path.write_text("\n".join(lines), encoding="utf-8")
    logger.info("Wrote assets report index → %s", index_path)
    return index_path
