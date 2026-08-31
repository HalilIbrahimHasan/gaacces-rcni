"""
Business review package — orchestrate existing exports and organize for stakeholders.

Copies already-generated outputs into outputs/business_review/.
Does not modify production logic or existing export locations.
"""

from __future__ import annotations

import html
import shutil
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from azure_reconciliation.business_month_assignment_trace import run_business_month_assignment_trace
from azure_reconciliation.business_ready_exports import run_business_ready_exports
from azure_reconciliation.full_data_exports import discover_issuer_year_pairs, run_full_data_exports
from azure_reconciliation.month_reassignment_investigation import run_month_reassignment_investigation
from azure_reconciliation.safe_export import ExportErrors
from azure_reconciliation.xml_business_reports import run_xml_business_reporting
from config.config import settings
from utils.logger import get_logger

logger = get_logger(__name__)

INVESTIGATION_FILES = [
    "cleaned_definition.md",
    "month_reassignment_matrix.xlsx",
    "month_basis_comparison_for_chandra.xlsx",
    "month_reassignment_reason_summary.xlsx",
    "month_boundary_analysis.xlsx",
    "business_validation.md",
    "final_engineering_conclusion.md",
    "final_remaining_gap_analysis.xlsx",
    "business_month_assignment_trace.xlsx",
]


@dataclass
class CopyResult:
    dest: Path
    copied: bool
    source: Path | None = None


@dataclass
class PackageResult:
    issuer: str
    year: str
    copies: list[CopyResult] = field(default_factory=list)
    missing: list[str] = field(default_factory=list)


def review_root() -> Path:
    return settings.outputs_path / "business_review"


def _zmonth(m: str) -> str:
    return str(m).strip().zfill(2)


def _copy_file(src: Path, dest: Path) -> CopyResult:
    if not src.exists():
        return CopyResult(dest=dest, copied=False, source=src)
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dest)
    logger.info("Copied %s → %s", src, dest)
    return CopyResult(dest=dest, copied=True, source=src)


def _issuer_year_copy_plan(issuer: str, year: str) -> list[tuple[Path, Path]]:
    """Source → destination pairs for one issuer/year."""
    root = review_root() / issuer / year
    full = settings.outputs_path / "full_data_exports" / issuer / year
    ready = settings.outputs_path / "business_data_exports" / issuer / year / "business_ready"
    assets_year = settings.assets_path / issuer / year / "reports"
    assets_issuer = settings.assets_path / issuer / "reports"
    debug = settings.outputs_path / "debug"

    pairs: list[tuple[Path, Path]] = [
        (full / "raw" / "raw_all_months.xlsx", root / "raw" / "raw_all_months.xlsx"),
        (full / "raw" / "raw_all_months.csv", root / "raw" / "raw_all_months.csv"),
        (full / "raw" / "raw_monthly_counts.xlsx", root / "raw" / "raw_monthly_counts.xlsx"),
        (full / "raw" / "raw_monthly_counts.csv", root / "raw" / "raw_monthly_counts.csv"),
        (full / "cleaned" / "cleaned_all_months.xlsx", root / "canonical" / "canonical_all_months.xlsx"),
        (full / "cleaned" / "cleaned_all_months.csv", root / "canonical" / "canonical_all_months.csv"),
        (full / "cleaned" / "cleaned_monthly_counts.xlsx", root / "canonical" / "canonical_monthly_counts.xlsx"),
        (ready / "business_ready_all_months.xlsx", root / "business_ready" / "business_ready_all_months.xlsx"),
        (ready / "business_ready_all_months.csv", root / "business_ready" / "business_ready_all_months.csv"),
        (ready / "business_ready_summary.xlsx", root / "business_ready" / "business_ready_summary.xlsx"),
        (ready / "business_ready_monthly_counts.xlsx", root / "business_ready" / "business_ready_monthly_counts.xlsx"),
        (ready / "business_ready_yearly_counts.xlsx", root / "business_ready" / "business_ready_yearly_counts.xlsx"),
        (assets_year / "issuer_year_rollup.xlsx", root / "reports" / "issuer_year_rollup.xlsx"),
        (assets_year / "issuer_year_rollup.html", root / "reports" / "issuer_year_rollup.html"),
        (assets_issuer / "issuer_all_years_rollup.xlsx", root / "reports" / "issuer_all_years_rollup.xlsx"),
        (assets_issuer / "issuer_all_years_rollup.html", root / "reports" / "issuer_all_years_rollup.html"),
    ]
    for name in INVESTIGATION_FILES:
        pairs.append((debug / name, root / "investigations" / name))
    return pairs


def _copy_enrollment_summaries(
    issuer: str,
    year: str,
    *,
    month_filter: str | None = None,
) -> list[CopyResult]:
    """Copy monthly enrollment_summary files from assets."""
    results: list[CopyResult] = []
    year_dir = settings.assets_path / issuer / year
    if not year_dir.exists():
        return results

    months = sorted(
        d.name for d in year_dir.iterdir()
        if d.is_dir() and d.name.isdigit()
    )
    if month_filter:
        zm = _zmonth(month_filter)
        months = [m for m in months if _zmonth(m) == zm]

    for month in months:
        zm = _zmonth(month)
        src_base = year_dir / zm / "reports"
        dest_base = review_root() / issuer / year / "reports" / "monthly" / zm
        for ext in ("xlsx", "html"):
            src = src_base / f"enrollment_summary.{ext}"
            dest = dest_base / f"enrollment_summary.{ext}"
            results.append(_copy_file(src, dest))
    return results


def _lineage_diagram_paths() -> tuple[Path | None, Path]:
    """Return (png_if_found, html_fallback_path)."""
    candidates = [
        settings.project_root / "data_lineage_diagram.png",
        settings.outputs_path / "data_lineage_diagram.png",
        settings.outputs_path / "debug" / "data_lineage_diagram.png",
    ]
    for p in candidates:
        if p.exists():
            return p, review_root() / "data_lineage_diagram.html"
    return None, review_root() / "data_lineage_diagram.html"


def _write_lineage_html(path: Path) -> None:
    body = """<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>Data Lineage</title>
<style>
body{font-family:sans-serif;margin:2em;background:#fafafa}
.flow{display:flex;flex-direction:column;align-items:center;gap:0}
.box{border:2px solid #333;border-radius:8px;padding:16px 32px;background:#fff;
  min-width:280px;text-align:center;font-weight:600}
.arrow{font-size:28px;color:#555;line-height:1.2}
.note{margin-top:2em;color:#444;max-width:520px;text-align:center}
</style></head><body>
<h1>834 Data Lineage</h1>
<div class="flow">
<div class="box">RAW XML<br><small>One row per XML transaction</small></div>
<div class="arrow">↓</div>
<div class="box">CANONICAL<br><small>Normalized 1:1 with raw</small></div>
<div class="arrow">↓</div>
<div class="box">BUSINESS READY<br><small>Post dedupe / latest-state / collapse</small></div>
<div class="arrow">↓</div>
<div class="box">MODEL H SUMMARY<br><small>Distinct enrollment counts</small></div>
<div class="arrow">↓</div>
<div class="box">ENROLLMENT REPORT<br><small>Chandra-like dashboard output</small></div>
</div>
<p class="note">Read-only export layers — production pipeline unchanged.</p>
</body></html>"""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body, encoding="utf-8")


def _install_lineage_diagram() -> dict[str, str]:
    png_src, html_path = _lineage_diagram_paths()
    _write_lineage_html(html_path)
    out: dict[str, str] = {"html": str(html_path)}
    dest_png = review_root() / "data_lineage_diagram.png"
    if png_src:
        shutil.copy2(png_src, dest_png)
        out["png"] = str(dest_png)
    else:
        out["png"] = ""
    return out


def _root_readme_md(
    packages: list[PackageResult],
    lineage: dict[str, str],
    *,
    filtered_enabled: bool = False,
) -> str:
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    lines = [
        "# Business Review Package",
        "",
        f"**Generated:** {now}",
        "",
        "## What is included",
        "",
        "This folder collects engineering validation exports for business review:",
        "",
        "- **raw/** — parsed XML transactions (original source truth, no cleanup)",
        "- **canonical/** — normalized canonical records (1:1 with raw, business month applied)",
        "- **business_ready/** — selected business reporting records (Model H input)",
        "- **reports/** — Chandra-like enrollment summaries and rollups",
        "- **investigations/** — read-only engineering investigations",
        "",
        "## Unfiltered vs filtered packages",
        "",
        "| Package | Location | Purpose |",
        "|---------|----------|---------|",
        "| **Unfiltered** | `outputs/business_review/` | Full source truth — all parsed records |",
        "| **Filtered** | `outputs/business_review_filtered/` | Chandra-aligned comparison using prior-year benefit effective filter |",
        "",
        "- **Raw XML** = original parsed XML transactions (one row per XML event).",
        "- **Business Ready** = selected business reporting records after dedupe, latest-state, and collapse.",
        "- The prior-year filter does **not** modify `source_data` or unfiltered exports.",
        "- When `FILTER_PRIOR_YEAR_BENEFIT_EFFECTIVE=true`, filtered reports are written separately under",
        "  `assets_filtered/` and `outputs/xml_business_reports_filtered/`.",
        "- Filter uses **benefit_effective_date** only — not selected_transaction_date.",
        "",
        "## RAW vs CANONICAL vs BUSINESS READY vs REPORTS",
        "",
        "| Layer | Grain | Row removal? |",
        "|-------|-------|--------------|",
        "| RAW | 1 XML transaction | No |",
        "| CANONICAL | 1 normalized transaction | No (flags only) |",
        "| BUSINESS READY | 1 selected business record | Yes (collapse) |",
        "| REPORTS | Aggregated enrollments | Yes (Model H) |",
        "",
        "Yearly **raw** and **canonical** counts often match because both are full record-level",
        "exports with no rows removed. **Business ready** counts are lower.",
        "",
        "- Monthly counts may differ when **source folder month** ≠ **business month**",
        "(see investigations/month_reassignment_matrix.xlsx).",
        "",
    ]
    if filtered_enabled:
        lines.extend([
            "## Prior-year benefit effective filter (enabled for this run)",
            "",
            "Filtered outputs for this run:",
            "",
            "- `outputs/business_review_filtered/<issuer>/<year>/filtered_comparison.xlsx`",
            "- `assets_filtered/<issuer>/<year>/` — filtered enrollment reports",
            "- `outputs/xml_business_reports_filtered/<issuer>/` — filtered XML business reports",
            "",
            "Compare **raw filtered** counts to Chandra raw extract.",
            "Compare **business ready filtered** counts to reporting input (not raw extract).",
            "Filter uses benefit_effective_date only — selected_transaction_date is for comparison.",
            "",
        ])
    lines.extend([
        "## Trace a dashboard number",
        "",
        "1. Open `reports/monthly/<month>/enrollment_summary.xlsx` or `issuer_year_rollup.xlsx`",
        "2. Note issuer, year, month, insurance type, status",
        "3. Open `business_ready/business_ready_summary.xlsx` — match `dashboard_group_key`",
        "4. Filter `business_ready/business_ready_all_months.xlsx` on that key",
        "5. Use `raw_transaction_keys` and `raw_source_files` to trace back to raw XML",
        "",
        "## Validation status",
        "",
        "- Parser validated against source_data XML",
        "- Canonical is 1:1 with raw at record level",
        "- Business Ready is post-dedupe / latest-state / business-transaction collapse",
        "- Model H summary is aggregated business output",
        "- Month reassignment is real and documented in investigations/",
        "",
        "## Known open item",
        "",
        "Chandra may apply additional eligibility/reporting logic not fully represented in XML.",
        "",
        "## Data lineage",
        "",
    ])
    if lineage.get("png"):
        lines.append(f"![Data lineage](data_lineage_diagram.png)")
    lines.append(f"[Interactive lineage diagram](data_lineage_diagram.html)")
    lines.append("")
    lines.append("## Issuer / year packages")
    lines.append("")
    for pkg in packages:
        rel = f"{pkg.issuer}/{pkg.year}/README.md"
        lines.append(f"- [{pkg.issuer} / {pkg.year}]({rel})")
    lines.append("")
    lines.append("## Not Generated / Not Available")
    lines.append("")
    all_missing = sorted({m for p in packages for m in p.missing})
    if all_missing:
        for m in all_missing:
            lines.append(f"- `{m}`")
    else:
        lines.append("(all expected files present)")
    lines.append("")
    return "\n".join(lines)


def _issuer_year_readme(pkg: PackageResult) -> str:
    copied = [c for c in pkg.copies if c.copied]
    missing = pkg.missing
    lines = [
        f"# Business Review — {pkg.issuer} / {pkg.year}",
        "",
        "## What this package contains (unfiltered)",
        "",
        "This is the **unfiltered** business review package — full source truth.",
        "",
        "- `raw/` — parsed XML transactions (original column names)",
        "- `canonical/` — normalized canonical (same row count as raw per year)",
        "- `business_ready/` — one row per selected business reporting record",
        "- `reports/` — enrollment summaries and rollups",
        "- `investigations/` — engineering investigations",
        "",
        "## Filtered comparison (if enabled)",
        "",
        "When `FILTER_PRIOR_YEAR_BENEFIT_EFFECTIVE=true`, a separate filtered package is written to",
        f"`outputs/business_review_filtered/{pkg.issuer}/{pkg.year}/`.",
        "That package applies the prior-year benefit effective filter for Chandra alignment.",
        "It does not modify this unfiltered folder or `source_data`.",
        "",
        f"**Files copied:** {len(copied)}",
        "",
    ]
    if missing:
        lines.append("## Not Generated / Not Available")
        lines.append("")
        for m in missing:
            lines.append(f"- `{m}`")
        lines.append("")
    return "\n".join(lines)


def _write_index_html(packages: list[PackageResult], lineage: dict[str, str]) -> Path:
    path = review_root() / "index.html"
    lines = [
        "<!DOCTYPE html>",
        "<html><head><meta charset='utf-8'>",
        "<title>Business Review Package</title>",
        "<style>body{font-family:sans-serif;margin:2em} ul{margin:8px 0}",
        "a{color:#0645ad} h2{margin-top:1.5em}</style>",
        "</head><body>",
        "<h1>Business Review Package</h1>",
        "<p><a href='README.md'>README</a>",
    ]
    if lineage.get("png"):
        lines.append(" | <a href='data_lineage_diagram.png'>Lineage (PNG)</a>")
    lines.append(" | <a href='data_lineage_diagram.html'>Lineage (HTML)</a></p>")

    for pkg in sorted(packages, key=lambda p: (p.issuer, p.year)):
        base = f"{pkg.issuer}/{pkg.year}"
        lines.append(f"<h2>{html.escape(pkg.issuer)} / {html.escape(pkg.year)}</h2>")
        lines.append("<ul>")
        links = [
            ("README", f"{base}/README.md"),
            ("Raw workbook", f"{base}/raw/raw_all_months.xlsx"),
            ("Canonical workbook", f"{base}/canonical/canonical_all_months.xlsx"),
            ("Business ready workbook", f"{base}/business_ready/business_ready_all_months.xlsx"),
            ("Business ready summary", f"{base}/business_ready/business_ready_summary.xlsx"),
            ("Year rollup", f"{base}/reports/issuer_year_rollup.xlsx"),
            ("All-years rollup", f"{base}/reports/issuer_all_years_rollup.xlsx"),
            ("Month reassignment", f"{base}/investigations/month_reassignment_matrix.xlsx"),
            ("Month assignment trace", f"{base}/investigations/business_month_assignment_trace.xlsx"),
            ("Cleaned definition", f"{base}/investigations/cleaned_definition.md"),
        ]
        for label, href in links:
            lines.append(f"<li><a href='{html.escape(href)}'>{html.escape(label)}</a></li>")
        lines.append("</ul>")
    lines.append("</body></html>")
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


def assemble_package(
    issuer: str,
    year: str,
    *,
    month_filter: str | None = None,
) -> PackageResult:
    """Copy all source files into business_review for one issuer/year."""
    pkg = PackageResult(issuer=issuer, year=year)
    for src, dest in _issuer_year_copy_plan(issuer, year):
        result = _copy_file(src, dest)
        pkg.copies.append(result)
        if not result.copied:
            pkg.missing.append(dest.relative_to(review_root()).as_posix())

    for result in _copy_enrollment_summaries(issuer, year, month_filter=month_filter):
        pkg.copies.append(result)
        if not result.copied:
            pkg.missing.append(result.dest.relative_to(review_root()).as_posix())

    readme_path = review_root() / issuer / year / "README.md"
    readme_path.parent.mkdir(parents=True, exist_ok=True)
    readme_path.write_text(_issuer_year_readme(pkg), encoding="utf-8")
    return pkg


def run_business_review_package(
    *,
    issuer_filter: str | None = None,
    year_filter: str | None = None,
    month_filter: str | None = None,
    parse_source: bool = False,
    expected_confirm: int | None = None,
    expected_cancel: int | None = None,
    expected_term: int | None = None,
    gap_month: str = "01",
    export_errors: ExportErrors | None = None,
) -> dict[str, Any]:
    """Orchestrate exports, investigations, and assemble business review package."""
    review_root().mkdir(parents=True, exist_ok=True)
    errors = export_errors or ExportErrors()

    settings.apply_xml_only_business_mode(True)

    logger.info("Step 1/5 — XML business reports")
    try:
        run_xml_business_reporting(
            issuer=issuer_filter,
            parse_source=parse_source,
            export_errors=errors,
        )
    except Exception as exc:
        logger.warning("XML business reports step failed: %s", exc)
        errors.record(f"XML business reports: {exc}")

    pairs = discover_issuer_year_pairs(issuer_filter=issuer_filter, year_filter=year_filter)
    if not pairs:
        raise RuntimeError("No issuer/year partitions found under source_data")

    for issuer, year in pairs:
        logger.info("Processing package for %s / %s", issuer, year)

        logger.info("Step 2/5 — Full data exports")
        try:
            run_full_data_exports(
                issuer_filter=issuer, year_filter=year,
                parse_source=parse_source, export_errors=errors,
            )
        except Exception as exc:
            logger.warning("Full data exports failed for %s/%s: %s", issuer, year, exc)
            errors.record(f"Full data exports {issuer}/{year}: {exc}")

        logger.info("Step 3/5 — Business ready exports")
        try:
            run_business_ready_exports(
                issuer_filter=issuer, year_filter=year,
                parse_source=parse_source, export_errors=errors,
            )
        except Exception as exc:
            logger.warning("Business ready exports failed for %s/%s: %s", issuer, year, exc)
            errors.record(f"Business ready exports {issuer}/{year}: {exc}")

        logger.info("Step 4/5 — Month reassignment investigation")
        try:
            run_month_reassignment_investigation(
                issuer=issuer, year=year, parse_source=parse_source,
            )
        except Exception as exc:
            logger.warning("Month reassignment investigation failed for %s/%s: %s", issuer, year, exc)
            errors.record(f"Month reassignment {issuer}/{year}: {exc}")

        logger.info("Step 4b — Business month assignment trace")
        try:
            run_business_month_assignment_trace(
                issuer=issuer, year=year, parse_source=parse_source,
            )
        except Exception as exc:
            logger.warning("Month assignment trace failed for %s/%s: %s", issuer, year, exc)
            errors.record(f"Month assignment trace {issuer}/{year}: {exc}")

        if any(v is not None for v in (expected_confirm, expected_cancel, expected_term)):
            logger.info("Step 5/5 — Final gap investigation (expected counts provided)")
            try:
                from azure_reconciliation.final_gap_investigation import run_final_gap_investigation

                run_final_gap_investigation(
                    issuer=issuer,
                    year=year,
                    month=gap_month,
                    parse_source=parse_source,
                    cli_confirm=expected_confirm,
                    cli_cancel=expected_cancel,
                    cli_term=expected_term,
                )
            except Exception as exc:
                logger.warning("Final gap investigation failed for %s/%s: %s", issuer, year, exc)
                errors.record(f"Final gap investigation {issuer}/{year}: {exc}")
        else:
            logger.info("Step 5/5 — Skipping final gap investigation (no expected counts)")

    packages = [assemble_package(i, y, month_filter=month_filter) for i, y in pairs]
    lineage = _install_lineage_diagram()

    filter_stats: dict[str, Any] | None = None
    filtered_reporting: list[dict[str, Any]] = []
    if settings.filter_prior_year_benefit_effective:
        logger.info("Step 6 — Prior-year benefit effective filter (FILTER_PRIOR_YEAR_BENEFIT_EFFECTIVE=true)")
        try:
            from azure_reconciliation.prior_year_benefit_filter import run_prior_year_filter_end_to_end

            filter_stats = run_prior_year_filter_end_to_end(
                issuer_filter=issuer_filter,
                year_filter=year_filter,
                parse_source=parse_source,
                export_errors=errors,
            )
            filtered_reporting = filter_stats.get("filtered_reporting", [])
            logger.info(
                "Filtered package → %s (%d issuer/years)",
                filter_stats.get("filtered_output_root"),
                filter_stats.get("filter_stats", {}).get("issuer_years", 0),
            )
        except Exception as exc:
            logger.warning("Prior-year benefit filter failed: %s", exc)
            errors.record(f"Prior-year benefit filter: {exc}")

    readme_path = review_root() / "README.md"
    readme_path.write_text(
        _root_readme_md(
            packages, lineage,
            filtered_enabled=settings.filter_prior_year_benefit_effective,
        ),
        encoding="utf-8",
    )
    index_path = _write_index_html(packages, lineage)

    return {
        "output_root": str(review_root()),
        "readme": str(readme_path),
        "index_html": str(index_path),
        "issuer_years": len(packages),
        "packages": packages,
        "lineage": lineage,
        "filtered_exports": filter_stats.get("filter_stats") if filter_stats else None,
        "filtered_reporting": filtered_reporting,
        "filtered_pipeline": filter_stats,
    }
