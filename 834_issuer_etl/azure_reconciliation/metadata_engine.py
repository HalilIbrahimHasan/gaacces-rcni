"""
Metadata engine — profiles for XML, Azure, issuers, and timeline.

All outputs under outputs/metadata/
"""

from __future__ import annotations

import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from azure_reconciliation.discovery_engine import DiscoverySelection
from azure_reconciliation.excel_exporter import write_excel_report
from azure_reconciliation.safe_export import safe_write_sqlite
from azure_reconciliation.source_coverage import CoverageReport, coverage_to_dataframes
from config.config import settings
from utils.logger import get_logger

logger = get_logger(__name__)


def _metadata_dir() -> Path:
    d = settings.outputs_path / "metadata"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _null_pct(df: pd.DataFrame, col: str) -> float:
    if df.empty or col not in df.columns:
        return 0.0
    return round(float(df[col].isna().mean()) * 100, 2)


def _profile_dataframe(
    df: pd.DataFrame,
    *,
    source: str,
    join_cols: list[str] | None = None,
) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame([{"source": source, "row_count": 0}])
    join_cols = join_cols or ["issuer", "enrollment_id", "enrollee_id", "insurance_type"]
    present_join = [c for c in join_cols if c in df.columns]
    dup_keys = 0
    if present_join:
        dup_keys = int(df.duplicated(subset=present_join).sum())
    status_col = next((c for c in ("canonical_status", "enrollment_status", "status") if c in df.columns), None)
    ins_col = next((c for c in ("insurance_type", "insurance_type_code") if c in df.columns), None)
    date_cols = [c for c in df.columns if "date" in c.lower()]
    date_ranges = {}
    for col in date_cols[:8]:
        try:
            dt = pd.to_datetime(df[col], errors="coerce")
            if dt.notna().any():
                date_ranges[col] = f"{dt.min()} .. {dt.max()}"
        except Exception:
            pass
    return pd.DataFrame([{
        "source": source,
        "row_count": len(df),
        "column_count": len(df.columns),
        "distinct_issuers": df["issuer"].nunique() if "issuer" in df.columns else None,
        "distinct_statuses": df[status_col].nunique() if status_col else None,
        "distinct_insurance_types": df[ins_col].nunique() if ins_col else None,
        "status_values": ", ".join(sorted(df[status_col].astype(str).unique()[:20])) if status_col else "",
        "insurance_types": ", ".join(sorted(df[ins_col].astype(str).unique()[:20])) if ins_col else "",
        "date_ranges": str(date_ranges),
        "duplicate_join_keys": dup_keys,
        "null_pct_issuer": _null_pct(df, "issuer"),
        "null_pct_enrollment_id": _null_pct(df, "enrollment_id") if "enrollment_id" in df.columns else _null_pct(df, "policy_id"),
        "null_pct_enrollee_id": _null_pct(df, "enrollee_id") if "enrollee_id" in df.columns else _null_pct(df, "member_id"),
    }])


def _write_sqlite(path: Path, tables: dict[str, pd.DataFrame]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(path) as conn:
        for name, df in tables.items():
            if df.empty:
                continue
            safe_write_sqlite(conn, name, df)
    logger.info("Metadata SQLite: %s (%d tables)", path, len(tables))


def generate_metadata(
    *,
    coverage: CoverageReport,
    xml_raw: pd.DataFrame,
    xml_lifecycle: pd.DataFrame,
    azure_raw: pd.DataFrame,
    azure_lifecycle: pd.DataFrame,
    discovery: DiscoverySelection | None,
    run_stats: dict[str, Any],
) -> dict[str, str]:
    if not settings.metadata_enabled:
        return {}

    meta_dir = _metadata_dir()
    cov_frames = coverage_to_dataframes(coverage)

    xml_profile = _profile_dataframe(xml_raw, source="xml_raw")
    xml_life_profile = _profile_dataframe(xml_lifecycle, source="xml_lifecycle")
    az_profile = _profile_dataframe(azure_raw, source="azure_raw")
    az_life_profile = _profile_dataframe(azure_lifecycle, source="azure_lifecycle")

    issuer_rows = []
    for issuer in coverage.issuers:
        issuer_rows.append({
            "issuer": issuer,
            "partitions": sum(1 for p in coverage.partitions if p.issuer == issuer),
            "files": sum(1 for f in coverage.files if f.issuer == issuer),
        })
    issuer_profile = pd.DataFrame(issuer_rows)

    timeline_rows = [
        {"issuer": p.issuer, "year": p.year, "month": p.month, "partition": p.label()}
        for p in coverage.partitions
    ]
    timeline_profile = pd.DataFrame(timeline_rows)

    selection_row = {}
    if discovery and discovery.candidate.table:
        c = discovery.candidate
        selection_row = {
            "selected_table": c.full_name,
            "selected_strategy": c.strategy_id,
            "confidence_score": c.confidence_score,
            "date_column": c.date_column,
            "status_column": c.status_column,
            "logic_type": c.logic_type,
            "row_count": c.row_count,
            "seed_bonus": c.seed_bonus,
            "missing_roles": ", ".join(c.profile.missing_roles),
        }
    selection_df = pd.DataFrame([selection_row] if selection_row else [])

    run_meta = pd.DataFrame([{
        "run_timestamp": datetime.now().isoformat(timespec="seconds"),
        **run_stats,
        **(selection_row or {}),
    }])

    sqlite_paths = {
        "xml_profile": meta_dir / "xml_profile.sqlite",
        "azure_profile": meta_dir / "azure_profile.sqlite",
        "issuer_profile": meta_dir / "issuer_profile.sqlite",
        "timeline_profile": meta_dir / "timeline_profile.sqlite",
    }
    _write_sqlite(sqlite_paths["xml_profile"], {
        "xml_raw_profile": xml_profile,
        "xml_lifecycle_profile": xml_life_profile,
        "xml_raw_sample": xml_raw.head(500),
    })
    _write_sqlite(sqlite_paths["azure_profile"], {
        "azure_raw_profile": az_profile,
        "azure_lifecycle_profile": az_life_profile,
        "azure_raw_sample": azure_raw.head(500),
        "selection": selection_df,
        "strategy_scores": discovery.all_scores if discovery else pd.DataFrame(),
    })
    _write_sqlite(sqlite_paths["issuer_profile"], {"issuers": issuer_profile, "coverage_summary": cov_frames["summary"]})
    _write_sqlite(sqlite_paths["timeline_profile"], {"timeline": timeline_profile, "files": cov_frames["files"]})

    xlsx_path = meta_dir / "xml_profile.xlsx"
    write_excel_report(xlsx_path, {
        "xml_raw_profile": xml_profile,
        "xml_lifecycle_profile": xml_life_profile,
        "coverage": cov_frames["summary"],
    })
    az_xlsx = meta_dir / "azure_profile.xlsx"
    write_excel_report(az_xlsx, {
        "azure_raw_profile": az_profile,
        "azure_lifecycle_profile": az_life_profile,
        "selection": selection_df,
        "strategy_scores": discovery.all_scores if discovery else pd.DataFrame(),
    })

    html_path = meta_dir / "run_metadata.html"
    html_path.write_text(_run_metadata_html(coverage, run_meta, selection_df, xml_profile, az_profile), encoding="utf-8")

    paths = {
        "xml_profile_sqlite": str(sqlite_paths["xml_profile"]),
        "azure_profile_sqlite": str(sqlite_paths["azure_profile"]),
        "issuer_profile_sqlite": str(sqlite_paths["issuer_profile"]),
        "timeline_profile_sqlite": str(sqlite_paths["timeline_profile"]),
        "xml_profile_xlsx": str(xlsx_path),
        "azure_profile_xlsx": str(az_xlsx),
        "run_metadata_html": str(html_path),
    }
    logger.info("Metadata outputs written to %s", meta_dir)
    return paths


def _run_metadata_html(
    coverage: CoverageReport,
    run_meta: pd.DataFrame,
    selection: pd.DataFrame,
    xml_profile: pd.DataFrame,
    az_profile: pd.DataFrame,
) -> str:
    window = coverage.coverage_window()
    return f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>Run Metadata</title>
<style>
body {{ font-family: Arial, sans-serif; margin: 24px; background: #121212; color: #eee; }}
table {{ border-collapse: collapse; width: 100%; margin: 12px 0; font-size: 13px; }}
th, td {{ border: 1px solid #444; padding: 6px; }}
th {{ background: #2B2B2B; }}
h1, h2 {{ color: #6eb6ff; }}
</style></head><body>
<h1>Azure Intelligence Run Metadata</h1>
<h2>Coverage</h2>
<p>Issuers: {', '.join(coverage.issuers) or 'none'}</p>
<p>Years: {', '.join(coverage.years) or 'none'}</p>
<p>Months: {', '.join(coverage.months) or 'none'}</p>
<p>Window: {window.get('earliest')} → {window.get('latest')}</p>
<h2>Run</h2>
{run_meta.to_html(index=False) if not run_meta.empty else '<p>No run metadata</p>'}
<h2>Azure Selection</h2>
{selection.to_html(index=False) if not selection.empty else '<p>No Azure selection</p>'}
<h2>XML Profile</h2>
{xml_profile.to_html(index=False) if not xml_profile.empty else '<p>No XML data</p>'}
<h2>Azure Profile</h2>
{az_profile.to_html(index=False) if not az_profile.empty else '<p>No Azure data</p>'}
</body></html>"""
