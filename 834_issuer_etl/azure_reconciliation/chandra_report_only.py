"""
Chandra-style report-only runner — independent, no .env flags, no legacy pipelines.

Hardcoded behavior:
  - XML-only parse from source_data
  - Prior-year benefit filter ON (benefit_effective_date only)
  - Chandra summary columns without Subscriber_Count
  - No Azure, legacy assets, full exports, business review, diagnostics, storage

Does NOT call main.py, run_business_review_package, or run_fast_business_reports.
"""

from __future__ import annotations

import shutil
import time
import traceback
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
from azure_reconciliation.chandra_nan_safe import (
    StageTracker,
    append_run_errors_log,
    safe_int,
    safe_sum,
    sanitize_business_ready_df,
    sanitize_chandra_summary_df,
    sanitize_dashboard_summary_df,
    sanitize_dataframe_pre_export,
    write_fatal_error_file,
    write_issuer_failure,
)
from azure_reconciliation.partition_discovery import Partition, discover_partitions
from azure_reconciliation.prior_year_benefit_filter import (
    FILTER_ACTION_EXCLUDE,
    apply_prior_year_benefit_filter,
    build_filter_audit,
)
from azure_reconciliation.reconciliation_analysis import _chandra_dashboard
from azure_reconciliation.safe_export import ExportErrors, safe_write_csv, safe_write_excel
from azure_reconciliation.xml_business_reports import PK, process_issuer_xml_business, _filter_year_month_lifecycle
from azure_reconciliation.xml_loader import load_xml_rows
from config.config import settings
from ingestion.file_discovery import discover_source_files
from utils.logger import get_logger

logger = get_logger(__name__)

# Hardcoded — this runner does not read feature flags from .env
PY_FILTER_ENABLED = True
PARSE_FROM_SOURCE = True

CHANDRA_BUSINESS_READY_COLUMNS = [
    "issuer", "year", "month", "business_month", "insurance_type", "status_Id",
    "enrolleeStatus", "canonical_enrollment_id", "canonical_enrollee_id",
    "policy_id", "member_id", "benefit_effective_date", "benefit_effective_year",
    "selected_transaction_date", "source_file", "dashboard_group_key",
    "raw_transaction_count", "raw_source_files", "raw_transaction_keys", "selection_reason",
]


def chandra_report_only_root() -> Path:
    return settings.outputs_path / "chandra_report_only"


def _zmonth(m: str) -> str:
    return str(m).strip().zfill(2)


def _stage_log(message: str) -> None:
    """Console stage marker for top-level crash diagnosis."""
    print(message, flush=True)


@dataclass
class RunState:
    """Loop guard and validation collectors."""

    processed_keys: set[str] = field(default_factory=set)
    month_status: list[dict[str, Any]] = field(default_factory=list)
    issuer_year_status: list[dict[str, Any]] = field(default_factory=list)
    skipped: list[dict[str, Any]] = field(default_factory=list)
    filter_summary: list[dict[str, Any]] = field(default_factory=list)
    chandra_parts: list[pd.DataFrame] = field(default_factory=list)
    br_parts: list[pd.DataFrame] = field(default_factory=list)
    issuers_discovered: set[str] = field(default_factory=set)
    months_discovered: set[str] = field(default_factory=set)
    issuers_attempted: set[str] = field(default_factory=set)
    issuers_successfully_written: set[str] = field(default_factory=set)
    issuers_failed: set[str] = field(default_factory=set)
    months_attempted: set[str] = field(default_factory=set)
    months_successfully_written: set[str] = field(default_factory=set)
    months_failed: set[str] = field(default_factory=set)
    missing_output_files: list[dict[str, Any]] = field(default_factory=list)
    nan_cleanup_audit: list[dict[str, Any]] = field(default_factory=list)
    export_errors: list[str] = field(default_factory=list)
    current_stage: str = "discover"
    active_issuer: str = ""
    active_month: str = ""
    last_stage_by_issuer: dict[str, str] = field(default_factory=dict)
    last_failure_context_by_issuer: dict[str, dict[str, Any]] = field(default_factory=dict)

    def month_key(self, issuer: str, year: str, month: str) -> str:
        return f"{issuer}|{year}|{_zmonth(month)}"

    def claim(self, issuer: str, year: str, month: str) -> bool:
        key = f"{issuer}|{year}|{_zmonth(month)}"
        if key in self.processed_keys:
            logger.warning("SKIP duplicate issuer/year/month key: %s", key)
            self.skipped.append({
                "issuer": issuer, "year": year, "month": _zmonth(month),
                "reason": "duplicate_key",
            })
            return False
        self.processed_keys.add(key)
        return True


def _apply_xml_only_mode() -> None:
    """Hardcoded runtime — XML only, no Azure."""
    settings.apply_xml_only_business_mode(True)


def _attach_source_file(work: pd.DataFrame, canonical: pd.DataFrame) -> pd.DataFrame:
    out = work.copy()
    if "source_file" in out.columns and out["source_file"].astype(str).str.strip().ne("").any():
        return out
    if canonical.empty:
        out["source_file"] = out.get("source_file", "")
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


def _prepare_business_ready(result: Any, *, issuer: str, year: str, xml_raw: pd.DataFrame) -> pd.DataFrame:
    df = build_business_ready_records(result, issuer=issuer, year=year, xml_raw=xml_raw)
    df = _attach_source_file(df, result.canonical)
    cols = [c for c in CHANDRA_BUSINESS_READY_COLUMNS if c in df.columns]
    return df[cols].copy()


def _filter_status(before: int, excluded: int, after: int) -> str:
    if before == 0:
        return "PASS"
    return "PASS" if before - excluded == after else "FAIL"


def _write_pair(
    path_base: Path,
    stem: str,
    df: pd.DataFrame,
    errors: ExportErrors | None,
    *,
    audit: list[dict[str, Any]] | None = None,
    year: str = "",
) -> None:
    if df.empty:
        return
    clean = sanitize_dataframe_pre_export(df, audit, year=year, context=stem)
    safe_write_excel(
        path_base / f"{stem}.xlsx", {stem: clean},
        export_errors=errors, drop_duplicate_value_columns=False,
    )
    safe_write_csv(
        path_base / f"{stem}.csv", clean,
        export_errors=errors, drop_duplicate_value_columns=False,
    )


def _count_source_files(issuer: str, year: str, month: str) -> int:
    return len(discover_source_files(
        settings.source_data_path,
        issuer_filter=issuer,
        year_filter=year,
        month_filter=month,
    ))


def _raw_rows_for_partition(xml_raw: pd.DataFrame, part: Partition) -> int:
    if xml_raw.empty:
        return 0
    mask = pd.Series([True] * len(xml_raw), index=xml_raw.index)
    for col, val in (("issuer", part.issuer), ("year", part.year)):
        if col in xml_raw.columns:
            mask &= xml_raw[col].astype(str) == str(val)
    if "month" in xml_raw.columns:
        mask &= xml_raw["month"].astype(str).map(_zmonth) == _zmonth(part.month)
    elif "source_month" in xml_raw.columns:
        mask &= xml_raw["source_month"].astype(str).map(_zmonth) == _zmonth(part.month)
    return safe_int(mask.sum(), 0)


def _parse_month_set(month_filter: str | None) -> set[str] | None:
    if not month_filter or not str(month_filter).strip():
        return None
    return {m.strip().zfill(2) for m in str(month_filter).split(",") if m.strip()}


def _parse_issuer_list(issuer_filter: str | None) -> list[str] | None:
    if not issuer_filter or not str(issuer_filter).strip():
        return None
    return [i.strip() for i in str(issuer_filter).split(",") if i.strip()]


def _statuses_present(chandra_month: pd.DataFrame) -> str:
    if chandra_month.empty or "enrolleeStatus" not in chandra_month.columns:
        return ""
    vals = sorted(chandra_month["enrolleeStatus"].astype(str).str.strip().unique())
    return ", ".join(v for v in vals if v and v not in ("nan", "None"))


def _issuer_out_base(issuer: str, year: str) -> Path:
    return chandra_report_only_root() / issuer / year


def _expected_issuer_files(issuer: str, year: str) -> list[tuple[Path, str]]:
    base = _issuer_out_base(issuer, year)
    return [
        (base / "business_ready" / "business_ready_all_months.xlsx", "issuer_business_ready_all_months"),
        (base / "business_ready" / "business_ready_summary.xlsx", "issuer_business_ready_summary"),
        (base / "reports" / "issuer_year_rollup.xlsx", "issuer_year_rollup"),
    ]


def _expected_month_file(issuer: str, year: str, month: str) -> tuple[Path, str]:
    path = (
        _issuer_out_base(issuer, year)
        / "reports" / "monthly" / _zmonth(month) / "enrollment_summary.xlsx"
    )
    return path, "monthly_enrollment_summary"


def _record_missing(
    state: RunState,
    *,
    issuer: str,
    year: str,
    month: str,
    path: Path,
    reason: str,
) -> None:
    entry = {
        "issuer": issuer,
        "year": year,
        "month": _zmonth(month) if month else "",
        "expected_file": str(path),
        "reason": reason,
    }
    state.missing_output_files.append(entry)
    logger.error(
        "MISSING OUTPUT issuer=%s year=%s month=%s file=%s reason=%s",
        issuer, year, month or "", path, reason,
    )


def _validate_issuer_outputs(
    state: RunState,
    issuer: str,
    year: str,
    expected_months: list[str],
) -> bool:
    """Validate expected output files exist on disk. Returns True if all present."""
    all_ok = True
    for path, label in _expected_issuer_files(issuer, year):
        if not path.is_file():
            _record_missing(
                state, issuer=issuer, year=year, month="",
                path=path, reason=f"missing_{label}",
            )
            all_ok = False
    for month in expected_months:
        path, label = _expected_month_file(issuer, year, month)
        if not path.is_file():
            _record_missing(
                state, issuer=issuer, year=year, month=month,
                path=path, reason=f"missing_{label}",
            )
            all_ok = False
    return all_ok


def _clear_outputs(
    year: str,
    *,
    issuer_filter: str | None = None,
) -> None:
    """
    Remove prior chandra_report_only outputs for the selected scope.

    Called exactly once at run start — never during the issuer loop.
    """
    root = chandra_report_only_root()
    year_dir = root / year
    if year_dir.exists():
        shutil.rmtree(year_dir)
        logger.info("Cleared %s", year_dir)

    issuers = _parse_issuer_list(issuer_filter)
    if issuers:
        for iss in issuers:
            target = root / iss / year
            if target.exists():
                shutil.rmtree(target)
                logger.info("Cleared %s", target)
        return

    if not root.exists():
        return
    for issuer_dir in root.iterdir():
        if not issuer_dir.is_dir() or not issuer_dir.name.isdigit():
            continue
        yr_path = issuer_dir / year
        if yr_path.exists():
            shutil.rmtree(yr_path)
            logger.info("Cleared %s", yr_path)


def _write_required_excel(
    path: Path,
    sheet_name: str,
    df: pd.DataFrame,
    export_errors: ExportErrors | None,
    *,
    audit: list[dict[str, Any]] | None = None,
    issuer: str = "",
    year: str = "",
    month: str = "",
) -> None:
    """Write a required output file (creates parent dirs). Empty df still writes headers."""
    path.parent.mkdir(parents=True, exist_ok=True)
    clean = sanitize_dataframe_pre_export(
        df, audit, issuer=issuer, year=year, month=month, context=sheet_name,
    )
    safe_write_excel(
        path, {sheet_name: clean},
        export_errors=export_errors,
        drop_duplicate_value_columns=False,
    )


def _write_csv_safe(
    path: Path,
    df: pd.DataFrame,
    export_errors: ExportErrors | None,
    *,
    audit: list[dict[str, Any]] | None = None,
    issuer: str = "",
    year: str = "",
    month: str = "",
) -> None:
    clean = sanitize_dataframe_pre_export(
        df, audit, issuer=issuer, year=year, month=month, context=path.name,
    )
    safe_write_csv(path, clean, export_errors=export_errors, drop_duplicate_value_columns=False)


def _write_dashboards(
    issuer: str,
    year: str,
    chandra_monthly: pd.DataFrame,
    out_dir: Path,
) -> None:
    """Lightweight Plotly dashboards — failures are logged only."""
    out_dir.mkdir(parents=True, exist_ok=True)
    if chandra_monthly.empty:
        return
    try:
        import plotly.graph_objects as go
    except ImportError:
        logger.warning("Plotly not installed — skipping dashboards for %s/%s", issuer, year)
        return

    try:
        work = chandra_monthly.copy()
        work["_month"] = work["GAA_Load_Date"].astype(str).str.split("/").str[0].str.zfill(2)

        fig1 = go.Figure()
        for status, grp in work.groupby("enrolleeStatus"):
            m = grp.groupby("_month", as_index=False)["Enrollment_Count"].sum()
            fig1.add_trace(go.Scatter(x=m["_month"], y=m["Enrollment_Count"], mode="lines+markers", name=str(status)))
        fig1.update_layout(title=f"{issuer}/{year} — Monthly Status Trend", template="plotly_white")
        fig1.write_html(str(out_dir / "monthly_status_trend.html"), include_plotlyjs="cdn")

        fig2 = go.Figure()
        for status, grp in work.groupby("enrolleeStatus"):
            m = grp.groupby("_month", as_index=False)["Enrollment_Count"].sum()
            fig2.add_trace(go.Bar(x=m["_month"], y=m["Enrollment_Count"], name=str(status)))
        fig2.update_layout(barmode="stack", title=f"{issuer}/{year} — Status Mix", template="plotly_white")
        fig2.write_html(str(out_dir / "status_mix.html"), include_plotlyjs="cdn")

        agg = work.groupby("_month", as_index=False).agg({
            "Enrollment_Count": "sum", "Enrollee_Count": "sum",
        })
        fig3 = go.Figure(data=[
            go.Bar(name="Enrollment_Count", x=agg["_month"], y=agg["Enrollment_Count"]),
            go.Bar(name="Enrollee_Count", x=agg["_month"], y=agg["Enrollee_Count"]),
        ])
        fig3.update_layout(barmode="group", title=f"{issuer}/{year} — Enrollment vs Enrollee", template="plotly_white")
        fig3.write_html(str(out_dir / "enrollment_vs_enrollee.html"), include_plotlyjs="cdn")
    except Exception as exc:
        logger.warning("Dashboard generation failed for %s/%s: %s", issuer, year, exc)


def _xml_for_partition(xml_raw: pd.DataFrame, part: Partition) -> pd.DataFrame:
    if xml_raw.empty:
        return xml_raw
    work = xml_raw.copy()
    for col, val in (("issuer", part.issuer), ("year", part.year)):
        if col in work.columns:
            work = work[work[col].astype(str) == str(val)]
    if "month" in work.columns:
        work = work[work["month"].astype(str).map(_zmonth) == _zmonth(part.month)]
    return work


def _lifecycle_for_partition(
    lifecycle: pd.DataFrame,
    xml_raw: pd.DataFrame,
    part: Partition,
) -> pd.DataFrame:
    """Lifecycle rows tied to a source_data partition (not business month)."""
    if lifecycle.empty:
        return lifecycle
    xml_part = _xml_for_partition(xml_raw, part)
    if xml_part.empty:
        return _filter_year_month_lifecycle(lifecycle, part.year, part.month)
    keys = [k for k in ("policy_id", "member_id") if k in lifecycle.columns and k in xml_part.columns]
    if keys:
        key_df = xml_part[keys].drop_duplicates()
        return lifecycle.merge(key_df, on=keys, how="inner")
    return _filter_year_month_lifecycle(lifecycle, part.year, part.month)


def _br_for_partition(br: pd.DataFrame, part: Partition, xml_raw: pd.DataFrame | None = None) -> pd.DataFrame:
    """Match business-ready rows to a source_data partition."""
    if br.empty:
        return br
    needle = f"/{part.year}/{_zmonth(part.month)}/"
    if "source_file" in br.columns:
        mask = br["source_file"].astype(str).str.contains(needle, regex=False)
        if mask.any():
            return br[mask].copy()
    if xml_raw is not None and not xml_raw.empty:
        xml_part = _xml_for_partition(xml_raw, part)
        keys = [k for k in ("policy_id", "member_id") if k in br.columns and k in xml_part.columns]
        if keys and not xml_part.empty:
            key_df = xml_part[keys].drop_duplicates()
            matched = br.merge(key_df, on=keys, how="inner")
            if not matched.empty:
                return matched
    if "month" in br.columns:
        return br[br["month"].astype(str).map(_zmonth) == _zmonth(part.month)].copy()
    return br.iloc[0:0].copy()


def _suppress_pipeline_side_effects():
    """Prevent collapse/lineage audit writes from shared pipeline helpers."""
    import azure_reconciliation.business_transaction_collapse as btc

    def _noop(*_a, **_k):
        return None

    btc.write_collapse_audits = _noop  # type: ignore[method-assign]


def process_issuer_xml_business_quiet(issuer: str, xml_raw: pd.DataFrame, partitions: list[Partition]):
    _suppress_pipeline_side_effects()
    return process_issuer_xml_business(issuer, xml_raw, partitions)


def process_issuer_year(
    issuer: str,
    year: str,
    pipeline_partitions: list[Partition],
    write_partitions: list[Partition],
    state: RunState,
    *,
    force: bool = False,
    dry_run: bool = False,
    debug_trace: bool = False,
    export_errors: ExportErrors | None = None,
) -> dict[str, Any] | None:
    """Process one issuer/year — pipeline uses all year partitions; writes scoped to write_partitions."""
    year_parts = sorted(
        [p for p in pipeline_partitions if p.issuer == issuer and p.year == year],
        key=lambda p: p.month,
    )
    write_parts = sorted(
        [p for p in write_partitions if p.issuer == issuer and p.year == year],
        key=lambda p: p.month,
    )
    if not year_parts:
        state.skipped.append({"issuer": issuer, "year": year, "month": "", "reason": "no_partitions"})
        return None

    out_base = chandra_report_only_root() / issuer / year
    marker = out_base / "business_ready" / "business_ready_all_months.xlsx"
    if marker.exists() and not force and not dry_run:
        logger.info("SKIP existing output for %s/%s (use --force)", issuer, year)
        state.skipped.append({"issuer": issuer, "year": year, "month": "", "reason": "exists_use_force"})
        return None

    write_months = {_zmonth(p.month) for p in write_parts}
    logger.info(
        "START issuer=%s year=%s pipeline_months=%s write_months=%s",
        issuer, year, [p.month for p in year_parts], sorted(write_months),
    )
    t0 = time.monotonic()
    reporting_year = str(year)
    stage = StageTracker(issuer, year, debug_trace=debug_trace)

    def _track_stage(stage_name: str, df: pd.DataFrame | None = None, label: str = "") -> None:
        stage.set_stage(stage_name, df, label)
        state.current_stage = stage.current_stage
        state.last_stage_by_issuer[issuer] = stage.current_stage
        state.last_failure_context_by_issuer[issuer] = stage.failure_context()

    _track_stage("load_xml")
    xml_raw = load_xml_rows(
        prefer_staging=not PARSE_FROM_SOURCE,
        issuer_filter=issuer,
        year_filter=year,
    )
    if xml_raw.empty:
        for part in year_parts:
            if write_months and _zmonth(part.month) not in write_months:
                continue
            mkey = state.month_key(issuer, year, part.month)
            state.months_attempted.add(mkey)
            state.month_status.append({
                "issuer": issuer, "year": year, "month": _zmonth(part.month),
                "source_file_count": _count_source_files(issuer, year, part.month),
                "raw_rows": 0, "business_ready_rows": 0, "summary_rows": 0,
                "statuses_present": "",
                "status": "SKIPPED", "message": "no XML rows parsed",
            })
        state.skipped.append({"issuer": issuer, "year": year, "month": "", "reason": "no_xml_rows"})
        logger.warning("No XML rows for %s/%s — no output folder created", issuer, year)
        return None

    raw_before = len(xml_raw)
    _track_stage("apply_prior_year_filter")
    raw_audit = build_filter_audit(xml_raw, issuer=issuer, reporting_year=reporting_year, dataset_label="RAW")
    raw_excluded = safe_int((raw_audit["filter_action"] == FILTER_ACTION_EXCLUDE).sum()) if PY_FILTER_ENABLED else 0

    _track_stage("process_issuer_xml_business")
    result = process_issuer_xml_business_quiet(issuer, xml_raw, year_parts)

    business_ready = _prepare_business_ready(result, issuer=issuer, year=year, xml_raw=xml_raw)
    _track_stage("prepare_business_ready", business_ready, "business_ready")
    business_ready = sanitize_business_ready_df(
        business_ready, state.nan_cleanup_audit, issuer=issuer, year=year,
    )
    br_before = len(business_ready)
    if PY_FILTER_ENABLED and not business_ready.empty:
        business_ready = apply_prior_year_benefit_filter(business_ready, reporting_year=reporting_year)
    br_excluded = br_before - len(business_ready)

    lifecycle = result.lifecycle_input[
        result.lifecycle_input["year"].astype(str) == str(year)
    ].copy()
    if PY_FILTER_ENABLED and not lifecycle.empty:
        lifecycle = apply_prior_year_benefit_filter(lifecycle, reporting_year=reporting_year)

    model_h = _chandra_dashboard(lifecycle, source="xml") if not lifecycle.empty else pd.DataFrame()
    _track_stage("chandra_summary", model_h, "model_h")
    chandra_monthly = to_chandra_business_summary(model_h)
    chandra_yearly = chandra_year_rollup(chandra_monthly)
    br_summary = business_ready_dashboard_summary(business_ready)

    export_br = business_ready
    export_chandra_monthly = chandra_monthly
    export_chandra_yearly = chandra_yearly
    if write_months and len(write_months) < len({_zmonth(p.month) for p in year_parts}):
        scoped = pd.concat(
            [_br_for_partition(business_ready, p, xml_raw) for p in write_parts],
            ignore_index=True,
        ).drop_duplicates()
        export_br = scoped
        br_summary = business_ready_dashboard_summary(export_br)
        scoped_lifecycle = pd.concat(
            [_lifecycle_for_partition(lifecycle, xml_raw, p) for p in write_parts],
            ignore_index=True,
        )
        scoped_h = _chandra_dashboard(scoped_lifecycle, source="xml") if not scoped_lifecycle.empty else pd.DataFrame()
        export_chandra_monthly = to_chandra_business_summary(scoped_h)
        export_chandra_yearly = chandra_year_rollup(export_chandra_monthly)

    export_br = sanitize_business_ready_df(
        export_br, state.nan_cleanup_audit, issuer=issuer, year=year,
    )
    br_summary = sanitize_dashboard_summary_df(
        br_summary, state.nan_cleanup_audit, issuer=issuer, year=year,
    )
    export_chandra_monthly = sanitize_chandra_summary_df(
        export_chandra_monthly, state.nan_cleanup_audit, issuer=issuer, year=year,
    )
    export_chandra_yearly = chandra_year_rollup(export_chandra_monthly)
    export_chandra_yearly = sanitize_chandra_summary_df(
        export_chandra_yearly, state.nan_cleanup_audit, issuer=issuer, year=year,
    )

    state.filter_summary.append({
        "issuer": issuer,
        "year": year,
        "month": "",
        "raw_prior_year_excluded": raw_excluded,
        "business_ready_prior_year_excluded": br_excluded,
        "filter_status": _filter_status(raw_before, raw_excluded, raw_before - raw_excluded),
    })

    if dry_run:
        expected_months: list[str] = []
        for part in year_parts:
            if write_months and _zmonth(part.month) not in write_months:
                continue
            mkey = state.month_key(issuer, year, part.month)
            state.months_attempted.add(mkey)
            month_lifecycle = _lifecycle_for_partition(lifecycle, xml_raw, part)
            month_h = _chandra_dashboard(month_lifecycle, source="xml") if not month_lifecycle.empty else pd.DataFrame()
            chandra_month = to_chandra_business_summary(month_h)
            br_month = _br_for_partition(business_ready, part, xml_raw)
            raw_rows = _raw_rows_for_partition(xml_raw, part)
            if not chandra_month.empty or not br_month.empty:
                expected_months.append(_zmonth(part.month))
            logger.info(
                "DONE issuer=%s year=%s month=%s raw=%d business_ready=%d summary_rows=%d",
                issuer, year, part.month, raw_rows, len(br_month), len(chandra_month),
            )
        elapsed = time.monotonic() - t0
        return {
            "issuer": issuer, "year": year, "dry_run": True,
            "business_ready_rows": len(export_br), "summary_rows": len(export_chandra_monthly),
            "expected_months": expected_months,
            "elapsed_sec": elapsed,
        }

    # Collect month results first — only create output dirs if at least one valid month.
    month_writes: list[dict[str, Any]] = []
    expected_months: list[str] = []

    for part in year_parts:
        if write_months and _zmonth(part.month) not in write_months:
            continue
        if not state.claim(issuer, year, part.month):
            continue

        mkey = state.month_key(issuer, year, part.month)
        state.active_month = _zmonth(part.month)
        state.months_attempted.add(mkey)
        logger.info("START issuer=%s year=%s month=%s", issuer, year, part.month)

        try:
            month_lifecycle = _lifecycle_for_partition(lifecycle, xml_raw, part)
            month_h = _chandra_dashboard(month_lifecycle, source="xml") if not month_lifecycle.empty else pd.DataFrame()
            chandra_month = to_chandra_business_summary(month_h)
            chandra_month = sanitize_chandra_summary_df(
                chandra_month, state.nan_cleanup_audit,
                issuer=issuer, year=year, month=_zmonth(part.month),
            )
            br_month = _br_for_partition(business_ready, part, xml_raw)
        except Exception as exc:
            logger.warning(
                "Summary row warning issuer=%s year=%s month=%s: %s",
                issuer, year, part.month, exc,
            )
            state.skipped.append({
                "issuer": issuer, "year": year, "month": _zmonth(part.month),
                "reason": f"summary_row_error: {exc}",
            })
            state.month_status.append({
                "issuer": issuer, "year": year, "month": _zmonth(part.month),
                "source_file_count": _count_source_files(issuer, year, part.month),
                "raw_rows": _raw_rows_for_partition(xml_raw, part),
                "business_ready_rows": 0, "summary_rows": 0,
                "statuses_present": "",
                "status": "WARN", "message": f"summary error: {exc}",
            })
            continue

        src_count = _count_source_files(issuer, year, part.month)
        raw_rows = _raw_rows_for_partition(xml_raw, part)
        status = "OK" if raw_rows > 0 or not chandra_month.empty else "WARN"
        message = "" if status == "OK" else "source files present but no parsed rows for partition month"

        if src_count == 0:
            state.skipped.append({
                "issuer": issuer, "year": year, "month": _zmonth(part.month),
                "reason": "no_source_files",
            })
            state.month_status.append({
                "issuer": issuer, "year": year, "month": _zmonth(part.month),
                "source_file_count": 0, "raw_rows": 0,
                "business_ready_rows": 0, "summary_rows": 0,
                "statuses_present": "",
                "status": "SKIPPED", "message": "no source files",
            })
            continue

        if chandra_month.empty and br_month.empty:
            xml_part = _xml_for_partition(xml_raw, part)
            part_audit = build_filter_audit(
                xml_part, issuer=issuer, reporting_year=reporting_year, dataset_label="RAW",
            ) if not xml_part.empty else pd.DataFrame()
            part_raw_excl = safe_int((part_audit["filter_action"] == FILTER_ACTION_EXCLUDE).sum()) if not part_audit.empty else 0
            state.filter_summary.append({
                "issuer": issuer, "year": year, "month": _zmonth(part.month),
                "raw_prior_year_excluded": part_raw_excl,
                "business_ready_prior_year_excluded": 0,
                "filter_status": "WARN",
            })
            state.month_status.append({
                "issuer": issuer, "year": year, "month": _zmonth(part.month),
                "source_file_count": src_count, "raw_rows": raw_rows,
                "business_ready_rows": 0, "summary_rows": 0,
                "statuses_present": "",
                "status": "WARN", "message": message or "no business rows after filter",
            })
            logger.info(
                "DONE issuer=%s year=%s month=%s raw=%d business_ready=0 summary_rows=0",
                issuer, year, part.month, raw_rows,
            )
            continue

        statuses = _statuses_present(chandra_month)
        xml_part = _xml_for_partition(xml_raw, part)
        part_audit = build_filter_audit(
            xml_part, issuer=issuer, reporting_year=reporting_year, dataset_label="RAW",
        ) if not xml_part.empty else pd.DataFrame()
        part_raw_excl = safe_int((part_audit["filter_action"] == FILTER_ACTION_EXCLUDE).sum()) if not part_audit.empty else 0
        state.filter_summary.append({
            "issuer": issuer, "year": year, "month": _zmonth(part.month),
            "raw_prior_year_excluded": part_raw_excl,
            "business_ready_prior_year_excluded": 0,
            "filter_status": "PASS",
        })

        month_writes.append({
            "part": part,
            "chandra_month": chandra_month,
            "src_count": src_count,
            "raw_rows": raw_rows,
            "br_rows": len(br_month),
            "summary_rows": len(chandra_month),
            "statuses": statuses,
            "status": status,
            "message": message,
        })
        expected_months.append(_zmonth(part.month))
        logger.info(
            "DONE issuer=%s year=%s month=%s raw=%d business_ready=%d summary_rows=%d",
            issuer, year, part.month, raw_rows, len(br_month), len(chandra_month),
        )

    has_output = bool(month_writes) or not export_br.empty
    if not has_output:
        state.skipped.append({
            "issuer": issuer, "year": year, "month": "",
            "reason": "no_valid_month_output",
        })
        logger.warning("No valid month output for %s/%s — no output folder created", issuer, year)
        return None

    out_base = _issuer_out_base(issuer, year)
    br_dir = out_base / "business_ready"
    rep_dir = out_base / "reports"
    monthly_dir = rep_dir / "monthly"
    dash_dir = out_base / "dashboards"

    _track_stage("write_reports")

    br_cols = [c for c in CHANDRA_BUSINESS_READY_COLUMNS if c in export_br.columns]
    br_out = export_br[br_cols] if not export_br.empty else pd.DataFrame(columns=br_cols)
    _write_required_excel(
        br_dir / "business_ready_all_months.xlsx",
        "business_ready_all_months", br_out, export_errors,
        audit=state.nan_cleanup_audit, issuer=issuer, year=year,
    )
    _write_csv_safe(
        br_dir / "business_ready_all_months.csv", br_out, export_errors,
        audit=state.nan_cleanup_audit, issuer=issuer, year=year,
    )
    _write_required_excel(
        br_dir / "business_ready_summary.xlsx",
        "Business_Ready_Summary", br_summary, export_errors,
        audit=state.nan_cleanup_audit, issuer=issuer, year=year,
    )

    rollup_cols = [c for c in CHANDRA_BUSINESS_COLUMNS_CORE if c in export_chandra_yearly.columns]
    rollup_out = export_chandra_yearly if not export_chandra_yearly.empty else pd.DataFrame(columns=rollup_cols)
    _write_required_excel(
        rep_dir / "issuer_year_rollup.xlsx",
        "issuer_year_rollup", rollup_out, export_errors,
        audit=state.nan_cleanup_audit, issuer=issuer, year=year,
    )
    _write_csv_safe(
        rep_dir / "issuer_year_rollup.csv", rollup_out, export_errors,
        audit=state.nan_cleanup_audit, issuer=issuer, year=year,
    )

    for mw in month_writes:
        part = mw["part"]
        chandra_month = mw["chandra_month"]
        month_out = monthly_dir / _zmonth(part.month)
        _write_required_excel(
            month_out / "enrollment_summary.xlsx",
            "enrollment_summary", chandra_month, export_errors,
            audit=state.nan_cleanup_audit, issuer=issuer, year=year, month=_zmonth(part.month),
        )
        _write_csv_safe(
            month_out / "enrollment_summary.csv", chandra_month, export_errors,
            audit=state.nan_cleanup_audit, issuer=issuer, year=year, month=_zmonth(part.month),
        )

        mkey = state.month_key(issuer, year, part.month)
        state.months_successfully_written.add(mkey)
        state.month_status.append({
            "issuer": issuer, "year": year, "month": _zmonth(part.month),
            "source_file_count": mw["src_count"], "raw_rows": mw["raw_rows"],
            "business_ready_rows": mw["br_rows"],
            "summary_rows": mw["summary_rows"],
            "statuses_present": mw["statuses"],
            "status": mw["status"], "message": mw["message"],
        })

    _write_dashboards(issuer, year, export_chandra_monthly, dash_dir)

    elapsed = time.monotonic() - t0
    logger.info(
        "DONE issuer=%s year=%s total_time=%.1fs business_ready=%d summary=%d months_written=%d",
        issuer, year, elapsed, len(export_br), len(export_chandra_monthly), len(expected_months),
    )

    if not export_chandra_monthly.empty:
        state.chandra_parts.append(export_chandra_monthly)
    if not export_br.empty:
        state.br_parts.append(export_br)

    return {
        "issuer": issuer,
        "year": year,
        "business_ready_rows": len(export_br),
        "summary_rows": len(export_chandra_monthly),
        "months": len(year_parts),
        "expected_months": expected_months,
        "elapsed_sec": elapsed,
        "output_dir": str(out_base),
        "chandra_monthly": chandra_monthly,
        "business_ready": business_ready,
    }


def _write_year_rollup(year: str, state: RunState, errors: ExportErrors | None) -> None:
    year_dir = chandra_report_only_root() / year
    year_dir.mkdir(parents=True, exist_ok=True)

    all_chandra = pd.concat(state.chandra_parts, ignore_index=True) if state.chandra_parts else pd.DataFrame(
        columns=CHANDRA_BUSINESS_COLUMNS_CORE,
    )
    all_br = pd.concat(state.br_parts, ignore_index=True) if state.br_parts else pd.DataFrame()

    _write_pair(
        year_dir, "all_issuers_chandra_summary", all_chandra, errors,
        audit=state.nan_cleanup_audit, year=year,
    )
    _write_pair(
        year_dir, "all_issuers_business_ready_records", all_br, errors,
        audit=state.nan_cleanup_audit, year=year,
    )

    if state.month_status:
        for (iss, yr), grp in pd.DataFrame(state.month_status).groupby(["issuer", "year"]):
            state.issuer_year_status.append({
                "issuer": iss,
                "year": yr,
                "months_processed": safe_int((grp["status"] == "OK").sum(), 0),
                "total_raw_rows": safe_sum(grp["raw_rows"]),
                "total_business_ready_rows": safe_sum(grp["business_ready_rows"]),
                "total_summary_rows": safe_sum(grp["summary_rows"]),
                "status": "OK" if (grp["status"] == "OK").any() else "WARN",
                "message": "",
            })

    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    lines = [
        "<!DOCTYPE html><html><head><meta charset='utf-8'>",
        "<title>Chandra Report Only</title>",
        "<style>body{font-family:system-ui,sans-serif;margin:2rem}ul{line-height:1.8}</style>",
        "</head><body>",
        f"<h1>Chandra Report Only — {year}</h1>",
        f"<p>Generated: {now}</p>",
        "<ul>",
        f"<li><a href='all_issuers_chandra_summary.xlsx'>All issuers Chandra summary</a></li>",
        f"<li><a href='all_issuers_business_ready_records.xlsx'>All issuers business ready</a></li>",
        f"<li><a href='run_summary.xlsx'>Run summary</a></li>",
        "</ul><h2>Issuers</h2><ul>",
    ]
    issuers = sorted({r["issuer"] for r in state.month_status if r.get("status") == "OK"})
    for iss in issuers:
        lines.append(
            f"<li><a href='../{iss}/{year}/reports/monthly/01/enrollment_summary.xlsx'>{iss}</a></li>"
        )
    lines.extend(["</ul></body></html>"])
    (year_dir / "report_index.html").write_text("\n".join(lines), encoding="utf-8")


def _write_run_summary(
    year: str,
    state: RunState,
    errors: ExportErrors | None,
    *,
    dry_run: bool = False,
) -> Path:
    year_dir = chandra_report_only_root() / year
    year_dir.mkdir(parents=True, exist_ok=True)

    month_df = pd.DataFrame(state.month_status) if state.month_status else pd.DataFrame(
        columns=["issuer", "year", "month", "source_file_count", "raw_rows",
                 "business_ready_rows", "summary_rows", "statuses_present", "status", "message"],
    )
    issuer_year_df = pd.DataFrame(state.issuer_year_status)
    if issuer_year_df.empty and not month_df.empty:
        rows = []
        for (issuer, yr), grp in month_df.groupby(["issuer", "year"]):
            rows.append({
                "issuer": issuer, "year": yr,
                "months_processed": safe_int((grp["status"] == "OK").sum(), 0),
                "total_raw_rows": safe_sum(grp["raw_rows"]),
                "total_business_ready_rows": safe_sum(grp["business_ready_rows"]),
                "total_summary_rows": safe_sum(grp["summary_rows"]),
                "status": "OK" if (grp["status"] == "OK").any() else "WARN",
                "message": "",
            })
        issuer_year_df = pd.DataFrame(rows)

    skipped_df = pd.DataFrame(state.skipped) if state.skipped else pd.DataFrame(
        columns=["issuer", "year", "month", "reason"],
    )
    filter_df = pd.DataFrame(state.filter_summary) if state.filter_summary else pd.DataFrame(
        columns=["issuer", "year", "month", "raw_prior_year_excluded",
                 "business_ready_prior_year_excluded", "filter_status"],
    )

    total_summary_rows = safe_sum(month_df["summary_rows"]) if not month_df.empty and "summary_rows" in month_df.columns else 0
    months_skipped = len(skipped_df)
    missing_count = len(state.missing_output_files)
    expected_monthly = len(state.months_successfully_written)
    actual_monthly = expected_monthly - sum(
        1 for entry in state.missing_output_files
        if "monthly_enrollment_summary" in str(entry.get("reason", ""))
    )

    if missing_count > 0 or state.issuers_failed:
        final_status = "FAIL"
    elif state.export_errors:
        final_status = "WARNINGS"
    elif (
        (not month_df.empty and (month_df["status"] == "WARN").any())
        or months_skipped > 0
    ):
        final_status = "WARNINGS"
    elif dry_run:
        final_status = "DRY_RUN"
    else:
        final_status = "SUCCESS"

    final_df = pd.DataFrame([{
        "year": year,
        "issuers_discovered": len(state.issuers_discovered),
        "issuers_attempted": len(state.issuers_attempted),
        "issuers_successfully_written": len(state.issuers_successfully_written),
        "issuers_failed": len(state.issuers_failed),
        "months_discovered": len(state.months_discovered),
        "months_attempted": len(state.months_attempted),
        "months_successfully_written": len(state.months_successfully_written),
        "months_failed": len(state.months_failed),
        "months_skipped": months_skipped,
        "expected_monthly_files": expected_monthly,
        "actual_monthly_files": actual_monthly,
        "missing_output_files_count": missing_count,
        "total_summary_rows": total_summary_rows,
        "final_status": final_status,
    }])

    missing_df = pd.DataFrame(state.missing_output_files) if state.missing_output_files else pd.DataFrame(
        columns=["issuer", "year", "month", "expected_file", "reason"],
    )
    nan_audit_df = pd.DataFrame(state.nan_cleanup_audit) if state.nan_cleanup_audit else pd.DataFrame(
        columns=["issuer", "year", "month", "column_name", "nan_count", "cleanup_action"],
    )

    path = year_dir / "run_summary.xlsx"
    if dry_run:
        return path

    safe_write_excel(
        path,
        {
            "Issuer_Month_Status": month_df,
            "Issuer_Year_Status": issuer_year_df,
            "Empty_Or_Skipped": skipped_df,
            "Filter_Audit_Summary": filter_df,
            "Final_Validation": final_df,
            "Missing_Output_Files": missing_df,
            "NaN_Cleanup_Audit": nan_audit_df,
        },
        export_errors=errors,
        drop_duplicate_value_columns=False,
    )
    return path


def _compute_final_status(state: RunState, *, dry_run: bool = False) -> str:
    if dry_run:
        return "DRY_RUN"
    if state.missing_output_files or state.issuers_failed:
        return "FAIL"
    if state.export_errors:
        return "WARNINGS"
    if state.skipped or any(r.get("status") == "WARN" for r in state.month_status):
        return "WARNINGS"
    return "SUCCESS"


def _issuer_failure_reason(state: RunState, issuer: str) -> str:
    for entry in state.missing_output_files:
        if entry.get("issuer") == issuer:
            return str(entry.get("reason", "missing_output"))
    for entry in state.skipped:
        if entry.get("issuer") == issuer and not entry.get("month"):
            return str(entry.get("reason", "skipped"))
    for err in state.export_errors:
        if issuer in err:
            return err
    return "unknown_failure"


_BENIGN_SKIP_REASONS = frozenset({
    "no_xml_rows",
    "no_valid_month_output",
    "no_source_files",
    "no_partitions",
    "month_not_found",
})


def _is_benign_skip(state: RunState, issuer: str) -> bool:
    for entry in state.skipped:
        if entry.get("issuer") != issuer:
            continue
        reason = str(entry.get("reason", ""))
        if reason in _BENIGN_SKIP_REASONS:
            return True
        if reason.startswith("no_"):
            return True
    return False


def run_chandra_report_only(
    *,
    year: str,
    issuer_filter: str | None = None,
    month_filter: str | None = None,
    force: bool = False,
    dry_run: bool = False,
    stop_on_error: bool = False,
    debug_trace: bool = False,
    export_errors: ExportErrors | None = None,
) -> dict[str, Any]:
    """
    Generate Chandra-style reports only — hardcoded safe behavior.

    Reads partition filters via caller (from .env YEAR_FILTER / ISSUER_FILTER / MONTH_FILTER).
    Does not read FILTER_PRIOR_YEAR_BENEFIT_EFFECTIVE or other .env feature flags.
    """
    state: RunState | None = None
    _stage_log("START run_chandra_report_only")
    try:
        _apply_xml_only_mode()
        root = chandra_report_only_root()
        root.mkdir(parents=True, exist_ok=True)

        if not dry_run:
            _clear_outputs(year, issuer_filter=issuer_filter)

        state = RunState()
        state.current_stage = "discover"
        _stage_log("START discovery")
        if debug_trace:
            print(f"CURRENT_STAGE=discover year={year} issuer={issuer_filter or 'all'}")
        all_partitions = discover_partitions(
            issuer_filter=issuer_filter,
            year_filter=year,
        )
        if not all_partitions:
            raise RuntimeError(
                f"No source_data partitions for year={year} issuer={issuer_filter or 'all'}"
            )
        _stage_log(
            f"DONE discovery issuers={len({p.issuer for p in all_partitions})} "
            f"months={len(all_partitions)}"
        )

        month_allow = _parse_month_set(month_filter)
        write_partitions = all_partitions
        if month_allow:
            write_partitions = [p for p in all_partitions if _zmonth(p.month) in month_allow]
            if not write_partitions:
                raise RuntimeError(
                    f"No source_data for year={year} month={month_filter} issuer={issuer_filter or 'all'}"
                )

        pairs = sorted({(p.issuer, p.year) for p in all_partitions})
        for p in all_partitions:
            state.issuers_discovered.add(p.issuer)
            state.months_discovered.add(state.month_key(p.issuer, p.year, p.month))

        results: list[dict[str, Any]] = []
        run_start = time.monotonic()

        logger.info(
            "Chandra report only: year=%s issuer=%s month=%s issuers=%d months=%d PY_filter=ON dry_run=%s",
            year, issuer_filter or "all", month_filter or "all",
            len(state.issuers_discovered), len(state.months_discovered), dry_run,
        )

        for iss, yr in pairs:
            state.active_issuer = iss
            state.active_month = ""
            state.current_stage = "issuer_loop"
            _stage_log(f"START issuer={iss}")
            state.issuers_attempted.add(iss)
            pipeline_parts = [p for p in all_partitions if p.issuer == iss and p.year == yr]
            write_parts = [p for p in write_partitions if p.issuer == iss and p.year == yr]
            if month_allow and not write_parts:
                state.skipped.append({
                    "issuer": iss, "year": yr, "month": ",".join(sorted(month_allow)),
                    "reason": "month_not_found",
                })
                if stop_on_error:
                    raise RuntimeError(f"Month filter not found for issuer {iss}/{yr}")
                continue

            issuer_failed = False
            try:
                _stage_log(f"START process_issuer_year issuer={iss}")
                res = process_issuer_year(
                    iss, yr, pipeline_parts, write_parts, state,
                    force=force, dry_run=dry_run, debug_trace=debug_trace,
                    export_errors=export_errors,
                )
                _stage_log(f"DONE process_issuer_year issuer={iss}")
                if res is None:
                    if not _is_benign_skip(state, iss):
                        state.issuers_failed.add(iss)
                        issuer_failed = True
                else:
                    results.append(res)
                    if not dry_run:
                        expected_months = res.get("expected_months", [])
                        if _validate_issuer_outputs(state, iss, yr, expected_months):
                            state.issuers_successfully_written.add(iss)
                        else:
                            state.issuers_failed.add(iss)
                            issuer_failed = True
                            for m in expected_months:
                                state.months_failed.add(state.month_key(iss, yr, m))
                    else:
                        state.issuers_successfully_written.add(iss)
            except Exception as exc:
                stage_name = state.last_stage_by_issuer.get(iss, state.current_stage)
                fail_ctx = dict(state.last_failure_context_by_issuer.get(iss, {}))
                fail_ctx["current_stage"] = stage_name
                logger.error("ERROR issuer=%s year=%s stage=%s reason=%s", iss, yr, stage_name, exc)
                traceback.print_exc()
                failed_path = None
                if not dry_run:
                    failed_path = write_issuer_failure(
                        iss, yr, exc, stage=stage_name,
                        context=fail_ctx,
                        debug_trace=debug_trace,
                    )
                    append_run_errors_log(yr, iss, exc, stage=stage_name, failed_path=failed_path)
                    state.missing_output_files.append({
                        "issuer": iss,
                        "year": yr,
                        "month": "",
                        "expected_file": str(failed_path) if failed_path else "",
                        "reason": f"exception at {stage_name}: {exc}",
                    })
                state.skipped.append({
                    "issuer": iss, "year": yr, "month": "",
                    "reason": f"exception at {stage_name}: {exc}; traceback={failed_path or 'dry_run'}",
                })
                state.export_errors.append(f"{iss}/{yr}: {exc}")
                state.issuers_failed.add(iss)
                issuer_failed = True
                if export_errors:
                    export_errors.record(f"Chandra report only {iss}/{yr}: {exc}")
                if stop_on_error:
                    raise

            if issuer_failed and stop_on_error and not dry_run:
                raise RuntimeError(
                    f"Stop on error: issuer {iss}/{yr} failed — "
                    f"{_issuer_failure_reason(state, iss)}"
                )
            _stage_log(f"DONE issuer={iss}")

        if not dry_run:
            state.current_stage = "write_year_rollup"
            _stage_log("START write_year_rollup")
            _write_year_rollup(year, state, export_errors)
            _stage_log("DONE write_year_rollup")

        state.current_stage = "write_run_summary"
        _stage_log("START write_run_summary")
        summary_path = _write_run_summary(year, state, export_errors, dry_run=dry_run)
        _stage_log("DONE write_run_summary")

        elapsed = time.monotonic() - run_start
        final_status = _compute_final_status(state, dry_run=dry_run)
        logger.info("Chandra report only complete in %.1fs → %s [%s]", elapsed, root, final_status)
        _stage_log(f"DONE run_chandra_report_only status={final_status}")

        failed_issuers = sorted(state.issuers_failed)
        return {
            "output_root": str(root),
            "year": year,
            "issuer_filter": issuer_filter,
            "month_filter": month_filter,
            "issuer_years": len(results),
            "results": results,
            "run_summary": str(summary_path),
            "elapsed_sec": elapsed,
            "month_status_count": len(state.month_status),
            "skipped_count": len(state.skipped),
            "final_status": final_status,
            "issuers_discovered": len(state.issuers_discovered),
            "issuers_attempted": len(state.issuers_attempted),
            "issuers_successfully_written": len(state.issuers_successfully_written),
            "issuers_failed": len(state.issuers_failed),
            "failed_issuers": failed_issuers,
            "months_discovered": len(state.months_discovered),
            "months_attempted": len(state.months_attempted),
            "months_successfully_written": len(state.months_successfully_written),
            "months_failed": len(state.months_failed),
            "missing_output_files_count": len(state.missing_output_files),
            "missing_output_files": state.missing_output_files,
        }
    except Exception as exc:
        extra = {}
        if state and state.active_issuer:
            extra = dict(state.last_failure_context_by_issuer.get(state.active_issuer, {}))
        write_fatal_error_file(
            exc,
            year=year,
            issuer_filter=issuer_filter,
            month_filter=month_filter,
            current_stage=state.current_stage if state else "init",
            active_issuer=state.active_issuer if state else "",
            active_month=state.active_month if state else "",
            extra_context=extra or None,
            debug_trace=debug_trace,
        )
        raise
