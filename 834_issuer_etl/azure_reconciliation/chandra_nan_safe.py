"""
NaN-safe numeric/string helpers and pre-export sanitization for Chandra report-only runner.

Does not change parser, canonical, lifecycle, or Model H business rules.
"""

from __future__ import annotations

import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from config.config import settings

CHANDRA_BUSINESS_COLUMNS_CORE = [
    "Coverage_Year",
    "GAA_HIOS_ID",
    "GAA_Load_Date",
    "Insurance_Type",
    "status_Id",
    "enrolleeStatus",
    "Enrollment_Count",
    "Enrollee_Count",
]

_STATUS_ID_MAP = {"CONFIRM": 1, "CANCEL": 2, "TERM": 3, "UNKNOWN": 0}

_STATUS_TO_DISPLAY = {
    "ENROLLED": "CONFIRM", "CONFIRM": "CONFIRM", "CONFIRMED": "CONFIRM",
    "ACTIVE": "CONFIRM", "EFFECTUATED": "CONFIRM", "REINSTATE": "CONFIRM",
    "CANCELLED": "CANCEL", "CANCEL": "CANCEL", "CANCELED": "CANCEL",
    "TERMINATED": "TERM", "TERM": "TERM", "UNKNOWN": "UNKNOWN",
}


def _enrollee_status_display(raw: object) -> str:
    key = str(raw or "UNKNOWN").strip().upper()
    return _STATUS_TO_DISPLAY.get(key, "UNKNOWN")

NAN_STRINGS = frozenset({"", "nan", "none", "<na>", "nat", "null"})

COUNT_COLUMNS = frozenset({
    "Enrollment_Count", "Enrollee_Count", "enrollment_count", "enrollee_count",
    "raw_transaction_count", "collapsed_event_count",
    "business_ready_records", "distinct_enrollment_ids", "distinct_enrollee_ids",
    "distinct_enrollment_count", "distinct_enrollee_count",
    "summary_rows", "raw_rows", "business_ready_rows",
})

YEAR_COLUMNS = frozenset({
    "year", "Coverage_Year", "benefit_effective_year", "reporting_year",
})

MONTH_COLUMNS = frozenset({"month", "GAA_Load_Date"})

STATUS_ID_COLUMNS = frozenset({"status_Id"})


def is_missing(val: object) -> bool:
    if val is None:
        return True
    try:
        if pd.isna(val):
            return True
    except (TypeError, ValueError):
        pass
    s = str(val).strip().lower()
    return s in NAN_STRINGS


def safe_int(val: object, default: int = 0) -> int:
    """Never raises — NaN/None/empty returns default."""
    if is_missing(val):
        return default
    try:
        num = float(val)  # type: ignore[arg-type]
        if pd.isna(num):
            return default
        return int(num)
    except (TypeError, ValueError, OverflowError):
        return default


def safe_optional_int(val: object, default: str | int = "") -> str | int:
    """Missing values return default (usually ''); valid numerics return int."""
    if is_missing(val):
        return default
    try:
        num = float(val)  # type: ignore[arg-type]
        if pd.isna(num):
            return default
        return int(num)
    except (TypeError, ValueError, OverflowError):
        return default


def safe_year(val: object) -> str:
    if is_missing(val):
        return ""
    s = str(val).strip()
    if s.lower() in NAN_STRINGS:
        return ""
    try:
        return str(int(float(s)))
    except (TypeError, ValueError):
        return s if s.isdigit() and len(s) == 4 else ""


def safe_month(val: object) -> str:
    if is_missing(val):
        return ""
    s = str(val).strip()
    if s.lower() in NAN_STRINGS or s.lower() == "all":
        return ""
    try:
        m = int(float(s))
        return str(m).zfill(2) if 1 <= m <= 12 else ""
    except (TypeError, ValueError):
        return s.zfill(2) if s.isdigit() and len(s) <= 2 else ""


def safe_status_id(
    status_id_val: object = None,
    *,
    enrollee_status: object = None,
) -> int | str:
    """CONFIRM=1, CANCEL=2, TERM=3; derive from enrolleeStatus; unknown => ''."""
    if not is_missing(status_id_val):
        n = safe_int(status_id_val, default=-1)
        if n in (1, 2, 3):
            return n
    if not is_missing(enrollee_status):
        es = _enrollee_status_display(enrollee_status)
        mapped = _STATUS_ID_MAP.get(str(es).strip().upper(), None)
        if mapped in (1, 2, 3):
            return mapped
    return ""


def safe_sum(series: pd.Series, default: int = 0) -> int:
    if series is None or len(series) == 0:
        return default
    try:
        return safe_int(pd.to_numeric(series, errors="coerce").sum(), default)
    except Exception:
        return default


# Backward-compatible aliases
safe_count_int = safe_int
safe_year_str = safe_year
safe_month_str = safe_month


def _series_nan_count(series: pd.Series) -> int:
    if series.empty:
        return 0
    mask = series.isna() | series.astype(str).str.strip().str.lower().isin(NAN_STRINGS)
    return safe_int(mask.sum(), 0)


def _record_nan_audit(
    audit: list[dict[str, Any]],
    *,
    issuer: str,
    year: str,
    month: str,
    column_name: str,
    nan_count: int,
    cleanup_action: str,
) -> None:
    if nan_count <= 0:
        return
    audit.append({
        "issuer": issuer,
        "year": year,
        "month": month,
        "column_name": column_name,
        "nan_count": nan_count,
        "cleanup_action": cleanup_action,
    })


def sample_nan_rows(df: pd.DataFrame, cols: list[str], limit: int = 5) -> pd.DataFrame:
    if df.empty:
        return df
    present = [c for c in cols if c in df.columns]
    if not present:
        return df.iloc[0:0]
    mask = pd.Series([False] * len(df), index=df.index)
    for col in present:
        mask |= df[col].isna() | df[col].astype(str).str.strip().str.lower().isin(NAN_STRINGS)
    return df.loc[mask].head(limit)


def sanitize_dataframe_pre_export(
    df: pd.DataFrame,
    audit: list[dict[str, Any]] | None = None,
    *,
    issuer: str = "",
    year: str = "",
    month: str = "",
    context: str = "",
) -> pd.DataFrame:
    """Final NaN-safe pass before any Excel/CSV write in Chandra report-only path."""
    if df is None or df.empty:
        return df if df is not None else pd.DataFrame()

    out = df.copy()
    audit = audit if audit is not None else []

    for col in out.columns:
        if col in YEAR_COLUMNS or col.endswith("_year") or col == "Coverage_Year":
            n = _series_nan_count(out[col])
            if n:
                _record_nan_audit(
                    audit, issuer=issuer, year=year, month=month,
                    column_name=col, nan_count=n,
                    cleanup_action=f"safe_year({context})",
                )
            out[col] = out[col].apply(safe_year)

        elif col in MONTH_COLUMNS and col != "GAA_Load_Date":
            n = _series_nan_count(out[col])
            if n:
                _record_nan_audit(
                    audit, issuer=issuer, year=year, month=month,
                    column_name=col, nan_count=n,
                    cleanup_action=f"safe_month({context})",
                )
            out[col] = out[col].apply(safe_month)

        elif col in STATUS_ID_COLUMNS:
            n = _series_nan_count(out[col])
            if n:
                _record_nan_audit(
                    audit, issuer=issuer, year=year, month=month,
                    column_name=col, nan_count=n,
                    cleanup_action=f"safe_status_id({context})",
                )
            enrollee = out["enrolleeStatus"] if "enrolleeStatus" in out.columns else pd.Series([None] * len(out))
            out[col] = [
                safe_status_id(sid, enrollee_status=est)
                for sid, est in zip(out[col], enrollee, strict=False)
            ]

        elif col in COUNT_COLUMNS or col.endswith("_count") or col.endswith("_rows"):
            numeric = pd.to_numeric(out[col], errors="coerce")
            n = safe_int(numeric.isna().sum(), 0)
            if n:
                _record_nan_audit(
                    audit, issuer=issuer, year=year, month=month,
                    column_name=col, nan_count=n,
                    cleanup_action=f"safe_int_fill_zero({context})",
                )
            out[col] = numeric.fillna(0).apply(lambda v: safe_int(v, 0))

    if "business_month" in out.columns and "year" in out.columns and "month" in out.columns:
        out["business_month"] = out["year"].astype(str) + "-" + out["month"].astype(str)

    if "GAA_Load_Date" in out.columns:
        out["GAA_Load_Date"] = out["GAA_Load_Date"].fillna("").astype(str).replace("nan", "")

    return out


def sanitize_business_ready_df(
    df: pd.DataFrame,
    audit: list[dict[str, Any]],
    *,
    issuer: str,
    year: str,
    month: str = "",
) -> pd.DataFrame:
    return sanitize_dataframe_pre_export(
        df, audit, issuer=issuer, year=year, month=month, context="business_ready",
    )


def sanitize_chandra_summary_df(
    df: pd.DataFrame,
    audit: list[dict[str, Any]],
    *,
    issuer: str,
    year: str,
    month: str = "",
) -> pd.DataFrame:
    out = sanitize_dataframe_pre_export(
        df, audit, issuer=issuer, year=year, month=month, context="chandra_summary",
    )
    if out.empty:
        return pd.DataFrame(columns=CHANDRA_BUSINESS_COLUMNS_CORE)
    cols = [c for c in CHANDRA_BUSINESS_COLUMNS_CORE if c in out.columns]
    return out[cols]


def sanitize_dashboard_summary_df(
    df: pd.DataFrame,
    audit: list[dict[str, Any]],
    *,
    issuer: str,
    year: str,
) -> pd.DataFrame:
    return sanitize_dataframe_pre_export(
        df, audit, issuer=issuer, year=year, month="", context="dashboard_summary",
    )


class StageTracker:
    """Tracks CURRENT_STAGE for Chandra report-only runner."""

    def __init__(self, issuer: str, year: str, *, debug_trace: bool = False) -> None:
        self.issuer = issuer
        self.year = year
        self.debug_trace = debug_trace
        self.current_stage = "init"
        self.last_df: pd.DataFrame | None = None
        self.last_df_label = ""

    def set_stage(self, stage: str, df: pd.DataFrame | None = None, label: str = "") -> None:
        self.current_stage = stage
        if df is not None:
            self.last_df = df
            self.last_df_label = label
        msg = f"CURRENT_STAGE={stage} issuer={self.issuer} year={self.year}"
        if self.debug_trace:
            print(msg)

    def failure_context(self) -> dict[str, Any]:
        ctx: dict[str, Any] = {
            "issuer": self.issuer,
            "year": self.year,
            "current_stage": self.current_stage,
            "last_df_label": self.last_df_label,
        }
        if self.last_df is not None and not self.last_df.empty:
            ctx["dataframe_columns"] = list(self.last_df.columns)
            sample_cols = [
                c for c in self.last_df.columns
                if c in YEAR_COLUMNS | MONTH_COLUMNS | STATUS_ID_COLUMNS | COUNT_COLUMNS
            ]
            ctx["nan_sample"] = sample_nan_rows(self.last_df, sample_cols).to_dict(orient="records")
        return ctx


def write_issuer_failure(
    issuer: str,
    year: str,
    exc: BaseException,
    *,
    stage: str,
    context: dict[str, Any] | None = None,
    debug_trace: bool = False,
) -> Path:
    """Write outputs/chandra_report_only/<issuer>/<year>/_FAILED.txt"""
    from config.config import settings
    out_dir = settings.outputs_path / "chandra_report_only" / issuer / year
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "_FAILED.txt"
    tb = traceback.format_exc()
    lines = [
        f"issuer={issuer}",
        f"year={year}",
        f"CURRENT_STAGE={stage}",
        f"exception={exc}",
        "",
        "=== TRACEBACK ===",
        tb,
    ]
    if context:
        lines.append("")
        lines.append("=== CONTEXT ===")
        for k, v in context.items():
            if k == "nan_sample" and isinstance(v, list):
                lines.append(f"{k}:")
                for row in v:
                    lines.append(f"  {row}")
            elif k == "dataframe_columns" and isinstance(v, list):
                lines.append(f"{k}: {', '.join(str(c) for c in v)}")
            else:
                lines.append(f"{k}: {v}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    if debug_trace:
        print(f"Wrote failure trace: {path}")
    return path


def write_fatal_error_file(
    exc: BaseException,
    *,
    year: str | None = None,
    issuer_filter: str | None = None,
    month_filter: str | None = None,
    current_stage: str = "",
    active_issuer: str | None = None,
    active_month: str | None = None,
    extra_context: dict[str, Any] | None = None,
    debug_trace: bool = False,
) -> Path:
    """
    Write outputs/chandra_report_only/_FATAL_ERROR.txt for top-level uncaught failures.

    Also appends to outputs/chandra_report_only/<year>/run_errors.log when year is known.
    """
    from config.config import settings

    root = settings.outputs_path / "chandra_report_only"
    root.mkdir(parents=True, exist_ok=True)
    path = root / "_FATAL_ERROR.txt"
    tb = traceback.format_exc()
    exc_type = type(exc).__name__
    exc_loc = traceback.format_exception_only(exc_type, exc)

    lines = [
        "CHANDRA REPORT ONLY — FATAL ERROR",
        f"exception_type={exc_type}",
        f"exception={exc}",
        f"CURRENT_STAGE={current_stage or 'unknown'}",
        f"year={year or ''}",
        f"issuer_filter={issuer_filter or 'ALL'}",
        f"month_filter={month_filter or 'ALL'}",
        f"active_issuer={active_issuer or ''}",
        f"active_month={active_month or ''}",
        "",
        "=== EXCEPTION SUMMARY ===",
        "".join(exc_loc).strip(),
        "",
        "=== FULL TRACEBACK (file + line) ===",
        tb,
    ]
    if extra_context:
        lines.append("")
        lines.append("=== CONTEXT ===")
        for k, v in extra_context.items():
            if k == "nan_sample" and isinstance(v, list):
                lines.append(f"{k}:")
                for row in v:
                    lines.append(f"  {row}")
            elif k == "dataframe_columns" and isinstance(v, list):
                lines.append(f"{k}: {', '.join(str(c) for c in v)}")
            else:
                lines.append(f"{k}: {v}")

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    if year:
        log_path = root / year / "run_errors.log"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
        entry = (
            f"[{ts}] FATAL year={year} stage={current_stage or 'unknown'} "
            f"issuer_filter={issuer_filter or 'ALL'} active_issuer={active_issuer or ''} "
            f"active_month={active_month or ''} error={exc}\n{tb}\n"
            f"fatal_file={path}\n---\n"
        )
        with log_path.open("a", encoding="utf-8") as fh:
            fh.write(entry)

    print(tb, end="")
    print(f"\nFATAL ERROR trace written to: {path}")
    if year:
        print(f"Also logged to: {root / year / 'run_errors.log'}")
    if debug_trace:
        print(f"CURRENT_STAGE={current_stage or 'unknown'}")

    return path


def append_run_errors_log(
    year: str,
    issuer: str,
    exc: BaseException,
    *,
    stage: str,
    failed_path: Path | None = None,
) -> Path:
    from config.config import settings
    log_path = settings.outputs_path / "chandra_report_only" / year / "run_errors.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
    entry = (
        f"[{ts}] issuer={issuer} year={year} stage={stage} "
        f"error={exc}\n{traceback.format_exc()}\n"
    )
    if failed_path:
        entry += f"failed_file={failed_path}\n"
    entry += "---\n"
    with log_path.open("a", encoding="utf-8") as fh:
        fh.write(entry)
    return log_path
