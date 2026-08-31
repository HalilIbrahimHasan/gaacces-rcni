"""
Safe export layer — sanitize DataFrames before SQLite / Excel / HTML / CSV writes.

Export failures are logged and recorded; they must not stop the pipeline.
"""

from __future__ import annotations

import json
import re
import sqlite3
import traceback
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pandas as pd

from config.config import settings
from utils.logger import get_logger

logger = get_logger(__name__)

EXCEL_MAX_ROWS = 50_000
HTML_MAX_ROWS = 5_000
SAMPLE_ROWS = 50_000
HTML_SAMPLE_ROWS = 2_000

_INVALID_COL = re.compile(r"[^\w]+", re.UNICODE)


@dataclass
class ExportErrors:
    """Collect export failures for debug output."""

    errors: list[str] = field(default_factory=list)

    def record(self, message: str) -> None:
        self.errors.append(message)
        logger.error("Export error: %s", message)

    def write_debug_file(self) -> Path:
        path = settings.outputs_path / "debug" / "export_errors.txt"
        path.parent.mkdir(parents=True, exist_ok=True)
        body = "\n".join(self.errors) if self.errors else "(no export errors)"
        path.write_text(body + "\n", encoding="utf-8")
        return path


def _unique_columns(names: list[str]) -> list[str]:
    seen: dict[str, int] = {}
    out: list[str] = []
    for name in names:
        base = name or "col"
        if base not in seen:
            seen[base] = 0
            out.append(base)
        else:
            seen[base] += 1
            out.append(f"{base}_{seen[base]}")
    return out


def _sanitize_column_name(name: Any) -> str:
    s = str(name).strip()
    s = _INVALID_COL.sub("_", s)
    s = s.strip("_") or "col"
    if s[0].isdigit():
        s = f"c_{s}"
    return s


def _cell_value(val: Any) -> Any:
    if val is None:
        return None
    if isinstance(val, float) and pd.isna(val):
        return None
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(val, (datetime, date, pd.Timestamp)):
        try:
            return val.isoformat() if hasattr(val, "isoformat") else str(val)
        except Exception:
            return str(val)
    if isinstance(val, Decimal):
        return float(val)
    if isinstance(val, (list, dict, set, tuple)):
        try:
            return json.dumps(val, default=str)
        except Exception:
            return str(val)
    if isinstance(val, (bytes, bytearray)):
        try:
            return val.decode("utf-8", errors="replace")
        except Exception:
            return str(val)
    if isinstance(val, (int, float, str, bool)):
        return val
    return str(val)


def safe_dataframe_for_export(
    df: pd.DataFrame | None,
    *,
    table_name: str = "",
    drop_duplicate_value_columns: bool = True,
) -> pd.DataFrame:
    """Sanitize a DataFrame for SQLite/Excel/HTML/CSV export."""
    if df is None or df.empty:
        logger.info(
            "safe_dataframe_for_export [%s]: empty (shape=0)",
            table_name or "unnamed",
        )
        return pd.DataFrame()

    work = df.copy()
    raw_cols = [str(c) for c in work.columns]
    clean_cols = [_sanitize_column_name(c) for c in raw_cols]
    clean_cols = _unique_columns(clean_cols)
    work.columns = clean_cols

    # Drop fully duplicated columns (identical values)
    if drop_duplicate_value_columns:
        dup_mask = work.T.duplicated()
        if dup_mask.any():
            drop_cols = list(work.columns[dup_mask])
            work = work.drop(columns=drop_cols)
            logger.warning(
                "safe_dataframe_for_export [%s]: dropped %d duplicate column(s): %s",
                table_name, len(drop_cols), drop_cols[:5],
            )

    for col in work.columns:
        series = work[col]
        if pd.api.types.is_datetime64_any_dtype(series):
            work[col] = pd.to_datetime(series, errors="coerce").dt.strftime("%Y-%m-%dT%H:%M:%S")
            work[col] = work[col].where(series.notna(), None)
        else:
            work[col] = series.map(_cell_value)

    work = work.where(pd.notnull(work), None)

    logger.info(
        "safe_dataframe_for_export [%s]: shape=%s columns=%s dtypes=%s",
        table_name or "unnamed",
        work.shape,
        list(work.columns),
        {c: type(work[c].dropna().iloc[0]).__name__ if work[c].notna().any() else "null"
         for c in work.columns},
    )
    return work


def _debug_dir() -> Path:
    d = settings.outputs_path / "debug"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _write_failed_export_debug(table_name: str, df: pd.DataFrame, exc: Exception) -> None:
    safe_name = _sanitize_column_name(table_name)
    dbg = _debug_dir()
    try:
        csv_path = dbg / f"{safe_name}_failed_export.csv"
        safe_dataframe_for_export(df, table_name=table_name).to_csv(csv_path, index=False)
    except Exception as inner:
        logger.warning("Could not write failed export CSV for %s: %s", table_name, inner)
    try:
        meta_path = dbg / f"{safe_name}_failed_export_columns.txt"
        lines = [
            f"table: {table_name}",
            f"exception: {exc}",
            f"traceback:\n{traceback.format_exc()}",
            "columns:",
        ]
        for col in df.columns:
            lines.append(f"  {col!r}: {df[col].dtype}")
        meta_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    except Exception as inner:
        logger.warning("Could not write failed export columns for %s: %s", table_name, inner)


def safe_write_sqlite(
    conn: sqlite3.Connection,
    table_name: str,
    df: pd.DataFrame,
    *,
    if_exists: str = "replace",
    export_errors: ExportErrors | None = None,
) -> bool:
    """Write sanitized DataFrame to SQLite; return False on failure."""
    if df.empty:
        return True
    safe = safe_dataframe_for_export(df, table_name=table_name)
    try:
        if if_exists == "replace":
            conn.execute(f'DROP TABLE IF EXISTS "{_sanitize_column_name(table_name)}"')
        safe.to_sql(
            _sanitize_column_name(table_name),
            conn,
            if_exists=if_exists,
            index=False,
        )
        logger.info("SQLite wrote %d row(s) to %s", len(safe), table_name)
        return True
    except Exception as exc:
        msg = f"SQLite export failed for {table_name}: {exc}"
        if export_errors:
            export_errors.record(msg)
        _write_failed_export_debug(table_name, df, exc)
        return False


def safe_replace_table_sqlite(
    db_path: Path,
    table_name: str,
    df: pd.DataFrame,
    *,
    export_errors: ExportErrors | None = None,
) -> bool:
    if df.empty:
        logger.warning("Skipping empty SQLite write to %s", table_name)
        return True
    db_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with sqlite3.connect(db_path) as conn:
            return safe_write_sqlite(
                conn, table_name, df, if_exists="replace", export_errors=export_errors,
            )
    except Exception as exc:
        msg = f"SQLite connection failed for {table_name}: {exc}"
        if export_errors:
            export_errors.record(msg)
        _write_failed_export_debug(table_name, df, exc)
        return False


def _summary_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame({"note": ["empty"]})
    rows = [{"column": c, "dtype": str(df[c].dtype), "non_null": int(df[c].notna().sum())} for c in df.columns]
    return pd.DataFrame(rows)


def safe_write_excel(
    output_path: Path,
    sheets: dict[str, pd.DataFrame],
    *,
    export_errors: ExportErrors | None = None,
    drop_duplicate_value_columns: bool = True,
) -> bool:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
            for name, df in sheets.items():
                safe_name = name[:31]
                if df.empty:
                    pd.DataFrame().to_excel(writer, sheet_name=safe_name, index=False)
                    continue
                safe = safe_dataframe_for_export(
                    df, table_name=f"excel:{name}",
                    drop_duplicate_value_columns=drop_duplicate_value_columns,
                )
                if len(safe) > EXCEL_MAX_ROWS:
                    _summary_dataframe(safe).to_excel(
                        writer, sheet_name=f"{safe_name[:25]}_sum"[:31], index=False,
                    )
                    safe.head(SAMPLE_ROWS).to_excel(
                        writer, sheet_name=f"{safe_name[:22]}_smp"[:31], index=False,
                    )
                else:
                    safe.to_excel(writer, sheet_name=safe_name, index=False)
        logger.info("Wrote Excel: %s (%d sheets)", output_path, len(sheets))
        return True
    except Exception as exc:
        msg = f"Excel export failed for {output_path}: {exc}"
        if export_errors:
            export_errors.record(msg)
        for name, df in sheets.items():
            if not df.empty:
                _write_failed_export_debug(f"excel_{name}", df, exc)
        return False


def safe_write_csv(
    output_path: Path,
    df: pd.DataFrame,
    *,
    table_name: str = "",
    export_errors: ExportErrors | None = None,
    drop_duplicate_value_columns: bool = True,
) -> bool:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        safe = safe_dataframe_for_export(
            df,
            table_name=table_name or output_path.stem,
            drop_duplicate_value_columns=drop_duplicate_value_columns,
        )
        safe.to_csv(output_path, index=False)
        logger.info("Wrote CSV: %s (%d rows)", output_path, len(safe))
        return True
    except Exception as exc:
        msg = f"CSV export failed for {output_path}: {exc}"
        if export_errors:
            export_errors.record(msg)
        return False


def _df_to_html_table(df: pd.DataFrame, *, max_rows: int = HTML_MAX_ROWS) -> str:
    if df.empty:
        return "<p><em>empty</em></p>"
    sample = safe_dataframe_for_export(df.head(max_rows), table_name="html")
    return sample.to_html(index=False, escape=True, border=1)


def safe_write_html_report(
    output_path: Path,
    *,
    title: str,
    summary_df: pd.DataFrame | None = None,
    detail_df: pd.DataFrame | None = None,
    extra_html: str = "",
    export_errors: ExportErrors | None = None,
) -> bool:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        parts = [
            "<!DOCTYPE html><html><head><meta charset='utf-8'>",
            f"<title>{title}</title>",
            "<style>body{font-family:sans-serif;margin:1.5rem}table{border-collapse:collapse}",
            "th,td{border:1px solid #ccc;padding:4px 8px}h2{margin-top:1.5rem}</style>",
            "</head><body>",
            f"<h1>{title}</h1>",
        ]
        if extra_html:
            parts.append(extra_html)
        if summary_df is not None:
            parts.append("<h2>Summary</h2>")
            parts.append(_df_to_html_table(summary_df, max_rows=HTML_MAX_ROWS))
        if detail_df is not None:
            parts.append("<h2>Detail sample</h2>")
            parts.append(_df_to_html_table(detail_df, max_rows=HTML_SAMPLE_ROWS))
        parts.append("</body></html>")
        output_path.write_text("\n".join(parts), encoding="utf-8")
        logger.info("Wrote HTML: %s", output_path)
        return True
    except Exception as exc:
        msg = f"HTML export failed for {output_path}: {exc}"
        if export_errors:
            export_errors.record(msg)
        return False


def csv_fallback_dir() -> Path:
    d = settings.outputs_path / "csv"
    d.mkdir(parents=True, exist_ok=True)
    return d


def write_csv_fallback(
    name: str,
    df: pd.DataFrame,
    *,
    export_errors: ExportErrors | None = None,
) -> Path | None:
    if df.empty:
        return None
    path = csv_fallback_dir() / f"{_sanitize_column_name(name)}.csv"
    if safe_write_csv(path, df, table_name=name, export_errors=export_errors):
        return path
    return None
