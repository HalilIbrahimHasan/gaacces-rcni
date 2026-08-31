"""Azure insert performance profiling for inbound automation."""

from __future__ import annotations

import statistics
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from azure_reconciliation.safe_export import safe_write_excel
from config.config import settings
from inbound_automation.azure_common import (
    DEFAULT_BATCH_SIZE,
    batch_size,
    connect_automation_engine,
    fast_executemany_enabled,
    require_env_gate,
)
from inbound_automation.azure_writer import INBOUND_AUTOMATION_COLUMNS, row_to_insert_params
from inbound_automation.discovery import discover_for_run
from inbound_automation.pipeline import _process_file
from inbound_automation.run_context import LoadRunContext
from parsers.parser_834 import Parser834
from utils.logger import get_logger

logger = get_logger(__name__)

INSERT_METHOD = (
    "SQLAlchemy connection.execute(text(INSERT...), list[dict]) "
    "→ pyodbc executemany (batched rows per call)"
)

SCHEMA = "dbo"
PROFILE_LOAD_RUN_PREFIX = "profile_insert_"


@dataclass
class BatchTiming:
    batch_size: int
    row_count: int
    duration_sec: float

    @property
    def rows_per_sec(self) -> float:
        if self.duration_sec <= 0:
            return 0.0
        return self.row_count / self.duration_sec


@dataclass
class InsertProfileResult:
    fast_executemany: bool
    batch_size_tested: int
    sample_row_count: int
    sample_file_count: int
    batch_timings: list[BatchTiming] = field(default_factory=list)
    commit_duration_sec: float = 0.0
    total_sql_duration_sec: float = 0.0
    rolled_back: bool = True

    @property
    def average_batch_duration_sec(self) -> float:
        if not self.batch_timings:
            return 0.0
        return statistics.mean(t.duration_sec for t in self.batch_timings)

    @property
    def average_rows_per_sec(self) -> float:
        if self.total_sql_duration_sec <= 0:
            return 0.0
        return self.sample_row_count / self.total_sql_duration_sec

    @property
    def average_files_per_sec(self) -> float:
        return 0.0  # insert-only benchmark; file/sec measured in file-load profile


@dataclass
class FileLoadTiming:
    source_file: str
    row_count: int
    sql_duration_sec: float
    commit_duration_sec: float
    total_duration_sec: float

    @property
    def rows_per_sec(self) -> float:
        if self.sql_duration_sec <= 0:
            return 0.0
        return self.row_count / self.sql_duration_sec


@dataclass
class FileLoadProfileResult:
    fast_executemany: bool
    batch_size: int
    file_timings: list[FileLoadTiming] = field(default_factory=list)

    @property
    def total_rows(self) -> int:
        return sum(f.row_count for f in self.file_timings)

    @property
    def total_sql_duration_sec(self) -> float:
        return sum(f.sql_duration_sec for f in self.file_timings)

    @property
    def total_commit_duration_sec(self) -> float:
        return sum(f.commit_duration_sec for f in self.file_timings)

    @property
    def average_rows_per_sec(self) -> float:
        if self.total_sql_duration_sec <= 0:
            return 0.0
        return self.total_rows / self.total_sql_duration_sec

    @property
    def average_files_per_sec(self) -> float:
        total_file_sec = sum(f.total_duration_sec for f in self.file_timings)
        if total_file_sec <= 0 or not self.file_timings:
            return 0.0
        return len(self.file_timings) / total_file_sec


def _build_insert_sql() -> text:
    col_list = ", ".join(f"[{c}]" for c in INBOUND_AUTOMATION_COLUMNS)
    placeholders = ", ".join(f":{c}" for c in INBOUND_AUTOMATION_COLUMNS)
    return text(
        f"INSERT INTO [{SCHEMA}].[inbound_automation] ({col_list}) VALUES ({placeholders})"
    )


def _collect_sample_rows(
    *,
    issuer: str | None,
    year: str | None,
    month: str | None,
    max_files: int,
) -> tuple[list[dict[str, Any]], int]:
    context = LoadRunContext.create(
        run_mode="profile",
        year_filter=year,
        all_years=False,
        issuer_filter=[issuer] if issuer else None,
        month_filter=month,
    )
    sources = discover_for_run(
        settings.source_data_path,
        year_filter=year,
        all_years=False,
        issuer_filter=[issuer] if issuer else None,
        month_filter=month,
    )
    if not sources:
        raise RuntimeError("No source files found for profiling filters.")

    parser = Parser834()
    loaded_at = datetime.now(timezone.utc)
    cli_year = year
    rows: list[dict[str, Any]] = []
    files_used = 0

    for source in sources[:max_files]:
        result = _process_file(
            source,
            parser=parser,
            context=context,
            cli_year=cli_year,
            loaded_at=loaded_at,
        )
        if result.rows:
            rows.extend(result.rows)
            files_used += 1

    if not rows:
        raise RuntimeError("Profiling found files but parsed zero rows.")

    return rows, files_used


def profile_insert_batch(
    engine: Engine,
    rows: list[dict[str, Any]],
    *,
    batch_size_tested: int,
    fast_executemany: bool,
) -> InsertProfileResult:
    """
    Benchmark insert throughput using a rolled-back transaction.

    No rows are persisted.
    """
    sql = _build_insert_sql()
    params = [row_to_insert_params(row) for row in rows]

    profile = InsertProfileResult(
        fast_executemany=fast_executemany,
        batch_size_tested=batch_size_tested,
        sample_row_count=len(params),
        sample_file_count=0,
    )

    commit_start = time.perf_counter()
    with engine.connect() as conn:
        trans = conn.begin()
        try:
            sql_start = time.perf_counter()
            for start in range(0, len(params), batch_size_tested):
                batch = params[start : start + batch_size_tested]
                batch_start = time.perf_counter()
                conn.execute(sql, batch)
                batch_duration = time.perf_counter() - batch_start
                profile.batch_timings.append(
                    BatchTiming(
                        batch_size=batch_size_tested,
                        row_count=len(batch),
                        duration_sec=batch_duration,
                    )
                )
            profile.total_sql_duration_sec = time.perf_counter() - sql_start
            trans.rollback()
            profile.rolled_back = True
        except Exception:
            trans.rollback()
            raise
    profile.commit_duration_sec = time.perf_counter() - commit_start

    return profile


def profile_file_load_timings(
    engine: Engine,
    rows_by_file: list[tuple[str, list[dict[str, Any]]]],
    *,
    batch_size_used: int,
    fast_executemany: bool,
) -> FileLoadProfileResult:
    """Profile per-file insert + commit with rollback (no persistence)."""
    sql = _build_insert_sql()
    result = FileLoadProfileResult(
        fast_executemany=fast_executemany,
        batch_size=batch_size_used,
    )

    for source_file, rows in rows_by_file:
        params = [row_to_insert_params(row) for row in rows]
        file_start = time.perf_counter()
        sql_duration = 0.0
        commit_duration = 0.0

        with engine.connect() as conn:
            trans = conn.begin()
            try:
                sql_start = time.perf_counter()
                for start in range(0, len(params), batch_size_used):
                    batch = params[start : start + batch_size_used]
                    conn.execute(sql, batch)
                sql_duration = time.perf_counter() - sql_start
                commit_start = time.perf_counter()
                trans.rollback()
                commit_duration = time.perf_counter() - commit_start
            except Exception:
                trans.rollback()
                raise

        result.file_timings.append(
            FileLoadTiming(
                source_file=source_file,
                row_count=len(params),
                sql_duration_sec=sql_duration,
                commit_duration_sec=commit_duration,
                total_duration_sec=time.perf_counter() - file_start,
            )
        )

    return result


def run_insert_profile(
    *,
    issuer: str | None,
    year: str,
    month: str | None,
    batch_sizes: list[int],
    max_files: int,
    output_dir: Path,
) -> dict[str, Any]:
    require_env_gate("insert profile")
    settings.refresh_from_env()

    sample_rows, files_used = _collect_sample_rows(
        issuer=issuer,
        year=year,
        month=month,
        max_files=max_files,
    )

    # Group rows by source_file for file-level profiling
    by_file: dict[str, list[dict[str, Any]]] = {}
    for row in sample_rows:
        by_file.setdefault(str(row["source_file"]), []).append(row)
    rows_by_file = list(by_file.items())

    summary_rows: list[dict[str, Any]] = []
    batch_detail_rows: list[dict[str, Any]] = []
    file_detail_rows: list[dict[str, Any]] = []

    for use_fast in (False, True):
        engine = connect_automation_engine(fast_executemany=use_fast)
        label = "fast_executemany=ON" if use_fast else "fast_executemany=OFF"

        for bs in batch_sizes:
            print(f"\nProfiling {label} batch_size={bs} ...")
            profile = profile_insert_batch(
                engine, sample_rows, batch_size_tested=bs, fast_executemany=use_fast,
            )
            profile.sample_file_count = files_used

            for bt in profile.batch_timings:
                batch_detail_rows.append({
                    "fast_executemany": use_fast,
                    "batch_size": bs,
                    "batch_row_count": bt.row_count,
                    "batch_duration_sec": round(bt.duration_sec, 4),
                    "batch_rows_per_sec": round(bt.rows_per_sec, 1),
                })

            summary_rows.append({
                "fast_executemany": use_fast,
                "batch_size": bs,
                "sample_rows": profile.sample_row_count,
                "sample_files": profile.sample_file_count,
                "batch_count": len(profile.batch_timings),
                "avg_batch_duration_sec": round(profile.average_batch_duration_sec, 4),
                "total_sql_duration_sec": round(profile.total_sql_duration_sec, 4),
                "commit_duration_sec": round(profile.commit_duration_sec, 4),
                "avg_rows_per_sec": round(profile.average_rows_per_sec, 1),
                "rolled_back": profile.rolled_back,
            })

            print(
                f"  rows/sec={profile.average_rows_per_sec:.1f} "
                f"sql_sec={profile.total_sql_duration_sec:.2f} "
                f"avg_batch_sec={profile.average_batch_duration_sec:.3f}"
            )

        # File-level profile at configured batch size
        bs = batch_sizes[0] if batch_sizes else batch_size()
        file_profile = profile_file_load_timings(
            engine, rows_by_file, batch_size_used=bs, fast_executemany=use_fast,
        )
        for ft in file_profile.file_timings:
            file_detail_rows.append({
                "fast_executemany": use_fast,
                "batch_size": bs,
                "source_file": ft.source_file,
                "row_count": ft.row_count,
                "sql_duration_sec": round(ft.sql_duration_sec, 4),
                "commit_duration_sec": round(ft.commit_duration_sec, 4),
                "total_duration_sec": round(ft.total_duration_sec, 4),
                "rows_per_sec": round(ft.rows_per_sec, 1),
            })

        summary_rows.append({
            "fast_executemany": use_fast,
            "batch_size": bs,
            "sample_rows": file_profile.total_rows,
            "sample_files": len(file_profile.file_timings),
            "batch_count": None,
            "avg_batch_duration_sec": None,
            "total_sql_duration_sec": round(file_profile.total_sql_duration_sec, 4),
            "commit_duration_sec": round(file_profile.total_commit_duration_sec, 4),
            "avg_rows_per_sec": round(file_profile.average_rows_per_sec, 1),
            "avg_files_per_sec": round(file_profile.average_files_per_sec, 2),
            "rolled_back": True,
            "profile_type": "per_file",
        })

    method_df = pd.DataFrame([
        {"item": "insert_method", "value": INSERT_METHOD},
        {"item": "not_used", "value": "row-by-row single execute"},
        {"item": "not_used", "value": "SQLAlchemy bulk_insert_mappings / ORM bulk"},
        {"item": "used", "value": "executemany via conn.execute(text, list[dict])"},
        {"item": "fast_executemany_default", "value": str(fast_executemany_enabled())},
        {"item": "default_batch_size", "value": str(DEFAULT_BATCH_SIZE)},
        {"item": "column_count", "value": str(len(INBOUND_AUTOMATION_COLUMNS))},
    ])

    out_path = output_dir / "insert_profile.xlsx"
    safe_write_excel(
        out_path,
        {
            "method": method_df,
            "summary": pd.DataFrame(summary_rows),
            "batch_detail": pd.DataFrame(batch_detail_rows),
            "file_detail": pd.DataFrame(file_detail_rows),
        },
        drop_duplicate_value_columns=False,
    )

    return {
        "output_path": out_path,
        "sample_rows": len(sample_rows),
        "sample_files": files_used,
        "summary": summary_rows,
    }
