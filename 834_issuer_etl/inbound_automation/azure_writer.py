"""Azure INSERT writer for inbound automation tables only."""

from __future__ import annotations

import statistics
import time
from datetime import date, datetime
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

from inbound_automation.azure_common import INBOUND_TABLES, batch_size
from inbound_automation.load_metrics import FileInsertMetrics
from inbound_automation.pipeline import FileProcessResult
from inbound_automation.run_context import LoadRunContext
from utils.logger import get_logger

logger = get_logger(__name__)

SCHEMA = "dbo"

ALLOWED_TABLES = frozenset(INBOUND_TABLES)

INBOUND_AUTOMATION_COLUMNS = [
    "load_run_id",
    "loaded_at",
    "folder_year",
    "folder_month",
    "filename_file_year",
    "filename_file_month",
    "source_file",
    "source_file_path",
    "file_hash",
    "row_number_in_file",
    "raw_record_hash",
    "parser_version",
    "runner_version",
    "git_commit",
    "coverage_year",
    "coverage_year_source",
    "warning_count",
    "insurance_type",
    "enrolleeStatus",
    "issuer",
    "year",
    "month",
    "file_name",
    "raw_xml_path",
    "created_at",
    "policy_id",
    "member_id",
    "subscriber_id",
    "exchg_assigned_enrollee_id",
    "issuer_subscriber_identifier",
    "issuer_indiv_identifier",
    "member_first_name",
    "member_last_name",
    "relationship",
    "subscriber_flag",
    "enrollee_event_type_code",
    "enrollee_event_reason_code",
    "action_code",
    "action_code_description",
    "maintenance_type_code",
    "additional_maint_reason_code",
    "coverage_status",
    "benefit_effective_date",
    "benefit_end_date",
    "member_maint_effective_date",
    "last_premium_paid_date",
    "request_submit_timestamp",
    "total_premium_amount",
    "individual_responsibility_amount",
    "aptc_amount",
    "user_fee_amount",
    "insurance_type_code",
    "health_coverage_policy_no",
    "household_or_employee_case_id",
    "rating_area",
    "source_exchg_id",
    "enrollment_action_code",
    "insurer_tax_id_number",
    "qtyn",
    "qtyy",
    "qtyt",
    "raw_payload",
    "raw_json",
]

_DATE_COLUMNS = frozenset({
    "benefit_effective_date",
    "benefit_end_date",
    "member_maint_effective_date",
})


def _parse_date(value: Any) -> date | None:
    if value is None or value == "":
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    text_val = str(value).strip()
    if not text_val:
        return None
    try:
        return date.fromisoformat(text_val[:10])
    except ValueError:
        return None


def _parse_loaded_at(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    if value is None:
        return datetime.now().astimezone()
    text_val = str(value).strip()
    if text_val.endswith("Z"):
        text_val = text_val[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(text_val)
    except ValueError:
        return datetime.now().astimezone()


def row_to_insert_params(enriched: dict[str, Any]) -> dict[str, Any]:
    params: dict[str, Any] = {}
    for col in INBOUND_AUTOMATION_COLUMNS:
        val = enriched.get(col)
        if col == "loaded_at":
            params[col] = _parse_loaded_at(val)
        elif col in _DATE_COLUMNS:
            params[col] = _parse_date(val)
        else:
            params[col] = val
    return params


def verify_tables_exist(engine: Engine) -> None:
    missing: list[str] = []
    with engine.connect() as conn:
        for table in INBOUND_TABLES:
            row = conn.exec_driver_sql(
                f"SELECT CASE WHEN OBJECT_ID(N'{SCHEMA}.{table}', N'U') IS NULL "
                f"THEN 0 ELSE 1 END"
            ).fetchone()
            if not row or int(row[0]) != 1:
                missing.append(f"{SCHEMA}.{table}")
    if missing:
        raise RuntimeError(
            "Required inbound_automation tables are missing: "
            + ", ".join(missing)
            + ". Run DBA DDL or --create-table first."
        )


def fetch_loaded_file_hashes(engine: Engine) -> dict[str, str]:
    sql = (
        f"SELECT file_hash, load_run_id FROM [{SCHEMA}].[inbound_automation_file_log] "
        f"WHERE parse_status = 'loaded'"
    )
    with engine.connect() as conn:
        rows = conn.exec_driver_sql(sql).fetchall()
    return {str(row[0]): str(row[1]) for row in rows}


def insert_run_log_start(engine: Engine, context: LoadRunContext) -> None:
    sql = text(f"""
        INSERT INTO [{SCHEMA}].[inbound_automation_run_log] (
            load_run_id, started_at, completed_at, run_mode, source_mode,
            year_filter, issuer_filter, month_filter,
            parser_version, runner_version, git_commit,
            files_discovered, files_parsed, files_loaded, files_skipped_duplicate,
            files_failed, rows_parsed, rows_inserted, rows_skipped,
            total_warning_count, status, error_summary, report_output_path
        ) VALUES (
            :load_run_id, :started_at, NULL, :run_mode, :source_mode,
            :year_filter, :issuer_filter, :month_filter,
            :parser_version, :runner_version, :git_commit,
            0, 0, 0, 0, 0, 0, 0, 0, NULL, 'running', NULL, :report_output_path
        )
    """)
    params = {
        "load_run_id": context.load_run_id,
        "started_at": context.started_at,
        "run_mode": context.run_mode,
        "source_mode": context.source_mode,
        "year_filter": context.year_filter or ("ALL" if context.all_years else None),
        "issuer_filter": ",".join(context.issuer_filter) if context.issuer_filter else None,
        "month_filter": context.month_filter,
        "parser_version": context.parser_version,
        "runner_version": context.runner_version,
        "git_commit": context.git_commit,
        "report_output_path": str(context.output_dir),
    }
    with engine.begin() as conn:
        conn.execute(sql, params)


def update_run_log_finish(
    engine: Engine,
    context: LoadRunContext,
    *,
    completed_at: datetime,
    stats: dict[str, Any],
    status: str,
    error_summary: str | None = None,
) -> None:
    sql = text(f"""
        UPDATE [{SCHEMA}].[inbound_automation_run_log]
        SET completed_at = :completed_at,
            files_discovered = :files_discovered,
            files_parsed = :files_parsed,
            files_loaded = :files_loaded,
            files_skipped_duplicate = :files_skipped_duplicate,
            files_failed = :files_failed,
            rows_parsed = :rows_parsed,
            rows_inserted = :rows_inserted,
            rows_skipped = :rows_skipped,
            total_warning_count = :total_warning_count,
            status = :status,
            error_summary = :error_summary
        WHERE load_run_id = :load_run_id
    """)
    params = {
        "load_run_id": context.load_run_id,
        "completed_at": completed_at,
        **stats,
        "status": status,
        "error_summary": error_summary,
    }
    with engine.begin() as conn:
        conn.execute(sql, params)


_FILE_LOG_INSERT_SQL = text(f"""
    INSERT INTO [{SCHEMA}].[inbound_automation_file_log] (
        load_run_id, loaded_at, issuer, folder_year, folder_month,
        filename_file_year, filename_file_month,
        source_file, source_file_path, file_hash, file_size_bytes,
        parse_status, row_count, parse_duration_ms, error_message
    ) VALUES (
        :load_run_id, :loaded_at, :issuer, :folder_year, :folder_month,
        :filename_file_year, :filename_file_month,
        :source_file, :source_file_path, :file_hash, :file_size_bytes,
        :parse_status, :row_count, :parse_duration_ms, :error_message
    )
""")

_FILE_LOG_UPDATE_SQL = text(f"""
    UPDATE [{SCHEMA}].[inbound_automation_file_log]
    SET load_run_id = :load_run_id,
        loaded_at = :loaded_at,
        issuer = :issuer,
        folder_year = :folder_year,
        folder_month = :folder_month,
        filename_file_year = :filename_file_year,
        filename_file_month = :filename_file_month,
        source_file = :source_file,
        source_file_path = :source_file_path,
        file_size_bytes = :file_size_bytes,
        parse_status = :parse_status,
        row_count = :row_count,
        parse_duration_ms = :parse_duration_ms,
        error_message = :error_message
    WHERE file_hash = :file_hash
""")


def _file_log_params(
    context: LoadRunContext,
    result: FileProcessResult,
    *,
    loaded_at: datetime,
) -> dict[str, Any]:
    return {
        "load_run_id": context.load_run_id,
        "loaded_at": loaded_at,
        "issuer": result.source.issuer,
        "folder_year": int(result.source.year),
        "folder_month": int(result.source.month),
        "filename_file_year": result.filename_file_year,
        "filename_file_month": result.filename_file_month,
        "source_file": result.source.file_name,
        "source_file_path": str(result.source.file_path),
        "file_hash": result.file_hash,
        "file_size_bytes": result.source.file_size,
        "parse_status": result.parse_status,
        "row_count": result.row_count,
        "parse_duration_ms": result.parse_duration_ms,
        "error_message": result.error_message,
    }


def _file_log_exists(conn, file_hash: str) -> bool:
    row = conn.execute(
        text(
            f"SELECT 1 FROM [{SCHEMA}].[inbound_automation_file_log] "
            f"WHERE file_hash = :file_hash"
        ),
        {"file_hash": file_hash},
    ).fetchone()
    return row is not None


def _write_file_log(conn, params: dict[str, Any], *, exists: bool) -> None:
    """Insert or update file_log by file_hash (never duplicate INSERT)."""
    if exists:
        conn.execute(_FILE_LOG_UPDATE_SQL, params)
    else:
        conn.execute(_FILE_LOG_INSERT_SQL, params)


def upsert_file_log(
    engine: Engine,
    context: LoadRunContext,
    result: FileProcessResult,
    *,
    loaded_at: datetime,
) -> None:
    """Write file_log outside a load transaction (retry-safe upsert)."""
    params = _file_log_params(context, result, loaded_at=loaded_at)
    with engine.begin() as conn:
        exists = _file_log_exists(conn, result.file_hash)
        _write_file_log(conn, params, exists=exists)


def insert_file_log(
    engine: Engine,
    context: LoadRunContext,
    result: FileProcessResult,
    *,
    loaded_at: datetime,
) -> None:
    """Backward-compatible alias for upsert_file_log."""
    upsert_file_log(engine, context, result, loaded_at=loaded_at)


def _build_row_insert_sql() -> text:
    col_list = ", ".join(f"[{c}]" for c in INBOUND_AUTOMATION_COLUMNS)
    placeholders = ", ".join(f":{c}" for c in INBOUND_AUTOMATION_COLUMNS)
    return text(
        f"INSERT INTO [{SCHEMA}].[inbound_automation] ({col_list}) VALUES ({placeholders})"
    )


def _insert_rows_in_batches(
    conn,
    row_sql: text,
    rows: list[dict[str, Any]],
    *,
    bs: int,
) -> tuple[int, list[float]]:
    """Execute batched inserts; build params per batch to limit peak memory."""
    batch_durations: list[float] = []
    inserted = 0
    for start in range(0, len(rows), bs):
        batch_rows = rows[start : start + bs]
        batch = [row_to_insert_params(row) for row in batch_rows]
        t0 = time.perf_counter()
        conn.execute(row_sql, batch)
        batch_durations.append(time.perf_counter() - t0)
        inserted += len(batch)
    return inserted, batch_durations


def insert_rows_batch(engine: Engine, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    row_sql = _build_row_insert_sql()
    bs = batch_size()
    inserted = 0
    with engine.begin() as conn:
        inserted, _ = _insert_rows_in_batches(conn, row_sql, rows, bs=bs)
    return inserted


def load_file_rows(
    engine: Engine,
    context: LoadRunContext,
    result: FileProcessResult,
    *,
    loaded_at: datetime,
    fast_executemany: bool = True,
) -> tuple[int, FileInsertMetrics]:
    """Insert all rows for one file plus file_log entry in one transaction."""
    bs = batch_size()
    metrics = FileInsertMetrics(batch_size=bs, fast_executemany=fast_executemany)
    t_load_start = time.perf_counter()

    if not result.rows:
        result.parse_status = "loaded"
        result.row_count = 0
        result.error_message = None
        file_params = _file_log_params(context, result, loaded_at=loaded_at)
        with engine.begin() as conn:
            exists = _file_log_exists(conn, result.file_hash)
            _write_file_log(conn, file_params, exists=exists)
        metrics.load_duration_ms = int((time.perf_counter() - t_load_start) * 1000)
        return 0, metrics

    row_sql = _build_row_insert_sql()
    row_count = len(result.rows)
    result.parse_status = "loaded"
    result.row_count = row_count
    result.error_message = None
    file_params = _file_log_params(context, result, loaded_at=loaded_at)

    batch_durations: list[float] = []
    file_log_duration_sec = 0.0
    with engine.begin() as conn:
        _, batch_durations = _insert_rows_in_batches(
            conn, row_sql, result.rows, bs=bs,
        )
        t_file_log = time.perf_counter()
        exists = _file_log_exists(conn, result.file_hash)
        _write_file_log(conn, file_params, exists=exists)
        file_log_duration_sec = time.perf_counter() - t_file_log

    load_duration_sec = time.perf_counter() - t_load_start
    insert_sql_sec = sum(batch_durations)
    commit_duration_sec = max(0.0, load_duration_sec - insert_sql_sec - file_log_duration_sec)

    metrics.row_count = row_count
    metrics.batch_count = len(batch_durations)
    metrics.insert_sql_duration_ms = int(insert_sql_sec * 1000)
    metrics.file_log_duration_ms = int(file_log_duration_sec * 1000)
    metrics.commit_duration_ms = int(commit_duration_sec * 1000)
    metrics.load_duration_ms = int(load_duration_sec * 1000)
    if batch_durations:
        metrics.avg_batch_duration_ms = round(
            statistics.mean(batch_durations) * 1000, 2,
        )
        metrics.max_batch_duration_ms = round(max(batch_durations) * 1000, 2)
        metrics.min_batch_duration_ms = round(min(batch_durations) * 1000, 2)
    if insert_sql_sec > 0:
        metrics.rows_per_sec = round(row_count / insert_sql_sec, 2)

    logger.info(
        "Loaded %s: %d rows in %d batches (batch_size=%d) — "
        "%.0f rows/sec SQL, load=%dms (sql=%dms commit=%dms)",
        result.source.file_name,
        row_count,
        metrics.batch_count,
        bs,
        metrics.rows_per_sec,
        metrics.load_duration_ms,
        metrics.insert_sql_duration_ms,
        metrics.commit_duration_ms,
    )
    return row_count, metrics
