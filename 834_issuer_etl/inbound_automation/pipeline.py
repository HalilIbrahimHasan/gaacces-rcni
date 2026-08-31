"""Inbound automation pipeline — discover, parse, enrich, optional Azure load."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from connectors.base_connector import SourceFile
from config.config import settings
from database.loaders import file_hash
from inbound_automation.discovery import discover_for_run
from inbound_automation.enrich import collect_file_warnings, enrich_parser_row
from inbound_automation.filename_utils import parse_filename_year_month
from inbound_automation.load_metrics import FileInsertMetrics
from inbound_automation.run_context import LoadRunContext
from ingestion.xml_reader import read_xml_bytes
from parsers.parser_834 import Parser834
from utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class FileProcessResult:
    source: SourceFile
    file_hash: str
    parse_status: str
    row_count: int = 0
    parse_duration_ms: int | None = None
    filename_file_year: int | None = None
    filename_file_month: int | None = None
    error_message: str | None = None
    prior_load_run_id: str | None = None
    warnings: list[str] = field(default_factory=list)
    rows: list[dict[str, Any]] = field(default_factory=list)
    insert_metrics: FileInsertMetrics | None = None


@dataclass
class RunResult:
    context: LoadRunContext
    file_results: list[FileProcessResult]
    enriched_rows: list[dict[str, Any]]
    completed_at: datetime
    rows_inserted: int = 0

    @property
    def files_discovered(self) -> int:
        return len(self.file_results)

    @property
    def files_parsed(self) -> int:
        return sum(
            1 for f in self.file_results
            if f.parse_status in ("parsed", "loaded")
        )

    @property
    def files_loaded(self) -> int:
        return sum(1 for f in self.file_results if f.parse_status == "loaded")

    @property
    def files_skipped_duplicate(self) -> int:
        return sum(1 for f in self.file_results if f.parse_status == "skipped_duplicate")

    @property
    def files_failed(self) -> int:
        return sum(1 for f in self.file_results if f.parse_status == "failed")

    @property
    def rows_parsed(self) -> int:
        return len(self.enriched_rows)

    @property
    def total_warning_count(self) -> int:
        return sum(int(r.get("warning_count") or 0) for r in self.enriched_rows)


# Backward-compatible alias used by reports module.
DryRunResult = RunResult


def _process_file(
    source: SourceFile,
    *,
    parser: Parser834,
    context: LoadRunContext,
    cli_year: str | None,
    loaded_at: datetime,
) -> FileProcessResult:
    fhash = file_hash(source.file_path)
    fn_year, fn_month = parse_filename_year_month(source.file_name)
    warnings = collect_file_warnings(source, fn_year, fn_month)

    started = time.perf_counter()
    try:
        xml_bytes = read_xml_bytes(source)
        records = parser.parse_file(
            xml_bytes,
            issuer=source.issuer,
            year=source.year,
            month=source.month,
            file_name=source.file_name,
            file_path=str(source.file_path),
        )
        enriched: list[dict[str, Any]] = []
        for idx, parser_row in enumerate(records, start=1):
            enriched.append(
                enrich_parser_row(
                    parser_row,
                    source=source,
                    file_hash=fhash,
                    row_number_in_file=idx,
                    load_run_id=context.load_run_id,
                    loaded_at=loaded_at,
                    cli_year=cli_year,
                    filename_file_year=fn_year,
                    filename_file_month=fn_month,
                    parser_version=context.parser_version,
                    runner_version=context.runner_version,
                    git_commit=context.git_commit,
                    file_warnings=warnings,
                )
            )
        duration_ms = int((time.perf_counter() - started) * 1000)
        return FileProcessResult(
            source=source,
            file_hash=fhash,
            parse_status="parsed",
            row_count=len(enriched),
            parse_duration_ms=duration_ms,
            filename_file_year=fn_year,
            filename_file_month=fn_month,
            warnings=warnings,
            rows=enriched,
        )
    except Exception as exc:
        duration_ms = int((time.perf_counter() - started) * 1000)
        msg = f"{source.file_path}: {exc}"
        logger.error("Parse failed: %s", msg)
        return FileProcessResult(
            source=source,
            file_hash=fhash,
            parse_status="failed",
            parse_duration_ms=duration_ms,
            filename_file_year=fn_year,
            filename_file_month=fn_month,
            error_message=msg,
            warnings=warnings,
        )


def run_dry_run(context: LoadRunContext) -> RunResult:
    """
    Execute Phase 1 dry-run: discover, parse, enrich.

    No Azure connection, no DDL, no database writes.
    """
    settings.refresh_from_env()
    source_root = settings.source_data_path
    cli_year = None if context.all_years else context.year_filter

    sources = discover_for_run(
        source_root,
        year_filter=context.year_filter,
        all_years=context.all_years,
        issuer_filter=context.issuer_filter,
        month_filter=context.month_filter,
    )

    parser = Parser834()
    loaded_at = datetime.now(timezone.utc)
    file_results: list[FileProcessResult] = []
    enriched_rows: list[dict[str, Any]] = []

    for source in sources:
        result = _process_file(
            source,
            parser=parser,
            context=context,
            cli_year=cli_year,
            loaded_at=loaded_at,
        )
        file_results.append(result)
        enriched_rows.extend(result.rows)

    return RunResult(
        context=context,
        file_results=file_results,
        enriched_rows=enriched_rows,
        completed_at=datetime.now(timezone.utc),
    )


def run_load(context: LoadRunContext) -> RunResult:
    """
    Execute Phase 2B load: discover, parse, enrich, insert to Azure.

    Writes only to inbound_automation, inbound_automation_run_log,
    and inbound_automation_file_log.
    """
    from inbound_automation.azure_common import (
        connect_automation_engine,
        fast_executemany_enabled,
        require_env_gate,
    )
    from inbound_automation.azure_writer import (
        fetch_loaded_file_hashes,
        insert_run_log_start,
        load_file_rows,
        update_run_log_finish,
        upsert_file_log,
        verify_tables_exist,
    )

    require_env_gate("load")
    settings.refresh_from_env()

    engine = connect_automation_engine()
    use_fast_executemany = fast_executemany_enabled()
    verify_tables_exist(engine)

    loaded_hashes = fetch_loaded_file_hashes(engine)
    logger.info("Found %d previously loaded file_hash value(s) in file_log", len(loaded_hashes))

    insert_run_log_start(engine, context)

    source_root = settings.source_data_path
    cli_year = None if context.all_years else context.year_filter
    sources = discover_for_run(
        source_root,
        year_filter=context.year_filter,
        all_years=context.all_years,
        issuer_filter=context.issuer_filter,
        month_filter=context.month_filter,
    )

    parser = Parser834()
    loaded_at = datetime.now(timezone.utc)
    file_results: list[FileProcessResult] = []
    enriched_rows: list[dict[str, Any]] = []
    rows_inserted = 0
    run_error: str | None = None

    try:
        for source in sources:
            fhash = file_hash(source.file_path)
            if fhash in loaded_hashes:
                file_results.append(
                    FileProcessResult(
                        source=source,
                        file_hash=fhash,
                        parse_status="skipped_duplicate",
                        prior_load_run_id=loaded_hashes[fhash],
                    )
                )
                continue

            result = _process_file(
                source,
                parser=parser,
                context=context,
                cli_year=cli_year,
                loaded_at=loaded_at,
            )

            if result.parse_status == "parsed" and result.rows:
                try:
                    inserted, insert_metrics = load_file_rows(
                        engine, context, result, loaded_at=loaded_at,
                        fast_executemany=use_fast_executemany,
                    )
                    result.insert_metrics = insert_metrics
                    rows_inserted += inserted
                    enriched_rows.extend(result.rows)
                    loaded_hashes[fhash] = context.load_run_id
                except Exception as exc:
                    result.parse_status = "failed"
                    result.error_message = f"{source.file_path}: Azure insert failed: {exc}"
                    logger.error("Azure insert failed for %s: %s", source.file_name, exc)
                    try:
                        upsert_file_log(engine, context, result, loaded_at=loaded_at)
                    except Exception as log_exc:
                        logger.error("Could not write file_log for %s: %s", source.file_name, log_exc)
            elif result.parse_status == "parsed":
                result.parse_status = "loaded"
                try:
                    upsert_file_log(engine, context, result, loaded_at=loaded_at)
                except Exception as log_exc:
                    logger.error("Could not write file_log for %s: %s", source.file_name, log_exc)
            elif result.parse_status == "failed":
                try:
                    upsert_file_log(engine, context, result, loaded_at=loaded_at)
                except Exception as log_exc:
                    logger.error("Could not write file_log for %s: %s", source.file_name, log_exc)

            file_results.append(result)

    except Exception as exc:
        run_error = str(exc)
        logger.exception("Load run failed: %s", exc)
    finally:
        completed_at = datetime.now(timezone.utc)
        status = "failed" if run_error else "success"
        stats = {
            "files_discovered": len(file_results),
            "files_parsed": sum(
                1 for f in file_results if f.parse_status != "skipped_duplicate"
            ),
            "files_loaded": sum(1 for f in file_results if f.parse_status == "loaded"),
            "files_skipped_duplicate": sum(
                1 for f in file_results if f.parse_status == "skipped_duplicate"
            ),
            "files_failed": sum(1 for f in file_results if f.parse_status == "failed"),
            "rows_parsed": len(enriched_rows),
            "rows_inserted": rows_inserted,
            "rows_skipped": sum(
                r.row_count for r in file_results if r.parse_status == "skipped_duplicate"
            ),
            "total_warning_count": sum(
                int(r.get("warning_count") or 0) for r in enriched_rows
            ),
        }
        try:
            update_run_log_finish(
                engine,
                context,
                completed_at=completed_at,
                stats=stats,
                status=status,
                error_summary=run_error,
            )
        except Exception as log_exc:
            logger.error("Could not update run_log: %s", log_exc)

    return RunResult(
        context=context,
        file_results=file_results,
        enriched_rows=enriched_rows,
        completed_at=datetime.now(timezone.utc),
        rows_inserted=rows_inserted,
    )
