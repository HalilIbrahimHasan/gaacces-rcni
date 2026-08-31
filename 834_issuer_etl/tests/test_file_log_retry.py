#!/usr/bin/env python3
"""Regression tests for inbound_automation file_log retry / upsert behavior."""

from __future__ import annotations

import sys
import unittest
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from connectors.base_connector import SourceFile  # noqa: E402
from inbound_automation.azure_writer import (  # noqa: E402
    _FILE_LOG_INSERT_SQL,
    _FILE_LOG_UPDATE_SQL,
    _file_log_exists,
    _write_file_log,
    upsert_file_log,
)
from inbound_automation.pipeline import FileProcessResult, run_load  # noqa: E402
from inbound_automation.run_context import LoadRunContext  # noqa: E402


class _ExecuteResult:
    def __init__(self, row):
        self._row = row

    def fetchone(self):
        return self._row


class _RecordingConn:
    def __init__(self, existing_hashes: set[str] | None = None):
        self.existing_hashes = existing_hashes or set()
        self.calls: list[tuple[object, dict | None]] = []

    def execute(self, sql, params=None):
        self.calls.append((sql, params))
        sql_text = str(sql)
        if "SELECT 1" in sql_text and params:
            fh = params["file_hash"]
            return _ExecuteResult((1,) if fh in self.existing_hashes else None)
        return _ExecuteResult(None)


class _FakeEngine:
    def __init__(self, conn: _RecordingConn):
        self._conn = conn

    @contextmanager
    def begin(self):
        yield self._conn


def _sample_result(*, status: str = "loaded", row_count: int = 1) -> FileProcessResult:
    source = SourceFile(
        issuer="68806",
        year="2026",
        month="02",
        file_name="from_68806_GA_834_INDV_2026-02-08T05344500.P.xml",
        file_path=Path("/data/68806/2026/02/sample.xml"),
        file_size=3051,
        source_type="local",
    )
    return FileProcessResult(
        source=source,
        file_hash="abc123hash",
        parse_status=status,
        row_count=row_count,
        parse_duration_ms=1,
        filename_file_year=2026,
        filename_file_month=2,
        error_message=None if status == "loaded" else "Azure insert failed",
    )


def _sample_context() -> LoadRunContext:
    return LoadRunContext(
        load_run_id="inbound_test_run",
        started_at=datetime(2026, 7, 10, tzinfo=timezone.utc),
        run_mode="load",
        source_mode="local",
        year_filter="2026",
        all_years=False,
        issuer_filter=["68806"],
        month_filter="02",
        output_dir=Path("/tmp/inbound_test"),
    )


class FileLogRetryTests(unittest.TestCase):
    def test_new_file_inserts_file_log(self) -> None:
        conn = _RecordingConn()
        params = {"file_hash": "new_hash", "parse_status": "loaded"}
        _write_file_log(conn, params, exists=False)
        self.assertEqual(len(conn.calls), 1)
        self.assertIs(conn.calls[0][0], _FILE_LOG_INSERT_SQL)

    def test_existing_failed_hash_updates_file_log(self) -> None:
        conn = _RecordingConn(existing_hashes={"retry_hash"})
        self.assertTrue(_file_log_exists(conn, "retry_hash"))
        params = {
            "file_hash": "retry_hash",
            "parse_status": "loaded",
            "error_message": None,
            "row_count": 1,
        }
        _write_file_log(conn, params, exists=True)
        sql_calls = [call[0] for call in conn.calls]
        self.assertIn(_FILE_LOG_UPDATE_SQL, sql_calls)
        self.assertNotIn(_FILE_LOG_INSERT_SQL, sql_calls)

    def test_upsert_file_log_new_file_uses_insert(self) -> None:
        conn = _RecordingConn()
        engine = _FakeEngine(conn)
        result = _sample_result()
        upsert_file_log(
            engine,
            _sample_context(),
            result,
            loaded_at=datetime(2026, 7, 10, tzinfo=timezone.utc),
        )
        self.assertEqual(len(conn.calls), 2)  # exists check + insert
        self.assertIs(conn.calls[-1][0], _FILE_LOG_INSERT_SQL)

    def test_upsert_file_log_failed_retry_updates_without_insert(self) -> None:
        conn = _RecordingConn(existing_hashes={"abc123hash"})
        engine = _FakeEngine(conn)
        result = _sample_result(status="failed", row_count=1)
        result.error_message = "Communication link failure"
        upsert_file_log(
            engine,
            _sample_context(),
            result,
            loaded_at=datetime(2026, 7, 10, tzinfo=timezone.utc),
        )
        sql_calls = [call[0] for call in conn.calls]
        self.assertIn(_FILE_LOG_UPDATE_SQL, sql_calls)
        self.assertNotIn(_FILE_LOG_INSERT_SQL, sql_calls)
        update_params = sql_calls and conn.calls[-1][1]
        self.assertEqual(update_params["parse_status"], "failed")
        self.assertIn("Communication link failure", update_params["error_message"])

    def test_loaded_file_skipped_in_pipeline(self) -> None:
        source = SourceFile(
            issuer="68806",
            year="2026",
            month="02",
            file_name="already_loaded.xml",
            file_path=Path("/data/already_loaded.xml"),
            file_size=100,
            source_type="local",
        )
        context = LoadRunContext(
            load_run_id="inbound_skip_test",
            started_at=datetime(2026, 7, 10, tzinfo=timezone.utc),
            run_mode="load",
            source_mode="local",
            year_filter="2026",
            all_years=False,
            issuer_filter=["68806"],
            month_filter="02",
            output_dir=Path("/tmp/inbound_skip_test"),
        )
        with (
            patch("inbound_automation.azure_common.require_env_gate"),
            patch("inbound_automation.pipeline.settings"),
            patch("inbound_automation.azure_common.connect_automation_engine", return_value=MagicMock()),
            patch("inbound_automation.azure_writer.verify_tables_exist"),
            patch("inbound_automation.azure_writer.insert_run_log_start"),
            patch("inbound_automation.azure_writer.update_run_log_finish"),
            patch(
                "inbound_automation.azure_writer.fetch_loaded_file_hashes",
                return_value={"loaded_hash": "prior_run"},
            ),
            patch(
                "inbound_automation.pipeline.discover_for_run",
                return_value=[source],
            ),
            patch("inbound_automation.pipeline.file_hash", return_value="loaded_hash"),
            patch("inbound_automation.pipeline._process_file") as mock_process,
            patch("inbound_automation.azure_writer.load_file_rows") as mock_load,
            patch("inbound_automation.azure_writer.upsert_file_log") as mock_upsert,
        ):
            result = run_load(context)

        mock_process.assert_not_called()
        mock_load.assert_not_called()
        mock_upsert.assert_not_called()
        self.assertEqual(len(result.file_results), 1)
        self.assertEqual(result.file_results[0].parse_status, "skipped_duplicate")

    def test_failed_file_retry_updates_to_loaded_in_load_file_rows(self) -> None:
        from inbound_automation.azure_writer import load_file_rows

        conn = _RecordingConn(existing_hashes={"abc123hash"})
        engine = _FakeEngine(conn)

        enriched_row = {
            "load_run_id": "inbound_test_run",
            "loaded_at": "2026-07-10T07:00:00+00:00",
            "folder_year": 2026,
            "folder_month": 2,
            "filename_file_year": 2026,
            "filename_file_month": 2,
            "source_file": "sample.xml",
            "source_file_path": "/data/sample.xml",
            "file_hash": "abc123hash",
            "row_number_in_file": 1,
            "raw_record_hash": "rowhash",
            "parser_version": "test",
            "runner_version": "test",
            "git_commit": None,
            "coverage_year": 2026,
            "coverage_year_source": "cli_filter",
            "warning_count": 0,
            "insurance_type": "Health",
            "enrolleeStatus": "CONFIRM",
            "issuer": "68806",
            "year": "2026",
            "month": "02",
            "file_name": "sample.xml",
            "raw_xml_path": "/data/sample.xml",
            "created_at": "2026-07-10T07:00:00+00:00",
            "policy_id": None,
            "member_id": "M1",
            "subscriber_id": "S1",
            "exchg_assigned_enrollee_id": None,
            "issuer_subscriber_identifier": "ISS1",
            "issuer_indiv_identifier": "IM1",
            "member_first_name": "A",
            "member_last_name": "B",
            "relationship": "18",
            "subscriber_flag": "Y",
            "enrollee_event_type_code": "021",
            "enrollee_event_reason_code": None,
            "action_code": "021",
            "action_code_description": "Confirmed/Effectuated",
            "maintenance_type_code": "021",
            "additional_maint_reason_code": "CONFIRM",
            "coverage_status": "Confirmed/Effectuated",
            "benefit_effective_date": "2026-02-01",
            "benefit_end_date": None,
            "member_maint_effective_date": "2026-02-01",
            "last_premium_paid_date": None,
            "request_submit_timestamp": None,
            "total_premium_amount": 100.0,
            "individual_responsibility_amount": 80.0,
            "aptc_amount": 20.0,
            "user_fee_amount": 3.25,
            "insurance_type_code": "HLT",
            "health_coverage_policy_no": "564309",
            "household_or_employee_case_id": "HH1",
            "rating_area": "R-GA001",
            "source_exchg_id": "GA0",
            "enrollment_action_code": "2",
            "insurer_tax_id_number": "123",
            "qtyn": "0",
            "qtyy": "1",
            "qtyt": "1",
            "raw_payload": "{}",
            "raw_json": "{}",
        }
        file_result = _sample_result()
        file_result.rows = [enriched_row]

        inserted, _metrics = load_file_rows(
            engine,
            _sample_context(),
            file_result,
            loaded_at=datetime(2026, 7, 10, tzinfo=timezone.utc),
        )
        self.assertEqual(inserted, 1)
        sql_calls = [call[0] for call in conn.calls]
        self.assertIn(_FILE_LOG_UPDATE_SQL, sql_calls)
        self.assertNotIn(_FILE_LOG_INSERT_SQL, sql_calls)
        update_call = next(call for call in conn.calls if call[0] is _FILE_LOG_UPDATE_SQL)
        self.assertEqual(update_call[1]["parse_status"], "loaded")
        self.assertIsNone(update_call[1]["error_message"])


if __name__ == "__main__":
    raise SystemExit(unittest.main())
