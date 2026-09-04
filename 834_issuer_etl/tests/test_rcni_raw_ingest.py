from __future__ import annotations

import gzip
from pathlib import Path
from uuid import uuid4

import pytest

from rcni.constants import (
    DQ_IDENTIFIER_FORMAT_WARNING,
    DQ_SCHEMA_DRIFT,
    DQ_UNQUOTED_COMMA,
    EXPECTED_HEADER,
    FILE_DISPOSITION_DUPLICATE,
    FILE_DISPOSITION_NEW,
    FILE_DISPOSITION_POSSIBLE_REPLACEMENT,
    FILE_STATUS_FAILED,
    FILE_STATUS_SKIPPED_DUPLICATE,
    FILE_STATUS_SUCCESS,
    QUALITY_STATUS_WARNING,
)
from rcni.raw_loader import process_local_file
from rcni.raw_parse import FileLineage, HeaderDecision, ParseCounters, stream_rcni_file
from rcni.raw_schema import header_mapping
from rcni.raw_store import MemoryRcniStore

FIXTURES = Path(__file__).resolve().parent / "fixtures" / "rcni"
SAMPLE_MAY = (
    Path(__file__).resolve().parents[1]
    / "last reports"
    / "15105"
    / "2026"
    / "05"
    / "16"
    / "3066767_888586925866"
    / "to_15105_INDV_MONTHLYDISCREPANCY_2026_20260517000035.OUT.good"
)

RCNI_NAME = "to_15105_INDV_MONTHLYDISCREPANCY_2026_20260717005507.OUT.good"


def _as_rcni(tmp_path: Path, src: Path, name: str = RCNI_NAME) -> Path:
    dest = tmp_path / name
    dest.write_bytes(src.read_bytes())
    return dest


def _lineage(path: Path, file_hash: str = "ab" * 32) -> FileLineage:
    return FileLineage(
        load_run_id="run-1",
        file_hash=file_hash,
        issuer_id="15105",
        coverage_year=2026,
        processing_year=2026,
        processing_month=7,
        processing_day=17,
        file_timestamp=None,
        source_file=path.name,
        source_path=str(path),
    )


class TestHeaderMapping:
    def test_expected_header_is_safe(self) -> None:
        index, safe, reason = header_mapping(EXPECTED_HEADER)
        assert safe
        assert reason is None
        assert index["HIX Value"] == 12
        assert index["Enrollment Status"] == 18

    def test_reordered_names_are_safe(self) -> None:
        reordered = EXPECTED_HEADER[1:] + EXPECTED_HEADER[:1]
        index, safe, reason = header_mapping(reordered)
        assert safe
        assert index["Exchange Assigned Policy ID"] == 18
        assert index["Plan ID"] == 0

    def test_wrong_names_are_unsafe(self) -> None:
        index, safe, reason = header_mapping(("Wrong Header A", "Wrong Header B"))
        assert index == {}
        assert not safe
        assert reason is not None


class TestStreamParse:
    def test_clean_rows_stage_three(self, tmp_path: Path) -> None:
        path = _as_rcni(tmp_path, FIXTURES / "clean_rows.csv")
        events = list(stream_rcni_file(path, _lineage(path)))
        header = events[0]
        counters = events[-1]
        rows = [e for e in events[1:-1] if isinstance(e, dict)]
        assert isinstance(header, HeaderDecision)
        assert header.mapping_safe
        assert isinstance(counters, ParseCounters)
        assert counters.source_records == 3
        assert counters.staged_records == 3
        assert counters.structural_malformed == 0
        first = rows[0]
        assert first["exchange_assigned_policy_id"] == "210549158"
        assert first["exchange_assigned_member_id"] == "1000650726"
        assert first["issuer_assigned_member_id"] == "798570012"
        assert first["discrepancy_reason_code"] == "2100C_AA"
        assert first["hix_value"] == "34 IDUS LANE"
        assert first["issuer_value"] == "186 CAIRNBURGH RD"
        assert first["date_of_discrepancy"] == "20260717"
        assert first["recon_file_name"].startswith("from_15105_INDV_MONTHLYRECON_")
        assert first["enrollment_status"] == "TERM"

    def test_gzip_matches_plain(self, tmp_path: Path) -> None:
        plain = _as_rcni(tmp_path, FIXTURES / "clean_rows.csv")
        gz_path = tmp_path / (RCNI_NAME + ".gz")
        with gzip.open(gz_path, "wb") as handle:
            handle.write(plain.read_bytes())
        plain_events = [e for e in stream_rcni_file(plain, _lineage(plain)) if isinstance(e, dict)]
        gz_events = [e for e in stream_rcni_file(gz_path, _lineage(gz_path)) if isinstance(e, dict)]
        assert [r["exchange_assigned_policy_id"] for r in plain_events] == [
            r["exchange_assigned_policy_id"] for r in gz_events
        ]
        assert [r["row_number_in_file"] for r in plain_events] == [
            r["row_number_in_file"] for r in gz_events
        ]
        assert [r["row_number_in_file"] for r in plain_events] == [1, 2, 3]

    def test_extra_comma_is_structural_not_staged(self, tmp_path: Path) -> None:
        path = _as_rcni(tmp_path, FIXTURES / "malformed_extra_comma.csv")
        events = list(stream_rcni_file(path, _lineage(path)))
        counters = events[-1]
        rows = [e for e in events[1:-1] if isinstance(e, dict)]
        issues = [e for e in events[1:-1] if not isinstance(e, dict)]
        assert counters.source_records == 3
        assert counters.staged_records == 2
        assert counters.structural_malformed == 1
        assert len(rows) == 2
        assert issues[0].issue_code == DQ_UNQUOTED_COMMA
        assert issues[0].raw_record is not None
        assert "UNIT 4" in (issues[0].raw_record or "")

    def test_schema_drift_quarantines_without_rows(self, tmp_path: Path) -> None:
        path = _as_rcni(tmp_path, FIXTURES / "wrong_header.csv")
        events = list(stream_rcni_file(path, _lineage(path)))
        header = events[0]
        counters = events[-1]
        assert isinstance(header, HeaderDecision)
        assert not header.mapping_safe
        assert header.issues[0].issue_code == DQ_SCHEMA_DRIFT
        assert counters.source_records == 0
        assert counters.staged_records == 0


class TestMemoryLoad:
    def test_clean_file_promotes_all_rows(self, tmp_path: Path) -> None:
        path = _as_rcni(tmp_path, FIXTURES / "clean_rows.csv")
        store = MemoryRcniStore()
        result = process_local_file(
            store, path, load_run_id=uuid4(), batch_size=2,
            processing_year=2026, processing_month=7, processing_day=17,
        )
        assert result.processing_status == FILE_STATUS_SUCCESS
        assert result.file_disposition == FILE_DISPOSITION_NEW
        assert result.rows_read == 3
        assert result.rows_loaded == 3
        assert result.rows_flagged == 0
        assert result.rows_rejected == 0
        assert result.metrics.batch_count == 2
        assert result.metrics.batch_timings[0].batch_number == 1
        assert result.metrics.batch_timings[0].rows_in_batch == 2
        assert result.metrics.batch_timings[1].rows_in_batch == 1
        assert result.metrics.total_file_duration_ms >= 0
        assert [r["row_number_in_file"] for r in store.raw] == [1, 2, 3]
        assert len(store.raw) == 3
        assert store.stage == []
        assert store.file_log[-1]["processing_status"] == FILE_STATUS_SUCCESS
        raw = store.raw[0]
        assert raw["exchange_assigned_policy_id"] == "210549158"
        assert raw["exchange_assigned_member_id"] == "1000650726"
        assert raw["issuer_assigned_member_id"] == "798570012"
        assert raw["discrepancy_reason_code"] == "2100C_AA"
        assert raw["hix_value"] == "34 IDUS LANE"
        assert raw["issuer_value"] == "186 CAIRNBURGH RD"
        assert raw["date_of_discrepancy"] == "20260717"
        assert raw["enrollment_status"] == "TERM"
        assert isinstance(raw["exchange_assigned_policy_id"], str)

    def test_malformed_rows_do_not_fail_file(self, tmp_path: Path) -> None:
        path = _as_rcni(tmp_path, FIXTURES / "malformed_extra_comma.csv")
        store = MemoryRcniStore()
        result = process_local_file(store, path, load_run_id=uuid4(), batch_size=50)
        assert result.processing_status == FILE_STATUS_SUCCESS
        assert result.file_disposition == FILE_DISPOSITION_NEW
        assert result.rows_read == 3
        assert result.rows_loaded == 2
        assert result.rows_rejected == 1
        assert len(store.raw) == 2
        assert {r["row_number_in_file"] for r in store.raw} == {1, 3}
        assert store.quality[0]["row_number_in_file"] == 2
        assert len(store.quality) == 1
        assert store.quality[0]["issue_code"] == DQ_UNQUOTED_COMMA
        assert store.quality[0]["raw_record"]
        assert {r["exchange_assigned_policy_id"] for r in store.raw} == {
            "210000010",
            "210000012",
        }

    def test_identifier_warning_still_loads(self, tmp_path: Path) -> None:
        path = _as_rcni(tmp_path, FIXTURES / "identifier_anomaly.csv")
        store = MemoryRcniStore()
        result = process_local_file(store, path, load_run_id=uuid4())
        assert result.processing_status == FILE_STATUS_SUCCESS
        assert result.rows_loaded == 3
        assert result.rows_rejected == 0
        assert all(r["quality_status"] in {"CLEAN", "WARNING"} for r in store.raw)
        warned = [r for r in store.raw if r["quality_status"] == QUALITY_STATUS_WARNING]
        assert len(warned) == 2
        codes = {q["issue_code"] for q in store.quality}
        assert codes == {DQ_IDENTIFIER_FORMAT_WARNING}

    def test_wrong_header_fails_file(self, tmp_path: Path) -> None:
        path = _as_rcni(tmp_path, FIXTURES / "wrong_header.csv")
        store = MemoryRcniStore()
        result = process_local_file(store, path, load_run_id=uuid4())
        assert result.processing_status == FILE_STATUS_FAILED
        assert result.rows_loaded == 0
        assert store.raw == []
        assert store.stage == []
        assert store.quality[0]["issue_code"] == DQ_SCHEMA_DRIFT

    def test_reordered_header_maps_by_name(self, tmp_path: Path) -> None:
        src = (FIXTURES / "clean_rows.csv").read_text(encoding="utf-8")
        lines = src.splitlines()
        cols = lines[0].split(",")
        # Move Enrollment Status to front; names still match the contract.
        cols = [cols[-1]] + cols[:-1]
        swapped_header = ",".join(cols)
        body_rows = []
        import csv
        import io
        reader = csv.reader(io.StringIO("\n".join(lines)))
        header = next(reader)
        for row in reader:
            body_rows.append([row[-1]] + row[:-1])
        dest = tmp_path / RCNI_NAME
        with dest.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(cols)
            writer.writerows(body_rows)
        store = MemoryRcniStore()
        result = process_local_file(store, dest, load_run_id=uuid4())
        assert result.processing_status == FILE_STATUS_SUCCESS
        assert result.rows_loaded == 3
        assert store.raw[0]["enrollment_status"] == "TERM"
        assert store.raw[0]["exchange_assigned_policy_id"] == "210549158"
        assert swapped_header.startswith("Enrollment Status")

    def test_duplicate_hash_is_skipped(self, tmp_path: Path) -> None:
        path = _as_rcni(tmp_path, FIXTURES / "clean_rows.csv")
        store = MemoryRcniStore()
        run_a = uuid4()
        run_b = uuid4()
        first = process_local_file(store, path, load_run_id=run_a)
        second = process_local_file(store, path, load_run_id=run_b)
        assert first.processing_status == FILE_STATUS_SUCCESS
        assert first.file_disposition == FILE_DISPOSITION_NEW
        assert second.processing_status == FILE_STATUS_SKIPPED_DUPLICATE
        assert second.file_disposition == FILE_DISPOSITION_DUPLICATE
        assert len(store.raw) == 3
        assert len(store.file_log) == 2
        assert store.file_log[-1]["processing_status"] == FILE_STATUS_SKIPPED_DUPLICATE
        assert store.file_log[-1]["file_disposition"] == FILE_DISPOSITION_DUPLICATE

    def test_possible_replacement_preserves_both(self, tmp_path: Path) -> None:
        first_path = _as_rcni(tmp_path, FIXTURES / "clean_rows.csv")
        store = MemoryRcniStore()
        first = process_local_file(store, first_path, load_run_id=uuid4())
        assert first.processing_status == FILE_STATUS_SUCCESS
        assert first.file_disposition == FILE_DISPOSITION_NEW
        second_dir = tmp_path / "replacement"
        second_dir.mkdir()
        second_path = _as_rcni(second_dir, FIXTURES / "identifier_anomaly.csv")
        second = process_local_file(store, second_path, load_run_id=uuid4())
        assert second.processing_status == FILE_STATUS_SUCCESS
        assert second.file_disposition == FILE_DISPOSITION_POSSIBLE_REPLACEMENT
        assert first.file_hash != second.file_hash
        assert len(store.raw) == 6
        hashes = {row["file_hash"] for row in store.raw}
        assert hashes == {first.file_hash, second.file_hash}
        success_rows = [r for r in store.file_log if r["processing_status"] == FILE_STATUS_SUCCESS]
        assert len(success_rows) == 2

    def test_promote_failure_rolls_back(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        path = _as_rcni(tmp_path, FIXTURES / "clean_rows.csv")
        store = MemoryRcniStore()

        def boom(*_args, **_kwargs):
            raise RuntimeError("simulated promote failure")

        monkeypatch.setattr(store, "promote_stage_to_raw", boom)
        result = process_local_file(store, path, load_run_id=uuid4())
        assert result.processing_status == FILE_STATUS_FAILED
        assert store.raw == []
        assert len(store.stage) == 3
        assert store.file_log[-1]["processing_status"] == FILE_STATUS_FAILED

    def test_promote_failure_keeps_quality_issues(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        path = _as_rcni(tmp_path, FIXTURES / "malformed_extra_comma.csv")
        store = MemoryRcniStore()

        def boom(*_args, **_kwargs):
            raise RuntimeError("simulated promote failure")

        monkeypatch.setattr(store, "promote_stage_to_raw", boom)
        result = process_local_file(store, path, load_run_id=uuid4())
        assert result.processing_status == FILE_STATUS_FAILED
        assert store.raw == []
        assert len(store.stage) == 2
        assert len(store.quality) == 1
        assert store.quality[0]["issue_code"] == DQ_UNQUOTED_COMMA
        assert store.quality[0]["raw_record"]

    def test_retry_clears_stale_stage_and_skips_duplicate_quality(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        path = _as_rcni(tmp_path, FIXTURES / "malformed_extra_comma.csv")
        store = MemoryRcniStore()
        original_promote = store.promote_stage_to_raw

        def boom(*_args, **_kwargs):
            raise RuntimeError("simulated promote failure")

        monkeypatch.setattr(store, "promote_stage_to_raw", boom)
        first = process_local_file(store, path, load_run_id=uuid4())
        assert first.processing_status == FILE_STATUS_FAILED
        assert len(store.stage) == 2
        assert len(store.quality) == 1

        monkeypatch.setattr(store, "promote_stage_to_raw", original_promote)
        second = process_local_file(store, path, load_run_id=uuid4())
        assert second.processing_status == FILE_STATUS_SUCCESS
        assert second.rows_loaded == 2
        assert store.stage == []
        assert len(store.raw) == 2
        assert len(store.quality) == 1

    def test_gzip_load_success(self, tmp_path: Path) -> None:
        gz_path = tmp_path / (RCNI_NAME + ".gz")
        with gzip.open(gz_path, "wb") as handle:
            handle.write((FIXTURES / "clean_rows.csv").read_bytes())
        store = MemoryRcniStore()
        result = process_local_file(store, gz_path, load_run_id=uuid4())
        assert result.processing_status == FILE_STATUS_SUCCESS
        assert result.rows_loaded == 3
        assert store.file_log[0]["compression_type"] == "gzip"


class TestFirstControlledFileParse:
    def test_may_15105_file_row_count(self) -> None:
        if not SAMPLE_MAY.is_file():
            pytest.skip("first controlled May 15105 file is not present locally")
        lineage = _lineage(SAMPLE_MAY, file_hash="cd" * 32)
        counters = None
        first_row = None
        for event in stream_rcni_file(SAMPLE_MAY, lineage):
            if isinstance(event, dict) and first_row is None:
                first_row = event
            if isinstance(event, ParseCounters):
                counters = event
        assert counters is not None
        assert counters.mapping_safe
        assert counters.source_records == 14268
        assert counters.staged_records == 14268
        assert counters.structural_malformed == 0
        assert first_row is not None
        for key in (
            "exchange_assigned_policy_id",
            "exchange_assigned_member_id",
            "issuer_assigned_member_id",
            "discrepancy_reason_code",
            "hix_value",
            "issuer_value",
            "date_of_discrepancy",
            "recon_file_name",
            "enrollment_status",
        ):
            assert key in first_row
            assert first_row[key] is None or isinstance(first_row[key], str)
