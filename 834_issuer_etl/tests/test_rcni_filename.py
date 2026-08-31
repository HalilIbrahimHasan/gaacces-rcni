from __future__ import annotations

from datetime import datetime

from rcni.filename import parse_rcni_filename
from rcni.matcher import (
    is_rcni_local_file,
    is_rcni_monthly_discrepancy_file,
    is_recon_input_file,
    is_rcni_sftp_archive_file,
    logical_filename,
)


class TestRcniFilenameParsing:
    def test_valid_compressed_filename(self) -> None:
        name = "to_15105_INDV_MONTHLYDISCREPANCY_2026_20260717005507.OUT.good.gz"
        meta = parse_rcni_filename(name)
        assert meta.parse_ok
        assert meta.direction == "TO"
        assert meta.issuer_id == "15105"
        assert meta.document_type == "INDV_MONTHLYDISCREPANCY"
        assert meta.plan_year == "2026"
        assert meta.file_timestamp == "20260717005507"
        assert meta.parsed_timestamp == datetime(2026, 7, 17, 0, 55, 7)
        assert meta.parsed_timestamp_display == "2026-07-17 00:55:07"
        assert meta.compressed_filename == name
        assert logical_filename(name) == name[:-3]

    def test_valid_decompressed_filename(self) -> None:
        name = "to_15105_INDV_MONTHLYDISCREPANCY_2025_20260717005653.OUT.good"
        meta = parse_rcni_filename(name)
        assert meta.parse_ok
        assert meta.issuer_id == "15105"
        assert meta.plan_year == "2025"
        assert meta.file_timestamp == "20260717005653"

    def test_directory_year_not_used_as_plan_year(self) -> None:
        name = "to_13535_INDV_MONTHLYDISCREPANCY_2025_20260127221729.OUT.good.gz"
        meta = parse_rcni_filename(name)
        assert meta.plan_year == "2025"
        assert meta.file_timestamp.startswith("2026")

    def test_wrong_document_type_rejected(self) -> None:
        name = "to_15105_INDV_SOMETHINGELSE_2026_20260717005507.OUT.good.gz"
        assert not is_rcni_monthly_discrepancy_file(name)
        assert not parse_rcni_filename(name).parse_ok

    def test_from_monthlyrecon_rejected(self) -> None:
        name = "from_15105_INDV_MONTHLYRECON_2025_20260116070119.IN.gz"
        assert is_recon_input_file(name)
        assert not is_rcni_sftp_archive_file(name)
        assert not is_rcni_monthly_discrepancy_file(name)

    def test_log_txt_gz_rejected(self) -> None:
        assert not is_rcni_sftp_archive_file("log.txt.gz")
        assert not is_rcni_monthly_discrepancy_file("log.txt.gz")
        assert not is_rcni_monthly_discrepancy_file("log.txt")

    def test_decompressed_allowed_locally_not_on_sftp(self) -> None:
        name = "to_15105_INDV_MONTHLYDISCREPANCY_2025_20260717005653.OUT.good"
        assert is_rcni_local_file(name)
        assert not is_rcni_sftp_archive_file(name)

    def test_all_good_files_are_not_accepted(self) -> None:
        assert not is_rcni_monthly_discrepancy_file("to_15105_OTHER_2026_20260717005507.OUT.good.gz")

    def test_issuer_and_timestamp_extraction(self) -> None:
        meta = parse_rcni_filename(
            "to_37301_INDV_MONTHLYDISCREPANCY_2026_20260709122334.OUT.good"
        )
        assert meta.issuer_id == "37301"
        assert meta.plan_year == "2026"
        assert meta.file_timestamp == "20260709122334"
        assert meta.parsed_timestamp == datetime(2026, 7, 9, 12, 23, 34)
