from __future__ import annotations

import gzip
from pathlib import Path

from rcni.constants import EXPECTED_COLUMN_COUNT, EXPECTED_HEADER, ISSUE_FIELD_COUNT, ISSUE_IDENTIFIER_NOT_NUMERIC, STATUS_CLEAN, STATUS_MALFORMED, STATUS_WARNING
from rcni.csv_validator import is_numeric_identifier, validate_rcni_csv
from rcni.reports import write_data_quality_warnings, write_structural_malformed
from rcni.status import overall_status
from rcni.staging import decompress_gzip_file

FIXTURES = Path(__file__).resolve().parent / "fixtures" / "rcni"
SAMPLES = Path(__file__).resolve().parents[1] / "last reports"


class TestRcniCsvValidation:
    def test_expected_19_column_header(self) -> None:
        result = validate_rcni_csv(FIXTURES / "clean_rows.csv")
        assert result.header_ok
        assert result.header_column_count == EXPECTED_COLUMN_COUNT
        assert result.header == EXPECTED_HEADER
        assert result.parsed_records == 3
        assert result.clean_records == 3
        assert result.malformed_records == 0

    def test_sample_file_header_not_modified(self) -> None:
        sample = SAMPLES / "to_15105_INDV_MONTHLYDISCREPANCY_2026_20260717005507.OUT.good"
        if not sample.is_file():
            import pytest
            pytest.skip("local July sample is not present in last reports/")
        before = sample.read_bytes()
        result = validate_rcni_csv(sample)
        after = sample.read_bytes()
        assert before == after
        assert result.header_ok
        assert result.parsed_records == 14415
        assert result.malformed_records == 0

    def test_quoted_comma_handled(self) -> None:
        result = validate_rcni_csv(FIXTURES / "clean_rows.csv")
        assert result.malformed_records == 0
        # Row 1 has quoted address fields containing no structural break.
        assert result.clean_records == 3

    def test_blank_hix_and_issuer_values_allowed(self) -> None:
        result = validate_rcni_csv(FIXTURES / "clean_rows.csv")
        assert result.malformed_records == 0
        assert result.header_ok

    def test_hix_text_vs_issuer_numeric_looking_allowed(self) -> None:
        result = validate_rcni_csv(FIXTURES / "identifier_anomaly.csv")
        hix_vs_numeric_ok = [
            i for i in result.issues if i.column_name in {"HIX Value", "Issuer Value"}
        ]
        assert hix_vs_numeric_ok == []
        assert result.malformed_records == 0

    def test_invalid_numeric_identifier_flagged_without_crash(self) -> None:
        result = validate_rcni_csv(FIXTURES / "identifier_anomaly.csv")
        id_issues = [i for i in result.issues if i.issue_type == ISSUE_IDENTIFIER_NOT_NUMERIC]
        assert len(id_issues) == 2
        assert {i.bad_value for i in id_issues} == {"HSM", "HSS"}
        assert all(i.column_name == "Exchange Assigned Member ID" for i in id_issues)
        assert result.malformed_records == 0
        assert result.parsed_records == 3

    def test_issuer_assigned_alphanumeric_id_is_accepted(self) -> None:
        result = validate_rcni_csv(FIXTURES / "identifier_anomaly.csv")
        issuer_id_issues = [
            i
            for i in result.issues
            if i.column_name in {"Issuer Assigned Member ID", "Issuer Assigned Subscriber ID"}
        ]
        assert issuer_id_issues == []
        es_row_flagged = [i for i in result.issues if i.bad_value == "ES7951835600"]
        assert es_row_flagged == []

    def test_exchange_assigned_bad_alpha_value_flagged(self) -> None:
        result = validate_rcni_csv(FIXTURES / "identifier_anomaly.csv")
        id_issues = [i for i in result.issues if i.issue_type == ISSUE_IDENTIFIER_NOT_NUMERIC]
        assert {i.bad_value for i in id_issues} == {"HSM", "HSS"}
        assert {i.column_name for i in id_issues} <= {
            "Exchange Assigned Member ID",
            "Exchange Assigned Subscriber ID",
        }
        assert result.malformed_records == 0
        assert result.structural_malformed_records == 0
        assert result.identifier_format_warnings == 2

    def test_structural_malformed_separated_from_warning(self, tmp_path: Path) -> None:
        structural = validate_rcni_csv(FIXTURES / "malformed_extra_comma.csv")
        warnings = validate_rcni_csv(FIXTURES / "identifier_anomaly.csv")
        assert structural.structural_malformed_records == 1
        assert structural.identifier_format_warnings == 0
        assert warnings.structural_malformed_records == 0
        assert warnings.identifier_format_warnings == 2
        write_structural_malformed(tmp_path, structural.issues + warnings.issues)
        write_data_quality_warnings(tmp_path, structural.issues + warnings.issues)
        malformed_text = (tmp_path / "structural_malformed.csv").read_text(encoding="utf-8")
        warning_text = (tmp_path / "data_quality_warnings.csv").read_text(encoding="utf-8")
        assert "FIELD_COUNT" in malformed_text
        assert "IDENTIFIER_NOT_NUMERIC" not in malformed_text
        assert "IDENTIFIER_NOT_NUMERIC" in warning_text
        assert "FIELD_COUNT" not in warning_text

    def test_malformed_status_driven_only_by_structural_issue(self) -> None:
        assert overall_status(["WARNING"]) == STATUS_WARNING
        assert overall_status(["CLEAN", "WARNING"]) == STATUS_WARNING
        assert overall_status(["MALFORMED", "WARNING"]) == STATUS_MALFORMED
        assert overall_status(["SCHEMA_MISMATCH"]) == STATUS_MALFORMED
        assert overall_status(["CLEAN"]) == STATUS_CLEAN
        warning_csv = validate_rcni_csv(FIXTURES / "identifier_anomaly.csv")
        assert warning_csv.malformed_records == 0
        assert overall_status(["WARNING"] if warning_csv.identifier_format_warnings else []) == STATUS_WARNING
        structural_csv = validate_rcni_csv(FIXTURES / "malformed_extra_comma.csv")
        assert structural_csv.malformed_records == 1
        assert overall_status(["MALFORMED"]) == STATUS_MALFORMED

    def test_is_numeric_identifier_does_not_strip_leading_zeros(self) -> None:
        assert is_numeric_identifier("00123")
        assert not is_numeric_identifier("HSM")

    def test_excess_column_malformed_row_detected(self) -> None:
        result = validate_rcni_csv(FIXTURES / "malformed_extra_comma.csv")
        assert result.malformed_records == 1
        assert result.parsed_records == 3
        assert result.clean_records == 2
        issue = next(i for i in result.issues if i.issue_type == ISSUE_FIELD_COUNT)
        assert issue.expected_column_count == 19
        assert issue.observed_column_count is not None
        assert issue.observed_column_count > 19
        assert issue.record_number == 2
        assert "12 MAIN ST" in issue.raw_record

    def test_raw_malformed_record_preserved(self) -> None:
        result = validate_rcni_csv(FIXTURES / "malformed_extra_comma.csv")
        issue = next(i for i in result.issues if i.issue_type == ISSUE_FIELD_COUNT)
        assert issue.raw_record
        assert "BAD" in issue.raw_record

    def test_schema_mismatch_header(self) -> None:
        result = validate_rcni_csv(FIXTURES / "wrong_header.csv")
        assert not result.header_ok
        assert result.header_mismatch_details

    def test_gzip_decompression(self, tmp_path: Path) -> None:
        src = FIXTURES / "clean_rows.csv"
        gz_path = tmp_path / "clean_rows.csv.gz"
        extracted = tmp_path / "clean_rows.csv"
        with gzip.open(gz_path, "wb") as handle:
            handle.write(src.read_bytes())
        original_gz = gz_path.read_bytes()
        decompress_gzip_file(gz_path, extracted)
        assert gz_path.read_bytes() == original_gz
        assert extracted.read_bytes() == src.read_bytes()
        result = validate_rcni_csv(extracted)
        assert result.header_ok
        assert result.parsed_records == 3

    def test_large_file_streaming_does_not_collect_all_rows(self, tmp_path: Path) -> None:
        header = ",".join(EXPECTED_HEADER)
        path = tmp_path / "large.OUT.good"
        rows = 25000
        with path.open("w", encoding="utf-8", newline="") as handle:
            handle.write(header + "\n")
            for i in range(rows):
                handle.write(
                    f"{i},15105GA002000201,LN,FN,{i},{i},SLN,SFN,{i},{i},"
                    f"1000C_AA,Agent Name,AAA,BBB,20260717,from_x.IN,N,Carrier,CONFIRM\n"
                )
        seen = {"count": 0}

        def cb(_issue) -> None:
            seen["count"] += 1

        result = validate_rcni_csv(
            path,
            issue_callback=cb,
            collect_issues=False,
        )
        assert result.parsed_records == rows
        assert result.clean_records == rows
        assert result.malformed_records == 0
        assert result.issues == []
        assert result.held_all_rows is False
        assert seen["count"] == 0
