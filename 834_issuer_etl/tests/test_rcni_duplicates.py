from __future__ import annotations

from rcni.duplicates import IdentityRecord, detect_duplicates
from rcni.filename import parse_rcni_filename


def _record(name: str, digest: str, path: str) -> IdentityRecord:
    meta = parse_rcni_filename(name)
    assert meta.logical_identity is not None
    issuer_id, document_type, plan_year, file_timestamp = meta.logical_identity
    return IdentityRecord(
        issuer_id=issuer_id,
        document_type=document_type,
        plan_year=plan_year,
        file_timestamp=file_timestamp,
        content_hash=digest,
        source_path=path,
        source_file=name,
    )


class TestRcniDuplicates:
    def test_logical_identity_from_parsed_metadata(self) -> None:
        meta = parse_rcni_filename(
            "to_15105_INDV_MONTHLYDISCREPANCY_2026_20260717005507.OUT.good.gz"
        )
        assert meta.logical_identity == (
            "15105",
            "INDV_MONTHLYDISCREPANCY",
            "2026",
            "20260717005507",
        )

    def test_same_hash_duplicate_recognized(self) -> None:
        name = "to_15105_INDV_MONTHLYDISCREPANCY_2026_20260717005507.OUT.good.gz"
        records = [
            _record(name, "abc123", "/archive/out/good/PAS/a/file.gz"),
            _record(name, "abc123", "/archive/out/good/PAS/b/file.gz"),
        ]
        issues = detect_duplicates(records)
        assert len(issues) == 2
        assert all("DUPLICATE" in i.issue_description for group in issues.values() for i in group)

    def test_same_logical_identity_different_hash_flagged(self) -> None:
        name = "to_15105_INDV_MONTHLYDISCREPANCY_2026_20260717005507.OUT.good.gz"
        records = [
            _record(name, "hash-one", "/path/one.gz"),
            _record(name, "hash-two", "/path/two.gz"),
        ]
        issues = detect_duplicates(records)
        assert len(issues) == 2
        assert all(
            "POSSIBLE_REPLACEMENT" in i.issue_description for group in issues.values() for i in group
        )
        assert set(issues) == {"/path/one.gz", "/path/two.gz"}

    def test_distinct_identities_not_flagged(self) -> None:
        records = [
            _record(
                "to_15105_INDV_MONTHLYDISCREPANCY_2026_20260717005507.OUT.good.gz",
                "aaa",
                "/a",
            ),
            _record(
                "to_15105_INDV_MONTHLYDISCREPANCY_2025_20260717005653.OUT.good.gz",
                "aaa",
                "/b",
            ),
        ]
        assert detect_duplicates(records) == {}
