"""Duplicate / replacement detection by canonical RCNI logical identity + hash."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass

from rcni.constants import (
    ISSUE_DUPLICATE_COPY,
    ISSUE_POSSIBLE_REPLACEMENT,
    STATUS_DUPLICATE,
    STATUS_POSSIBLE_REPLACEMENT,
)
from rcni.csv_validator import RowIssue


@dataclass
class IdentityRecord:
    """One discovered file occurrence."""

    issuer_id: str
    document_type: str
    plan_year: str
    file_timestamp: str
    content_hash: str
    source_path: str
    source_file: str

    @property
    def logical_identity(self) -> tuple[str, str, str, str]:
        return (self.issuer_id, self.document_type, self.plan_year, self.file_timestamp)

    @property
    def logical_identity_key(self) -> str:
        return "|".join(self.logical_identity)


def detect_duplicates(records: list[IdentityRecord]) -> dict[str, list[RowIssue]]:
    """
    CASE 1: same logical identity + same SHA-256 → DUPLICATE copy
    CASE 2: same logical identity + different SHA-256 → POSSIBLE_REPLACEMENT

    Logical identity = (issuer_id, document_type, plan_year, file_timestamp).
    Does not discard either file.
    """
    by_identity: dict[tuple[str, str, str, str], list[IdentityRecord]] = defaultdict(list)
    for record in records:
        if not record.content_hash or not all(record.logical_identity):
            continue
        by_identity[record.logical_identity].append(record)

    issues_by_path: dict[str, list[RowIssue]] = defaultdict(list)
    for identity, group in by_identity.items():
        if len(group) < 2:
            continue
        hashes = {item.content_hash for item in group}
        ident_label = "|".join(identity)
        if len(hashes) == 1:
            issue_type = ISSUE_DUPLICATE_COPY
            description = (
                f"Duplicate copy of {ident_label}: {len(group)} occurrences share "
                f"the same SHA-256 {group[0].content_hash[:16]}…"
            )
            status_note = STATUS_DUPLICATE
        else:
            issue_type = ISSUE_POSSIBLE_REPLACEMENT
            hash_list = ", ".join(sorted(h[:16] + "…" for h in hashes))
            description = (
                f"Possible replacement of {ident_label}: {len(group)} occurrences "
                f"with different SHA-256 hashes ({hash_list}). Both preserved."
            )
            status_note = STATUS_POSSIBLE_REPLACEMENT

        for item in group:
            issues_by_path[item.source_path].append(
                RowIssue(
                    source_file=item.source_file,
                    source_path=item.source_path,
                    record_number=None,
                    physical_line_number=None,
                    issue_type=issue_type,
                    issue_description=f"{description} [{status_note}]",
                    expected_column_count=19,
                    observed_column_count=None,
                    column_name=None,
                    bad_value=item.content_hash,
                    raw_record="",
                )
            )
    return dict(issues_by_path)
