"""Parse RCNI Monthly Discrepancy filename metadata.

Plan year comes from the filename, never from the archive directory.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

RCNI_FILENAME_RE = re.compile(
    r"^(?P<direction>to)_"
    r"(?P<issuer_id>\d+)_"
    r"(?P<document_type>INDV_MONTHLYDISCREPANCY)_"
    r"(?P<plan_year>\d{4})_"
    r"(?P<file_timestamp>\d{14})"
    r"\.OUT\.good(?:\.gz)?$",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class RcniFilenameMetadata:
    original_filename: str
    compressed_filename: str | None
    direction: str
    issuer_id: str
    document_type: str
    plan_year: str
    file_timestamp: str
    parsed_timestamp: datetime | None
    parse_ok: bool
    parse_error: str | None = None

    @property
    def parsed_timestamp_display(self) -> str | None:
        if self.parsed_timestamp is None:
            return None
        return self.parsed_timestamp.strftime("%Y-%m-%d %H:%M:%S")

    @property
    def logical_filename(self) -> str:
        name = self.original_filename
        if name.lower().endswith(".gz"):
            return name[:-3]
        return name

    @property
    def logical_identity(self) -> tuple[str, str, str, str] | None:
        """Canonical RCNI identity: issuer, document type, plan year, file timestamp."""
        if not self.parse_ok:
            return None
        return (self.issuer_id, self.document_type, self.plan_year, self.file_timestamp)

    @property
    def logical_identity_key(self) -> str:
        ident = self.logical_identity
        if ident is None:
            return ""
        return "|".join(ident)


def _parse_timestamp(raw: str) -> datetime | None:
    try:
        return datetime.strptime(raw, "%Y%m%d%H%M%S")
    except ValueError:
        return None


def parse_rcni_filename(filename: str) -> RcniFilenameMetadata:
    original = Path(filename).name
    compressed = original if original.lower().endswith(".gz") else (
        original + ".gz" if original.endswith(".OUT.good") else None
    )
    match = RCNI_FILENAME_RE.match(original)
    if not match:
        return RcniFilenameMetadata(
            original_filename=original,
            compressed_filename=compressed,
            direction="",
            issuer_id="",
            document_type="",
            plan_year="",
            file_timestamp="",
            parsed_timestamp=None,
            parse_ok=False,
            parse_error=(
                "Filename does not match "
                "to_{issuer}_INDV_MONTHLYDISCREPANCY_{plan_year}_{timestamp}.OUT.good[.gz]"
            ),
        )

    ts = match.group("file_timestamp")
    parsed_ts = _parse_timestamp(ts)
    parse_error = None if parsed_ts is not None else (
        f"Timestamp {ts} is not a valid YYYYMMDDHHMMSS value"
    )
    return RcniFilenameMetadata(
        original_filename=original,
        compressed_filename=compressed,
        direction=match.group("direction").upper(),
        issuer_id=match.group("issuer_id"),
        document_type=match.group("document_type"),
        plan_year=match.group("plan_year"),
        file_timestamp=ts,
        parsed_timestamp=parsed_ts,
        parse_ok=parsed_ts is not None,
        parse_error=parse_error,
    )
