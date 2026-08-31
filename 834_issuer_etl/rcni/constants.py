"""Configurable RCNI file matching and source schema constants."""

from __future__ import annotations

from typing import Final

# Canonical SFTP archive for Exchange-generated Monthly Discrepancy output.
DEFAULT_RCNI_BASE_PATH: Final = "/archive/out/good/PAS"

# RCNI must never scan issuer MONTHLYRECON input archives.
FORBIDDEN_INBOUND_PATH_FRAGMENT: Final = "/archive/in/"

RCNI_DIRECTION_PREFIX: Final = "to_"
RCNI_DOCUMENT_TOKEN: Final = "_INDV_MONTHLYDISCREPANCY_"
RCNI_COMPRESSED_SUFFIX: Final = ".OUT.good.gz"
RCNI_DECOMPRESSED_SUFFIX: Final = ".OUT.good"

# Rejected populations / artifacts (checked after the positive matcher).
REJECT_DIRECTION_PREFIX: Final = "from_"
REJECT_RECON_TOKEN: Final = "_INDV_MONTHLYRECON_"
REJECT_LOG_NAMES: Final = frozenset({"log.txt", "log.txt.gz", "log.txt.xz"})

EXPECTED_HEADER: Final = (
    "Exchange Assigned Policy ID",
    "Plan ID",
    "Member Last Name",
    "Member First Name",
    "Exchange Assigned Member ID",
    "Issuer Assigned Member ID",
    "Subscriber Last Name",
    "Subscriber First Name",
    "Exchange Assigned Subscriber ID",
    "Issuer Assigned Subscriber ID",
    "Discrepancy Reason Code",
    "Discrepancy Reason Text",
    "HIX Value",
    "Issuer Value",
    "Date of Discrepancy",
    "Recon File Name",
    "Autofixed by HIX",
    "Assignee",
    "Enrollment Status",
)

EXPECTED_COLUMN_COUNT: Final = len(EXPECTED_HEADER)

# Identifiers are stored as strings. Non-empty non-digit values are warnings,
# not parser crashes. HIX Value / Issuer Value are intentionally excluded.
NUMERIC_IDENTIFIER_COLUMNS: Final = (
    "Exchange Assigned Policy ID",
    "Exchange Assigned Member ID",
    "Issuer Assigned Member ID",
    "Exchange Assigned Subscriber ID",
    "Issuer Assigned Subscriber ID",
)

HIX_VALUE_COLUMN: Final = "HIX Value"
ISSUER_VALUE_COLUMN: Final = "Issuer Value"

STATUS_CLEAN: Final = "CLEAN"
STATUS_WARNING: Final = "WARNING"
STATUS_MALFORMED: Final = "MALFORMED"
STATUS_FILENAME_METADATA_MISMATCH: Final = "FILENAME_METADATA_MISMATCH"
STATUS_SCHEMA_MISMATCH: Final = "SCHEMA_MISMATCH"
STATUS_DUPLICATE: Final = "DUPLICATE"
STATUS_POSSIBLE_REPLACEMENT: Final = "POSSIBLE_REPLACEMENT"

# Worst-first ranking for a single overall status per file.
STATUS_PRIORITY: Final = (
    STATUS_MALFORMED,
    STATUS_SCHEMA_MISMATCH,
    STATUS_FILENAME_METADATA_MISMATCH,
    STATUS_POSSIBLE_REPLACEMENT,
    STATUS_DUPLICATE,
    STATUS_WARNING,
    STATUS_CLEAN,
)

ISSUE_HEADER_MISMATCH: Final = "HEADER_MISMATCH"
ISSUE_FIELD_COUNT: Final = "FIELD_COUNT"
ISSUE_IDENTIFIER_NOT_NUMERIC: Final = "IDENTIFIER_NOT_NUMERIC"
ISSUE_FILENAME_ISSUER_MISMATCH: Final = "FILENAME_ISSUER_MISMATCH"
ISSUE_FILENAME_UNPARSEABLE: Final = "FILENAME_UNPARSEABLE"
ISSUE_DUPLICATE_COPY: Final = "DUPLICATE_COPY"
ISSUE_POSSIBLE_REPLACEMENT: Final = "POSSIBLE_REPLACEMENT"
ISSUE_PARSE_ERROR: Final = "PARSE_ERROR"
