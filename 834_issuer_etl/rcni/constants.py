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
REJECT_LOG_NAMES: Final = frozenset(
    {
        "log.txt",
        "log.txt.gz",
        "log.txt.xz",
        "last-status-outbound-log.txt",
    }
)

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

# Exchange Assigned Member/Subscriber IDs: companion guide requires 10 numeric
# characters (REF*17 / REF*0F). Issuer-assigned IDs may be alphanumeric.
# Exchange Assigned Policy ID (REF*1L) is mandatory but is not given a numeric
# character rule in the companion guide, so it is not numeric-checked.
NUMERIC_IDENTIFIER_COLUMNS: Final = (
    "Exchange Assigned Member ID",
    "Exchange Assigned Subscriber ID",
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

STRUCTURAL_ISSUE_TYPES: Final = frozenset(
    {
        ISSUE_HEADER_MISMATCH,
        ISSUE_FIELD_COUNT,
        ISSUE_PARSE_ERROR,
    }
)
IDENTIFIER_ISSUE_TYPES: Final = frozenset({ISSUE_IDENTIFIER_NOT_NUMERIC})

# Phase 2 file-log processing_status values (pipeline outcome).
FILE_STATUS_DISCOVERED: Final = "DISCOVERED"
FILE_STATUS_DOWNLOADING: Final = "DOWNLOADING"
FILE_STATUS_DOWNLOADED: Final = "DOWNLOADED"
FILE_STATUS_VALIDATING: Final = "VALIDATING"
FILE_STATUS_LOADING: Final = "LOADING"
FILE_STATUS_SUCCESS: Final = "SUCCESS"
FILE_STATUS_FAILED: Final = "FAILED"
FILE_STATUS_SKIPPED_DUPLICATE: Final = "SKIPPED_DUPLICATE"

LOADED_FILE_STATUSES: Final = frozenset({FILE_STATUS_SUCCESS})

# Phase 2 file_disposition values (logical identity class). Independent of
# processing_status. A replacement that loaded is SUCCESS + POSSIBLE_REPLACEMENT.
FILE_DISPOSITION_NEW: Final = "NEW"
FILE_DISPOSITION_DUPLICATE: Final = "DUPLICATE"
FILE_DISPOSITION_POSSIBLE_REPLACEMENT: Final = "POSSIBLE_REPLACEMENT"

# Phase 2 data-quality issue_code values (stored in dbo.rcni_data_quality_issue).
DQ_COLUMN_COUNT_MISMATCH: Final = "COLUMN_COUNT_MISMATCH"
DQ_UNQUOTED_COMMA: Final = "UNQUOTED_COMMA"
DQ_BROKEN_QUOTE: Final = "BROKEN_QUOTE"
DQ_MULTILINE_FIELD: Final = "MULTILINE_FIELD"
DQ_ENCODING_ISSUE: Final = "ENCODING_ISSUE"
DQ_IDENTIFIER_FORMAT_WARNING: Final = "IDENTIFIER_FORMAT_WARNING"
DQ_HEADER_MISMATCH: Final = "HEADER_MISMATCH"
DQ_SCHEMA_DRIFT: Final = "SCHEMA_DRIFT"
DQ_COUNT_MISMATCH: Final = "COUNT_MISMATCH"
DQ_OTHER: Final = "OTHER"

DOCUMENT_TYPE_RCNI: Final = "INDV_MONTHLYDISCREPANCY"
QUALITY_STATUS_CLEAN: Final = "CLEAN"
QUALITY_STATUS_WARNING: Final = "WARNING"

DEFAULT_RCNI_AZURE_BATCH_SIZE: Final = 3000
