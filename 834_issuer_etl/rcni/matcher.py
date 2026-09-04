"""RCNI filename matcher — one configurable place, no scattered literals."""

from __future__ import annotations

from pathlib import Path

from rcni.constants import (
    REJECT_DIRECTION_PREFIX,
    REJECT_LOG_NAMES,
    REJECT_RECON_TOKEN,
    RCNI_COMPRESSED_SUFFIX,
    RCNI_DECOMPRESSED_SUFFIX,
    RCNI_DIRECTION_PREFIX,
    RCNI_DOCUMENT_TOKEN,
)


def _basename(filename: str) -> str:
    return Path(filename).name


def is_log_artifact(filename: str) -> bool:
    name = _basename(filename)
    lower = name.lower()
    if lower in {n.lower() for n in REJECT_LOG_NAMES}:
        return True
    if lower.startswith("log.txt"):
        return True
    if lower.endswith("-log.txt") or lower.endswith("-log.txt.gz"):
        return True
    return False


def is_recon_input_file(filename: str) -> bool:
    """Issuer MONTHLYRECON input — not RCNI."""
    name = _basename(filename)
    lower = name.lower()
    if lower.startswith(REJECT_DIRECTION_PREFIX):
        return True
    if REJECT_RECON_TOKEN.lower() in lower:
        return True
    if lower.endswith(".in") or lower.endswith(".in.gz"):
        return True
    return False


def has_valid_rcni_suffix(filename: str) -> bool:
    """Physical packaging only: compressed and uncompressed archive forms."""
    name = _basename(filename)
    return name.endswith(RCNI_COMPRESSED_SUFFIX) or name.endswith(RCNI_DECOMPRESSED_SUFFIX)


def _matches_rcni_family(name: str) -> bool:
    if is_log_artifact(name) or is_recon_input_file(name):
        return False
    if not name.startswith(RCNI_DIRECTION_PREFIX):
        return False
    if RCNI_DOCUMENT_TOKEN not in name:
        return False
    return True


def rcni_sftp_reject_reason(filename: str) -> str | None:
    """
    Exact live-SFTP matcher rejection reason.

    Returns None when is_rcni_sftp_archive_file() would accept the name.
    Compression is not a business-identification rule: both .OUT.good and
    .OUT.good.gz are valid source forms.
    """
    name = _basename(filename)
    lower = name.lower()
    if is_log_artifact(name):
        return "log artifact"
    if lower.startswith(REJECT_DIRECTION_PREFIX):
        return f"inbound {REJECT_DIRECTION_PREFIX} prefix"
    if REJECT_RECON_TOKEN.lower() in lower:
        return f"contains {REJECT_RECON_TOKEN}"
    if lower.endswith(".in") or lower.endswith(".in.gz"):
        return "inbound .IN / .IN.gz suffix"
    if not name.startswith(RCNI_DIRECTION_PREFIX):
        return f"does not start with {RCNI_DIRECTION_PREFIX}"
    if RCNI_DOCUMENT_TOKEN not in name:
        return f"does not contain {RCNI_DOCUMENT_TOKEN}"
    if not has_valid_rcni_suffix(name):
        return (
            f"does not end with {RCNI_DECOMPRESSED_SUFFIX} "
            f"or {RCNI_COMPRESSED_SUFFIX}"
        )
    return None


def is_rcni_sftp_archive_file(filename: str) -> bool:
    """
    Live SFTP archive matcher.

    Valid source files:
      to_*_INDV_MONTHLYDISCREPANCY_*.OUT.good.gz
      to_*_INDV_MONTHLYDISCREPANCY_*.OUT.good
    """
    return rcni_sftp_reject_reason(filename) is None


def is_rcni_local_file(filename: str) -> bool:
    """Local fixtures use the same business matcher as live SFTP."""
    return is_rcni_sftp_archive_file(filename)


def is_rcni_monthly_discrepancy_file(filename: str, *, require_gzip: bool = False) -> bool:
    """
    Business identification does not depend on gzip.

    require_gzip is retained for call-site compatibility and is ignored.
    """
    del require_gzip
    return is_rcni_sftp_archive_file(filename)


def logical_filename(filename: str) -> str:
    """Strip a trailing .gz so compressed and extracted names share a basename."""
    name = _basename(filename)
    if name.lower().endswith(".gz"):
        return name[:-3]
    return name
