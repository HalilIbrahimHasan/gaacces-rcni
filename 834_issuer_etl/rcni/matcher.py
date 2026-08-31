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
    return lower.startswith("log.txt")


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


def _matches_rcni_family(name: str) -> bool:
    if is_log_artifact(name) or is_recon_input_file(name):
        return False
    if not name.startswith(RCNI_DIRECTION_PREFIX):
        return False
    if RCNI_DOCUMENT_TOKEN not in name:
        return False
    return True


def is_rcni_sftp_archive_file(filename: str) -> bool:
    """
    Live SFTP archive matcher.

    Canonical archived input:
      to_*_INDV_MONTHLYDISCREPANCY_*.OUT.good.gz
    """
    name = _basename(filename)
    return _matches_rcni_family(name) and name.endswith(RCNI_COMPRESSED_SUFFIX)


def is_rcni_local_file(filename: str) -> bool:
    """Local fixtures may be decompressed (.OUT.good) or still gzipped."""
    name = _basename(filename)
    if not _matches_rcni_family(name):
        return False
    return name.endswith(RCNI_COMPRESSED_SUFFIX) or name.endswith(RCNI_DECOMPRESSED_SUFFIX)


def is_rcni_monthly_discrepancy_file(filename: str, *, require_gzip: bool = True) -> bool:
    """
    require_gzip=True  → live SFTP (.OUT.good.gz only)
    require_gzip=False → local/decompressed fixtures also allowed
    """
    if require_gzip:
        return is_rcni_sftp_archive_file(filename)
    return is_rcni_local_file(filename)


def logical_filename(filename: str) -> str:
    """Strip a trailing .gz so compressed and extracted names share a basename."""
    name = _basename(filename)
    if name.lower().endswith(".gz"):
        return name[:-3]
    return name
