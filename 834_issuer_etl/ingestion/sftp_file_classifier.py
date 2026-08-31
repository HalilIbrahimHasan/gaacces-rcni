"""Classify remote SFTP filenames for ingestion and audit."""

from __future__ import annotations

import fnmatch


def local_xml_name_from_remote(filename: str) -> str:
    """Map remote compressed/plain name to flat local .xml filename."""
    lower = filename.lower()
    if lower.endswith(".xml.xz"):
        return filename[:-3]
    if lower.endswith(".xml.gz"):
        return filename[:-3]
    return filename


def _skip_reason(filename: str) -> str | None:
    """Return skip category or None if not an explicit skip."""
    lower = filename.lower()
    if lower.startswith("to_"):
        return "to"
    if "log" in lower and (lower.endswith(".txt") or lower.endswith(".txt.xz") or lower == "log.txt"):
        return "log"
    if lower.endswith(".txt") or lower.endswith(".txt.gz") or lower.endswith(".txt.xz"):
        return "other"
    if ".edi" in lower:
        return "edi"
    if ".dtl" in lower:
        return "other"
    if "tracking" in lower:
        return "tracking"
    if "report" in lower:
        return "report"
    if "summary" in lower:
        return "summary"
    if "log" in lower:
        return "log"
    return None


def classify_sftp_filename(issuer: str, filename: str) -> str:
    """
    Classify a remote filename.

    Returns: valid_xml, valid_xz, valid_gz, edi, tracking, report, summary, log, to, other
    """
    skip = _skip_reason(filename)
    if skip:
        return skip

    xz_pat = f"from_{issuer}_GA_834_INDV_*.xml.xz"
    gz_pat = f"from_{issuer}_GA_834_INDV_*.xml.gz"
    xml_pat = f"from_{issuer}_GA_834_INDV_*.xml"

    if fnmatch.fnmatch(filename, xz_pat):
        return "valid_xz"
    if fnmatch.fnmatch(filename, gz_pat):
        return "valid_gz"
    if fnmatch.fnmatch(filename, xml_pat):
        lower = filename.lower()
        if lower.endswith(".xml") and not lower.endswith((".xml.gz", ".xml.xz")):
            return "valid_xml"
    return "other"


def is_valid_834_file(issuer: str, filename: str) -> bool:
    return classify_sftp_filename(issuer, filename) in ("valid_xml", "valid_gz", "valid_xz")


def is_valid_834_gz(issuer: str, filename: str) -> bool:
    return is_valid_834_file(issuer, filename)
