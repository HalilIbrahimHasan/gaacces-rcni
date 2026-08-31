"""
Dynamic file discovery under source_data/{issuer}/{year}/{month}.

Supports nested folders, zipped and unzipped XML. No hardcoded issuers.
"""

from __future__ import annotations

from pathlib import Path

from connectors.base_connector import SourceFile
from ingestion.zip_handler import expand_archives
from utils.logger import get_logger

logger = get_logger(__name__)

ARCHIVE_EXTS = {".zip", ".gz", ".tar", ".tgz"}
XML_EXTS = {".xml"}


def _normalize_month(name: str) -> str | None:
    if not name.isdigit() or len(name) > 2:
        return None
    m = int(name)
    return str(m).zfill(2) if 1 <= m <= 12 else None


def _normalize_year(name: str) -> str | None:
    return name if name.isdigit() and len(name) == 4 else None


def _issuer_ok(name: str) -> bool:
    """Issuer folders must be exactly 5 digits."""
    return name.isdigit() and len(name) == 5 and not name.startswith(".")


def discover_source_files(
    source_root: Path,
    issuer_filter: str | None = None,
    year_filter: str | None = None,
    month_filter: str | None = None,
    extracted_root: Path | None = None,
) -> list[SourceFile]:
    """
    Discover all ingestible files under issuer/year/month partitions.

    Walks nested subfolders. Expands zip archives to extracted/ (originals kept).
    """
    files: list[SourceFile] = []
    if not source_root.exists():
        logger.warning("Source root missing: %s", source_root)
        return files

    for issuer_dir in sorted(source_root.iterdir(), key=lambda p: p.name):
        if not issuer_dir.is_dir() or not _issuer_ok(issuer_dir.name):
            continue
        if issuer_filter and issuer_dir.name != issuer_filter:
            continue

        for year_dir in sorted(issuer_dir.iterdir(), key=lambda p: p.name):
            if not year_dir.is_dir():
                continue
            year = _normalize_year(year_dir.name)
            if not year:
                continue
            if year_filter and year != year_filter:
                continue

            for month_dir in sorted(year_dir.iterdir(), key=lambda p: p.name):
                if not month_dir.is_dir():
                    continue
                month = _normalize_month(month_dir.name)
                if not month:
                    continue
                if month_filter and month != month_filter:
                    continue

                _scan_partition(
                    issuer_dir.name, year, month, month_dir,
                    files, extracted_root,
                )

    logger.info("Discovered %d source file(s)", len(files))
    for f in files:
        logger.info(
            "  issuer=%s year=%s month=%s file=%s size=%d",
            f.issuer, f.year, f.month, f.file_name, f.file_size,
        )
    return files


def _scan_partition(
    issuer: str,
    year: str,
    month: str,
    root: Path,
    out: list[SourceFile],
    extracted_root: Path | None,
) -> None:
    """Recursively collect XML and archives from a month partition."""
    for path in sorted(root.rglob("*")):
        if not path.is_file() or path.name.startswith("."):
            continue
        suffix = path.suffix.lower()
        if suffix in ARCHIVE_EXTS:
            for extracted in expand_archives(path, extracted_root, issuer, year, month):
                if extracted.suffix.lower() in XML_EXTS:
                    out.append(_to_source_file(issuer, year, month, extracted))
            continue
        if suffix in XML_EXTS:
            out.append(_to_source_file(issuer, year, month, path))


def _to_source_file(issuer: str, year: str, month: str, path: Path) -> SourceFile:
    return SourceFile(
        issuer=issuer,
        year=year,
        month=month,
        file_name=path.name,
        file_path=path,
        file_size=path.stat().st_size,
        source_type="local",
    )
