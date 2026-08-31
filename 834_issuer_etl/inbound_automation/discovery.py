"""Dynamic source_data discovery with CLI filters."""

from __future__ import annotations

from pathlib import Path

from connectors.base_connector import SourceFile
from ingestion.file_discovery import discover_source_files
from utils.logger import get_logger

logger = get_logger(__name__)


def _normalize_month(month: str | int) -> str:
    return str(int(month)).zfill(2)


def discover_for_run(
    source_root: Path,
    *,
    year_filter: str | None,
    all_years: bool,
    issuer_filter: list[str] | None,
    month_filter: str | None,
) -> list[SourceFile]:
    """
    Discover XML source files under source_data with dynamic issuer discovery.

    When issuer_filter is set, discovery runs per issuer. Otherwise all issuers
    under matching year/month partitions are returned.
    """
    year = None if all_years else year_filter
    month = _normalize_month(month_filter) if month_filter else None

    if issuer_filter:
        files: list[SourceFile] = []
        for issuer in issuer_filter:
            batch = discover_source_files(
                source_root,
                issuer_filter=issuer,
                year_filter=year,
                month_filter=month,
            )
            files.extend(batch)
        logger.info(
            "Discovered %d file(s) for issuers=%s year=%s month=%s",
            len(files),
            ",".join(issuer_filter),
            year or "ALL",
            month or "ALL",
        )
        return files

    files = discover_source_files(
        source_root,
        issuer_filter=None,
        year_filter=year,
        month_filter=month,
    )
    logger.info(
        "Discovered %d file(s) for all issuers year=%s month=%s",
        len(files),
        year or "ALL",
        month or "ALL",
    )
    return files
