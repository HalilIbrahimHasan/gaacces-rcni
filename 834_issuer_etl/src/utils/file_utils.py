"""
File and path utilities for the 834 Issuer ETL framework.

Handles issuer discovery, filename parsing, and output directory creation
so that extract and load stages stay decoupled from filesystem details.
"""

import re
from pathlib import Path

from config import ASSETS_DIR, SOURCE_DATA_DIR
from utils.logger import get_logger

logger = get_logger(__name__)

# Pattern to extract issuer ID from filenames like from_64357_GA_834_...
_ISSUER_FILENAME_PATTERN = re.compile(r"from_(\d+)_", re.IGNORECASE)


def discover_issuer_ids(source_dir: Path | None = None) -> list[str]:
    """
    Discover all issuer IDs from subfolders under ``source_data``.

    Each immediate child directory whose name is numeric is treated as an
    issuer folder. This keeps the pipeline dynamic as new issuers are added.

    Args:
        source_dir: Root source directory; defaults to ``SOURCE_DATA_DIR``.

    Returns:
        Sorted list of issuer ID strings.
    """
    root = source_dir or SOURCE_DATA_DIR
    if not root.exists():
        logger.warning("Source data directory does not exist: %s", root)
        return []

    issuer_ids = sorted(
        d.name
        for d in root.iterdir()
        if d.is_dir() and d.name.isdigit()
    )
    logger.info("Discovered %d issuer(s): %s", len(issuer_ids), issuer_ids)
    return issuer_ids


def get_issuer_source_dir(issuer_id: str, source_dir: Path | None = None) -> Path:
    """
    Return the source folder path for a given issuer.

    Args:
        issuer_id: Numeric issuer identifier (e.g. ``64357``).
        source_dir: Optional override for the source root.

    Returns:
        Path to ``source_data/{issuer_id}/``.
    """
    root = source_dir or SOURCE_DATA_DIR
    return root / issuer_id


def list_xml_files(issuer_id: str, source_dir: Path | None = None) -> list[Path]:
    """
    List all ``*.xml`` files for an issuer, sorted by name.

    Args:
        issuer_id: Issuer to scan.
        source_dir: Optional source root override.

    Returns:
        Sorted list of XML file paths.
    """
    issuer_dir = get_issuer_source_dir(issuer_id, source_dir)
    if not issuer_dir.exists():
        logger.warning("Issuer directory not found: %s", issuer_dir)
        return []
    files = sorted(issuer_dir.glob("*.xml"))
    logger.info("Found %d XML file(s) for issuer %s", len(files), issuer_id)
    return files


def parse_issuer_from_filename(filename: str) -> str | None:
    """
    Extract issuer ID from a standard 834 filename when folder name is absent.

    Args:
        filename: Base name or full path of an XML file.

    Returns:
        Issuer ID string or ``None`` if the pattern does not match.
    """
    match = _ISSUER_FILENAME_PATTERN.search(Path(filename).name)
    return match.group(1) if match else None


def ensure_issuer_asset_dirs(issuer_id: str) -> dict[str, Path]:
    """
    Create and return all output directories for an issuer under ``assets/``.

    Ensures excel, cleaned_xml, sqlite, dashboards, and validation_reports
    folders exist before export/load stages write files.

    Args:
        issuer_id: Issuer identifier.

    Returns:
        Dict mapping logical names to created directory paths.
    """
    base = ASSETS_DIR / issuer_id
    dirs = {
        "base": base,
        "excel": base / "excel",
        "cleaned_xml": base / "cleaned_xml",
        "sqlite": base / "sqlite",
        "dashboards": base / "dashboards",
        "validation_reports": base / "validation_reports",
    }
    for path in dirs.values():
        path.mkdir(parents=True, exist_ok=True)
    return dirs


def parse_file_date_from_filename(filename: str) -> str | None:
    """
    Extract a YYYYMMDD date token from filenames like ``..._20260204071545.xml``.

    Args:
        filename: XML file name.

    Returns:
        Eight-digit date string or ``None``.
    """
    match = re.search(r"_(\d{8})\d{6}\.xml$", Path(filename).name, re.IGNORECASE)
    return match.group(1) if match else None
