"""
Partition discovery for issuer/year/month folder structure.

Fully folder-driven: walks every numeric issuer folder under source_data and
creates a partition for each year/month folder that contains XML files.
No config issuer list, no CLI filters, no hardcoded issuers.
"""

from __future__ import annotations

import shutil
from dataclasses import dataclass, field
from pathlib import Path

import config
from utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class SourcePartition:
    """A single issuer/year/month data partition under ``source_data``."""

    issuer_id: str
    year: str
    month: str
    input_path: Path
    xml_files: list[Path] = field(default_factory=list)
    output_path: Path = field(default_factory=Path)
    source_root: Path = field(default_factory=Path)

    @property
    def source_period(self) -> str:
        return f"{self.year}-{self.month}"

    @property
    def period_key(self) -> str:
        return f"{self.issuer_id}_{self.year}_{self.month}"

    @property
    def file_count(self) -> int:
        return len(self.xml_files)

    def __post_init__(self) -> None:
        if not self.output_path or str(self.output_path) == ".":
            self.output_path = (
                config.ASSETS_DIR / self.issuer_id / self.year / self.month
            )


Partition = SourcePartition


def iter_source_roots(explicit_root: Path | None = None) -> list[Path]:
    """
    Return all source_data directories to scan.

    Checks the project ``source_data/`` first, then a workspace-level sibling
    ``../source_data`` if it exists (common when data is added at repo root).
    """
    if explicit_root is not None:
        return [explicit_root.resolve()]

    roots: list[Path] = []
    primary = config.SOURCE_DATA_DIR.resolve()
    roots.append(primary)

    alt = config.PROJECT_ROOT.parent / "source_data"
    alt_resolved = alt.resolve()
    if alt_resolved != primary and alt_resolved.exists():
        roots.append(alt_resolved)

    return roots


def _normalize_year(name: str) -> str | None:
    if name.isdigit() and len(name) == 4:
        return name
    return None


def _normalize_month(name: str) -> str | None:
    """Accept 1- or 2-digit month folders (e.g. ``2`` or ``02``) → ``02``."""
    if not name.isdigit() or len(name) > 2:
        return None
    month_int = int(name)
    if month_int < 1 or month_int > 12:
        return None
    return str(month_int).zfill(2)


def _is_issuer_folder(name: str) -> bool:
    """Issuer folders are numeric (typically 5 digits, any length accepted)."""
    return name.isdigit() and not name.startswith(".")


def log_source_tree_scan(source_roots: list[Path] | None = None) -> None:
    """
    Print a full diagnostic of every folder under source_data before processing.

    Shows issuer → year → month → XML counts, plus warnings for misplaced files.
    """
    roots = source_roots or iter_source_roots()
    logger.info("SOURCE TREE SCAN")
    logger.info("-" * 60)

    for root in roots:
        logger.info("Scanning: %s (exists=%s)", root, root.exists())
        if not root.exists():
            continue

        issuer_dirs = sorted(
            (p for p in root.iterdir() if p.is_dir() and _is_issuer_folder(p.name)),
            key=lambda p: p.name,
        )
        if not issuer_dirs:
            logger.info("  (no numeric issuer folders)")
            _warn_misplaced_xml(root)
            continue

        for issuer_dir in issuer_dirs:
            logger.info("  issuer=%s", issuer_dir.name)
            year_dirs = sorted(
                (p for p in issuer_dir.iterdir() if p.is_dir()),
                key=lambda p: p.name,
            )
            if not year_dirs:
                direct_xml = sorted(issuer_dir.glob("*.xml"))
                if direct_xml:
                    logger.warning(
                        "    MISPLACED: %d XML file(s) directly under issuer — "
                        "move to %s/{year}/{month}/",
                        len(direct_xml),
                        issuer_dir.name,
                    )
                else:
                    logger.info("    (no year folders)")
                continue

            for year_dir in year_dirs:
                year = _normalize_year(year_dir.name)
                if year is None:
                    logger.info(
                        "    skip folder '%s' (not a 4-digit year)",
                        year_dir.name,
                    )
                    continue

                logger.info("    year=%s", year)
                month_dirs = sorted(
                    (p for p in year_dir.iterdir() if p.is_dir()),
                    key=lambda p: p.name,
                )
                year_xml = sorted(year_dir.glob("*.xml"))
                if year_xml:
                    logger.warning(
                        "      MISPLACED: %d XML file(s) under year — "
                        "move to %s/%s/{month}/",
                        len(year_xml),
                        issuer_dir.name,
                        year,
                    )

                if not month_dirs:
                    logger.info("      (no month folders)")
                    continue

                for month_dir in month_dirs:
                    month = _normalize_month(month_dir.name)
                    xml_files = sorted(month_dir.glob("*.xml"))
                    if month is None:
                        logger.info(
                            "      skip folder '%s' (not a valid month 01-12)",
                            month_dir.name,
                        )
                        continue
                    logger.info(
                        "      month=%s  xml_files=%d  path=%s",
                        month,
                        len(xml_files),
                        month_dir,
                    )

    logger.info("-" * 60)


def _warn_misplaced_xml(root: Path) -> None:
    """Warn about XML files sitting at issuer or year level instead of month."""
    if not root.exists():
        return
    for issuer_dir in root.iterdir():
        if not issuer_dir.is_dir() or not _is_issuer_folder(issuer_dir.name):
            continue
        direct = sorted(issuer_dir.glob("*.xml"))
        if direct:
            logger.warning(
                "MISPLACED XML at %s — need source_data/%s/{year}/{month}/*.xml",
                issuer_dir,
                issuer_dir.name,
            )
        for year_dir in issuer_dir.iterdir():
            if not year_dir.is_dir():
                continue
            year_xml = sorted(year_dir.glob("*.xml"))
            if year_xml:
                logger.warning(
                    "MISPLACED XML at %s — need source_data/.../%s/{month}/*.xml",
                    year_dir,
                    year_dir.name,
                )


def _discover_in_root(root: Path) -> list[SourcePartition]:
    """Discover partitions inside a single source_data root."""
    partitions: list[SourcePartition] = []

    if not root.exists():
        return partitions

    for issuer_dir in sorted(root.iterdir(), key=lambda p: p.name):
        if not issuer_dir.is_dir() or not _is_issuer_folder(issuer_dir.name):
            continue

        for year_dir in sorted(issuer_dir.iterdir(), key=lambda p: p.name):
            if not year_dir.is_dir():
                continue
            year = _normalize_year(year_dir.name)
            if year is None:
                continue

            for month_dir in sorted(year_dir.iterdir(), key=lambda p: p.name):
                if not month_dir.is_dir():
                    continue
                month = _normalize_month(month_dir.name)
                if month is None:
                    continue

                xml_files = sorted(month_dir.glob("*.xml"))
                if not xml_files:
                    continue

                partitions.append(
                    SourcePartition(
                        issuer_id=issuer_dir.name,
                        year=year,
                        month=month,
                        input_path=month_dir,
                        xml_files=xml_files,
                        output_path=(
                            config.ASSETS_DIR / issuer_dir.name / year / month
                        ),
                        source_root=root,
                    )
                )

    return partitions


def discover_partitions(source_root: Path | None = None) -> list[SourcePartition]:
    """
    Discover every valid issuer/year/month partition across all source roots.

    Deduplicates by (issuer_id, year, month); project source_data wins on conflict.
    """
    roots = iter_source_roots(source_root)
    seen: set[tuple[str, str, str]] = set()
    partitions: list[SourcePartition] = []

    log_source_tree_scan(roots)

    for root in roots:
        for part in _discover_in_root(root):
            key = (part.issuer_id, part.year, part.month)
            if key in seen:
                logger.warning(
                    "Duplicate partition %s — already found, skipping %s",
                    part.period_key,
                    root,
                )
                continue
            seen.add(key)
            partitions.append(part)

    partitions.sort(key=lambda p: (p.issuer_id, p.year, p.month))

    logger.info("Discovered partitions: %d", len(partitions))
    for p in partitions:
        logger.info(
            "  issuer=%s, year=%s, month=%s, files=%d, root=%s",
            p.issuer_id,
            p.year,
            p.month,
            p.file_count,
            p.source_root,
        )
    if not partitions:
        logger.warning(
            "No partitions found. Required layout: "
            "source_data/{issuer_id}/{year}/{month}/*.xml "
            "(issuer= numeric, year=4 digits, month=01-12)"
        )
        _warn_misplaced_xml(roots[0] if roots else config.SOURCE_DATA_DIR)

    return partitions


def ensure_partition_asset_dirs(partition: SourcePartition) -> dict[str, Path]:
    """Create monthly output directories for a partition."""
    base = partition.output_path
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


def ensure_rollup_asset_dirs(issuer_id: str) -> dict[str, Path]:
    """Create issuer-level rollup output directories."""
    base = config.ASSETS_DIR / issuer_id / "rollups"
    dirs = {
        "base": base,
        "excel": base / "excel",
        "sqlite": base / "sqlite",
        "dashboards": base / "dashboards",
        "validation_reports": base / "validation_reports",
    }
    for path in dirs.values():
        path.mkdir(parents=True, exist_ok=True)
    return dirs


def monthly_output_stem(partition: SourcePartition) -> str:
    return partition.period_key


def rollup_output_stem(issuer_id: str) -> str:
    return f"{issuer_id}_all_periods"


def list_source_issuer_ids(source_roots: list[Path] | None = None) -> set[str]:
    """Return every numeric issuer folder name under source_data."""
    ids: set[str] = set()
    for root in source_roots or iter_source_roots():
        if not root.exists():
            continue
        for p in root.iterdir():
            if p.is_dir() and _is_issuer_folder(p.name):
                ids.add(p.name)
    return ids


def list_asset_issuer_ids() -> set[str]:
    """Return every numeric issuer folder name under assets/."""
    ids: set[str] = set()
    if not config.ASSETS_DIR.exists():
        return ids
    for p in config.ASSETS_DIR.iterdir():
        if p.is_dir() and _is_issuer_folder(p.name):
            ids.add(p.name)
    return ids


def log_issuer_gaps(
    partitions: list[SourcePartition],
    source_roots: list[Path] | None = None,
) -> None:
    """
    Warn when source_data has issuer folders with no valid partitions,
    or assets/ contains leftover folders from old runs or git history.
    """
    source_ids = list_source_issuer_ids(source_roots)
    active_ids = {p.issuer_id for p in partitions}
    asset_ids = list_asset_issuer_ids()

    skipped = sorted(source_ids - active_ids)
    if skipped:
        logger.warning(
            "Issuers in source_data with NO valid partitions (check year/month/XML): %s",
            ", ".join(skipped),
        )
        for iid in skipped:
            logger.warning(
                "  → %s needs: source_data/%s/{year}/{month}/*.xml",
                iid,
                iid,
            )

    stale_assets = sorted(asset_ids - active_ids)
    if stale_assets:
        logger.warning(
            "Stale assets/ issuer folders (old run or git) — will be removed: %s",
            ", ".join(stale_assets),
        )


def clean_stale_asset_issuers(active_issuer_ids: set[str]) -> list[str]:
    """
    Delete assets/{issuer}/ folders for issuers not in the current run.

    assets/ is regenerated every run; leftover folders (e.g. 64357 from git)
    confuse Explorer even when the console shows the correct issuers.
    """
    removed: list[str] = []
    if not config.ASSETS_DIR.exists():
        return removed

    for path in sorted(config.ASSETS_DIR.iterdir(), key=lambda p: p.name):
        if not path.is_dir() or not _is_issuer_folder(path.name):
            continue
        if path.name in active_issuer_ids:
            continue
        shutil.rmtree(path)
        removed.append(path.name)
        logger.info("Removed stale assets folder: %s", path)

    if removed:
        logger.info("Cleaned stale asset issuers: %s", ", ".join(removed))
    return removed
