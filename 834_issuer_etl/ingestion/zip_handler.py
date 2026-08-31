"""Zip/archive expansion — originals are never deleted."""

from __future__ import annotations

import zipfile
from pathlib import Path

from config.config import settings
from utils.logger import get_logger

logger = get_logger(__name__)


def expand_archives(
    archive_path: Path,
    extracted_root: Path | None,
    issuer: str,
    year: str,
    month: str,
) -> list[Path]:
    """
    Extract archive contents to extracted/{issuer}/{year}/{month}/.

    Returns paths to extracted XML files. Original archive is preserved.
    """
    root = extracted_root or settings.extracted_path
    target_dir = root / issuer / year / month / archive_path.stem
    target_dir.mkdir(parents=True, exist_ok=True)
    extracted: list[Path] = []

    suffix = archive_path.suffix.lower()
    if suffix == ".zip":
        try:
            with zipfile.ZipFile(archive_path, "r") as zf:
                for member in zf.namelist():
                    if member.endswith("/"):
                        continue
                    dest = target_dir / Path(member).name
                    if dest.exists():
                        logger.info("Skip already extracted: %s", dest)
                        extracted.append(dest)
                        continue
                    with zf.open(member) as src, open(dest, "wb") as dst:
                        dst.write(src.read())
                    extracted.append(dest)
                    logger.info("Extracted %s → %s", member, dest)
        except zipfile.BadZipFile as exc:
            logger.error("Bad zip %s: %s", archive_path, exc)
    else:
        logger.warning("Unsupported archive type (skipped): %s", archive_path)

    return [p for p in extracted if p.suffix.lower() == ".xml"]
