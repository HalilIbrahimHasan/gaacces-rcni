"""Local staging layout and gzip decompress — originals are never modified."""

from __future__ import annotations

import gzip
from dataclasses import dataclass
from pathlib import Path

from rcni.discovery import RcniCandidate
from rcni.matcher import logical_filename
from utils.logger import get_logger

logger = get_logger(__name__)

DECOMPRESS_CHUNK = 1024 * 1024


@dataclass(frozen=True)
class LocalStagingPaths:
    compressed_path: Path | None
    extracted_path: Path


def staging_paths(local_root: Path, candidate: RcniCandidate) -> LocalStagingPaths:
    """
    Stage under SFTP/archive processing date, never filename plan year:

      assets/rcni/{issuer}/{processing_year}/{processing_month}/[{processing_day}/]
    """
    base = (
        local_root
        / candidate.issuer
        / candidate.processing_year
        / candidate.processing_month
    )
    if candidate.processing_day:
        base = base / candidate.processing_day
    extracted_name = logical_filename(candidate.filename)
    compressed_path = None
    if candidate.filename.lower().endswith(".gz"):
        compressed_path = base / "compressed" / candidate.filename
    return LocalStagingPaths(
        compressed_path=compressed_path,
        extracted_path=base / "extracted" / extracted_name,
    )


def decompress_gzip_file(src: Path, dest: Path) -> int:
    """Stream-decompress gzip to dest. Never overwrites src."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    written = 0
    tmp = dest.with_suffix(dest.suffix + ".partial")
    with gzip.open(src, "rb") as gz_in, tmp.open("wb") as out:
        while True:
            chunk = gz_in.read(DECOMPRESS_CHUNK)
            if not chunk:
                break
            out.write(chunk)
            written += len(chunk)
    tmp.replace(dest)
    logger.info("Decompressed %s → %s (%d bytes)", src, dest, written)
    return written
