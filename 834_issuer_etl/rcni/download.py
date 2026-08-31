"""Download RCNI files from SFTP and decompress locally. Source is immutable."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from rcni.discovery import RcniCandidate
from rcni.staging import LocalStagingPaths, decompress_gzip_file, staging_paths
from rcni.settings import RcniScope
from utils.hashing import sha256_file
from utils.logger import get_logger

logger = get_logger(__name__)

DOWNLOAD_CHUNK_SIZE = 1024 * 1024


def _download_streaming(sftp, remote_path: str, local_path: Path) -> int:
    """Stream remote bytes to disk using the open SFTP client (same Paramiko API as 834)."""
    dest = Path(local_path)
    dest.parent.mkdir(parents=True, exist_ok=True)
    written = 0
    tmp = dest.with_suffix(dest.suffix + ".partial")
    try:
        with sftp.open(remote_path, "rb") as remote_f, tmp.open("wb") as local_f:
            while True:
                chunk = remote_f.read(DOWNLOAD_CHUNK_SIZE)
                if not chunk:
                    break
                local_f.write(chunk)
                written += len(chunk)
        tmp.replace(dest)
        logger.info("Downloaded %s → %s (%d bytes)", remote_path, dest, written)
        return written
    except Exception:
        if tmp.exists():
            tmp.unlink(missing_ok=True)
        raise


@dataclass
class DownloadedFile:
    candidate: RcniCandidate
    paths: LocalStagingPaths
    compressed_size: int
    extracted_size: int
    compressed_hash: str
    content_hash: str
    downloaded: bool
    decompressed: bool
    error: str | None = None


def download_and_stage(
    sftp,
    candidate: RcniCandidate,
    scope: RcniScope,
) -> DownloadedFile:
    paths = staging_paths(scope.local_root, candidate)
    paths.extracted_path.parent.mkdir(parents=True, exist_ok=True)
    if paths.compressed_path is not None:
        paths.compressed_path.parent.mkdir(parents=True, exist_ok=True)

    downloaded = False
    decompressed = False
    remote_is_gz = candidate.filename.lower().endswith(".gz")

    try:
        if remote_is_gz:
            assert paths.compressed_path is not None
            if paths.compressed_path.exists() and not scope.force_download:
                logger.info("Skipping existing compressed file: %s", paths.compressed_path)
            else:
                _download_streaming(sftp, candidate.remote_path, paths.compressed_path)
                downloaded = True

            need_extract = (
                scope.force_download
                or downloaded
                or not paths.extracted_path.exists()
            )
            if need_extract:
                decompress_gzip_file(paths.compressed_path, paths.extracted_path)
                decompressed = True
            else:
                logger.info("Skipping existing extracted file: %s", paths.extracted_path)
        else:
            if paths.extracted_path.exists() and not scope.force_download:
                logger.info("Skipping existing extracted file: %s", paths.extracted_path)
            else:
                _download_streaming(sftp, candidate.remote_path, paths.extracted_path)
                downloaded = True
                decompressed = True

        compressed_size = (
            paths.compressed_path.stat().st_size
            if paths.compressed_path is not None and paths.compressed_path.exists()
            else 0
        )
        extracted_size = paths.extracted_path.stat().st_size if paths.extracted_path.exists() else 0
        compressed_hash = (
            sha256_file(paths.compressed_path)
            if paths.compressed_path is not None and paths.compressed_path.exists()
            else ""
        )
        content_hash = sha256_file(paths.extracted_path) if paths.extracted_path.exists() else ""

        return DownloadedFile(
            candidate=candidate,
            paths=paths,
            compressed_size=compressed_size,
            extracted_size=extracted_size,
            compressed_hash=compressed_hash,
            content_hash=content_hash,
            downloaded=downloaded,
            decompressed=decompressed,
        )
    except Exception as exc:
        logger.error("Failed to stage %s: %s", candidate.remote_path, exc)
        return DownloadedFile(
            candidate=candidate,
            paths=paths,
            compressed_size=0,
            extracted_size=0,
            compressed_hash="",
            content_hash="",
            downloaded=downloaded,
            decompressed=decompressed,
            error=str(exc),
        )


def stage_local_file(path: Path, candidate: RcniCandidate, scope: RcniScope) -> DownloadedFile:
    """Hash/validate an already-local sample without copying unless needed."""
    src = Path(path)
    paths = LocalStagingPaths(
        compressed_path=src if src.name.lower().endswith(".gz") else None,
        extracted_path=src if not src.name.lower().endswith(".gz") else src,
    )
    if src.name.lower().endswith(".gz"):
        staged = staging_paths(scope.local_root, candidate)
        staged.extracted_path.parent.mkdir(parents=True, exist_ok=True)
        decompress_gzip_file(src, staged.extracted_path)
        paths = LocalStagingPaths(compressed_path=src, extracted_path=staged.extracted_path)

    extracted = paths.extracted_path
    compressed = paths.compressed_path
    return DownloadedFile(
        candidate=candidate,
        paths=paths,
        compressed_size=compressed.stat().st_size if compressed and compressed.exists() else 0,
        extracted_size=extracted.stat().st_size if extracted.exists() else 0,
        compressed_hash=sha256_file(compressed) if compressed and compressed.exists() else "",
        content_hash=sha256_file(extracted) if extracted.exists() else "",
        downloaded=False,
        decompressed=src.name.lower().endswith(".gz"),
    )
