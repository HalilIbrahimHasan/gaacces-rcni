"""Unlimited-depth recursive SFTP tree walk for 834 partitions."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterator

from ingestion.sftp_file_classifier import classify_sftp_filename, local_xml_name_from_remote
from utils.logger import get_logger

logger = get_logger(__name__)

_DIR_MODE = 0o040000
_MODE_MASK = 0o170000


def _is_dir_attr(entry) -> bool | None:
    mode = getattr(entry, "st_mode", None)
    if mode is None:
        return None
    return (mode & _MODE_MASK) == _DIR_MODE


def list_remote_attrs(sftp, path: str) -> list:
    """
    Complete directory listing.

    Some SFTP servers return only the first READDIR batch (often two names)
    unless further READDIR requests are pipelined. Union listdir_iter
    (pipelined) with listdir_attr so month/year enumeration is not capped.
    """
    by_name: dict[str, Any] = {}

    def _add(entry) -> None:
        name = getattr(entry, "filename", None)
        if not name or name in {".", ".."}:
            return
        by_name[name] = entry

    if hasattr(sftp, "listdir_iter"):
        try:
            try:
                iterator = sftp.listdir_iter(path, read_aheads=50)
            except TypeError:
                iterator = sftp.listdir_iter(path)
            for entry in iterator:
                _add(entry)
        except OSError as exc:
            logger.warning("listdir_iter failed for %s: %s", path, exc)
        except EOFError:
            pass

    if hasattr(sftp, "listdir_attr"):
        try:
            for entry in sftp.listdir_attr(path):
                _add(entry)
        except OSError as exc:
            logger.warning("listdir_attr failed for %s: %s", path, exc)

    if not by_name and hasattr(sftp, "listdir"):
        try:
            for name in sftp.listdir(path):
                _add(type("Ent", (), {"filename": name, "st_mode": None})())
        except OSError as exc:
            logger.warning("Cannot list directory %s: %s", path, exc)
            return []

    return list(by_name.values())


@dataclass
class PartitionWalkResult:
    issuer: str
    year: str
    month: str
    folders_scanned: int = 0
    max_depth: int = 0
    files_scanned: int = 0
    valid_xml: int = 0
    valid_xml_gz: int = 0
    valid_xml_xz: int = 0
    skipped_to: int = 0
    skipped_report: int = 0
    skipped_tracking: int = 0
    skipped_edi: int = 0
    skipped_other: int = 0
    valid_files: list[dict[str, Any]] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    @property
    def valid_total(self) -> int:
        return self.valid_xml + self.valid_xml_gz + self.valid_xml_xz


def list_remote_dirs(sftp, path: str) -> list[str]:
    dirs: list[str] = []
    for entry in list_remote_attrs(sftp, path):
        is_dir = _is_dir_attr(entry)
        if is_dir is False:
            continue
        dirs.append(entry.filename)
    return sorted(dirs)


def list_remote_files(sftp, path: str) -> list[str]:
    files: list[str] = []
    for entry in list_remote_attrs(sftp, path):
        is_dir = _is_dir_attr(entry)
        if is_dir is True:
            continue
        files.append(entry.filename)
    return sorted(files)


def _walk_folder(
    sftp,
    issuer: str,
    year: str,
    month: str,
    remote_path: str,
    depth: int,
    result: PartitionWalkResult,
) -> None:
    result.folders_scanned += 1
    result.max_depth = max(result.max_depth, depth)

    subdirs = list_remote_dirs(sftp, remote_path)
    files = list_remote_files(sftp, remote_path)

    logger.info(
        "Entering folder depth=%d path=%s subfolders=%d files=%d",
        depth,
        remote_path,
        len(subdirs),
        len(files),
    )

    for filename in files:
        result.files_scanned += 1
        classification = classify_sftp_filename(issuer, filename)

        if classification == "valid_xml":
            result.valid_xml += 1
            result.valid_files.append(
                {
                    "remote_path": f"{remote_path}/{filename}",
                    "filename": filename,
                    "local_name": local_xml_name_from_remote(filename),
                    "format": "xml",
                }
            )
            logger.info("Valid file (xml): %s/%s", remote_path, filename)
        elif classification == "valid_gz":
            result.valid_xml_gz += 1
            result.valid_files.append(
                {
                    "remote_path": f"{remote_path}/{filename}",
                    "filename": filename,
                    "local_name": local_xml_name_from_remote(filename),
                    "format": "gz",
                }
            )
            logger.info("Valid file (gz): %s/%s", remote_path, filename)
        elif classification == "valid_xz":
            result.valid_xml_xz += 1
            result.valid_files.append(
                {
                    "remote_path": f"{remote_path}/{filename}",
                    "filename": filename,
                    "local_name": local_xml_name_from_remote(filename),
                    "format": "xz",
                }
            )
            logger.info("Valid file (xz): %s/%s", remote_path, filename)
        elif classification == "to":
            result.skipped_to += 1
            logger.debug("Skipped (to_): %s/%s", remote_path, filename)
        elif classification == "report":
            result.skipped_report += 1
            logger.debug("Skipped (report): %s/%s", remote_path, filename)
        elif classification == "tracking":
            result.skipped_tracking += 1
            logger.debug("Skipped (tracking): %s/%s", remote_path, filename)
        elif classification == "edi":
            result.skipped_edi += 1
            logger.debug("Skipped (edi): %s/%s", remote_path, filename)
        elif classification in ("summary", "log", "other"):
            result.skipped_other += 1
            logger.debug("Skipped (%s): %s/%s", classification, remote_path, filename)

    for subdir in subdirs:
        child_path = f"{remote_path}/{subdir}"
        _walk_folder(sftp, issuer, year, month, child_path, depth + 1, result)


def walk_partition(
    sftp,
    issuer: str,
    year: str,
    month: str,
    remote_root: str,
) -> PartitionWalkResult:
    """Recursively scan SFTP_ROOT/{issuer}/{year}/{month} with unlimited depth."""
    month_path = f"{remote_root.rstrip('/')}/{issuer}/{year}/{month}"
    result = PartitionWalkResult(issuer=issuer, year=year, month=month)

    logger.info(
        "Walking partition %s/%s/%s starting at %s",
        issuer,
        year,
        month,
        month_path,
    )

    try:
        sftp.stat(month_path)
    except OSError as exc:
        msg = f"Partition path not found: {month_path} ({exc})"
        logger.warning(msg)
        result.errors.append(msg)
        return result

    _walk_folder(sftp, issuer, year, month, month_path, depth=0, result=result)

    logger.info(
        "Partition %s/%s/%s complete: folders=%d max_depth=%d files_scanned=%d "
        "valid_xml=%d valid_gz=%d valid_xz=%d valid_total=%d",
        issuer,
        year,
        month,
        result.folders_scanned,
        result.max_depth,
        result.files_scanned,
        result.valid_xml,
        result.valid_xml_gz,
        result.valid_xml_xz,
        result.valid_total,
    )
    return result


# Backward-compatible aliases used by older call sites / tests.
_list_dirs = list_remote_dirs
_list_files = list_remote_files


@dataclass(frozen=True)
class RemoteFileEntry:
    """One remote file found during an unlimited-depth SFTP walk."""

    remote_path: str
    filename: str
    parent_path: str
    depth: int


@dataclass
class RemoteWalkStats:
    folders_scanned: int = 0
    max_depth: int = 0
    files_scanned: int = 0
    errors: list[str] = field(default_factory=list)


def walk_remote_files(sftp, start_path: str) -> Iterator[RemoteFileEntry]:
    """
    Recursively yield every file under start_path (unlimited depth).

    File-type filtering is the caller's responsibility so 834 XML and RCNI
    CSV matchers can share the same traversal.
    """
    yield from _walk_remote_files(sftp, start_path, depth=0, stats=None)


def walk_remote_files_with_stats(
    sftp,
    start_path: str,
) -> tuple[list[RemoteFileEntry], RemoteWalkStats]:
    stats = RemoteWalkStats()
    try:
        sftp.stat(start_path)
    except OSError as exc:
        msg = f"Path not found: {start_path} ({exc})"
        logger.warning(msg)
        stats.errors.append(msg)
        return [], stats

    files = list(_walk_remote_files(sftp, start_path, depth=0, stats=stats))
    return files, stats


def _walk_remote_files(
    sftp,
    remote_path: str,
    depth: int,
    stats: RemoteWalkStats | None,
) -> Iterator[RemoteFileEntry]:
    if stats is not None:
        stats.folders_scanned += 1
        stats.max_depth = max(stats.max_depth, depth)

    subdirs = list_remote_dirs(sftp, remote_path)
    files = list_remote_files(sftp, remote_path)

    logger.info(
        "Entering folder depth=%d path=%s subfolders=%d files=%d",
        depth,
        remote_path,
        len(subdirs),
        len(files),
    )

    for filename in files:
        if stats is not None:
            stats.files_scanned += 1
        yield RemoteFileEntry(
            remote_path=f"{remote_path.rstrip('/')}/{filename}",
            filename=filename,
            parent_path=remote_path,
            depth=depth,
        )

    for subdir in subdirs:
        child_path = f"{remote_path.rstrip('/')}/{subdir}"
        yield from _walk_remote_files(sftp, child_path, depth + 1, stats)
