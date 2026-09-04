"""Unlimited-depth recursive SFTP tree walk for 834 partitions."""

from __future__ import annotations

import stat as statmod
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Iterator, TextIO

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


def _entry_type_label(entry) -> str:
    is_dir = _is_dir_attr(entry)
    if is_dir is True:
        return "directory"
    if is_dir is False:
        return "file"
    return "unknown"


def _safe_listing_names(entries: list, *, from_strings: bool = False) -> list[str]:
    names: list[str] = []
    for entry in entries:
        name = entry if from_strings else getattr(entry, "filename", None)
        if not name or name in {".", ".."}:
            continue
        names.append(name)
    return names


def probe_listing_apis(sftp, path: str) -> dict[str, Any]:
    """
    Call listdir / listdir_attr / listdir_iter independently.

    Do not union the results. Errors are recorded per API so a truncated
    READDIR can be attributed to a specific method.
    """
    result: dict[str, Any] = {
        "path": path,
        "listdir": [],
        "listdir_error": None,
        "listdir_attr": [],
        "listdir_attr_error": None,
        "listdir_iter": [],
        "listdir_iter_error": None,
    }

    if hasattr(sftp, "listdir"):
        try:
            result["listdir"] = [
                name for name in sftp.listdir(path) if name not in {".", ".."}
            ]
        except Exception as exc:  # noqa: BLE001 — diagnostic must show the raw API failure
            result["listdir_error"] = f"{type(exc).__name__}: {exc}"
    else:
        result["listdir_error"] = "listdir not available"

    if hasattr(sftp, "listdir_attr"):
        try:
            result["listdir_attr"] = list(sftp.listdir_attr(path))
        except Exception as exc:  # noqa: BLE001
            result["listdir_attr_error"] = f"{type(exc).__name__}: {exc}"
    else:
        result["listdir_attr_error"] = "listdir_attr not available"

    if hasattr(sftp, "listdir_iter"):
        try:
            try:
                iterator = sftp.listdir_iter(path, read_aheads=50)
            except TypeError:
                iterator = sftp.listdir_iter(path)
            try:
                result["listdir_iter"] = list(iterator)
            finally:
                close = getattr(iterator, "close", None)
                if callable(close):
                    close()
        except Exception as exc:  # noqa: BLE001
            result["listdir_iter_error"] = f"{type(exc).__name__}: {exc}"
    else:
        result["listdir_iter_error"] = "listdir_iter not available"

    return result


def _print_probe(probe: dict[str, Any], emit: Callable[[str], None]) -> None:
    listdir_names = probe["listdir"]
    attr_entries = probe["listdir_attr"]
    iter_entries = probe["listdir_iter"]
    attr_names = _safe_listing_names(attr_entries)
    iter_names = _safe_listing_names(iter_entries)

    emit(f"REMOTE DIR: {probe['path']}")
    emit(
        "LISTING COUNTS: "
        f"listdir()={len(listdir_names)}"
        f"{'' if not probe['listdir_error'] else ' ERROR=' + probe['listdir_error']}  "
        f"listdir_attr()={len(attr_names)}"
        f"{'' if not probe['listdir_attr_error'] else ' ERROR=' + probe['listdir_attr_error']}  "
        f"listdir_iter()={len(iter_names)}"
        f"{'' if not probe['listdir_iter_error'] else ' ERROR=' + probe['listdir_iter_error']}"
    )

    # Print every raw attr-bearing entry (attr and iter). Names-only listdir
    # rows are included when they did not appear in attr/iter.
    printed: set[str] = set()
    for source, entries, from_strings in (
        ("listdir_attr", attr_entries, False),
        ("listdir_iter", iter_entries, False),
        ("listdir", listdir_names, True),
    ):
        for entry in entries:
            if from_strings:
                name = entry
                mode = None
                size = None
                kind = "unknown"
            else:
                name = getattr(entry, "filename", None)
                mode = getattr(entry, "st_mode", None)
                size = getattr(entry, "st_size", None)
                kind = _entry_type_label(entry)
            if not name or name in {".", ".."}:
                continue
            key = f"{source}:{name}"
            if key in printed:
                continue
            printed.add(key)
            full_path = f"{probe['path'].rstrip('/')}/{name}"
            emit("ENTRY:")
            emit(f"name={name}")
            emit(f"type={kind}")
            emit(f"mode={mode if mode is not None else ''}")
            emit(f"size={size if size is not None else ''}")
            emit(f"full_path={full_path}")
            emit(f"api={source}")


def _stat_is_dir(sftp, path: str) -> bool | None:
    try:
        attr = sftp.stat(path)
    except OSError:
        return None
    mode = getattr(attr, "st_mode", None)
    if mode is None:
        return None
    return bool(statmod.S_ISDIR(mode))


@dataclass
class ListingTraceResult:
    folders_visited: int = 0
    files_seen_before_matcher: int = 0
    files_accepted_by_matcher: int = 0
    files_rejected_by_matcher: int = 0
    rejected: list[tuple[str, str]] = field(default_factory=list)
    accepted: list[str] = field(default_factory=list)
    lines: list[str] = field(default_factory=list)


def trace_remote_tree(
    sftp,
    start_path: str,
    *,
    reject_reason,
    file: TextIO | None = None,
) -> ListingTraceResult:
    """
    Print the complete raw tree under start_path before matcher filtering.

    Each directory is probed with listdir / listdir_attr / listdir_iter
    independently (no union). Recursion uses any name that any API returned
    plus a follow-up stat() so a wrong st_mode cannot hide child folders.
    """
    result = ListingTraceResult()

    def emit(line: str) -> None:
        result.lines.append(line)
        print(line)
        if file is not None:
            file.write(line + "\n")

    emit("ROOT:")
    emit(start_path)
    emit("")

    def _walk(path: str) -> None:
        result.folders_visited += 1
        probe = probe_listing_apis(sftp, path)
        _print_probe(probe, emit)

        child_names: dict[str, Any] = {}
        for entry in probe["listdir_attr"] + probe["listdir_iter"]:
            name = getattr(entry, "filename", None)
            if name and name not in {".", ".."}:
                child_names[name] = entry
        for name in probe["listdir"]:
            if name not in child_names:
                child_names[name] = type("Ent", (), {"filename": name, "st_mode": None})()

        files: list[str] = []
        dirs: list[str] = []
        for name, entry in sorted(child_names.items()):
            full_path = f"{path.rstrip('/')}/{name}"
            listing_kind = _entry_type_label(entry)
            st_dir = _stat_is_dir(sftp, full_path)
            emit(
                f"CLASSIFY: name={name} listing_type={listing_kind} "
                f"stat_isdir={st_dir if st_dir is not None else 'stat-failed'}"
            )
            is_dir = st_dir if st_dir is not None else (listing_kind == "directory" or listing_kind == "unknown")
            if is_dir:
                dirs.append(name)
            else:
                files.append(name)

        emit(
            f"DIR SUMMARY: path={path} unique_names={len(child_names)} "
            f"classified_dirs={len(dirs)} classified_files={len(files)}"
        )
        emit("")

        for filename in files:
            result.files_seen_before_matcher += 1
            reason = reject_reason(filename)
            if reason is None:
                result.files_accepted_by_matcher += 1
                result.accepted.append(f"{path.rstrip('/')}/{filename}")
            else:
                result.files_rejected_by_matcher += 1
                result.rejected.append((filename, reason))
                emit("REJECTED FILE:")
                emit(filename)
                emit(f"reason={reason}")
                emit("")

        for subdir in dirs:
            _walk(f"{path.rstrip('/')}/{subdir}")

    try:
        sftp.stat(start_path)
    except OSError as exc:
        emit(f"ROOT STAT FAILED: {start_path} ({exc})")
        return result

    _walk(start_path)
    emit(f"folders_visited={result.folders_visited}")
    emit(f"files_seen_before_matcher={result.files_seen_before_matcher}")
    emit(f"files_accepted_by_matcher={result.files_accepted_by_matcher}")
    emit(f"files_rejected_by_matcher={result.files_rejected_by_matcher}")
    if result.accepted:
        emit("ACCEPTED FILES:")
        for path in result.accepted:
            emit(f"  {path}")
    return result


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
