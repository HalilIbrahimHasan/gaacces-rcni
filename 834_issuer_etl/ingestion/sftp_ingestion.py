"""
SFTP ingestion — download 834 XML from remote folders at any depth.

Remote start path per partition:
    SFTP_ROOT/{issuer}/{year}/{month}

Then recursively walks all subfolders (unlimited depth).

Local layout (unchanged):
    source_data/{issuer}/{year}/{month}/*.xml
"""

from __future__ import annotations

import gzip
import lzma
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path

import paramiko

from config.config import settings
from ingestion.sftp_file_classifier import local_xml_name_from_remote
from ingestion.sftp_filters import log_effective_filters
from ingestion.sftp_summary import (
    PartitionSummary,
    count_local_xmls,
    export_summaries,
    print_console_summary,
)
from ingestion.sftp_tree_walk import list_remote_dirs, walk_partition
from utils.logger import get_logger

logger = get_logger(__name__)


@contextmanager
def sftp_connection(
    host: str,
    port: int,
    username: str,
    password: str,
) -> Iterator:
    """
    Same Paramiko Transport connect used by ingest_from_sftp / SFTPSourceConnector.

    Credentials: SFTP_HOST, SFTP_PORT, SFTP_USERNAME (or SFTP_USER), SFTP_PASSWORD.
    """
    if not host or not username:
        raise RuntimeError(
            "SFTP credentials missing. Set SFTP_HOST, SFTP_USERNAME, SFTP_PASSWORD in .env"
        )
    transport = paramiko.Transport((host, int(port)))
    sftp = None
    try:
        transport.connect(username=username, password=password)
        sftp = paramiko.SFTPClient.from_transport(transport)
        yield sftp
        if sftp is not None:
            sftp.close()
    finally:
        transport.close()


def _issuer_ok(name: str) -> bool:
    return name.isdigit() and len(name) == 5


def _year_ok(name: str) -> bool:
    return name.isdigit() and len(name) == 4


def _month_ok(name: str) -> bool:
    if not name.isdigit() or len(name) > 2:
        return False
    return 1 <= int(name) <= 12


def _normalize_month(name: str) -> str:
    return str(int(name)).zfill(2)


@dataclass
class IssuerPartitionDiagnostic:
    issuer: str
    issuer_path: str
    year_directories_found: list[str]
    years_accepted: list[str]
    years_rejected: list[tuple[str, str]]
    months_found_by_year: dict[str, list[str]] = field(default_factory=dict)
    months_accepted_by_year: dict[str, list[str]] = field(default_factory=dict)
    months_rejected_by_year: dict[str, list[tuple[str, str]]] = field(default_factory=dict)


def _reject_reason_issuer(name: str, issuer_allow: set[str] | None) -> str | None:
    if not _issuer_ok(name):
        return "not a 5-digit issuer directory"
    if issuer_allow is not None and name not in issuer_allow:
        return f"issuer filter {sorted(issuer_allow)}"
    return None


def _reject_reason_year(name: str, year_allow: set[str] | None) -> str | None:
    if not _year_ok(name):
        return "not a 4-digit year directory"
    if year_allow is not None and name not in year_allow:
        return f"year filter {sorted(year_allow)}"
    return None


def _reject_reason_month(name: str, month_allow: set[str] | None) -> str | None:
    if not _month_ok(name):
        return "not a calendar month directory (01-12)"
    month_norm = _normalize_month(name)
    if month_allow is not None and month_norm not in month_allow:
        return f"month filter {sorted(month_allow)}"
    return None


def enumerate_remote_partitions(
    sftp,
    remote_root: str,
    issuer_allow: set[str] | None = None,
    year_allow: set[str] | None = None,
    month_allow: set[str] | None = None,
) -> tuple[list[tuple[str, str, str]], list[IssuerPartitionDiagnostic]]:
    """
    Enumerate (issuer, year, month) partitions using complete directory listings.

    Empty allow-sets (None) mean ALL. There is no month cap.
    """
    partitions: list[tuple[str, str, str]] = []
    diagnostics: list[IssuerPartitionDiagnostic] = []
    root = remote_root.rstrip("/")

    try:
        issuer_dirs = list_remote_dirs(sftp, root)
    except OSError as exc:
        logger.error("Cannot list SFTP root %s: %s", root, exc)
        return partitions, diagnostics

    for issuer in issuer_dirs:
        issuer_reason = _reject_reason_issuer(issuer, issuer_allow)
        if issuer_reason:
            logger.info("Skipping SFTP issuer entry %s: %s", issuer, issuer_reason)
            continue
        issuer_path = f"{root}/{issuer}"
        year_dirs = list_remote_dirs(sftp, issuer_path)
        diag = IssuerPartitionDiagnostic(
            issuer=issuer,
            issuer_path=issuer_path,
            year_directories_found=list(year_dirs),
            years_accepted=[],
            years_rejected=[],
        )
        for year in year_dirs:
            year_reason = _reject_reason_year(year, year_allow)
            if year_reason:
                diag.years_rejected.append((year, year_reason))
                continue
            diag.years_accepted.append(year)
            year_path = f"{issuer_path}/{year}"
            month_dirs = list_remote_dirs(sftp, year_path)
            diag.months_found_by_year[year] = list(month_dirs)
            accepted: list[str] = []
            rejected: list[tuple[str, str]] = []
            for month in month_dirs:
                month_reason = _reject_reason_month(month, month_allow)
                if month_reason:
                    rejected.append((month, month_reason))
                    continue
                accepted.append(_normalize_month(month))
                partitions.append((issuer, year, month))
            diag.months_accepted_by_year[year] = accepted
            diag.months_rejected_by_year[year] = rejected
        diagnostics.append(diag)

    logger.info("SFTP partitions selected: %d", len(partitions))
    for p in partitions:
        logger.info("  partition: %s/%s/%s", *p)
    return partitions, diagnostics


def list_remote_partitions(
    sftp,
    remote_root: str,
    issuer_allow: set[str] | None = None,
    year_allow: set[str] | None = None,
    month_allow: set[str] | None = None,
) -> list[tuple[str, str, str]]:
    """List (issuer, year, month) tuples present on SFTP matching filters."""
    partitions, _ = enumerate_remote_partitions(
        sftp, remote_root, issuer_allow, year_allow, month_allow
    )
    return partitions


def print_partition_diagnostics(diagnostics: list[IssuerPartitionDiagnostic]) -> None:
    print("\nRCNI PARTITION DIAGNOSTIC")
    print("-" * 100)
    if not diagnostics:
        print("  (no issuer directories listed)")
        print("-" * 100)
        return
    for diag in diagnostics:
        print(f"issuer {diag.issuer}  path={diag.issuer_path}")
        print(f"  year directories found : {diag.year_directories_found or '[]'}")
        print(f"  years accepted         : {diag.years_accepted or '[]'}")
        if diag.years_rejected:
            print("  years rejected:")
            for name, reason in diag.years_rejected:
                print(f"    {name}: {reason}")
        for year in diag.years_accepted:
            found = diag.months_found_by_year.get(year, [])
            accepted = diag.months_accepted_by_year.get(year, [])
            rejected = diag.months_rejected_by_year.get(year, [])
            print(f"  under {year} month directories found: {found or '[]'}")
            print(f"    months accepted: {accepted or '[]'}")
            if rejected:
                print("    months rejected:")
                for name, reason in rejected:
                    print(f"      {name}: {reason}")
            else:
                print("    months rejected: []")
        print()
    print("-" * 100)


def _decompress_remote_bytes(filename: str, raw: bytes) -> bytes:
    lower = filename.lower()
    if lower.endswith(".xml.xz"):
        return lzma.decompress(raw)
    if lower.endswith(".xml.gz"):
        return gzip.decompress(raw)
    return raw


def _download_remote_file(
    sftp,
    remote_file: str,
    filename: str,
    local_path: Path,
    *,
    keep_compressed: bool = False,
) -> bool:
    try:
        with sftp.open(remote_file, "rb") as remote_f:
            raw = remote_f.read()

        lower = filename.lower()
        if lower.endswith((".xml.gz", ".xml.xz")):
            xml_bytes = _decompress_remote_bytes(filename, raw)
            local_path.write_bytes(xml_bytes)
            logger.info(
                "Decompressed %s → %s (%d bytes)",
                remote_file,
                local_path,
                len(xml_bytes),
            )
            if keep_compressed:
                compressed_path = local_path.parent / filename
                compressed_path.write_bytes(raw)
                logger.info("Kept compressed copy: %s", compressed_path)
        else:
            local_path.write_bytes(raw)
            logger.info(
                "Downloaded %s → %s (%d bytes)",
                remote_file,
                local_path,
                len(raw),
            )
        return True
    except Exception as exc:
        logger.error("Failed download/decompress %s: %s", remote_file, exc)
        return False


def download_partition(
    sftp,
    remote_root: str,
    issuer: str,
    year: str,
    month: str,
    local_root: Path,
    *,
    force_download: bool = False,
    keep_compressed: bool = False,
) -> PartitionSummary:
    """Recursively download valid files under the month folder into flat local month dir."""
    month_norm = _normalize_month(month)
    local_month = local_root / issuer / year / month_norm
    local_month.mkdir(parents=True, exist_ok=True)

    walk = walk_partition(sftp, issuer, year, month, remote_root)
    summary = PartitionSummary(
        issuer=issuer,
        year=year,
        month=month_norm,
        folders_scanned=walk.folders_scanned,
        max_depth=walk.max_depth,
        files_scanned=walk.files_scanned,
        valid_xml=walk.valid_xml,
        valid_xml_gz=walk.valid_xml_gz,
        valid_xml_xz=walk.valid_xml_xz,
        skipped_to_files=walk.skipped_to,
        skipped_report_files=walk.skipped_report,
        skipped_tracking_files=walk.skipped_tracking,
        skipped_edi_files=walk.skipped_edi,
        skipped_other_files=walk.skipped_other,
    )

    for entry in walk.valid_files:
        local_xml_name = entry["local_name"]
        local_path = local_month / local_xml_name
        if local_path.exists() and not force_download:
            summary.existing += 1
            logger.info("Skipping existing local XML: %s", local_path)
            continue

        if _download_remote_file(
            sftp,
            entry["remote_path"],
            entry["filename"],
            local_path,
            keep_compressed=keep_compressed,
        ):
            summary.downloaded += 1
        else:
            summary.failed += 1

    summary.local_xml_final_count = count_local_xmls(local_root, issuer, year, month_norm)
    remote_names = {e["local_name"] for e in walk.valid_files}
    local_names = {p.name for p in local_month.glob("*.xml") if p.is_file()}
    summary.missing_count = len(remote_names - local_names)

    logger.info(
        "Partition %s/%s/%s download: downloaded=%d existing=%d failed=%d "
        "local_final=%d missing=%d",
        issuer,
        year,
        month_norm,
        summary.downloaded,
        summary.existing,
        summary.failed,
        summary.local_xml_final_count,
        summary.missing_count,
    )
    return summary


def ingest_from_sftp(
    host: str,
    port: int,
    username: str,
    password: str,
    remote_root: str,
    local_root: Path | None = None,
    issuer_allow: set[str] | None = None,
    year_allow: set[str] | None = None,
    month_allow: set[str] | None = None,
    *,
    force_download: bool = False,
    keep_compressed: bool = False,
    reports_dir: Path | None = None,
) -> tuple[int, list[PartitionSummary]]:
    """
    Connect to SFTP, download valid XML files, flatten into source_data.

    Returns (total_downloaded, partition summaries).
    """
    local_root = local_root or settings.source_data_path
    reports_dir = reports_dir or settings.reports_path
    local_root.mkdir(parents=True, exist_ok=True)

    log_effective_filters(issuer_allow, year_allow, month_allow)

    summaries: list[PartitionSummary] = []
    total_downloaded = 0

    with sftp_connection(host, port, username, password) as sftp:
        partitions = list_remote_partitions(
            sftp, remote_root, issuer_allow, year_allow, month_allow,
        )
        for issuer, year, month in partitions:
            summary = download_partition(
                sftp,
                remote_root,
                issuer,
                year,
                month,
                local_root,
                force_download=force_download,
                keep_compressed=keep_compressed,
            )
            summaries.append(summary)
            total_downloaded += summary.downloaded

    print_console_summary(summaries)
    export_summaries(summaries, reports_dir)
    logger.info("SFTP ingestion complete: %d file(s) downloaded", total_downloaded)
    return total_downloaded, summaries
