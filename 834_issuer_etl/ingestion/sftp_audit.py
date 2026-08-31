"""
SFTP audit mode — recursively scan remote folders at any depth without downloading.
"""

from __future__ import annotations

from pathlib import Path

from config.config import settings
from ingestion.sftp_filters import log_effective_filters
from ingestion.sftp_ingestion import list_remote_partitions, sftp_connection
from ingestion.sftp_summary import (
    PartitionSummary,
    count_local_xmls,
    export_summaries,
    print_console_summary,
)
from ingestion.sftp_tree_walk import walk_partition
from utils.logger import get_logger

logger = get_logger(__name__)


def audit_partition(
    sftp,
    remote_root: str,
    issuer: str,
    year: str,
    month: str,
    local_root: Path,
) -> PartitionSummary:
    """Audit one issuer/year/month partition without downloading."""
    from ingestion.sftp_ingestion import _normalize_month

    month_norm = _normalize_month(month)
    walk = walk_partition(sftp, issuer, year, month, remote_root)

    remote_names = {e["local_name"] for e in walk.valid_files}
    local_count = count_local_xmls(local_root, issuer, year, month_norm)
    local_month = local_root / issuer / year / month_norm
    local_names = (
        {p.name for p in local_month.glob("*.xml") if p.is_file()}
        if local_month.is_dir()
        else set()
    )
    missing = remote_names - local_names

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
        local_xml_final_count=local_count,
        missing_count=len(missing),
    )

    logger.info(
        "SFTP AUDIT %s/%s/%s | folders=%d max_depth=%d files_scanned=%d "
        "valid_xml=%d valid_gz=%d valid_xz=%d valid_total=%d "
        "local_xml=%d missing=%d",
        issuer,
        year,
        month_norm,
        summary.folders_scanned,
        summary.max_depth,
        summary.files_scanned,
        summary.valid_xml,
        summary.valid_xml_gz,
        summary.valid_xml_xz,
        summary.valid_total,
        summary.local_xml_final_count,
        summary.missing_count,
    )

    if missing:
        sample = sorted(missing)[:20]
        logger.warning(
            "SFTP AUDIT %s/%s/%s missing local XML (%d): %s%s",
            issuer,
            year,
            month_norm,
            len(missing),
            "; ".join(sample),
            " ..." if len(missing) > 20 else "",
        )

    if walk.valid_files:
        sample_paths = [e["remote_path"] for e in walk.valid_files[:5]]
        logger.info(
            "SFTP AUDIT sample valid paths %s/%s/%s: %s",
            issuer,
            year,
            month_norm,
            "; ".join(sample_paths),
        )

    return summary


def run_sftp_audit(
    host: str,
    port: int,
    username: str,
    password: str,
    remote_root: str,
    local_root: Path | None = None,
    issuer_allow: set[str] | None = None,
    year_allow: set[str] | None = None,
    month_allow: set[str] | None = None,
    reports_dir: Path | None = None,
) -> list[PartitionSummary]:
    """
    Connect to SFTP, recursively audit remote folders.

    Does not download or modify any local files.
    """
    local_root = local_root or settings.source_data_path
    reports_dir = reports_dir or settings.reports_path

    log_effective_filters(issuer_allow, year_allow, month_allow)
    logger.info("SFTP AUDIT ONLY — recursive unlimited depth (no download)")

    summaries: list[PartitionSummary] = []

    with sftp_connection(host, port, username, password) as sftp:
        partitions = list_remote_partitions(
            sftp, remote_root, issuer_allow, year_allow, month_allow,
        )
        if not partitions:
            logger.warning("SFTP audit: no remote partitions matched filters")

        for issuer, year, month in partitions:
            summaries.append(
                audit_partition(sftp, remote_root, issuer, year, month, local_root)
            )

    print_console_summary(summaries)
    export_summaries(summaries, reports_dir)
    logger.info("SFTP AUDIT COMPLETE — %d partition(s)", len(summaries))
    return summaries
