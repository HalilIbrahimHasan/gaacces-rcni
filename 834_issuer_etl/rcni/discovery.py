"""SFTP discovery of RCNI Monthly Discrepancy files under /archive/out/good/PAS."""

from __future__ import annotations

from dataclasses import dataclass, field

from ingestion.sftp_filters import partition_matches
from ingestion.sftp_ingestion import enumerate_remote_partitions, print_partition_diagnostics
from ingestion.sftp_tree_walk import (
    RemoteFileEntry,
    trace_remote_tree,
    walk_remote_files_with_stats,
)
from rcni.archive_path import ArchivePathMetadata, parse_archive_path
from rcni.filename import RcniFilenameMetadata, parse_rcni_filename
from rcni.matcher import logical_filename, rcni_sftp_reject_reason
from rcni.settings import RcniScope
from utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class RcniCandidate:
    issuer: str
    processing_year: str
    processing_month: str
    processing_day: str | None
    nested_relative: str | None
    remote_path: str
    filename: str
    logical_name: str
    filename_meta: RcniFilenameMetadata
    archive_meta: ArchivePathMetadata
    issuer_mismatch: bool
    skipped_reason: str | None = None

    @property
    def plan_year(self) -> str:
        return self.filename_meta.plan_year

    @property
    def plan_year_differs_from_processing_year(self) -> bool:
        return bool(self.plan_year) and bool(self.processing_year) and self.plan_year != self.processing_year


@dataclass
class DiscoveryResult:
    candidates: list[RcniCandidate] = field(default_factory=list)
    skipped: list[RcniCandidate] = field(default_factory=list)
    partitions: list[tuple[str, str, str]] = field(default_factory=list)
    folders_scanned: int = 0
    files_scanned: int = 0
    errors: list[str] = field(default_factory=list)
    partition_diagnostics: list = field(default_factory=list)
    listing_trace_lines: list[str] = field(default_factory=list)


def discover_rcni_candidates(
    sftp,
    scope: RcniScope,
    *,
    trace_listing: bool = False,
) -> DiscoveryResult:
    """
    1. list_remote_partitions (existing 834 issuer/year/month selection)
    2. recursive walk of ALL descendants under the month directory
    3. keep files matching the live RCNI gzip matcher
    """
    result = DiscoveryResult()
    partitions, diagnostics = enumerate_remote_partitions(
        sftp,
        scope.base_path,
        issuer_allow=scope.issuer_allow,
        year_allow=scope.year_allow,
        month_allow=scope.month_allow,
    )
    result.partitions = partitions
    result.partition_diagnostics = diagnostics
    print_partition_diagnostics(diagnostics)
    if not partitions:
        logger.warning(
            "No SFTP partitions matched issuer=%s year=%s month=%s under %s",
            scope.issuer_display,
            scope.year_display,
            scope.month_display,
            scope.base_path,
        )

    for issuer, year, month in partitions:
        month_norm = str(int(month)).zfill(2) if month.isdigit() else month
        month_path = f"{scope.base_path.rstrip('/')}/{issuer}/{year}/{month}"
        if trace_listing:
            print("\n======== RAW SFTP TREE (before matcher) ========")
            trace = trace_remote_tree(
                sftp, month_path, reject_reason=rcni_sftp_reject_reason
            )
            result.listing_trace_lines.extend(trace.lines)
            print("======== END RAW SFTP TREE ========\n")
            print(
                f"TRACE {issuer}/{year}/{month_norm}: "
                f"folders_visited={trace.folders_visited} "
                f"files_seen_before_matcher={trace.files_seen_before_matcher} "
                f"files_accepted_by_matcher={trace.files_accepted_by_matcher} "
                f"files_rejected_by_matcher={trace.files_rejected_by_matcher}"
            )
        files, stats = walk_remote_files_with_stats(sftp, month_path)
        result.folders_scanned += stats.folders_scanned
        result.files_scanned += stats.files_scanned
        result.errors.extend(stats.errors)
        logger.info(
            "Production walk %s/%s/%s folders=%d files_scanned=%d",
            issuer,
            year,
            month_norm,
            stats.folders_scanned,
            stats.files_scanned,
        )

        for entry in files:
            _classify_entry(result, entry, scope, issuer, year, month_norm)

    logger.info(
        "RCNI discovery: partitions=%d folders=%d files_scanned=%d candidates=%d skipped=%d",
        len(result.partitions),
        result.folders_scanned,
        result.files_scanned,
        len(result.candidates),
        len(result.skipped),
    )
    return result


def _classify_entry(
    result: DiscoveryResult,
    entry: RemoteFileEntry,
    scope: RcniScope,
    issuer: str,
    year: str,
    month: str,
) -> None:
    archive_meta = parse_archive_path(
        entry.remote_path,
        remote_root=scope.base_path,
        issuer=issuer,
        year=year,
        month=month,
    )
    filename_meta = parse_rcni_filename(entry.filename)
    issuer_mismatch = bool(
        filename_meta.parse_ok
        and filename_meta.issuer_id
        and filename_meta.issuer_id != issuer
    )

    candidate = RcniCandidate(
        issuer=issuer,
        processing_year=year,
        processing_month=month,
        processing_day=archive_meta.processing_day,
        nested_relative=archive_meta.nested_relative,
        remote_path=entry.remote_path,
        filename=entry.filename,
        logical_name=logical_filename(entry.filename),
        filename_meta=filename_meta,
        archive_meta=archive_meta,
        issuer_mismatch=issuer_mismatch,
    )

    reject_reason = rcni_sftp_reject_reason(entry.filename)
    if reject_reason is not None:
        candidate.skipped_reason = reject_reason
        result.skipped.append(candidate)
        return

    if not partition_matches(
        issuer, year, month, scope.issuer_allow, scope.year_allow, scope.month_allow
    ):
        candidate.skipped_reason = "filter_mismatch"
        result.skipped.append(candidate)
        return

    result.candidates.append(candidate)
    logger.info(
        "RCNI candidate issuer=%s proc=%s/%s/%s plan_year=%s file=%s path=%s%s",
        issuer,
        year,
        month,
        archive_meta.processing_day or "—",
        filename_meta.plan_year or "?",
        entry.filename,
        entry.remote_path,
        " ISSUER_MISMATCH" if issuer_mismatch else "",
    )
