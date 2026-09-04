"""RCNI Phase 1 pipeline: discover, optional download, validate. No Azure SQL."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ingestion.sftp_ingestion import sftp_connection
from rcni.archive_path import parse_archive_path
from rcni.constants import (
    ISSUE_FILENAME_ISSUER_MISMATCH,
    ISSUE_FILENAME_UNPARSEABLE,
    STATUS_CLEAN,
    STATUS_FILENAME_METADATA_MISMATCH,
    STATUS_MALFORMED,
    STATUS_SCHEMA_MISMATCH,
    STATUS_WARNING,
    STRUCTURAL_ISSUE_TYPES,
)
from rcni.csv_validator import CsvValidationResult, RowIssue, validate_rcni_csv
from rcni.discovery import DiscoveryResult, RcniCandidate, discover_rcni_candidates
from rcni.download import DownloadedFile, download_and_stage, stage_local_file
from rcni.duplicates import IdentityRecord, detect_duplicates
from rcni.filename import parse_rcni_filename
from rcni.matcher import is_rcni_local_file, logical_filename
from rcni.reports import (
    FileValidationSummary,
    print_candidate_inventory,
    write_data_quality_warnings,
    write_discovery_inventory,
    write_run_manifest,
    write_structural_malformed,
    write_validation_summary,
)
from rcni.settings import RcniScope
from rcni.status import overall_status
from utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class Phase1Result:
    scope: RcniScope
    mode: str
    discovery: DiscoveryResult
    inventory_rows: list[dict[str, Any]]
    summaries: list[FileValidationSummary] = field(default_factory=list)
    issues: list[RowIssue] = field(default_factory=list)
    report_paths: dict[str, str] = field(default_factory=dict)
    azure_sql_writes: bool = False
    source_files_modified: bool = False


def _candidate_inventory_row(candidate: RcniCandidate) -> dict[str, Any]:
    meta = candidate.filename_meta
    return {
        "issuer": candidate.issuer,
        "processing_year": candidate.processing_year,
        "processing_month": candidate.processing_month,
        "processing_day": candidate.processing_day,
        "plan_year": meta.plan_year,
        "filename": candidate.filename,
        "logical_filename": candidate.logical_name,
        "source_path": candidate.remote_path,
        "nested_relative": candidate.nested_relative,
        "file_timestamp": meta.file_timestamp,
        "parsed_timestamp": meta.parsed_timestamp_display,
        "issuer_mismatch": candidate.issuer_mismatch,
        "plan_year_differs_from_processing_year": candidate.plan_year_differs_from_processing_year,
        "filename_parse_ok": meta.parse_ok,
    }


def _filename_flags(candidate: RcniCandidate) -> tuple[list[str], list[RowIssue]]:
    flags: list[str] = []
    issues: list[RowIssue] = []
    if not candidate.filename_meta.parse_ok:
        flags.append(STATUS_FILENAME_METADATA_MISMATCH)
        issues.append(
            RowIssue(
                source_file=candidate.filename,
                source_path=candidate.remote_path,
                record_number=None,
                physical_line_number=None,
                issue_type=ISSUE_FILENAME_UNPARSEABLE,
                issue_description=candidate.filename_meta.parse_error or "Unparseable RCNI filename",
                expected_column_count=19,
                observed_column_count=None,
                column_name=None,
                bad_value=candidate.filename,
                raw_record=candidate.filename,
            )
        )
    if candidate.issuer_mismatch:
        flags.append(STATUS_FILENAME_METADATA_MISMATCH)
        issues.append(
            RowIssue(
                source_file=candidate.filename,
                source_path=candidate.remote_path,
                record_number=None,
                physical_line_number=None,
                issue_type=ISSUE_FILENAME_ISSUER_MISMATCH,
                issue_description=(
                    f"RED FLAG: filename issuer {candidate.filename_meta.issuer_id} "
                    f"does not match directory issuer {candidate.issuer}. "
                    "Neither value was rewritten."
                ),
                expected_column_count=19,
                observed_column_count=None,
                column_name="issuer_id",
                bad_value=candidate.filename_meta.issuer_id,
                raw_record=candidate.filename,
            )
        )
    return flags, issues


def _identity_record(candidate: RcniCandidate, content_hash: str, source_path: str | None = None) -> IdentityRecord:
    meta = candidate.filename_meta
    return IdentityRecord(
        issuer_id=meta.issuer_id,
        document_type=meta.document_type,
        plan_year=meta.plan_year,
        file_timestamp=meta.file_timestamp,
        content_hash=content_hash,
        source_path=source_path or candidate.remote_path,
        source_file=candidate.filename,
    )


def run_discover_only(scope: RcniScope) -> Phase1Result:
    logger.info(
        "RCNI discover-only — base=%s issuer=%s year=%s month=%s (no download, no SQL)",
        scope.base_path,
        scope.issuer_display,
        scope.year_display,
        scope.month_display,
    )
    scope.reports_dir.mkdir(parents=True, exist_ok=True)
    scope.logs_dir.mkdir(parents=True, exist_ok=True)

    with sftp_connection(
        scope.sftp_host,
        scope.sftp_port,
        scope.sftp_user,
        scope.sftp_password,
    ) as sftp:
        discovery = discover_rcni_candidates(sftp, scope)

    inventory_rows = [_candidate_inventory_row(c) for c in discovery.candidates]
    print_candidate_inventory(inventory_rows)
    inventory_path = write_discovery_inventory(scope.reports_dir, inventory_rows)
    manifest = write_run_manifest(
        scope.reports_dir,
        {
            "mode": "discover-only",
            "base_path": scope.base_path,
            "issuer": scope.issuer_display,
            "year": scope.year_display,
            "month": scope.month_display,
            "candidates": len(discovery.candidates),
            "files_scanned": discovery.files_scanned,
            "folders_scanned": discovery.folders_scanned,
            "partitions": [f"{a}/{b}/{c}" for a, b, c in discovery.partitions],
            "files_by_processing_month": _counts_by_month(discovery.candidates),
        },
    )
    return Phase1Result(
        scope=scope,
        mode="discover-only",
        discovery=discovery,
        inventory_rows=inventory_rows,
        report_paths={"discovery_inventory": str(inventory_path), "manifest": str(manifest)},
    )


def _counts_by_month(candidates: list[RcniCandidate]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for candidate in candidates:
        key = candidate.processing_month or "?"
        counts[key] = counts.get(key, 0) + 1
    return dict(sorted(counts.items()))


def _write_issue_reports(
    reports_dir: Path, issues: list[RowIssue]
) -> tuple[str, str]:
    structural_path = write_structural_malformed(reports_dir, issues)
    warnings_path = write_data_quality_warnings(reports_dir, issues)
    return str(structural_path), str(warnings_path)


def _summarize_file(
    candidate: RcniCandidate,
    downloaded: DownloadedFile,
    csv_result,
    extra_flags: list[str],
    extra_issues: list[RowIssue],
) -> tuple[FileValidationSummary, list[RowIssue]]:
    flags, filename_issues = _filename_flags(candidate)
    flags.extend(extra_flags)
    issues = list(filename_issues) + list(extra_issues) + list(csv_result.issues)

    schema_status = STATUS_CLEAN if csv_result.header_ok else STATUS_SCHEMA_MISMATCH
    if not csv_result.header_ok:
        flags.append(STATUS_SCHEMA_MISMATCH)
        flags.append(STATUS_MALFORMED)
    if csv_result.structural_malformed_records or csv_result.malformed_records:
        flags.append(STATUS_MALFORMED)
    if csv_result.identifier_format_warnings or csv_result.identifier_warnings:
        flags.append(STATUS_WARNING)

    other_quality = 0
    for issue in issues:
        if (
            issue.issue_type not in STRUCTURAL_ISSUE_TYPES
            and issue.issue_type != "DOWNLOAD_ERROR"
            and issue.issue_type not in {"IDENTIFIER_NOT_NUMERIC"}
        ):
            other_quality += 1
    if extra_flags:
        # DOWNLOAD_ERROR is structural.
        pass
    if other_quality:
        flags.append(STATUS_WARNING)

    filename_status = (
        STATUS_FILENAME_METADATA_MISMATCH
        if STATUS_FILENAME_METADATA_MISMATCH in flags
        else STATUS_CLEAN
    )

    summary = FileValidationSummary(
        issuer=candidate.issuer,
        directory_processing_year=candidate.processing_year,
        directory_processing_month=candidate.processing_month,
        directory_processing_day=candidate.processing_day,
        plan_year=candidate.filename_meta.plan_year,
        source_filename=candidate.filename,
        source_sftp_path=candidate.remote_path,
        compressed_size=downloaded.compressed_size,
        content_hash=downloaded.content_hash,
        compressed_hash=downloaded.compressed_hash,
        header_column_count=csv_result.header_column_count,
        parsed_records=csv_result.parsed_records,
        clean_records=csv_result.clean_records,
        malformed_records=csv_result.structural_malformed_records or csv_result.malformed_records,
        identifier_warnings=csv_result.identifier_format_warnings or csv_result.identifier_warnings,
        structural_malformed_records=(
            csv_result.structural_malformed_records or csv_result.malformed_records
        ),
        identifier_format_warnings=(
            csv_result.identifier_format_warnings or csv_result.identifier_warnings
        ),
        other_quality_warnings=other_quality,
        schema_header_status=schema_status,
        filename_metadata_status=filename_status,
        overall_status=overall_status(flags),
        issuer_mismatch=candidate.issuer_mismatch,
        plan_year_differs_from_processing_year=candidate.plan_year_differs_from_processing_year,
        local_extracted_path=str(downloaded.paths.extracted_path),
        flags=sorted(set(flags)) if flags else [STATUS_CLEAN],
    )
    return summary, issues


def run_discover_and_validate(scope: RcniScope) -> Phase1Result:
    logger.info(
        "RCNI discover+validate — base=%s issuer=%s year=%s month=%s (no SQL)",
        scope.base_path,
        scope.issuer_display,
        scope.year_display,
        scope.month_display,
    )
    scope.reports_dir.mkdir(parents=True, exist_ok=True)
    scope.logs_dir.mkdir(parents=True, exist_ok=True)
    scope.local_root.mkdir(parents=True, exist_ok=True)

    with sftp_connection(
        scope.sftp_host,
        scope.sftp_port,
        scope.sftp_user,
        scope.sftp_password,
    ) as sftp:
        discovery = discover_rcni_candidates(sftp, scope)
        inventory_rows = [_candidate_inventory_row(c) for c in discovery.candidates]
        print_candidate_inventory(inventory_rows)
        inventory_path = write_discovery_inventory(scope.reports_dir, inventory_rows)

        downloaded_files: list[DownloadedFile] = []
        summaries: list[FileValidationSummary] = []
        all_issues: list[RowIssue] = []
        identities: list[IdentityRecord] = []

        for candidate in discovery.candidates:
            staged = download_and_stage(sftp, candidate, scope)
            downloaded_files.append(staged)
            if staged.error or not staged.paths.extracted_path.exists():
                extra = [
                    RowIssue(
                        source_file=candidate.filename,
                        source_path=candidate.remote_path,
                        record_number=None,
                        physical_line_number=None,
                        issue_type="DOWNLOAD_ERROR",
                        issue_description=staged.error or "Extracted file missing after download",
                        expected_column_count=19,
                        observed_column_count=None,
                        column_name=None,
                        bad_value=None,
                        raw_record="",
                    )
                ]
                csv_result = CsvValidationResult(
                    header=None,
                    header_column_count=0,
                    header_ok=False,
                    header_mismatch_details=staged.error or "Extracted file missing after download",
                )
                summary, issues = _summarize_file(
                    candidate, staged, csv_result, [STATUS_MALFORMED], extra
                )
                summaries.append(summary)
                all_issues.extend(issues)
                continue

            csv_result = validate_rcni_csv(
                staged.paths.extracted_path,
                source_file=candidate.filename,
                source_path=candidate.remote_path,
            )
            summary, issues = _summarize_file(candidate, staged, csv_result, [], [])
            summaries.append(summary)
            all_issues.extend(issues)
            identities.append(_identity_record(candidate, staged.content_hash))

    dup_issues = detect_duplicates(identities)
    for summary in summaries:
        extras = dup_issues.get(summary.source_sftp_path, [])
        if not extras:
            continue
        for issue in extras:
            all_issues.append(issue)
            if "POSSIBLE_REPLACEMENT" in issue.issue_description:
                summary.flags.append("POSSIBLE_REPLACEMENT")
            else:
                summary.flags.append("DUPLICATE")
        summary.flags = sorted(set(summary.flags))
        summary.overall_status = overall_status(summary.flags)

    summary_path = write_validation_summary(scope.reports_dir, summaries)
    structural_path, warnings_path = _write_issue_reports(scope.reports_dir, all_issues)
    manifest = write_run_manifest(
        scope.reports_dir,
        {
            "mode": "validate",
            "base_path": scope.base_path,
            "issuer": scope.issuer_display,
            "year": scope.year_display,
            "month": scope.month_display,
            "candidates": len(discovery.candidates),
            "files_validated": len(summaries),
            "partitions": [f"{a}/{b}/{c}" for a, b, c in discovery.partitions],
            "files_by_processing_month": _counts_by_month(discovery.candidates),
            "structural_malformed_records": sum(
                s.structural_malformed_records for s in summaries
            ),
            "identifier_format_warnings": sum(
                s.identifier_format_warnings for s in summaries
            ),
            "other_quality_warnings": sum(s.other_quality_warnings for s in summaries),
            "azure_sql_writes": False,
            "source_files_modified": False,
        },
    )
    _print_validation_table(summaries)
    return Phase1Result(
        scope=scope,
        mode="validate",
        discovery=discovery,
        inventory_rows=inventory_rows,
        summaries=summaries,
        issues=all_issues,
        report_paths={
            "discovery_inventory": str(inventory_path),
            "validation_summary": str(summary_path),
            "structural_malformed": structural_path,
            "data_quality_warnings": warnings_path,
            "manifest": str(manifest),
        },
    )


def _print_validation_table(summaries: list[FileValidationSummary]) -> None:
    print("\nRCNI VALIDATION SUMMARY")
    print("-" * 120)
    if not summaries:
        print("  (no files validated)")
        print("-" * 120)
        return
    for item in summaries:
        print(
            f"{item.issuer}  proc={item.directory_processing_year}/"
            f"{item.directory_processing_month}/{item.directory_processing_day or '?'}  "
            f"plan_year={item.plan_year or '?'}  status={item.overall_status}"
        )
        print(f"  file   : {item.source_filename}")
        print(f"  path   : {item.source_sftp_path}")
        print(
            f"  size   : compressed={item.compressed_size}  "
            f"hash={item.content_hash[:16] + '…' if item.content_hash else '-'}"
        )
        print(
            f"  csv    : header_cols={item.header_column_count}  "
            f"parsed={item.parsed_records}  clean={item.clean_records}  "
            f"structural_malformed={item.structural_malformed_records}  "
            f"id_format_warnings={item.identifier_format_warnings}  "
            f"other_warnings={item.other_quality_warnings}"
        )
        print(
            f"  schema={item.schema_header_status}  "
            f"filename={item.filename_metadata_status}  flags={','.join(item.flags)}"
        )
    print("-" * 120)
    print(f"Files validated: {len(summaries)}")
    print(
        "Totals: structural_malformed="
        f"{sum(s.structural_malformed_records for s in summaries)}  "
        f"identifier_format_warnings={sum(s.identifier_format_warnings for s in summaries)}  "
        f"other_quality_warnings={sum(s.other_quality_warnings for s in summaries)}"
    )
    print("Azure SQL writes: NONE")
    print("Source files modified: NO")


def _candidate_from_local_path(path: Path) -> RcniCandidate:
    meta = parse_rcni_filename(path.name)
    issuer = meta.issuer_id or "unknown"
    year = month = day = None
    # If the file already sits in issuer/year/month[/day]/..., capture archive metadata.
    parts = path.resolve().parts
    for i, part in enumerate(parts):
        if part.isdigit() and len(part) == 5 and i + 2 < len(parts):
            y, m = parts[i + 1], parts[i + 2]
            if y.isdigit() and len(y) == 4 and m.isdigit() and 1 <= int(m) <= 12:
                issuer = part
                year, month = y, str(int(m)).zfill(2)
                if i + 3 < len(parts) and parts[i + 3].isdigit() and 1 <= int(parts[i + 3]) <= 31:
                    day = str(int(parts[i + 3])).zfill(2)
                break
    year = year or ""
    month = month or ""
    archive_meta = parse_archive_path(
        str(path),
        remote_root=str(path.parent),
        issuer=issuer,
        year=year or "0000",
        month=month or "00",
    )
    return RcniCandidate(
        issuer=issuer,
        processing_year=year,
        processing_month=month,
        processing_day=day,
        nested_relative=archive_meta.nested_relative,
        remote_path=str(path),
        filename=path.name,
        logical_name=logical_filename(path.name),
        filename_meta=meta,
        archive_meta=archive_meta,
        issuer_mismatch=bool(
            meta.parse_ok and meta.issuer_id and issuer != "unknown" and meta.issuer_id != issuer
        ),
    )


def run_validate_local(scope: RcniScope, local_dir: Path) -> Phase1Result:
    """Validate already-local RCNI files. Does not connect to SFTP or Azure."""
    logger.info("RCNI validate-local — dir=%s (no SFTP, no SQL)", local_dir)
    scope.reports_dir.mkdir(parents=True, exist_ok=True)
    scope.logs_dir.mkdir(parents=True, exist_ok=True)

    paths = sorted(
        p for p in Path(local_dir).iterdir()
        if p.is_file() and is_rcni_local_file(p.name)
    )
    candidates = [_candidate_from_local_path(p) for p in paths]
    discovery = DiscoveryResult(candidates=candidates, files_scanned=len(list(Path(local_dir).iterdir())))
    inventory_rows = [_candidate_inventory_row(c) for c in candidates]
    print_candidate_inventory(inventory_rows)
    inventory_path = write_discovery_inventory(scope.reports_dir, inventory_rows)

    summaries: list[FileValidationSummary] = []
    all_issues: list[RowIssue] = []
    identities: list[IdentityRecord] = []

    for candidate, path in zip(candidates, paths, strict=True):
        original_bytes = path.read_bytes()
        staged = stage_local_file(path, candidate, scope)
        if path.read_bytes() != original_bytes:
            raise RuntimeError(f"Source file was modified: {path}")
        csv_result = validate_rcni_csv(
            path if not path.name.lower().endswith(".gz") else staged.paths.extracted_path,
            source_file=candidate.filename,
            source_path=str(path),
        )
        summary, issues = _summarize_file(candidate, staged, csv_result, [], [])
        summaries.append(summary)
        all_issues.extend(issues)
        identities.append(_identity_record(candidate, staged.content_hash, str(path)))

    dup_issues = detect_duplicates(identities)
    for summary in summaries:
        extras = dup_issues.get(summary.source_sftp_path, [])
        for issue in extras:
            all_issues.append(issue)
            summary.flags.append(
                "POSSIBLE_REPLACEMENT" if "POSSIBLE_REPLACEMENT" in issue.issue_description else "DUPLICATE"
            )
        if extras:
            summary.flags = sorted(set(summary.flags))
            summary.overall_status = overall_status(summary.flags)

    summary_path = write_validation_summary(scope.reports_dir, summaries)
    structural_path, warnings_path = _write_issue_reports(scope.reports_dir, all_issues)
    manifest = write_run_manifest(
        scope.reports_dir,
        {
            "mode": "validate-local",
            "local_dir": str(local_dir),
            "candidates": len(candidates),
            "structural_malformed_records": sum(
                s.structural_malformed_records for s in summaries
            ),
            "identifier_format_warnings": sum(
                s.identifier_format_warnings for s in summaries
            ),
            "other_quality_warnings": sum(s.other_quality_warnings for s in summaries),
            "azure_sql_writes": False,
            "source_files_modified": False,
        },
    )
    _print_validation_table(summaries)
    return Phase1Result(
        scope=scope,
        mode="validate-local",
        discovery=discovery,
        inventory_rows=inventory_rows,
        summaries=summaries,
        issues=all_issues,
        report_paths={
            "discovery_inventory": str(inventory_path),
            "validation_summary": str(summary_path),
            "structural_malformed": structural_path,
            "data_quality_warnings": warnings_path,
            "manifest": str(manifest),
        },
    )

