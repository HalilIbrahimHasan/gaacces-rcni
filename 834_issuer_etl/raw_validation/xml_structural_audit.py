"""Structural XML completeness audit for Parser834 compatibility."""

from __future__ import annotations

import traceback
import xml.etree.ElementTree as ET
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from azure_reconciliation.safe_export import safe_write_excel
from config.config import settings
from connectors.base_connector import SourceFile
from ingestion.file_discovery import discover_source_files
from ingestion.xml_reader import read_xml_bytes
from parsers.parser_834 import Parser834
from utils.logger import get_logger

logger = get_logger(__name__)

AUDIT_YEARS = ("2025", "2026")
REPEATED_BLOCK_TAGS = ("healthCoverage", "memberReportingCategory", "enrollmentEvents")


def _local_tag(tag: str) -> str:
    return tag.split("}", 1)[-1] if "}" in tag else tag


def _has_namespace(root: ET.Element) -> bool:
    if "}" in root.tag:
        return True
    for el in root.iter():
        if "}" in el.tag:
            return True
    return False


def _parser_compatible_counts(root: ET.Element) -> tuple[int, int, int]:
    """Mirror Parser834.parse_file loops exactly."""
    enrollment_count = 0
    direct_enrollee_count = 0
    empty_enrollment_count = 0
    for enrollment in root.findall("enrollment"):
        enrollment_count += 1
        enrollees = enrollment.findall("enrollee")
        if not enrollees:
            empty_enrollment_count += 1
        direct_enrollee_count += len(enrollees)
    return enrollment_count, direct_enrollee_count, empty_enrollment_count


def _recursive_enrollee_count(root: ET.Element) -> int:
    return sum(1 for el in root.iter() if _local_tag(el.tag) == "enrollee")


def _nested_enrollee_count(root: ET.Element) -> int:
    """Enrollees found recursively but not as direct child of enrollment."""
    direct_ids = set()
    for enrollment in root.findall("enrollment"):
        for enrollee in enrollment.findall("enrollee"):
            direct_ids.add(id(enrollee))
    nested = 0
    for el in root.iter():
        if _local_tag(el.tag) != "enrollee":
            continue
        if id(el) not in direct_ids:
            nested += 1
    return nested


def _analyze_enrollee_blocks(enrollee: ET.Element) -> dict[str, Any]:
    blocks = {tag: enrollee.findall(tag) for tag in REPEATED_BLOCK_TAGS}
    block_counts = {tag: len(blocks[tag]) for tag in REPEATED_BLOCK_TAGS}
    repeated_child_tags: list[str] = []
    for hc in blocks["healthCoverage"]:
        child_names = [_local_tag(child.tag) for child in list(hc)]
        dupes = [name for name, cnt in Counter(child_names).items() if cnt > 1]
        if dupes:
            repeated_child_tags.extend(dupes)
    return {
        "block_counts": block_counts,
        "repeated_health_child_tags": sorted(set(repeated_child_tags)),
    }


def _analyze_file(source: SourceFile, parser: Parser834) -> dict[str, Any]:
    row: dict[str, Any] = {
        "issuer": source.issuer,
        "folder_year": int(source.year),
        "folder_month": int(source.month),
        "source_file": source.file_name,
        "source_file_path": str(source.file_path),
        "file_size_bytes": source.file_size,
        "parse_error": "",
    }
    try:
        xml_bytes = read_xml_bytes(source)
        root = ET.fromstring(xml_bytes)
        enrollment_count, direct_enrollee_count, empty_enrollment_count = (
            _parser_compatible_counts(root)
        )
        recursive_enrollee_count = _recursive_enrollee_count(root)
        nested_enrollee_count = _nested_enrollee_count(root)
        has_namespace = _has_namespace(root)

        hc_gt1 = 0
        mrc_gt1 = 0
        ee_gt1 = 0
        repeated_hc_child_tags: set[str] = set()

        for enrollment in root.findall("enrollment"):
            for enrollee in enrollment.findall("enrollee"):
                analysis = _analyze_enrollee_blocks(enrollee)
                counts = analysis["block_counts"]
                if counts["healthCoverage"] > 1:
                    hc_gt1 += 1
                if counts["memberReportingCategory"] > 1:
                    mrc_gt1 += 1
                if counts["enrollmentEvents"] > 1:
                    ee_gt1 += 1
                repeated_hc_child_tags.update(analysis["repeated_health_child_tags"])

        parser_rows = parser.parse_file(
            xml_bytes,
            issuer=source.issuer,
            year=source.year,
            month=source.month,
            file_name=source.file_name,
            file_path=str(source.file_path),
        )
        parser_row_count = len(parser_rows)

        row.update(
            {
                "enrollment_count": enrollment_count,
                "direct_enrollee_count": direct_enrollee_count,
                "recursive_enrollee_count": recursive_enrollee_count,
                "nested_enrollee_diff": recursive_enrollee_count - direct_enrollee_count,
                "nested_enrollee_count": nested_enrollee_count,
                "empty_enrollment_count": empty_enrollment_count,
                "has_namespace": has_namespace,
                "enrollees_with_multiple_healthCoverage": hc_gt1,
                "enrollees_with_multiple_memberReportingCategory": mrc_gt1,
                "enrollees_with_multiple_enrollmentEvents": ee_gt1,
                "has_repeated_healthCoverage_child_tags": bool(repeated_hc_child_tags),
                "repeated_healthCoverage_child_tags": ", ".join(sorted(repeated_hc_child_tags)),
                "parser_row_count": parser_row_count,
                "parser_matches_direct_enrollee_count": (
                    parser_row_count == direct_enrollee_count
                ),
            }
        )
    except Exception as exc:
        row["parse_error"] = f"{type(exc).__name__}: {exc}"
        logger.error("Audit failed for %s: %s", source.file_name, exc)
    return row


@dataclass
class StructuralAuditResult:
    source_root: Path
    output_dir: Path
    file_rows: list[dict[str, Any]] = field(default_factory=list)
    years: tuple[str, ...] = AUDIT_YEARS

    @property
    def successful_rows(self) -> list[dict[str, Any]]:
        return [r for r in self.file_rows if not r.get("parse_error")]

    @property
    def failed_rows(self) -> list[dict[str, Any]]:
        return [r for r in self.file_rows if r.get("parse_error")]


def discover_audit_files(source_root: Path, years: tuple[str, ...] = AUDIT_YEARS) -> list[SourceFile]:
    files: list[SourceFile] = []
    seen_paths: set[str] = set()
    for year in years:
        batch = discover_source_files(
            source_root,
            issuer_filter=None,
            year_filter=year,
            month_filter=None,
        )
        for source in batch:
            key = str(source.file_path.resolve())
            if key not in seen_paths:
                seen_paths.add(key)
                files.append(source)
    return files


def run_structural_audit(
    *,
    source_root: Path | None = None,
    output_dir: Path | None = None,
    years: tuple[str, ...] = AUDIT_YEARS,
) -> StructuralAuditResult:
    settings.refresh_from_env()
    root = (source_root or settings.source_data_path).resolve()
    out = output_dir or (settings.project_root / "last reports")
    out.mkdir(parents=True, exist_ok=True)

    sources = discover_audit_files(root, years=years)
    logger.info(
        "Structural audit: %d file(s) under %s for years %s",
        len(sources),
        root,
        ", ".join(years),
    )

    parser = Parser834()
    file_rows: list[dict[str, Any]] = []
    for idx, source in enumerate(sources, start=1):
        if idx % 500 == 0 or idx == len(sources):
            logger.info("Audited %d / %d files", idx, len(sources))
        file_rows.append(_analyze_file(source, parser))

    result = StructuralAuditResult(
        source_root=root,
        output_dir=out,
        file_rows=file_rows,
        years=years,
    )
    write_structural_audit_reports(result)
    return result


def _summary_rows(result: StructuralAuditResult) -> list[dict[str, Any]]:
    ok = result.successful_rows
    total_files = len(result.file_rows)
    parsed_files = len(ok)
    failed_files = len(result.failed_rows)

    def _sum(key: str) -> int:
        return int(sum(int(r.get(key) or 0) for r in ok))

    mismatches = [
        r for r in ok if not r.get("parser_matches_direct_enrollee_count", False)
    ]
    namespace_files = [r for r in ok if r.get("has_namespace")]
    nested_files = [r for r in ok if int(r.get("nested_enrollee_count") or 0) > 0]
    empty_enrollment_files = [r for r in ok if int(r.get("empty_enrollment_count") or 0) > 0]
    repeated_block_files = [
        r
        for r in ok
        if int(r.get("enrollees_with_multiple_healthCoverage") or 0) > 0
        or int(r.get("enrollees_with_multiple_memberReportingCategory") or 0) > 0
        or int(r.get("enrollees_with_multiple_enrollmentEvents") or 0) > 0
        or r.get("has_repeated_healthCoverage_child_tags")
    ]

    return [
        {"metric": "source_root", "value": str(result.source_root)},
        {"metric": "audit_years", "value": ", ".join(result.years)},
        {"metric": "files_discovered", "value": total_files},
        {"metric": "files_audited_successfully", "value": parsed_files},
        {"metric": "files_with_xml_or_audit_errors", "value": failed_files},
        {"metric": "total_enrollment_elements", "value": _sum("enrollment_count")},
        {"metric": "total_direct_enrollee_count", "value": _sum("direct_enrollee_count")},
        {"metric": "total_recursive_enrollee_count", "value": _sum("recursive_enrollee_count")},
        {
            "metric": "total_nested_enrollee_diff",
            "value": _sum("nested_enrollee_diff"),
        },
        {"metric": "total_parser_row_count", "value": _sum("parser_row_count")},
        {
            "metric": "parser_row_count_equals_direct_enrollee_count",
            "value": _sum("parser_row_count") == _sum("direct_enrollee_count"),
        },
        {"metric": "files_with_namespace", "value": len(namespace_files)},
        {"metric": "files_with_nested_enrollees", "value": len(nested_files)},
        {"metric": "files_with_empty_enrollments", "value": len(empty_enrollment_files)},
        {"metric": "files_with_repeated_enrollee_blocks", "value": len(repeated_block_files)},
        {"metric": "files_with_parser_row_mismatches", "value": len(mismatches)},
    ]


def write_structural_audit_reports(result: StructuralAuditResult) -> dict[str, Path]:
    out = result.output_dir
    ok = result.successful_rows
    paths = {
        "structural_audit_summary": out / "structural_audit_summary.xlsx",
        "files_with_namespace": out / "files_with_namespace.xlsx",
        "files_with_nested_enrollees": out / "files_with_nested_enrollees.xlsx",
        "empty_enrollments": out / "empty_enrollments.xlsx",
        "repeated_enrollee_blocks": out / "repeated_enrollee_blocks.xlsx",
        "parser_row_count_mismatches": out / "parser_row_count_mismatches.xlsx",
    }

    namespace_rows = [r for r in ok if r.get("has_namespace")]
    nested_rows = [r for r in ok if int(r.get("nested_enrollee_count") or 0) > 0]
    empty_rows = [r for r in ok if int(r.get("empty_enrollment_count") or 0) > 0]
    repeated_rows = [
        r
        for r in ok
        if int(r.get("enrollees_with_multiple_healthCoverage") or 0) > 0
        or int(r.get("enrollees_with_multiple_memberReportingCategory") or 0) > 0
        or int(r.get("enrollees_with_multiple_enrollmentEvents") or 0) > 0
        or r.get("has_repeated_healthCoverage_child_tags")
    ]
    mismatch_rows = [
        r for r in ok if not r.get("parser_matches_direct_enrollee_count", False)
    ]
    error_rows = result.failed_rows

    import pandas as pd

    summary_df = pd.DataFrame(_summary_rows(result))
    file_df = pd.DataFrame(ok)

    safe_write_excel(
        paths["structural_audit_summary"],
        {
            "summary": summary_df,
            "per_file": file_df,
            "errors": pd.DataFrame(error_rows) if error_rows else pd.DataFrame(),
        },
        drop_duplicate_value_columns=False,
    )
    safe_write_excel(
        paths["files_with_namespace"],
        {"files_with_namespace": pd.DataFrame(namespace_rows)},
        drop_duplicate_value_columns=False,
    )
    safe_write_excel(
        paths["files_with_nested_enrollees"],
        {"files_with_nested_enrollees": pd.DataFrame(nested_rows)},
        drop_duplicate_value_columns=False,
    )
    safe_write_excel(
        paths["empty_enrollments"],
        {"empty_enrollments": pd.DataFrame(empty_rows)},
        drop_duplicate_value_columns=False,
    )
    safe_write_excel(
        paths["repeated_enrollee_blocks"],
        {"repeated_enrollee_blocks": pd.DataFrame(repeated_rows)},
        drop_duplicate_value_columns=False,
    )
    safe_write_excel(
        paths["parser_row_count_mismatches"],
        {"parser_row_count_mismatches": pd.DataFrame(mismatch_rows)},
        drop_duplicate_value_columns=False,
    )
    return paths
