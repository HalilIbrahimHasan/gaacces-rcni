"""Export SFTP ingestion/audit summary reports."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd

from utils.logger import get_logger

logger = get_logger(__name__)

SUMMARY_COLUMNS = [
    "issuer",
    "year",
    "month",
    "folders_scanned",
    "max_depth",
    "files_scanned",
    "valid_xml",
    "valid_xml_gz",
    "valid_xml_xz",
    "valid_total",
    "downloaded",
    "existing",
    "failed",
    "skipped_to_files",
    "skipped_report_files",
    "skipped_tracking_files",
    "skipped_edi_files",
    "skipped_other_files",
    "local_xml_final_count",
    "missing_count",
    "status",
]


@dataclass
class PartitionSummary:
    issuer: str
    year: str
    month: str
    folders_scanned: int = 0
    max_depth: int = 0
    files_scanned: int = 0
    valid_xml: int = 0
    valid_xml_gz: int = 0
    valid_xml_xz: int = 0
    downloaded: int = 0
    existing: int = 0
    failed: int = 0
    skipped_to_files: int = 0
    skipped_report_files: int = 0
    skipped_tracking_files: int = 0
    skipped_edi_files: int = 0
    skipped_other_files: int = 0
    local_xml_final_count: int = 0
    missing_count: int = 0

    @property
    def valid_total(self) -> int:
        return self.valid_xml + self.valid_xml_gz + self.valid_xml_xz

    @property
    def status(self) -> str:
        if self.failed > 0:
            return "FAILED"
        if self.valid_total == 0:
            return "NO_VALID_FILES"
        if self.missing_count > 0:
            return "MISSING"
        return "OK"

    def to_row(self) -> dict[str, Any]:
        return {
            "issuer": self.issuer,
            "year": self.year,
            "month": self.month,
            "folders_scanned": self.folders_scanned,
            "max_depth": self.max_depth,
            "files_scanned": self.files_scanned,
            "valid_xml": self.valid_xml,
            "valid_xml_gz": self.valid_xml_gz,
            "valid_xml_xz": self.valid_xml_xz,
            "valid_total": self.valid_total,
            "downloaded": self.downloaded,
            "existing": self.existing,
            "failed": self.failed,
            "skipped_to_files": self.skipped_to_files,
            "skipped_report_files": self.skipped_report_files,
            "skipped_tracking_files": self.skipped_tracking_files,
            "skipped_edi_files": self.skipped_edi_files,
            "skipped_other_files": self.skipped_other_files,
            "local_xml_final_count": self.local_xml_final_count,
            "missing_count": self.missing_count,
            "status": self.status,
        }


def count_local_xmls(source_data_dir: Path, issuer: str, year: str, month: str) -> int:
    month_dir = source_data_dir / issuer / year / month
    if not month_dir.is_dir():
        return 0
    return sum(1 for p in month_dir.iterdir() if p.is_file() and p.suffix.lower() == ".xml")


def print_console_summary(summaries: list[PartitionSummary]) -> None:
    logger.info("SFTP summary by partition:")
    logger.info(
        "issuer/year/month | valid_total | downloaded | existing | local_final | missing | status"
    )
    for s in summaries:
        logger.info(
            "%s/%s/%s | %d | %d | %d | %d | %d | %s",
            s.issuer,
            s.year,
            s.month,
            s.valid_total,
            s.downloaded,
            s.existing,
            s.local_xml_final_count,
            s.missing_count,
            s.status,
        )


def export_summaries(summaries: list[PartitionSummary], reports_dir: Path) -> tuple[Path, Path]:
    reports_dir.mkdir(parents=True, exist_ok=True)
    rows = [s.to_row() for s in summaries]
    df = pd.DataFrame(rows, columns=SUMMARY_COLUMNS)

    csv_path = reports_dir / "sftp_ingestion_summary.csv"
    xlsx_path = reports_dir / "sftp_ingestion_summary.xlsx"
    df.to_csv(csv_path, index=False)
    df.to_excel(xlsx_path, index=False)

    logger.info("Wrote summary CSV: %s", csv_path)
    logger.info("Wrote summary XLSX: %s", xlsx_path)
    return csv_path, xlsx_path
