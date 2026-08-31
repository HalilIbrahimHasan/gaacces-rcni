"""
Full source_data coverage discovery — issuers, years, months, files, dates.

Read-only walk of source_data/. Never modifies source_data.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from azure_reconciliation.partition_discovery import Partition, discover_partitions
from config.config import settings
from utils.logger import get_logger

logger = get_logger(__name__)

_XML_SUFFIXES = {".xml", ".XML"}


@dataclass
class FileCoverage:
    issuer: str
    year: str
    month: str
    file_name: str
    file_path: str
    file_size: int
    file_mtime: str | None


@dataclass
class CoverageReport:
    partitions: list[Partition] = field(default_factory=list)
    files: list[FileCoverage] = field(default_factory=list)

    @property
    def issuers(self) -> list[str]:
        return sorted({p.issuer for p in self.partitions})

    @property
    def years(self) -> list[str]:
        return sorted({p.year for p in self.partitions})

    @property
    def months(self) -> list[str]:
        return sorted({p.month for p in self.partitions})

    @property
    def partition_labels(self) -> list[str]:
        return [p.label() for p in self.partitions]

    def coverage_window(self) -> dict[str, str | None]:
        if not self.partitions:
            return {"earliest": None, "latest": None}
        sorted_parts = sorted(self.partitions, key=lambda p: p.sort_key)
        first, last = sorted_parts[0], sorted_parts[-1]
        return {
            "earliest": f"{first.year}-{first.month.zfill(2)}",
            "latest": f"{last.year}-{last.month.zfill(2)}",
        }


def discover_source_coverage(
    *,
    issuer_filter: str | None = None,
    year_filter: str | None = None,
    month_filter: str | None = None,
) -> CoverageReport:
    """Discover partitions and file-level metadata from source_data."""
    partitions = discover_partitions(
        issuer_filter=issuer_filter,
        year_filter=year_filter,
        month_filter=month_filter,
    )
    files: list[FileCoverage] = []

    for part in partitions:
        part_path = part.path
        if not part_path.is_dir():
            continue
        for fp in sorted(part_path.iterdir()):
            if not fp.is_file() or fp.suffix not in _XML_SUFFIXES:
                continue
            mtime = None
            try:
                mtime = datetime.fromtimestamp(fp.stat().st_mtime).isoformat(timespec="seconds")
            except OSError:
                pass
            files.append(FileCoverage(
                issuer=part.issuer,
                year=part.year,
                month=part.month,
                file_name=fp.name,
                file_path=str(fp),
                file_size=fp.stat().st_size if fp.exists() else 0,
                file_mtime=mtime,
            ))

    report = CoverageReport(partitions=partitions, files=files)
    window = report.coverage_window()
    logger.info(
        "Source coverage: %d partitions, %d files, issuers=%s years=%s months=%s window=%s..%s",
        len(partitions), len(files), report.issuers, report.years, report.months,
        window.get("earliest"), window.get("latest"),
    )
    return report


def coverage_to_dataframes(report: CoverageReport) -> dict[str, pd.DataFrame]:
    part_rows = [
        {"issuer": p.issuer, "year": p.year, "month": p.month, "partition": p.label()}
        for p in report.partitions
    ]
    file_rows = [
        {
            "issuer": f.issuer, "year": f.year, "month": f.month,
            "file_name": f.file_name, "file_size": f.file_size, "file_mtime": f.file_mtime,
        }
        for f in report.files
    ]
    window = report.coverage_window()
    summary = pd.DataFrame([{
        "partition_count": len(report.partitions),
        "file_count": len(report.files),
        "issuer_count": len(report.issuers),
        "issuers": ", ".join(report.issuers),
        "years": ", ".join(report.years),
        "months": ", ".join(report.months),
        "coverage_earliest": window.get("earliest"),
        "coverage_latest": window.get("latest"),
    }])
    return {
        "partitions": pd.DataFrame(part_rows),
        "files": pd.DataFrame(file_rows),
        "summary": summary,
    }


def coverage_summary_dict(report: CoverageReport) -> dict[str, Any]:
    window = report.coverage_window()
    return {
        "issuers": report.issuers,
        "years": report.years,
        "months": report.months,
        "partition_count": len(report.partitions),
        "file_count": len(report.files),
        "coverage_earliest": window.get("earliest"),
        "coverage_latest": window.get("latest"),
    }
