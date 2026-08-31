"""
Runtime audit — confirms production runs use real source_data + live Azure only.

Write-only outputs/; previous outputs are never read except optional discovery
seed rankings when ENABLE_FULL_DISCOVERY=true.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from config.config import settings
from utils.logger import get_logger

logger = get_logger(__name__)

FIXED_AZURE_TABLE = "dbo.834_Inbound_test"

_audit: "DataSourceAudit | None" = None


@dataclass
class DataSourceAudit:
    synthetic_data_used: bool = False
    synthetic_data_reason: str = ""
    previous_outputs_read: bool = False
    previous_outputs_read_paths: list[str] = field(default_factory=list)
    xml_source: str = "source_data"
    xml_load_path: str = "source_data_direct"
    xml_files_read_count: int = 0
    xml_rows_loaded: int = 0
    azure_source: str = FIXED_AZURE_TABLE
    azure_rows_fetched: int = 0
    azure_live_fetch: bool = True
    selected_issuers: list[str] = field(default_factory=list)
    selected_years: list[str] = field(default_factory=list)
    selected_months: list[str] = field(default_factory=list)
    enable_full_discovery: bool = False
    overall_accuracy_from_current_run: bool = True

    def mark_outputs_read(self, path: str | Path) -> None:
        self.previous_outputs_read = True
        p = str(path)
        if p not in self.previous_outputs_read_paths:
            self.previous_outputs_read_paths.append(p)

    def mark_synthetic(self, reason: str) -> None:
        self.synthetic_data_used = True
        self.synthetic_data_reason = reason


def reset_audit() -> DataSourceAudit:
    global _audit
    _audit = DataSourceAudit()
    return _audit


def get_audit() -> DataSourceAudit:
    global _audit
    if _audit is None:
        _audit = DataSourceAudit()
    return _audit


def log_data_source_startup(*, enable_full_discovery: bool = False) -> None:
    """Print and log mandatory data-source mode lines at runner startup."""
    audit = get_audit()
    audit.enable_full_discovery = enable_full_discovery
    audit.azure_source = FIXED_AZURE_TABLE if settings.use_fixed_azure_candidate else "discovery_live"

    synthetic = audit.synthetic_data_used or _env_synthetic_flag()
    mode = "SYNTHETIC" if synthetic else "REAL"

    lines = [
        f"DATA_SOURCE_MODE={mode}",
        "XML_SOURCE=source_data",
        f"AZURE_SOURCE={audit.azure_source}",
        f"SYNTHETIC_DATA_USED={'true' if synthetic else 'false'}",
    ]
    for line in lines:
        print(line)
        logger.info(line)


def _env_synthetic_flag() -> bool:
    import os
    return str(os.getenv("USE_SYNTHETIC_DATA", "")).strip().lower() in {"true", "1", "yes"}


def record_xml_load(
    *,
    rows: int,
    files_read: int,
    load_path: str,
) -> None:
    audit = get_audit()
    audit.xml_rows_loaded += int(rows)
    audit.xml_files_read_count += int(files_read)
    audit.xml_load_path = load_path
    if load_path == "staging_sqlite":
        audit.xml_source = "staging_sqlite (derived from source_data pipeline)"
    else:
        audit.xml_source = "source_data"


def record_azure_fetch(*, rows: int, table: str = FIXED_AZURE_TABLE) -> None:
    audit = get_audit()
    audit.azure_rows_fetched += int(rows)
    audit.azure_source = table
    audit.azure_live_fetch = True


def record_partitions(*, issuers: list[str], years: list[str], months: list[str]) -> None:
    audit = get_audit()
    audit.selected_issuers = sorted({str(i) for i in issuers if str(i).strip()})
    audit.selected_years = sorted({str(y) for y in years if str(y).strip()})
    audit.selected_months = sorted({str(m).zfill(2) for m in months if str(m).strip()})


def write_data_source_audit(path: Path | None = None) -> Path:
    audit = get_audit()
    out = path or (settings.outputs_path / "debug" / "data_source_audit.txt")
    out.parent.mkdir(parents=True, exist_ok=True)

    lines = [
        "DATA SOURCE AUDIT",
        "=" * 40,
        f"generated_at: {datetime.now(timezone.utc).isoformat(timespec='seconds')}",
        "",
        f"DATA_SOURCE_MODE={'SYNTHETIC' if audit.synthetic_data_used or _env_synthetic_flag() else 'REAL'}",
        f"XML_SOURCE={audit.xml_source}",
        f"AZURE_SOURCE={audit.azure_source}",
        f"SYNTHETIC_DATA_USED={'true' if audit.synthetic_data_used or _env_synthetic_flag() else 'false'}",
        f"synthetic_data_reason: {audit.synthetic_data_reason or 'none'}",
        "",
        f"xml_files_read_count: {audit.xml_files_read_count}",
        f"xml_rows_loaded: {audit.xml_rows_loaded}",
        f"xml_load_path: {audit.xml_load_path}",
        f"azure_rows_fetched: {audit.azure_rows_fetched}",
        f"azure_live_fetch: {audit.azure_live_fetch}",
        "",
        f"previous_outputs_read: {audit.previous_outputs_read}",
        f"previous_outputs_read_paths: {audit.previous_outputs_read_paths or 'none'}",
        f"enable_full_discovery: {audit.enable_full_discovery}",
        f"overall_accuracy_from_current_run: {audit.overall_accuracy_from_current_run}",
        "",
        f"selected_issuers: {', '.join(audit.selected_issuers) or 'none'}",
        f"selected_years: {', '.join(audit.selected_years) or 'none'}",
        f"selected_months: {', '.join(audit.selected_months) or 'none'}",
        "",
        "relationship_thresholds:",
        f"  RELATIONSHIP_MIN_RECORD_MATCH_RATE={settings.relationship_min_record_match_rate}",
        f"  RELATIONSHIP_MIN_STATUS_MATCH_RATE={settings.relationship_min_status_match_rate}",
    ]
    out.write_text("\n".join(lines) + "\n", encoding="utf-8")
    logger.info("Wrote data source audit: %s", out)
    return out


def audit_summary_dict() -> dict[str, Any]:
    audit = get_audit()
    return {
        "data_source_mode": "SYNTHETIC" if audit.synthetic_data_used else "REAL",
        "xml_source": audit.xml_source,
        "azure_source": audit.azure_source,
        "synthetic_data_used": audit.synthetic_data_used,
        "xml_files_read_count": audit.xml_files_read_count,
        "azure_rows_fetched": audit.azure_rows_fetched,
        "previous_outputs_read": audit.previous_outputs_read,
    }
