"""
834 Issuer ETL — main orchestrator.

Fully folder-driven: discovers every issuer/year/month under source_data/
automatically. Add folders and XML files, then run:

    python run.py
    python src/main.py

No config issuer list, no CLI filters, no manual updates for new issuers.
"""

from __future__ import annotations

import sys
from pathlib import Path

# Bootstrap src/ on sys.path before any local imports (works from any CWD).
_SRC_DIR = Path(__file__).resolve().parent
if str(_SRC_DIR) not in sys.path:
    sys.path.insert(0, str(_SRC_DIR))

import json  # noqa: E402

import pandas as pd  # noqa: E402
from config import log_path_configuration  # noqa: E402
from dashboard.plotly_dashboard import PlotlyDashboard  # noqa: E402
from dataclasses import dataclass, field  # noqa: E402
from extract.xml_reader import LocalFileSource, XmlReader  # noqa: E402
from load.excel_exporter import ExcelExporter  # noqa: E402
from load.sqlite_loader import SqliteLoader  # noqa: E402
from load.xml_exporter import XmlExporter  # noqa: E402
from transform.cleaner import DataCleaner  # noqa: E402
from transform.kpi_builder import KpiBuilder  # noqa: E402
from transform.xml_parser import Xml834Parser  # noqa: E402
from utils.logger import get_logger  # noqa: E402
from utils.partition import (  # noqa: E402
    SourcePartition,
    clean_stale_asset_issuers,
    discover_partitions,
    ensure_partition_asset_dirs,
    ensure_rollup_asset_dirs,
    iter_source_roots,
    log_issuer_gaps,
    monthly_output_stem,
    rollup_output_stem,
)
from validate.data_quality_validator import DataQualityValidator  # noqa: E402
from validate.schema_validator import SchemaValidator  # noqa: E402

logger = get_logger(__name__)


@dataclass
class PartitionRunResult:
    partition: SourcePartition
    status: str
    output_path: Path | None = None
    enrollee_count: int = 0
    error: str | None = None


@dataclass
class PipelineSummary:
    discovered: int = 0
    processed: int = 0
    skipped: int = 0
    failed: int = 0
    results: list[PartitionRunResult] = field(default_factory=list)
    rollup_issuers: list[str] = field(default_factory=list)
    failed_rollups: list[str] = field(default_factory=list)


class IssuerEtlPipeline:
    """End-to-end ETL — processes every discovered partition."""

    def __init__(self, source: LocalFileSource | None = None) -> None:
        self.reader = XmlReader(source=source or LocalFileSource())
        self.parser = Xml834Parser()
        self.cleaner = DataCleaner()
        self.kpi_builder = KpiBuilder()
        self.schema_validator = SchemaValidator()
        self.dq_validator = DataQualityValidator()
        self.excel_exporter = ExcelExporter()
        self.xml_exporter = XmlExporter()
        self.sqlite_loader = SqliteLoader()
        self.dashboard = PlotlyDashboard()

    def run(self) -> PipelineSummary:
        partitions = discover_partitions()
        summary = PipelineSummary(discovered=len(partitions))
        active_issuers = {p.issuer_id for p in partitions}

        log_issuer_gaps(partitions)
        clean_stale_asset_issuers(active_issuers)

        if not partitions:
            logger.error("Nothing to process.")
            logger.error(
                "Add XML files under: %s/{issuer_id}/{year}/{month}/*.xml",
                iter_source_roots()[0],
            )
            self._print_summary(summary)
            return summary

        issuer_monthly_dfs: dict[str, list[pd.DataFrame]] = {}

        for partition in partitions:
            logger.info(
                "Processing issuer=%s year=%s month=%s files=%d path=%s",
                partition.issuer_id,
                partition.year,
                partition.month,
                partition.file_count,
                partition.input_path,
            )
            try:
                cleaned_df, run_result = self._process_partition(partition)
                summary.results.append(run_result)
                if run_result.status == "processed":
                    summary.processed += 1
                    issuer_monthly_dfs.setdefault(partition.issuer_id, []).append(
                        cleaned_df
                    )
                elif run_result.status == "skipped":
                    summary.skipped += 1
                else:
                    summary.failed += 1
            except Exception as exc:
                summary.failed += 1
                logger.error(
                    "Partition %s failed: %s",
                    partition.period_key,
                    exc,
                    exc_info=True,
                )
                summary.results.append(
                    PartitionRunResult(
                        partition=partition,
                        status="failed",
                        error=str(exc),
                    )
                )

        for iid, dfs in sorted(issuer_monthly_dfs.items()):
            if not dfs:
                continue
            try:
                self._process_rollup(iid, dfs)
                summary.rollup_issuers.append(iid)
            except Exception as exc:
                summary.failed_rollups.append(iid)
                logger.error("Rollup for issuer %s failed: %s", iid, exc, exc_info=True)

        self._print_summary(summary)
        return summary

    def _process_partition(
        self, partition: SourcePartition
    ) -> tuple[pd.DataFrame | None, PartitionRunResult]:
        asset_dirs = ensure_partition_asset_dirs(partition)
        output_stem = monthly_output_stem(partition)
        records = self.reader.get_file_records(partition)

        all_rows: list[dict] = []
        for record in records:
            try:
                xml_bytes = self.reader.read_xml_content(record)
                rows = self.parser.parse(
                    xml_bytes,
                    source_file=record.source_file,
                    issuer_id=record.issuer_id,
                )
                all_rows.extend(rows)
            except Exception as exc:
                logger.error("Failed to parse %s: %s", record.source_file, exc)

        if not all_rows:
            return None, PartitionRunResult(
                partition=partition,
                status="skipped",
                output_path=partition.output_path,
            )

        cleaned_df = self.cleaner.clean(pd.DataFrame(all_rows), partition=partition)
        validation_df = self.dq_validator.results_to_dataframe(
            self.schema_validator.validate(cleaned_df, partition.issuer_id)
            + self.dq_validator.validate(cleaned_df, partition.issuer_id)
        )
        missingness_df = self.dq_validator.build_missingness_df(cleaned_df)
        file_profile_df = self.dq_validator.build_file_profile_df(cleaned_df)
        kpis = self.kpi_builder.build_kpis(cleaned_df, partition.issuer_id)
        kpi_summary_df = self.kpi_builder.kpis_to_summary_df(kpis)

        self._export_partition_outputs(
            cleaned_df, kpis, kpi_summary_df, validation_df,
            missingness_df, file_profile_df, output_stem, asset_dirs, partition,
        )

        return cleaned_df, PartitionRunResult(
            partition=partition,
            status="processed",
            output_path=partition.output_path,
            enrollee_count=len(cleaned_df),
        )

    def _process_rollup(self, issuer_id: str, monthly_dfs: list[pd.DataFrame]) -> None:
        combined_df = pd.concat(monthly_dfs, ignore_index=True)
        asset_dirs = ensure_rollup_asset_dirs(issuer_id)
        output_stem = rollup_output_stem(issuer_id)
        validation_df = self.dq_validator.results_to_dataframe(
            self.schema_validator.validate(combined_df, issuer_id)
            + self.dq_validator.validate(combined_df, issuer_id)
        )
        missingness_df = self.dq_validator.build_missingness_df(combined_df)
        file_profile_df = self.dq_validator.build_file_profile_df(combined_df)
        kpis = self.kpi_builder.build_kpis(combined_df, issuer_id)
        kpi_summary_df = self.kpi_builder.kpis_to_summary_df(kpis)

        self.excel_exporter.export_enrollees(combined_df, output_stem, asset_dirs["excel"])
        self.excel_exporter.export_kpis(
            kpis, kpi_summary_df, output_stem, asset_dirs["excel"], is_rollup=True
        )
        self.excel_exporter.export_validation_report(
            validation_df, missingness_df, file_profile_df,
            output_stem, asset_dirs["excel"],
        )
        self.sqlite_loader.load(
            combined_df, kpi_summary_df, validation_df,
            output_stem, asset_dirs["sqlite"], rollup=True,
        )
        self.dashboard.generate(
            kpis, kpi_summary_df, validation_df, missingness_df,
            output_stem, asset_dirs["dashboards"],
            title=f"Issuer {issuer_id} — All Periods Rollup Dashboard",
            is_rollup=True,
        )
        json_path = asset_dirs["validation_reports"] / f"validation_report_{output_stem}.json"
        json_path.parent.mkdir(parents=True, exist_ok=True)
        json_path.write_text(
            json.dumps(validation_df.to_dict(orient="records"), indent=2),
            encoding="utf-8",
        )

    def _export_partition_outputs(
        self, cleaned_df, kpis, kpi_summary_df, validation_df,
        missingness_df, file_profile_df, output_stem, asset_dirs, partition,
    ) -> None:
        self.excel_exporter.export_enrollees(cleaned_df, output_stem, asset_dirs["excel"])
        self.excel_exporter.export_kpis(
            kpis, kpi_summary_df, output_stem, asset_dirs["excel"]
        )
        self.excel_exporter.export_validation_report(
            validation_df, missingness_df, file_profile_df,
            output_stem, asset_dirs["excel"],
        )
        self.xml_exporter.export_enrollees(cleaned_df, output_stem, asset_dirs["cleaned_xml"])
        self.sqlite_loader.load(
            cleaned_df, kpi_summary_df, validation_df,
            output_stem, asset_dirs["sqlite"], rollup=False,
        )
        self.dashboard.generate(
            kpis, kpi_summary_df, validation_df, missingness_df,
            output_stem, asset_dirs["dashboards"],
            title=f"Issuer {partition.issuer_id} — {partition.year}/{partition.month} Dashboard",
            is_rollup=False,
        )
        json_path = asset_dirs["validation_reports"] / f"validation_report_{output_stem}.json"
        json_path.parent.mkdir(parents=True, exist_ok=True)
        json_path.write_text(
            json.dumps(validation_df.to_dict(orient="records"), indent=2),
            encoding="utf-8",
        )

    @staticmethod
    def _print_summary(summary: PipelineSummary) -> None:
        logger.info("=" * 60)
        logger.info("PROCESSING SUMMARY")
        logger.info("=" * 60)
        logger.info("Total partitions discovered : %d", summary.discovered)
        logger.info("Total partitions processed  : %d", summary.processed)
        logger.info("Total partitions skipped    : %d", summary.skipped)
        logger.info("Total partitions failed     : %d", summary.failed)
        if summary.rollup_issuers:
            logger.info(
                "Rollups created for issuers : %s",
                ", ".join(summary.rollup_issuers),
            )
        else:
            logger.info("Rollups created for issuers : (none)")
        if summary.failed_rollups:
            logger.info(
                "Rollups failed for issuers  : %s",
                ", ".join(summary.failed_rollups),
            )
        for result in summary.results:
            p = result.partition
            if result.status == "processed":
                logger.info(
                    "  [OK]   issuer=%s year=%s month=%s enrollees=%d → %s",
                    p.issuer_id, p.year, p.month, result.enrollee_count, result.output_path,
                )
            elif result.status == "skipped":
                logger.info("  [SKIP] issuer=%s year=%s month=%s", p.issuer_id, p.year, p.month)
            else:
                logger.info(
                    "  [FAIL] issuer=%s year=%s month=%s — %s",
                    p.issuer_id, p.year, p.month, result.error or "unknown",
                )
        logger.info("=" * 60)


def main() -> None:
    """CLI entry point — auto-discovers all issuers from source_data/."""
    log_path_configuration()
    summary = IssuerEtlPipeline(source=LocalFileSource()).run()
    if summary.failed > 0 or summary.failed_rollups:
        sys.exit(1)


if __name__ == "__main__":
    main()
