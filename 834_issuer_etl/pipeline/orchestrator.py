"""
Full pipeline orchestrator: ingest → parse → load → validate → reconcile → report.
"""

from __future__ import annotations

from config.config import settings
from connectors.base_connector import SourceConnector
from connectors.ftp_connector import FTPSourceConnector
from connectors.local_connector import LocalSourceConnector
from connectors.sftp_connector import SFTPSourceConnector
from database.db import Database
from database.loaders import DataLoader
from ingestion.xml_reader import read_xml_bytes
from parsers.parser_834 import Parser834
from reconciliation.business_rules import apply_business_rules
from reconciliation.premium_validation import apply_premium_validation
from reconciliation.user_fee_calculation import apply_user_fees
from pipeline.assets_exporter import export_assets
from reporting.report_runner import run_kpi_reports
from utils.cleanup import clean_workspace
from utils.logger import get_logger
from validation.load_validation import run_load_validation

logger = get_logger(__name__)


def get_connector() -> SourceConnector:
    mode = settings.processing_mode
    if mode == "ftp":
        return FTPSourceConnector()
    if mode == "sftp":
        return SFTPSourceConnector()
    return LocalSourceConnector()


class Pipeline:
    def __init__(self) -> None:
        if settings.clean_on_start:
            clean_workspace(clear_source=False)

        settings.ensure_dirs()
        self.db = Database()
        self.db.init_schema()
        self.loader = DataLoader(self.db)
        self.parser = Parser834()

    def ingest_and_load(self) -> dict:
        connector = get_connector()
        logger.info("Processing mode: %s", connector.mode())
        sources = connector.sync()
        stats = {
            "files_discovered": len(sources),
            "files_processed": 0,
            "files_skipped": 0,
            "files_failed": 0,
            "records_loaded": 0,
        }

        for source in sources:
            try:
                file_id, is_dup = self.loader.register_file(source)
                if is_dup:
                    stats["files_skipped"] += 1
                    continue

                xml_bytes = read_xml_bytes(source)
                records = self.parser.parse_file(
                    xml_bytes,
                    issuer=source.issuer,
                    year=source.year,
                    month=source.month,
                    file_name=source.file_name,
                    file_path=str(source.file_path),
                )
                for r in records:
                    r["file_id"] = file_id

                count = self.loader.load_records(file_id, records)
                self.loader.mark_file_status(file_id, "success")
                stats["files_processed"] += 1
                stats["records_loaded"] += count
            except Exception as exc:
                stats["files_failed"] += 1
                logger.error("Failed %s: %s", source.file_name, exc, exc_info=True)
                self.loader.log_parse_error(source, str(exc))
                try:
                    self.loader.mark_file_status(file_id, "failed", str(exc))
                except Exception:
                    pass

        logger.info("Ingestion complete: %s", stats)
        return stats

    def reconcile(self) -> None:
        logger.info("Running reconciliation rules...")
        apply_premium_validation(self.db)
        apply_user_fees(self.db)
        apply_business_rules(self.db)

    def validate(self, issuer: str | None = None) -> None:
        run_load_validation(self.db, issuer)

    def report(self, issuer: str | None = None) -> None:
        run_kpi_reports(self.db, issuer)

    def export_assets(self, issuer: str | None = None) -> dict[str, int]:
        return export_assets(self.db.conn, issuer or settings.issuer_filter)

    def run_full(self, issuer: str | None = None) -> dict:
        stats = self.ingest_and_load()
        self.reconcile()
        self.validate(issuer or settings.issuer_filter)
        self.report(issuer or settings.issuer_filter)

        if settings.fast_business_report_mode:
            settings.apply_fast_business_report_mode(True)
            logger.info("FAST_BUSINESS_REPORT_MODE=true — using lightweight reporting path")

        if settings.generate_legacy_assets_reports and not settings.fast_business_report_mode:
            asset_stats = self.export_assets(issuer or settings.issuer_filter)
            stats["asset_partitions"] = asset_stats["partitions"]
            stats["asset_rollups"] = asset_stats["rollups"]
        elif settings.fast_business_report_mode:
            logger.info("Skipping legacy assets export (GENERATE_LEGACY_ASSETS_REPORTS=false)")
            stats["asset_partitions"] = 0
            stats["asset_rollups"] = 0
        else:
            asset_stats = self.export_assets(issuer or settings.issuer_filter)
            stats["asset_partitions"] = asset_stats["partitions"]
            stats["asset_rollups"] = asset_stats["rollups"]

        if settings.fast_business_report_mode:
            try:
                from azure_reconciliation.fast_business_reports import run_fast_business_reports
                from azure_reconciliation.safe_export import ExportErrors

                fast_stats = run_fast_business_reports(
                    issuer_filter=issuer or settings.issuer_filter,
                    year_filter=settings.year_filter,
                    month_filter=settings.month_filter,
                    all_issuers=settings.issuer_filter is None and settings.year_filter is not None,
                    parse_source=False,
                    export_errors=ExportErrors(),
                )
                stats["fast_business_reports"] = fast_stats
                logger.info("Fast business reports: %s", fast_stats.get("output_root"))
            except Exception as exc:
                logger.warning("Fast business reports skipped — %s", exc)

            if settings.generate_storage_estimate:
                try:
                    from azure_reconciliation.storage_estimator import run_storage_estimator

                    storage_stats = run_storage_estimator(
                        issuer_filter=issuer or settings.issuer_filter,
                        year_filter=settings.year_filter,
                        month_filter=settings.month_filter,
                        all_issuers=settings.issuer_filter is None and settings.year_filter is not None,
                    )
                    stats["storage_estimates"] = storage_stats
                except Exception as exc:
                    logger.warning("Storage estimator skipped — %s", exc)
        else:
            try:
                from azure_reconciliation.safe_export import ExportErrors
                from azure_reconciliation.xml_business_reports import run_xml_business_reporting

                xml_stats = run_xml_business_reporting(
                    issuer=issuer or settings.issuer_filter,
                    parse_source=False,
                    export_errors=ExportErrors(),
                    disable_azure=True,
                )
                stats["xml_business_reports"] = xml_stats
                logger.info("XML business + assets-style reports: %s", xml_stats.get("output_root"))
            except Exception as exc:
                logger.warning("XML business reports skipped — %s", exc)

        if settings.enable_full_data_export and not settings.fast_business_report_mode:
            try:
                from azure_reconciliation.full_data_exports import run_full_data_exports
                from azure_reconciliation.safe_export import ExportErrors

                export_stats = run_full_data_exports(
                    issuer_filter=issuer or settings.issuer_filter,
                    year_filter=settings.year_filter,
                    parse_source=False,
                    export_errors=ExportErrors(),
                )
                stats["full_data_exports"] = export_stats
                logger.info("Full data exports: %s", export_stats.get("output_root"))
            except Exception as exc:
                logger.warning("Full data exports skipped — %s", exc)

        if settings.filter_prior_year_benefit_effective and settings.generate_filtered_reports:
            try:
                from azure_reconciliation.prior_year_benefit_filter import run_prior_year_filter_end_to_end
                from azure_reconciliation.safe_export import ExportErrors

                filtered_stats = run_prior_year_filter_end_to_end(
                    issuer_filter=issuer or settings.issuer_filter,
                    year_filter=settings.year_filter,
                    parse_source=False,
                    export_errors=ExportErrors(),
                )
                stats["prior_year_filter"] = filtered_stats
                logger.info(
                    "Prior-year filtered pipeline complete → %s",
                    filtered_stats.get("filtered_output_root"),
                )
            except Exception as exc:
                logger.warning("Prior-year filter pipeline skipped — %s", exc)

        if (
            not settings.xml_only_business_mode
            and settings.azure_enabled
            and settings.azure_reconciliation_enabled
        ):
            try:
                from azure_reconciliation.intelligence_pipeline import run_intelligence_pipeline

                recon_stats = run_intelligence_pipeline(
                    issuer_filter=issuer or settings.issuer_filter,
                    year_filter=settings.year_filter,
                    month_filter=settings.month_filter,
                    prefer_staging=True,
                    skip_azure=False,
                )
                stats["azure_reconciliation"] = recon_stats
                logger.info("Azure intelligence stats: %s", recon_stats)
            except Exception as exc:
                logger.warning("Azure intelligence skipped — %s", exc)

        return stats

    def close(self) -> None:
        self.db.close()
