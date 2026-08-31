"""SFTP connector — downloads from remote day/batch folders, then runs local pipeline."""

from __future__ import annotations

from connectors.base_connector import SourceConnector, SourceFile
from connectors.local_connector import LocalSourceConnector
from config.config import settings
from ingestion.sftp_audit import run_sftp_audit
from ingestion.sftp_filters import filters_from_settings
from ingestion.sftp_ingestion import ingest_from_sftp
from utils.logger import get_logger

logger = get_logger(__name__)


class SFTPSourceConnector(SourceConnector):
    """
    Download 834 XML from SFTP, flatten into source_data/{issuer}/{year}/{month}/,
    then delegate to the existing local discovery + pipeline.
    """

    def mode(self) -> str:
        return "sftp"

    def sync(self) -> list[SourceFile]:
        if not settings.sftp_host or not settings.sftp_user:
            logger.error(
                "SFTP credentials missing. Set SFTP_HOST, SFTP_USERNAME, SFTP_PASSWORD in .env"
            )
            logger.info("Falling back to local source_data scan.")
            return LocalSourceConnector().sync()

        issuer_allow, year_allow, month_allow = filters_from_settings(settings)

        logger.info(
            "Connecting to SFTP %s:%d as %s (root=%s)",
            settings.sftp_host,
            settings.sftp_port,
            settings.sftp_user,
            settings.sftp_remote_path,
        )

        try:
            if settings.sftp_audit_only:
                run_sftp_audit(
                    host=settings.sftp_host,
                    port=settings.sftp_port,
                    username=settings.sftp_user,
                    password=settings.sftp_password,
                    remote_root=settings.sftp_remote_path,
                    local_root=settings.source_data_path,
                    issuer_allow=issuer_allow,
                    year_allow=year_allow,
                    month_allow=month_allow,
                    reports_dir=settings.reports_path,
                )
                logger.info("SFTP audit complete (no download performed)")
                return []

            count, _ = ingest_from_sftp(
                host=settings.sftp_host,
                port=settings.sftp_port,
                username=settings.sftp_user,
                password=settings.sftp_password,
                remote_root=settings.sftp_remote_path,
                local_root=settings.source_data_path,
                issuer_allow=issuer_allow,
                year_allow=year_allow,
                month_allow=month_allow,
                force_download=settings.force_download,
                keep_compressed=settings.keep_compressed,
                reports_dir=settings.reports_path,
            )
            logger.info("SFTP sync: %d XML file(s) placed in %s", count, settings.source_data_path)
        except Exception as exc:
            logger.error("SFTP ingestion failed: %s", exc, exc_info=True)
            raise

        return LocalSourceConnector().sync()
