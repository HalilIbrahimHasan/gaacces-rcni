"""FTP connector placeholder — does not connect yet."""

from __future__ import annotations

from connectors.base_connector import SourceConnector, SourceFile
from config.config import settings
from utils.logger import get_logger

logger = get_logger(__name__)


class FTPSourceConnector(SourceConnector):
    """Future FTP download → source_data, then same local pipeline."""

    def mode(self) -> str:
        return "ftp"

    def sync(self) -> list[SourceFile]:
        logger.warning(
            "FTP mode is not implemented yet. Host=%s user=%s path=%s",
            settings.ftp_host,
            settings.ftp_user,
            settings.ftp_remote_path,
        )
        logger.info("Falling back to local source_data scan.")
        from connectors.local_connector import LocalSourceConnector

        return LocalSourceConnector().sync()
