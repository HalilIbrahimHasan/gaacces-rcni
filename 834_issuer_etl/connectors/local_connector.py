"""Local filesystem connector — default mode."""

from __future__ import annotations

from connectors.base_connector import SourceConnector, SourceFile
from config.config import settings
from ingestion.file_discovery import discover_source_files


class LocalSourceConnector(SourceConnector):
    def mode(self) -> str:
        return "local"

    def sync(self) -> list[SourceFile]:
        return discover_source_files(
            settings.source_data_path,
            issuer_filter=settings.issuer_filter,
            year_filter=settings.year_filter,
            month_filter=settings.month_filter,
        )
