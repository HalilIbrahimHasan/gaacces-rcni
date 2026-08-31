"""Abstract source connector interface."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path


@dataclass
class SourceFile:
    issuer: str
    year: str
    month: str
    file_name: str
    file_path: Path
    file_size: int
    source_type: str  # local | ftp | sftp


class SourceConnector(ABC):
    """Download or locate source files before ingestion."""

    @abstractmethod
    def sync(self) -> list[SourceFile]:
        """Return files ready for ingestion (download if remote)."""

    @abstractmethod
    def mode(self) -> str:
        """Connector mode label."""
