"""
XML file reader with pluggable data-source abstraction.

Partitions are discovered at issuer/year/month granularity. Paths come from
``config.SOURCE_DATA_DIR`` (code-anchored), not the terminal CWD.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path

import config
from utils.logger import get_logger
from utils.partition import SourcePartition, discover_partitions

logger = get_logger(__name__)


class DataSource(ABC):
    """Abstract contract for locating and reading partitioned issuer XML files."""

    @abstractmethod
    def discover_partitions(self) -> list[SourcePartition]:
        """Return all partitions discovered under the source root."""

    @abstractmethod
    def read_bytes(self, file_id: str) -> bytes:
        """Read raw XML bytes for a given file identifier."""


class LocalFileSource(DataSource):
    """Read 834 XML from ``source_data/{issuer}/{year}/{month}/`` on local disk."""

    def __init__(self, source_dir: Path | None = None) -> None:
        self.source_dir = source_dir or config.SOURCE_DATA_DIR

    def discover_partitions(self) -> list[SourcePartition]:
        return discover_partitions(source_root=self.source_dir)

    def read_bytes(self, file_id: str) -> bytes:
        path = Path(file_id)
        logger.debug("Reading local file: %s", path)
        return path.read_bytes()


@dataclass
class XmlFileRecord:
    """Metadata wrapper for a single XML input file within a partition."""

    issuer_id: str
    source_year: str
    source_month: str
    source_period: str
    source_file: str
    file_path: str


class XmlReader:
    """High-level reader that enumerates partition XML files via a ``DataSource``."""

    def __init__(self, source: DataSource | None = None) -> None:
        self.source = source or LocalFileSource()

    def get_file_records(self, partition: SourcePartition) -> list[XmlFileRecord]:
        """Build file records for all XML files in a partition."""
        records = [
            XmlFileRecord(
                issuer_id=partition.issuer_id,
                source_year=partition.year,
                source_month=partition.month,
                source_period=partition.source_period,
                source_file=xml_path.name,
                file_path=str(xml_path),
            )
            for xml_path in partition.xml_files
        ]
        logger.info(
            "Prepared %d XML file record(s) for partition %s",
            len(records),
            partition.period_key,
        )
        return records

    def read_xml_content(self, record: XmlFileRecord) -> bytes:
        """Read raw XML bytes for a file record."""
        return self.source.read_bytes(record.file_path)
