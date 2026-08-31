"""Read XML bytes from discovered source files."""

from __future__ import annotations

from pathlib import Path

from connectors.base_connector import SourceFile
from utils.logger import get_logger

logger = get_logger(__name__)


def read_xml_bytes(source_file: SourceFile) -> bytes:
    path = Path(source_file.file_path)
    logger.debug("Reading XML: %s", path)
    return path.read_bytes()
