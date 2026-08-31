"""
Remove all artifacts from previous pipeline runs.

Every new run starts clean — only current source_data is processed.
"""

from __future__ import annotations

import shutil
from pathlib import Path

from config.config import settings
from utils.logger import get_logger

logger = get_logger(__name__)


def _rm_tree(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
        logger.info("Removed: %s", path)


def _rm_file(path: Path) -> None:
    if path.exists() and path.is_file():
        path.unlink()
        logger.info("Removed: %s", path)


def clean_output_dirs() -> None:
    """Wipe data/, reports/, extracted/, logs/, and legacy assets/."""
    data_dir = settings.project_root / "data"
    _rm_tree(data_dir)
    _rm_tree(settings.reports_path)
    _rm_tree(settings.extracted_path)
    _rm_tree(settings.logs_path)

    _rm_tree(settings.assets_path)

    settings.ensure_dirs()
    logger.info("Output directories reset — no traces from previous runs")


def clean_source_data() -> None:
    """Remove every issuer folder under source_data."""
    root = settings.source_data_path
    if not root.exists():
        root.mkdir(parents=True, exist_ok=True)
        return
    for child in root.iterdir():
        if child.name.startswith("."):
            continue
        if child.is_dir():
            shutil.rmtree(child)
            logger.info("Removed source issuer folder: %s", child.name)
        elif child.is_file():
            child.unlink()


def clean_workspace(clear_source: bool = False) -> None:
    """Full workspace reset before a new run."""
    logger.info("Cleaning workspace...")
    clean_output_dirs()
    if clear_source:
        clean_source_data()
    logger.info("Workspace clean")
