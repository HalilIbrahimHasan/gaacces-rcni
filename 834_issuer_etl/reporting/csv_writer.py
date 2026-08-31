"""CSV report writer."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from utils.logger import get_logger

logger = get_logger(__name__)


def write_csv(df: pd.DataFrame, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    logger.info("Wrote CSV: %s (%d rows)", path, len(df))
    return path
