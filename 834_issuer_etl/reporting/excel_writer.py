"""Excel report writer."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from utils.logger import get_logger

logger = get_logger(__name__)


def write_excel(
    df: pd.DataFrame,
    path: Path,
    sheet_name: str = "data",
    extra_sheets: dict[str, pd.DataFrame] | None = None,
) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name=sheet_name, index=False)
        if extra_sheets:
            for name, sheet_df in extra_sheets.items():
                sheet_df.to_excel(writer, sheet_name=name[:31], index=False)
    logger.info("Wrote Excel: %s", path)
    return path
