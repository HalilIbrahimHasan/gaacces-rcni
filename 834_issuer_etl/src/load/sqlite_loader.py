"""SQLite loader — persists enrollees, KPIs, validation, and enrollment summaries."""

import sqlite3
from datetime import datetime
from pathlib import Path

import pandas as pd

from config import (
    TABLE_ENROLLEES,
    TABLE_ENROLLEES_ROLLUP,
    TABLE_ENROLLMENT_SUMMARY,
    TABLE_ENROLLMENT_SUMMARY_ROLLUP,
    TABLE_KPIS,
    TABLE_KPIS_ROLLUP,
    TABLE_VALIDATION,
    TABLE_VALIDATION_ROLLUP,
)
from transform.enrollment_summary import OUTPUT_COLUMNS
from utils.logger import get_logger

logger = get_logger(__name__)


class SqliteLoader:
    """Load cleaned data into per-partition or rollup SQLite databases."""

    def load(
        self,
        df: pd.DataFrame,
        kpi_summary_df: pd.DataFrame,
        validation_df: pd.DataFrame,
        output_stem: str,
        output_dir: Path,
        *,
        enrollment_summary_df: pd.DataFrame | None = None,
        rollup: bool = False,
    ) -> Path:
        output_dir.mkdir(parents=True, exist_ok=True)
        db_path = output_dir / f"issuer_{output_stem}.db"
        enrollee_table = TABLE_ENROLLEES_ROLLUP if rollup else TABLE_ENROLLEES
        kpi_table = TABLE_KPIS_ROLLUP if rollup else TABLE_KPIS
        validation_table = TABLE_VALIDATION_ROLLUP if rollup else TABLE_VALIDATION
        summary_table = (
            TABLE_ENROLLMENT_SUMMARY_ROLLUP if rollup else TABLE_ENROLLMENT_SUMMARY
        )

        with sqlite3.connect(db_path) as conn:
            summary_export = (
                enrollment_summary_df[OUTPUT_COLUMNS].copy()
                if enrollment_summary_df is not None and not enrollment_summary_df.empty
                else pd.DataFrame(columns=OUTPUT_COLUMNS)
            )
            summary_export.to_sql(summary_table, conn, if_exists="replace", index=False)

            export_df = df.copy()
            if not export_df.empty:
                export_df.insert(0, "id", range(1, len(export_df) + 1))
            export_df.to_sql(enrollee_table, conn, if_exists="replace", index=False)

            kpi_df = kpi_summary_df.copy()
            kpi_df["output_stem"] = output_stem
            kpi_df["load_timestamp"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            kpi_df.to_sql(kpi_table, conn, if_exists="replace", index=False)

            if validation_df.empty:
                pd.DataFrame(columns=[
                    "issuer_id", "check_category", "check_name", "status",
                    "message", "details", "affected_count",
                ]).to_sql(validation_table, conn, if_exists="replace", index=False)
            else:
                validation_df.to_sql(
                    validation_table, conn, if_exists="replace", index=False
                )

        logger.info("Loaded SQLite database: %s", db_path)
        return db_path
