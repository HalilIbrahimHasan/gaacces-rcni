"""
Data cleaning and standardization for parsed 834 enrollee rows.

Transforms raw string values into typed, consistently named columns and
applies PII masking before data reaches exports or dashboards.
"""

from datetime import datetime

import pandas as pd

from config import (
    DATE_COLUMNS,
    EXPORT_PII,
    NUMERIC_COLUMNS,
    PII_COLUMNS,
    REQUIRED_COLUMNS,
)
from utils.file_utils import parse_file_date_from_filename
from utils.logger import get_logger
from utils.partition import SourcePartition

logger = get_logger(__name__)

_MASK_VALUE = "***MASKED***"


class DataCleaner:
    """
    Clean and standardize a DataFrame of parsed 834 enrollee records.

    Ensures downstream validation, KPI, and export stages receive uniform
    column names, types, and safe (non-PII) values by default.
    """

    def clean(
        self, df: pd.DataFrame, partition: SourcePartition | None = None
    ) -> pd.DataFrame:
        """
        Run the full cleaning pipeline on a raw parsed DataFrame.

        Steps: strip strings, convert dates/numerics, add metadata columns,
        mask PII, and align column order to the configured schema.

        Args:
            df: Raw DataFrame from ``Xml834Parser``.

        Returns:
            Cleaned DataFrame ready for validation and export.
        """
        if df.empty:
            logger.warning("Received empty DataFrame — returning as-is with schema")
            return self._ensure_schema(df, partition)

        cleaned = df.copy()

        cleaned = self._strip_strings(cleaned)
        cleaned = self._convert_dates(cleaned)
        cleaned = self._convert_numerics(cleaned)
        cleaned = self._add_metadata(cleaned, partition)
        cleaned = self._mask_pii(cleaned)
        cleaned = self._ensure_schema(cleaned, partition)

        logger.info("Cleaned %d enrollee row(s)", len(cleaned))
        return cleaned

    def _strip_strings(self, df: pd.DataFrame) -> pd.DataFrame:
        """Strip leading/trailing whitespace from all object columns."""
        for col in df.select_dtypes(include=["object", "string"]).columns:
            df[col] = df[col].apply(
                lambda v: v.strip() if isinstance(v, str) else v
            )
        return df

    def _convert_dates(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert YYYYMMDD source values to ISO date strings (YYYY-MM-DD).

        Invalid or missing values become ``NaT`` then empty string for export
        compatibility.
        """
        for col in DATE_COLUMNS:
            if col not in df.columns:
                continue
            df[col] = pd.to_datetime(
                df[col].astype(str).str.replace(r"\.0$", "", regex=True),
                format="%Y%m%d",
                errors="coerce",
            ).dt.strftime("%Y-%m-%d")
            df[col] = df[col].fillna("")
        return df

    def _convert_numerics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Coerce amount and quantity fields to float; invalid values become NaN."""
        for col in NUMERIC_COLUMNS:
            if col not in df.columns:
                continue
            df[col] = pd.to_numeric(df[col], errors="coerce")
        return df

    def _add_metadata(
        self, df: pd.DataFrame, partition: SourcePartition | None = None
    ) -> pd.DataFrame:
        """
        Add partition columns, ``load_timestamp``, ``file_date``, and IDs.

        When a ``SourcePartition`` is provided, stamps ``source_year``,
        ``source_month``, and ``source_period`` on every row.
        """
        df["load_timestamp"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        if partition is not None:
            df["issuer_id"] = partition.issuer_id
            df["source_year"] = partition.year
            df["source_month"] = partition.month
            df["source_period"] = partition.source_period

        if "file_date" not in df.columns:
            df["file_date"] = ""

        # Fill file_date from GS04 when empty
        if "gs04" in df.columns:
            mask = (df["file_date"] == "") | df["file_date"].isna()
            df.loc[mask, "file_date"] = df.loc[mask, "gs04"]

        # Fallback: parse date from filename
        if "source_file" in df.columns:
            still_empty = (df["file_date"] == "") | df["file_date"].isna()
            df.loc[still_empty, "file_date"] = df.loc[still_empty, "source_file"].apply(
                lambda f: self._filename_date_to_iso(f) or ""
            )

        return df

    @staticmethod
    def _filename_date_to_iso(filename: str) -> str | None:
        """Convert embedded filename date token to ISO format."""
        raw = parse_file_date_from_filename(filename)
        if not raw:
            return None
        try:
            return datetime.strptime(raw, "%Y%m%d").strftime("%Y-%m-%d")
        except ValueError:
            return None

    def _mask_pii(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Mask PII columns unless ``EXPORT_PII`` is enabled in config.

        Default behavior protects sensitive member data in all outputs.
        """
        if EXPORT_PII:
            logger.warning("EXPORT_PII=True — sensitive fields will NOT be masked")
            return df

        for col in PII_COLUMNS:
            if col in df.columns:
                df[col] = _MASK_VALUE
        return df

    def _ensure_schema(
        self, df: pd.DataFrame, partition: SourcePartition | None = None
    ) -> pd.DataFrame:
        """
        Add missing required columns and order columns per config schema.

        PII columns are included only when ``EXPORT_PII`` is True.
        """
        for col in REQUIRED_COLUMNS:
            if col not in df.columns:
                df[col] = None

        if EXPORT_PII:
            for col in PII_COLUMNS:
                if col not in df.columns:
                    df[col] = None

        export_cols = list(REQUIRED_COLUMNS)
        if EXPORT_PII:
            export_cols.extend(c for c in PII_COLUMNS if c not in export_cols)

        # Keep any extra parsed columns at the end for debugging
        extra = [c for c in df.columns if c not in export_cols]
        return df[export_cols + extra]
