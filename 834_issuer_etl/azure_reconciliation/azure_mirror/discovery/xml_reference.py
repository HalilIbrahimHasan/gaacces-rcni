"""Load XML enrollment summaries from assets for strategy comparison."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from config.config import settings
from utils.logger import get_logger

logger = get_logger(__name__)

OUTPUT_COLUMNS = [
    "Coverage_Year",
    "GAA_HIOS_ID",
    "GAA_Load_Date",
    "Insurance_Type",
    "status_id",
    "enrolleeStatus",
    "Enrollment_Count",
    "Enrollee_Count",
]


def load_xml_summaries(issuer: str) -> pd.DataFrame:
    """
    Load Hari-format enrollment summaries from assets/{issuer}/{year}/{month}/excel/.
    Does not read XML from assets — only existing summary Excel outputs.
    """
    base = settings.assets_path / issuer
    frames: list[pd.DataFrame] = []

    if not base.exists():
        logger.info("No XML summary assets for issuer %s", issuer)
        return pd.DataFrame()

    for year_dir in sorted(base.iterdir()):
        if not year_dir.is_dir() or not year_dir.name.isdigit():
            continue
        for month_dir in sorted(year_dir.iterdir()):
            if not month_dir.is_dir():
                continue
            stem = f"enrollment_summary_{issuer}_{year_dir.name}_{month_dir.name}"
            for ext in (".xlsx", ".csv"):
                path = month_dir / "excel" / f"{stem}{ext}"
                if not path.exists():
                    continue
                try:
                    df = pd.read_csv(path) if ext == ".csv" else pd.read_excel(path)
                    df["_source_year"] = year_dir.name
                    df["_source_month"] = str(month_dir.name).zfill(2)
                    df["_xml_summary_path"] = str(path)
                    frames.append(df)
                except Exception as exc:
                    logger.warning("Could not read XML summary %s: %s", path, exc)

    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    logger.info("Loaded %d XML summary row(s) from assets for issuer %s", len(combined), issuer)
    return combined


def xml_monthly_totals(xml_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate XML summaries to issuer/year/month/status totals."""
    if xml_df.empty:
        return pd.DataFrame(columns=[
            "year", "month", "insurance_type", "status",
            "enrollment_count", "enrollee_count",
        ])

    df = xml_df.copy()
    df["year"] = df.get("_source_year", df.get("Coverage_Year", "")).astype(str)
    df["month"] = df.get("_source_month", "").astype(str).str.zfill(2)
    df["insurance_type"] = df.get("Insurance_Type", "(unknown)").astype(str)
    df["status"] = df.get("enrolleeStatus", "(unknown)").astype(str)

    for col in ("Enrollment_Count", "Enrollee_Count"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    agg = (
        df.groupby(["year", "month", "insurance_type", "status"], dropna=False)
        .agg(
            enrollment_count=("Enrollment_Count", "sum"),
            enrollee_count=("Enrollee_Count", "sum"),
        )
        .reset_index()
    )
    return agg


def xml_month_status_pivot(xml_totals: pd.DataFrame) -> pd.DataFrame:
    """Wide view: month x status enrollee totals for pattern comparison."""
    if xml_totals.empty:
        return pd.DataFrame()
    return xml_totals.pivot_table(
        index=["year", "month"],
        columns="status",
        values="enrollee_count",
        aggfunc="sum",
        fill_value=0,
    ).reset_index()
