"""Shared DataFrame column helpers — avoid str/Series confusion."""

from __future__ import annotations

import re

import pandas as pd

from utils.logger import get_logger

logger = get_logger(__name__)

_TRAILING_ZERO = re.compile(r"^(-?\d+)\.0+$")


def zmonth(m: str) -> str:
    return str(m).strip().zfill(2)


def normalize_id(val: object) -> str:
    """Normalize ID values: strip, drop .0 suffix, preserve leading zeros, blank nulls."""
    if val is None:
        return ""
    try:
        if pd.isna(val):
            return ""
    except (TypeError, ValueError):
        pass
    s = str(val).strip()
    if s.lower() in ("nan", "none", "null", "<na>", "nat"):
        return ""
    m = _TRAILING_ZERO.match(s)
    if m:
        s = m.group(1)
    return s


def normalize_id_series(series: pd.Series) -> pd.Series:
    return series.map(normalize_id)


def find_col(df: pd.DataFrame, *names: str) -> str | None:
    """Resolve column name case-insensitively."""
    if df is None or df.empty:
        return None
    lower_map = {c.lower(): c for c in df.columns}
    for name in names:
        if not name:
            continue
        if name in df.columns:
            return name
        hit = lower_map.get(name.lower())
        if hit:
            return hit
    return None


def col_series(df: pd.DataFrame, *names: str, default: str = "") -> pd.Series:
    """
    Return the first existing column as a Series.
    Never return a bare string (fixes 'str' object has no attribute apply/astype).
    """
    if df is None or df.empty:
        return pd.Series(dtype=object)
    resolved = find_col(df, *names)
    if resolved:
        return df[resolved]
    logger.debug(
        "Column(s) %s not in dataframe (cols=%s) — using default",
        names, list(df.columns)[:12],
    )
    return pd.Series([default] * len(df), index=df.index)
