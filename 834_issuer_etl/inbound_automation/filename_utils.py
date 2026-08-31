"""Filename timestamp parsing for 834 source files."""

from __future__ import annotations

import re
from pathlib import Path

# Supported filename timestamp patterns (year/month extraction only).
#
# 1) Existing patterns:
#    - ...INDV_YYYYMMDDhhmmss.xml
#    - ..._YYYYMMDDhhmmss.xml
#
# 2) ISO-like patterns (new):
#    - ...YYYY-MM-DDThhmmss....xml
#      Example: 2025-01-04T14264700.P.xml
#
# 3) 14-digit timestamp with suffixes (new):
#    - ...YYYYMMDDhhmmss_1.xml
#    - ...YYYYMMDDhhmmss.P.xml
#    - ...YYYYMMDDhhmmss_extra.xml
#
# Safety: prefer anchored patterns near end-of-filename. Only extracts when a
# clear year+month is present.
_FILENAME_TS_AFTER_INDV = re.compile(
    r"(?:INDV|INVD)_(\d{4})(\d{2})(\d{2})\d{6}\.xml$",
    re.IGNORECASE,
)
_FILENAME_TS_FALLBACK = re.compile(r"_(\d{4})(\d{2})(\d{2})\d{6}\.xml$", re.IGNORECASE)

_FILENAME_TS_ISO_AFTER_INDV = re.compile(
    r"(?:INDV|INVD)_(\d{4})-(\d{2})-(\d{2})T\d{6,8}[^/]*\.xml$",
    re.IGNORECASE,
)
_FILENAME_TS_ISO_FALLBACK = re.compile(
    r"_(\d{4})-(\d{2})-(\d{2})T\d{6,8}[^/]*\.xml$",
    re.IGNORECASE,
)

_FILENAME_TS_14_SUFFIX_AFTER_INDV = re.compile(
    r"(?:INDV|INVD)_(\d{4})(\d{2})(\d{2})\d{6}[^/]*\.xml$",
    re.IGNORECASE,
)
_FILENAME_TS_14_SUFFIX_FALLBACK = re.compile(
    r"_(\d{4})(\d{2})(\d{2})\d{6}[^/]*\.xml$",
    re.IGNORECASE,
)


def parse_filename_year_month(filename: str) -> tuple[int | None, int | None]:
    """
    Parse YYYYMMDD from standard 834 filenames.

    Returns (year, month) as integers, or (None, None) when not parseable.
    """
    name = Path(str(filename)).name
    match = (
        _FILENAME_TS_AFTER_INDV.search(name)
        or _FILENAME_TS_FALLBACK.search(name)
        or _FILENAME_TS_ISO_AFTER_INDV.search(name)
        or _FILENAME_TS_ISO_FALLBACK.search(name)
        or _FILENAME_TS_14_SUFFIX_AFTER_INDV.search(name)
        or _FILENAME_TS_14_SUFFIX_FALLBACK.search(name)
    )
    if not match:
        return None, None
    return int(match.group(1)), int(match.group(2))
