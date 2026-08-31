"""Parse and apply SFTP partition filters (issuer, year, month)."""

from __future__ import annotations

from utils.logger import get_logger

logger = get_logger(__name__)


def _normalize_month(value: str) -> str:
    return str(int(value)).zfill(2)


def parse_csv_filter(raw: str | None, *, normalizer=None) -> set[str] | None:
    """
    Parse comma-separated filter values.

    Returns None when empty (= ALL), else a set of normalized values.
    """
    if raw is None or not str(raw).strip():
        return None
    items: list[str] = []
    for part in str(raw).split(","):
        part = part.strip()
        if not part:
            continue
        items.append(normalizer(part) if normalizer else part)
    return set(items) if items else None


def format_filter_display(allow: set[str] | None) -> str:
    if allow is None:
        return "ALL"
    return "[" + ", ".join(sorted(allow)) + "]"


def log_effective_filters(
    issuer_allow: set[str] | None,
    year_allow: set[str] | None,
    month_allow: set[str] | None,
) -> None:
    logger.info("Effective filters:")
    logger.info("  issuers=%s", format_filter_display(issuer_allow))
    logger.info("  years=%s", format_filter_display(year_allow))
    logger.info("  months=%s", format_filter_display(month_allow))


def partition_matches(
    issuer: str,
    year: str,
    month: str,
    issuer_allow: set[str] | None,
    year_allow: set[str] | None,
    month_allow: set[str] | None,
) -> bool:
    month_norm = _normalize_month(month)
    if issuer_allow is not None and issuer not in issuer_allow:
        return False
    if year_allow is not None and year not in year_allow:
        return False
    if month_allow is not None and month_norm not in month_allow:
        return False
    return True


def filters_from_settings(settings) -> tuple[set[str] | None, set[str] | None, set[str] | None]:
    return (
        parse_csv_filter(settings.issuer_filter),
        parse_csv_filter(settings.year_filter),
        parse_csv_filter(settings.month_filter, normalizer=_normalize_month),
    )
