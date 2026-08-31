"""RCNI scope — existing 834 filter/SFTP contract, plus RCNI_BASE_PATH."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from config.config import PROJECT_ROOT, settings
from ingestion.sftp_filters import filters_from_settings, format_filter_display
from rcni.constants import DEFAULT_RCNI_BASE_PATH, FORBIDDEN_INBOUND_PATH_FRAGMENT


@dataclass(frozen=True)
class RcniScope:
    base_path: str
    issuer_allow: set[str] | None
    year_allow: set[str] | None
    month_allow: set[str] | None
    force_download: bool
    keep_compressed: bool
    sftp_host: str
    sftp_port: int
    sftp_user: str
    sftp_password: str
    local_root: Path
    reports_dir: Path
    logs_dir: Path

    @property
    def issuer_display(self) -> str:
        return format_filter_display(self.issuer_allow)

    @property
    def year_display(self) -> str:
        return format_filter_display(self.year_allow)

    @property
    def month_display(self) -> str:
        return format_filter_display(self.month_allow)


def resolve_rcni_scope(
    *,
    issuer: str | None = None,
    year: str | None = None,
    month: str | None = None,
) -> RcniScope:
    """
    Same 834 startup sequence as main.py:

      settings.refresh_from_env() → apply_cli_filters → filters_from_settings

    Empty ISSUER_FILTER / YEAR_FILTER / MONTH_FILTER means ALL (None allow-set).
    CLI --issuer/--year/--month override those settings when provided.
    """
    settings.refresh_from_env()
    settings.apply_cli_filters(issuer, year, month)
    issuer_allow, year_allow, month_allow = filters_from_settings(settings)

    base_path = (os.getenv("RCNI_BASE_PATH") or "").strip() or DEFAULT_RCNI_BASE_PATH
    if FORBIDDEN_INBOUND_PATH_FRAGMENT in base_path.replace("\\", "/"):
        raise SystemExit(
            f"Refusing RCNI base path {base_path!r}: RCNI must not traverse "
            f"{FORBIDDEN_INBOUND_PATH_FRAGMENT} (issuer MONTHLYRECON input)."
        )

    return RcniScope(
        base_path=base_path.rstrip("/") or DEFAULT_RCNI_BASE_PATH,
        issuer_allow=issuer_allow,
        year_allow=year_allow,
        month_allow=month_allow,
        force_download=settings.force_download,
        keep_compressed=settings.keep_compressed,
        sftp_host=settings.sftp_host,
        sftp_port=settings.sftp_port,
        sftp_user=settings.sftp_user,
        sftp_password=settings.sftp_password,
        local_root=PROJECT_ROOT / "assets" / "rcni",
        reports_dir=PROJECT_ROOT / "outputs" / "rcni" / "validation",
        logs_dir=PROJECT_ROOT / "logs" / "rcni",
    )
