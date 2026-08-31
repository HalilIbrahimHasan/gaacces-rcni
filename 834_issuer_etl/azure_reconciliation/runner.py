"""
Azure vs XML enrollment reconciliation runner.

Extension only — delegates to intelligence_pipeline.
"""

from __future__ import annotations

from typing import Any

from azure_reconciliation.intelligence_pipeline import run_intelligence_pipeline


def run_reconciliation(
    *,
    issuer_filter: str | None = None,
    year_filter: str | None = None,
    month_filter: str | None = None,
    prefer_staging: bool = True,
    skip_azure: bool = False,
    run_validation: bool = True,
) -> dict[str, Any]:
    """Execute full Azure vs XML reconciliation for discovered source_data scope."""
    return run_intelligence_pipeline(
        issuer_filter=issuer_filter,
        year_filter=year_filter,
        month_filter=month_filter,
        prefer_staging=prefer_staging,
        skip_azure=skip_azure,
    )
