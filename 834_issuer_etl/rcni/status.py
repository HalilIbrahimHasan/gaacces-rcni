"""Choose overall file status from collected flags (worst first)."""

from __future__ import annotations

from rcni.constants import STATUS_CLEAN, STATUS_MALFORMED, STATUS_SCHEMA_MISMATCH, STATUS_WARNING


def overall_status(flags: list[str]) -> str:
    """File-level status: MALFORMED (structural/schema), WARNING, or CLEAN."""
    unique = [flag for flag in flags if flag]
    if STATUS_MALFORMED in unique or STATUS_SCHEMA_MISMATCH in unique:
        return STATUS_MALFORMED
    if unique and unique != [STATUS_CLEAN]:
        return STATUS_WARNING
    return STATUS_CLEAN
