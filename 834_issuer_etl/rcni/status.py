"""Choose overall file status from collected flags (worst first)."""

from __future__ import annotations

from rcni.constants import STATUS_CLEAN, STATUS_PRIORITY


def overall_status(flags: list[str]) -> str:
    unique = [flag for flag in flags if flag]
    if not unique:
        return STATUS_CLEAN
    for candidate in STATUS_PRIORITY:
        if candidate in unique:
            return candidate
    return unique[0]
