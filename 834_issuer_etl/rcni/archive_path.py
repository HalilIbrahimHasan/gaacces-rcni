"""Optional archive-path observations. Day/batch are not required path positions."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ArchivePathMetadata:
    issuer_id: str
    processing_year: str
    processing_month: str
    processing_day: str | None
    nested_relative: str | None
    source_path: str


def parse_archive_path(
    remote_path: str,
    *,
    remote_root: str,
    issuer: str,
    year: str,
    month: str,
) -> ArchivePathMetadata:
    """
    Observe folders under {root}/{issuer}/{year}/{month}/.

    Selection and discovery do not require a day or batch folder.
    If a 1–2 digit calendar day appears as a path component under month,
    record it as processing_day for staging/metadata only.
    Remaining nested folders are recorded as nested_relative.
    """
    root = remote_root.rstrip("/")
    rel = remote_path
    if rel.startswith(root + "/"):
        rel = rel[len(root) + 1 :]
    parts = [p for p in rel.split("/") if p]
    dirs = parts[:-1] if parts else []

    # dirs: issuer, year, month, [anything...]
    after_month = dirs[3:] if len(dirs) >= 3 else []

    processing_day: str | None = None
    nested: list[str] = list(after_month)
    for i, component in enumerate(after_month):
        if component.isdigit() and 1 <= int(component) <= 31 and len(component) <= 2:
            processing_day = str(int(component)).zfill(2)
            nested = after_month[:i] + after_month[i + 1 :]
            break

    return ArchivePathMetadata(
        issuer_id=issuer,
        processing_year=year,
        processing_month=str(int(month)).zfill(2) if str(month).isdigit() else month,
        processing_day=processing_day,
        nested_relative="/".join(nested) if nested else None,
        source_path=remote_path,
    )
