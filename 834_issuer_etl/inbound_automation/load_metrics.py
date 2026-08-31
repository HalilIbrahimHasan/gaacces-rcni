"""Per-file Azure insert timing metrics for inbound automation loads."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class FileInsertMetrics:
    """Per-file Azure insert timing (SQL batches + transaction commit)."""

    row_count: int = 0
    batch_size: int = 0
    batch_count: int = 0
    insert_sql_duration_ms: int = 0
    file_log_duration_ms: int = 0
    commit_duration_ms: int = 0
    load_duration_ms: int = 0
    avg_batch_duration_ms: float = 0.0
    max_batch_duration_ms: float = 0.0
    min_batch_duration_ms: float = 0.0
    rows_per_sec: float = 0.0
    fast_executemany: bool = True
