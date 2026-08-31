"""Streaming SHA-256 hashing — shared by 834 inventory and RCNI validation."""

from __future__ import annotations

import hashlib
from pathlib import Path

CHUNK_SIZE = 65536


def sha256_file(path: str | Path, chunk_size: int = CHUNK_SIZE) -> str:
    """Return hex SHA-256 of a file without loading it entirely into memory."""
    h = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()
