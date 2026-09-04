"""Reuse the proven 834 Azure SQL connection path. No new credentials.

RCNI writes go through inbound_automation.azure_common, which wraps
azure_reconciliation.azure_client.build_connection_url()
(ActiveDirectoryInteractive, SERVER/DATABASE/USERNAME/DRIVER).

This module does not connect on import. Callers must request a connection
explicitly. Phase 2 review: do not invoke connect_rcni_engine() until DDL
is approved and a controlled load is requested.
"""

from __future__ import annotations

import os

from sqlalchemy.engine import Engine

from inbound_automation.azure_common import (
    connect_automation_engine,
    fast_executemany_enabled,
    get_automation_engine,
    presence_flags,
)
from rcni.constants import DEFAULT_RCNI_AZURE_BATCH_SIZE


def rcni_batch_size() -> int:
    """
    Configurable insert batch size.

    Order:
      1. RCNI_AZURE_BATCH_SIZE (tuning flag, not a secret)
      2. DEFAULT_RCNI_AZURE_BATCH_SIZE (3000, in the 2000–5000 band)
    Does not inherit inbound's 1000 default.
    """
    raw = os.getenv("RCNI_AZURE_BATCH_SIZE", "").strip()
    if raw:
        try:
            return max(1, int(raw))
        except ValueError:
            pass
    return DEFAULT_RCNI_AZURE_BATCH_SIZE


def rcni_fast_executemany_enabled() -> bool:
    """Reuse inbound_automation fast_executemany unless RCNI_AZURE_FAST_EXECUTEMANY is set."""
    raw = os.getenv("RCNI_AZURE_FAST_EXECUTEMANY", "").strip().strip('"').strip("'")
    if raw:
        return raw.lower() in {"true", "1", "yes", "y", "t"}
    return fast_executemany_enabled()


def get_rcni_engine(*, fast_executemany: bool | None = None) -> Engine:
    """Create engine with the existing Azure URL + fast_executemany."""
    use_fast = rcni_fast_executemany_enabled() if fast_executemany is None else fast_executemany
    return get_automation_engine(fast_executemany=use_fast)


def connect_rcni_engine(*, fast_executemany: bool | None = None) -> Engine:
    """
    Interactive Azure connect using SERVER/DATABASE/USERNAME.

    Not called by Phase 2 unit tests or run_rcni.py.
    """
    use_fast = rcni_fast_executemany_enabled() if fast_executemany is None else fast_executemany
    print(f"  rcni_batch_size: {rcni_batch_size()}")
    return connect_automation_engine(fast_executemany=use_fast)


def azure_presence_flags() -> dict[str, bool]:
    return presence_flags()
