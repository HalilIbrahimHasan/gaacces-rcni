"""Shared Azure connection helpers for inbound automation (DDL + load)."""

from __future__ import annotations

import os
import sys

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from azure_reconciliation.azure_client import build_connection_url, test_azure_connection_query
from config.config import reload_env, settings

TRUTHY = frozenset({"true", "1", "yes", "y", "t"})

INBOUND_TABLES = (
    "inbound_automation",
    "inbound_automation_run_log",
    "inbound_automation_file_log",
)

# Default insert batch size (override via INBOUND_AUTOMATION_BATCH_SIZE).
DEFAULT_BATCH_SIZE = 1000


def env_enabled() -> bool:
    raw = os.getenv("INBOUND_AUTOMATION_ENABLED", "").strip().strip('"').strip("'")
    return bool(raw) and raw.lower() in TRUTHY


def batch_size() -> int:
    raw = os.getenv("INBOUND_AUTOMATION_BATCH_SIZE", "").strip()
    if raw:
        try:
            return max(1, int(raw))
        except ValueError:
            pass
    return DEFAULT_BATCH_SIZE


def fast_executemany_enabled() -> bool:
    raw = os.getenv("INBOUND_AUTOMATION_FAST_EXECUTEMANY", "true").strip().strip('"').strip("'")
    return raw.lower() in TRUTHY


def require_env_gate(action: str) -> None:
    settings.refresh_from_env()
    reload_env()
    if not env_enabled():
        print(
            f"INBOUND_AUTOMATION_ENABLED is missing or not 'true'. "
            f"Failing safely — no Azure {action}.",
            file=sys.stderr,
        )
        raise SystemExit(1)


def presence_flags() -> dict[str, bool]:
    settings.refresh_from_env()
    reload_env()
    return {
        "SERVER present": bool(os.getenv("SERVER", "").strip()),
        "DATABASE present": bool(os.getenv("DATABASE", "").strip()),
        "USERNAME present": bool(os.getenv("USERNAME", "").strip()),
        "DRIVER present": bool(os.getenv("DRIVER", "").strip()),
        "AZURE_SQL_SCHEMA present": bool(os.getenv("AZURE_SQL_SCHEMA", "").strip()),
    }


def get_automation_engine(*, fast_executemany: bool | None = None) -> Engine:
    """
    Create SQLAlchemy engine for inbound automation writes.

    Reuses build_connection_url() from azure_client (existing SERVER/DATABASE/USERNAME).
    Does not modify azure_client.py.
    """
    use_fast = fast_executemany_enabled() if fast_executemany is None else fast_executemany
    return create_engine(
        build_connection_url(),
        future=True,
        fast_executemany=use_fast,
    )


def connect_automation_engine(*, fast_executemany: bool | None = None) -> Engine:
    """Connect using the existing project Azure config pattern."""
    flags = presence_flags()
    print("Azure config presence (no secrets):")
    for key, present in flags.items():
        print(f"  {key}: {present}")

    if not (flags["SERVER present"] and flags["DATABASE present"] and flags["USERNAME present"]):
        print(
            "Azure config missing: SERVER, DATABASE, and USERNAME are required.",
            file=sys.stderr,
        )
        raise SystemExit(1)

    use_fast = fast_executemany_enabled() if fast_executemany is None else fast_executemany
    print(f"  fast_executemany: {use_fast}")
    print(f"  batch_size: {batch_size()}")

    try:
        engine = get_automation_engine(fast_executemany=use_fast)
        test_azure_connection_query(engine)
        print("Azure connection successful")
        return engine
    except Exception as exc:
        print(f"Azure connection failed: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
