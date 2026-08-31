"""
Read-only Azure SQL access via SQLAlchemy.

Never INSERT/UPDATE/DELETE/ALTER. SELECT only.

Authentication: ActiveDirectoryInteractive (passwordless — no PWD, no Trusted_Connection).
"""

from __future__ import annotations

import os
from typing import Any
from urllib.parse import quote_plus

import pandas as pd
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine

from config.config import ENV_FILE
from dotenv import load_dotenv
from utils.logger import get_logger

logger = get_logger(__name__)

load_dotenv(ENV_FILE, override=True)

AUTH_MODE = "ActiveDirectoryInteractive"
DEFAULT_ENROLLMENTS_TABLE = "Enrollments_PY2026"
DEFAULT_SCHEMA = "dbo"
DEFAULT_DRIVER = "ODBC Driver 17 for SQL Server"
# Interactive AAD login needs time for browser sign-in (default 5 minutes)
DEFAULT_CONNECTION_TIMEOUT = 300

AZURE_TABLE_CANDIDATES = [
    "Enrollments_PY2026",
    "Enrollments_PY2526_01312026",
    "Enrollments_PY2526_02272026",
    "Enrollments_PY2526_03312026",
    "Enrollments_PY2526_04302026",
    "Enrollments_PY2526_FEB10",
    "Enrollments_PY2526_DEC31",
    "DuplicateEnrollment_Overlap",
    "834_Inbound_test",
    "834_Inbound_header_test",
    "monthly_discrepancy",
    "monthly_discrepancy_PY2026",
    "CarrierInvoice",
    "CarrierInvoice_test",
]


def _reload_env() -> None:
    load_dotenv(ENV_FILE, override=True)


def _env(key: str, legacy: str = "") -> str:
    _reload_env()
    val = os.getenv(key) or os.getenv(legacy) or ""
    return str(val).strip().strip('"').strip("'")


def _connection_timeout() -> int:
    """Seconds to wait during connect (includes interactive AAD sign-in)."""
    raw = _env("AZURE_CONNECTION_TIMEOUT", "CONNECTION_TIMEOUT")
    if raw:
        try:
            return max(30, int(raw))
        except ValueError:
            logger.warning("Invalid CONNECTION_TIMEOUT=%r — using default %s", raw, DEFAULT_CONNECTION_TIMEOUT)
    return DEFAULT_CONNECTION_TIMEOUT


def _azure_settings() -> dict[str, str]:
    return {
        "server": _env("AZURE_SQL_SERVER", "SERVER"),
        "database": _env("AZURE_SQL_DATABASE", "DATABASE"),
        "username": _env("AZURE_SQL_USERNAME", "USERNAME"),
        "driver": _env("AZURE_SQL_DRIVER", "DRIVER") or DEFAULT_DRIVER,
        "table": os.getenv("AZURE_ENROLLMENTS_TABLE", DEFAULT_ENROLLMENTS_TABLE),
        "schema": os.getenv("AZURE_SQL_SCHEMA", DEFAULT_SCHEMA),
    }


def get_connection_meta() -> dict[str, Any]:
    """Return connection diagnostics (no secrets)."""
    cfg = _azure_settings()
    return {
        "connection_mode": AUTH_MODE,
        "connection_timeout": _connection_timeout(),
        "driver": cfg["driver"],
        "server": cfg["server"],
        "database": cfg["database"],
        "username_present": bool(cfg["username"]),
    }


def log_connection_failure(meta: dict[str, Any], exc: Exception | str | None = None) -> None:
    msg = (
        "Azure connection failed — connection_mode=%s driver=%s server=%s "
        "database=%s username_present=%s"
    )
    args = (
        meta.get("connection_mode"),
        meta.get("driver"),
        meta.get("server"),
        meta.get("database"),
        meta.get("username_present"),
    )
    if exc is not None:
        logger.warning(msg + " error=%s", *args, exc)
    else:
        logger.warning(msg, *args)


def azure_configured() -> bool:
    """True when SERVER, DATABASE, and USERNAME are set. PASSWORD is not used."""
    cfg = _azure_settings()
    return bool(cfg["server"] and cfg["database"] and cfg["username"])


def build_odbc_connect_string() -> str:
    """
    Build ActiveDirectoryInteractive ODBC connection string.

    Pattern:
        DRIVER={DRIVER};SERVER={SERVER};DATABASE={DATABASE};UID={USERNAME};
        Authentication=ActiveDirectoryInteractive;Encrypt=yes;
        TrustServerCertificate=yes;Connection Timeout=<seconds>;
    """
    cfg = _azure_settings()
    if not azure_configured():
        raise RuntimeError(
            "Azure SQL settings missing. Set SERVER, DATABASE, and USERNAME in .env"
        )

    timeout = _connection_timeout()
    return (
        f"DRIVER={{{cfg['driver']}}};"
        f"SERVER={cfg['server']};"
        f"DATABASE={cfg['database']};"
        f"UID={cfg['username']};"
        f"Authentication={AUTH_MODE};"
        "Encrypt=yes;"
        "TrustServerCertificate=yes;"
        f"Connection Timeout={timeout};"
    )


def build_connection_url() -> str:
    conn_str = build_odbc_connect_string()
    return "mssql+pyodbc:///?odbc_connect=" + quote_plus(conn_str)


def get_engine() -> Engine:
    return create_engine(build_connection_url(), future=True)


def print_azure_connection_diagnostics(runner_name: str) -> dict[str, Any]:
    """Print connection settings (no secrets) before attempting Azure login."""
    cfg = _azure_settings()
    meta = {
        "runner_name": runner_name,
        "server_present": bool(cfg["server"]),
        "database_present": bool(cfg["database"]),
        "username_present": bool(cfg["username"]),
        "driver": cfg["driver"],
        "authentication_mode": AUTH_MODE,
        "connection_timeout": _connection_timeout(),
    }
    lines = [
        f"runner name: {runner_name}",
        f"server present: {meta['server_present']}",
        f"database present: {meta['database_present']}",
        f"username present: {meta['username_present']}",
        f"driver: {meta['driver']}",
        f"authentication mode: {meta['authentication_mode']}",
        f"connection timeout: {meta['connection_timeout']} seconds",
    ]
    for line in lines:
        print(line)
        logger.info("Azure connection diagnostic — %s", line)
    return meta


def test_azure_connection_query(engine: Engine) -> None:
    """Force ODBC connect + interactive login via a real query."""
    with engine.connect() as conn:
        conn.execute(text("SELECT 1 AS connection_test"))


def connect_azure(
    *,
    runner_name: str = "azure",
    strict: bool = False,
) -> tuple[Engine | None, dict[str, Any]]:
    """
    Shared Azure connection entry point for all runners.

    Uses ActiveDirectoryInteractive + SELECT 1 AS connection_test to trigger login.

    When strict=True, prints full exception and exits with code 1 on failure.
    When strict=False, logs and returns (None, meta) on failure (mirror pipeline).
    """
    import sys

    meta = print_azure_connection_diagnostics(runner_name)
    meta["connection_mode"] = AUTH_MODE

    if not azure_configured():
        meta["connected"] = False
        meta["error"] = "SERVER, DATABASE, and USERNAME required"
        msg = meta["error"]
        if strict:
            print(f"Azure connection failed: {msg}", file=sys.stderr)
            sys.exit(1)
        log_connection_failure(meta, msg)
        return None, meta

    logger.info("Azure connection mode: %s", AUTH_MODE)
    logger.info(
        "Azure connection timeout: %s seconds (complete browser sign-in within this window)",
        meta["connection_timeout"],
    )
    print("CONNECTING TO AZURE")
    logger.info("CONNECTING TO AZURE")

    try:
        engine = get_engine()
        test_azure_connection_query(engine)
        meta["connected"] = True
        logger.info("Azure connection successful")
        print("Azure connection successful")
        return engine, meta
    except Exception as exc:
        meta["connected"] = False
        meta["error"] = str(exc)
        if strict:
            import traceback

            print(f"Azure connection failed: {exc}", file=sys.stderr)
            traceback.print_exc()
            logger.exception("Azure connection failed")
            sys.exit(1)
        log_connection_failure(meta, exc)
        return None, meta


def verify_azure_connection() -> tuple[Engine | None, dict[str, Any]]:
    """
    Test Azure connectivity via ActiveDirectoryInteractive.

    Delegates to connect_azure (non-strict). Never raises.
    """
    return connect_azure(runner_name="verify_azure_connection", strict=False)


def list_table_columns(engine: Engine, schema: str, table: str) -> list[str]:
    try:
        insp = inspect(engine)
        cols = insp.get_columns(table, schema=schema)
        return [c["name"] for c in cols]
    except Exception as exc:
        logger.warning("Could not inspect %s.%s: %s", schema, table, exc)
        return []


def list_available_tables(engine: Engine, schema: str = DEFAULT_SCHEMA) -> list[str]:
    try:
        insp = inspect(engine)
        return sorted(insp.get_table_names(schema=schema))
    except Exception as exc:
        logger.warning("Could not list Azure tables: %s", exc)
        return []


def _pick_filter_column(columns: list[str], candidates: list[str]) -> str | None:
    norm = {_c.lower(): _c for _c in columns}
    for cand in candidates:
        if cand.lower() in norm:
            return norm[cand.lower()]
    return None


def fetch_enrollments_partition(
    engine: Engine,
    *,
    issuer: str,
    year: str,
    month: str,
    schema: str | None = None,
    table: str | None = None,
    limit: int | None = None,
) -> pd.DataFrame:
    """
    SELECT-only query for one issuer/year/month partition.

    Dynamically picks filter columns present in the table.
    """
    cfg = _azure_settings()
    schema = schema or cfg["schema"]
    table = table or cfg["table"]
    full_table = f"[{schema}].[{table}]"

    columns = list_table_columns(engine, schema, table)
    if not columns:
        logger.error("Azure table %s has no readable columns", full_table)
        return pd.DataFrame()

    issuer_col = _pick_filter_column(columns, ["hios_issuer_id", "issuer_id", "hios_id"])
    year_col = _pick_filter_column(columns, ["coverage_year", "plan_year", "benefit_year"])
    month_cols = [
        _pick_filter_column(columns, c)
        for c in [
            ["enrollment_last_update_date"],
            ["enrollee_last_update_date"],
            ["benefit_effective_date"],
            ["enrollment_create_date"],
        ]
    ]
    month_col = next((c for c in month_cols if c), None)

    if not issuer_col:
        logger.warning("Azure table missing issuer column — returning empty frame")
        return pd.DataFrame()

    clauses = [f"[{issuer_col}] = :issuer"]
    params: dict[str, Any] = {"issuer": issuer}

    if year_col:
        clauses.append(f"CAST([{year_col}] AS VARCHAR(4)) = :year")
        params["year"] = str(year)

    if month_col:
        clauses.append(
            f"(MONTH([{month_col}]) = :month_int OR FORMAT([{month_col}], 'MM') = :month_str)"
        )
        params["month_int"] = int(month)
        params["month_str"] = str(month).zfill(2)

    where = " AND ".join(clauses)
    sql = f"SELECT * FROM {full_table} WHERE {where}"
    if limit:
        sql += f" ORDER BY (SELECT NULL) OFFSET 0 ROWS FETCH NEXT {int(limit)} ROWS ONLY"

    logger.info("Azure query: %s params=%s", sql[:200], params)
    with engine.connect() as conn:
        df = pd.read_sql(text(sql), conn, params=params)
    logger.info("Azure rows fetched: %d for %s/%s/%s", len(df), issuer, year, month)
    return df


def fetch_enrollments_issuer(
    engine: Engine,
    *,
    issuer: str,
    years: list[str] | None = None,
    schema: str | None = None,
    table: str | None = None,
) -> pd.DataFrame:
    """Fetch all rows for an issuer (optional year filter) for lifecycle comparison."""
    cfg = _azure_settings()
    schema = schema or cfg["schema"]
    table = table or cfg["table"]
    full_table = f"[{schema}].[{table}]"
    columns = list_table_columns(engine, schema, table)
    issuer_col = _pick_filter_column(columns, ["hios_issuer_id", "issuer_id", "hios_id"])
    year_col = _pick_filter_column(columns, ["coverage_year", "plan_year"])

    if not issuer_col:
        return pd.DataFrame()

    clauses = [f"[{issuer_col}] = :issuer"]
    params: dict[str, Any] = {"issuer": issuer}
    if years and year_col:
        placeholders = ", ".join(f":y{i}" for i in range(len(years)))
        clauses.append(f"CAST([{year_col}] AS VARCHAR(4)) IN ({placeholders})")
        for i, y in enumerate(years):
            params[f"y{i}"] = str(y)

    sql = f"SELECT * FROM {full_table} WHERE {' AND '.join(clauses)}"
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params=params)
