"""Azure DDL executor for inbound automation Phase 2A (create tables only).

Safety:
- Executes only the approved DDL file `sql/inbound_automation_ddl.sql`.
- No INSERT/UPDATE/DELETE (except none here).
- No DROP/TRUNCATE/ALTER existing production tables (DDL uses IF NOT EXISTS).
"""

from __future__ import annotations

import sys
from pathlib import Path

from sqlalchemy.engine import Engine

from inbound_automation.azure_common import (
    INBOUND_TABLES,
    connect_automation_engine,
    require_env_gate,
)
from config.config import settings


def _split_sql_batches(sql_text: str) -> list[str]:
    """Split SQL script into batches separated by a standalone `GO` line."""
    batches: list[str] = []
    buf: list[str] = []

    for line in sql_text.splitlines():
        if line.strip().upper() == "GO":
            batch = "\n".join(buf).strip()
            if batch:
                batches.append(batch)
            buf = []
        else:
            buf.append(line)

    tail = "\n".join(buf).strip()
    if tail:
        batches.append(tail)
    return batches


def _execute_ddl_file(*, engine: Engine, ddl_path: Path) -> None:
    ddl_text = ddl_path.read_text(encoding="utf-8")
    batches = _split_sql_batches(ddl_text)
    if not batches:
        raise RuntimeError(f"No SQL batches found in DDL file: {ddl_path}")

    with engine.connect() as conn:
        for i, batch in enumerate(batches, start=1):
            conn.exec_driver_sql(batch)
            print(f"Executed DDL batch {i}/{len(batches)}")


def _table_exists_sql(schema: str, table: str) -> str:
    return (
        f"SELECT CASE WHEN OBJECT_ID(N'{schema}.{table}', N'U') IS NULL "
        f"THEN 0 ELSE 1 END AS exists_flag"
    )


def _row_count_sql(schema: str, table: str) -> str:
    return f"SELECT COUNT(1) AS row_count FROM [{schema}].[{table}]"


def _run_scalar(engine: Engine, sql: str) -> int:
    with engine.connect() as conn:
        row = conn.exec_driver_sql(sql).fetchone()
        if row is None:
            return 0
        return int(row[0])


def create_phase2a_tables(*, ddl_path: Path, runner_name: str = "inbound_automation_phase2a") -> int:
    settings.refresh_from_env()
    require_env_gate("DDL")

    engine = connect_automation_engine()

    schema = "dbo"
    tables = list(INBOUND_TABLES)

    print("Pre-check table existence:")
    pre_exists: dict[str, int] = {}
    for table in tables:
        flag = _run_scalar(engine, _table_exists_sql(schema, table))
        pre_exists[table] = flag
        state = "EXISTS" if flag else "MISSING"
        print(f"  {schema}.{table}: {state}")

    print(f"Executing approved DDL from: {ddl_path}")
    _execute_ddl_file(engine=engine, ddl_path=ddl_path)

    print("Post-check table existence + row counts:")
    for table in tables:
        exists_flag = _run_scalar(engine, _table_exists_sql(schema, table))
        row_count = _run_scalar(engine, _row_count_sql(schema, table))
        created_or_existing = (
            "created (was missing)" if pre_exists.get(table, 0) == 0 and exists_flag == 1 else "already existed"
        )
        print(f"  {schema}.{table}: exists={exists_flag} row_count={row_count} ({created_or_existing})")

    print("\nVerification SQL (run manually in Azure if desired):")
    for table in tables:
        print(f"- Table exists: {schema}.{table}")
        print(f"  {_table_exists_sql(schema, table)}")
        print(f"- Row count (should be 0 for fresh tables): {schema}.{table}")
        print(f"  {_row_count_sql(schema, table)}")

    return 0
