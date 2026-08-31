"""CLI argument parsing for inbound automation runner."""

from __future__ import annotations

import argparse
import sys

from inbound_automation.azure_ddl import create_phase2a_tables
from inbound_automation.pipeline import run_dry_run, run_load
from inbound_automation.reports import write_run_reports
from inbound_automation.run_context import LoadRunContext
from utils.logger import get_logger

logger = get_logger(__name__)


def _parse_issuer_list(values: list[str] | None) -> list[str] | None:
    if not values:
        return None
    issuers: list[str] = []
    for value in values:
        for part in value.split(","):
            part = part.strip()
            if part:
                issuers.append(part)
    return issuers or None


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Inbound automation — Parser834 raw ingestion to Azure SQL.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_inbound_automation_load.py --year 2025 --dry-run
  python run_inbound_automation_load.py --issuer 15105 --year 2025 --month 10 --dry-run
  python run_inbound_automation_load.py --issuer 15105 --year 2025 --month 10 --load
  python run_inbound_automation_load.py --year 2025 --load
  python run_inbound_automation_load.py --create-table
        """.strip(),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Discover, parse, enrich, and write Excel reports only (no Azure writes).",
    )
    parser.add_argument(
        "--create-table",
        action="store_true",
        help="Create inbound_automation tables in Azure SQL (requires INBOUND_AUTOMATION_ENABLED=true).",
    )
    parser.add_argument(
        "--load",
        action="store_true",
        help="Insert parsed rows into Azure SQL (requires INBOUND_AUTOMATION_ENABLED=true).",
    )
    parser.add_argument(
        "--year",
        type=str,
        help="Coverage/partition year filter (required unless --all-years).",
    )
    parser.add_argument(
        "--all-years",
        action="store_true",
        help="Discover all year folders under source_data.",
    )
    parser.add_argument(
        "--issuer",
        action="append",
        metavar="ISSUER",
        help="Issuer HIOS id filter (repeatable or comma-separated). Default: all issuers.",
    )
    parser.add_argument(
        "--month",
        type=str,
        help="Month filter (1-12). Default: all months.",
    )
    parser.add_argument(
        "--source",
        choices=["local", "sftp"],
        default="local",
        help="Source mode (v1: local only).",
    )
    return parser


def _validate_args(args: argparse.Namespace) -> None:
    modes = [name for name, enabled in (
        ("dry_run", args.dry_run),
        ("create_table", args.create_table),
        ("load", args.load),
    ) if enabled]

    if not modes:
        raise SystemExit(
            "Error: explicit mode required.\n"
            "Examples:\n"
            "  python run_inbound_automation_load.py --year 2025 --dry-run\n"
            "  python run_inbound_automation_load.py --issuer 15105 --year 2025 --month 10 --load"
        )
    if len(modes) > 1:
        raise SystemExit(f"Error: mutually exclusive modes: {', '.join(modes)}")

    if args.dry_run or args.load:
        if not args.all_years and not args.year:
            raise SystemExit("Error: --year is required unless --all-years is set.")

        if args.all_years and args.year:
            raise SystemExit("Error: --year and --all-years are mutually exclusive.")

        if args.month:
            try:
                month = int(args.month)
                if not 1 <= month <= 12:
                    raise ValueError
            except ValueError:
                raise SystemExit("Error: --month must be an integer between 1 and 12.")

    if args.source == "sftp":
        raise SystemExit("Error: --source sftp is not implemented.")


def _print_run_header(context: LoadRunContext, *, azure_writes: str) -> None:
    issuer_filter = context.issuer_filter
    print(f"\nINBOUND AUTOMATION — {context.run_mode.upper().replace('_', ' ')}")
    print(f"  load_run_id     : {context.load_run_id}")
    print(f"  year filter     : {context.year_filter or 'ALL'}")
    print(f"  issuer filter   : {', '.join(issuer_filter) if issuer_filter else 'ALL'}")
    print(f"  month filter    : {context.month_filter or 'ALL'}")
    print(f"  output dir      : {context.output_dir}")
    print(f"  Azure writes    : {azure_writes}")
    print("  Parser834       : unmodified raw parse")


def _print_run_footer(result) -> int:
    ctx = result.context
    print("\nRun complete.")
    print(f"  Files discovered        : {result.files_discovered}")
    print(f"  Files parsed            : {result.files_parsed}")
    print(f"  Files loaded            : {result.files_loaded}")
    print(f"  Files skipped (dup hash): {result.files_skipped_duplicate}")
    print(f"  Files failed            : {result.files_failed}")
    print(f"  Rows parsed             : {result.rows_parsed}")
    print(f"  Rows inserted           : {result.rows_inserted}")
    print(f"  Warnings (rows)         : {result.total_warning_count}")
    print(f"  Reports                 : {ctx.output_dir}")

    report_paths = write_run_reports(result)
    for name, path in report_paths.items():
        print(f"    {name}: {path}")

    if result.files_discovered == 0:
        print("\nNo source files matched filters.")
        return 1
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    _validate_args(args)

    if args.create_table:
        from inbound_automation.constants import PROJECT_ROOT
        ddl_path = PROJECT_ROOT / "sql" / "inbound_automation_ddl.sql"
        print("\nINBOUND AUTOMATION — CREATE TABLES (Phase 2A)")
        return create_phase2a_tables(ddl_path=ddl_path, runner_name="inbound_automation_create_tables")

    issuer_filter = _parse_issuer_list(args.issuer)
    run_mode = "load" if args.load else "dry_run"
    context = LoadRunContext.create(
        run_mode=run_mode,
        year_filter=args.year,
        all_years=args.all_years,
        issuer_filter=issuer_filter,
        month_filter=args.month,
        source_mode=args.source,
    )

    if args.load:
        _print_run_header(
            context,
            azure_writes="dbo.inbound_automation + run_log + file_log",
        )
        result = run_load(context)
        return _print_run_footer(result)

    _print_run_header(context, azure_writes="NONE")
    result = run_dry_run(context)
    return _print_run_footer(result)


if __name__ == "__main__":
    sys.exit(main())
