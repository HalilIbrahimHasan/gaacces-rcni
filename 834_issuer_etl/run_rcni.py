#!/usr/bin/env python3
"""
RCNI Phase 1 — discovery and validation only.

Does not create SQL tables and does not write to Azure SQL.

Examples:
    python run_rcni.py --discover-only --issuer 15105 --year 2026 --month 07
    python run_rcni.py --validate --issuer 15105 --year 2026 --month 07
    python run_rcni.py --validate-local "last reports"
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from rcni.pipeline import (  # noqa: E402
    run_discover_and_validate,
    run_discover_only,
    run_validate_local,
)
from rcni.settings import resolve_rcni_scope  # noqa: E402
from utils.logger import get_logger  # noqa: E402

logger = get_logger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="RCNI Monthly Discrepancy — Phase 1 discovery/validation (no SQL).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_rcni.py --discover-only --issuer 15105 --year 2026 --month 07
  python run_rcni.py --validate --issuer 15105 --year 2026 --month 07
  python run_rcni.py --validate-local "last reports"
        """.strip(),
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--discover-only",
        action="store_true",
        help="List matching SFTP candidates only (no download, no SQL).",
    )
    mode.add_argument(
        "--validate",
        action="store_true",
        help="Discover, download, decompress, validate (no SQL).",
    )
    mode.add_argument(
        "--validate-local",
        metavar="DIR",
        help="Validate already-local RCNI files (no SFTP, no SQL).",
    )
    parser.add_argument("--issuer", help="Override ISSUER_FILTER from .env")
    parser.add_argument("--year", help="Override YEAR_FILTER (SFTP processing year, not plan year)")
    parser.add_argument("--month", help="Override MONTH_FILTER (SFTP processing month)")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    scope = resolve_rcni_scope(issuer=args.issuer, year=args.year, month=args.month)

    if args.validate_local:
        mode_label = "validate-local"
    elif args.discover_only:
        mode_label = "discover-only"
    else:
        mode_label = "validate"

    print("\nRCNI PHASE 1")
    print(f"  mode           : {mode_label}")
    print(f"  base path      : {scope.base_path}")
    print(f"  issuer         : {scope.issuer_display}")
    print(f"  processing year: {scope.year_display}")
    print(f"  processing month: {scope.month_display}")
    print("  Azure SQL      : DISABLED")
    print()

    if args.validate_local:
        result = run_validate_local(scope, Path(args.validate_local))
    elif args.discover_only:
        result = run_discover_only(scope)
    else:
        result = run_discover_and_validate(scope)

    print("\nReports:")
    for name, path in result.report_paths.items():
        print(f"  {name}: {path}")
    print("\nAzure SQL writes: NONE")
    print("Source files modified: NO")
    return 0 if result.discovery.candidates or result.mode == "discover-only" else 1


if __name__ == "__main__":
    raise SystemExit(main())
