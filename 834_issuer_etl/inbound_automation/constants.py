"""Constants for inbound automation runner."""

from __future__ import annotations

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

PARSER_VERSION = "parser_834_staging_aligned"
RUNNER_VERSION = "1.0.0-phase2b-load"

OUTPUT_ROOT = PROJECT_ROOT / "outputs" / "inbound_automation"

SUPPORTED_MODES = frozenset({"dry_run", "load"})
