"""Load run context and identifiers."""

from __future__ import annotations

import subprocess
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from inbound_automation.constants import OUTPUT_ROOT, PARSER_VERSION, PROJECT_ROOT, RUNNER_VERSION


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def resolve_git_commit() -> str | None:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            timeout=5,
            cwd=PROJECT_ROOT,
        )
        if result.returncode == 0:
            return result.stdout.strip()[:100]
    except (OSError, subprocess.SubprocessError):
        pass
    return None


def new_load_run_id(now: datetime | None = None) -> str:
    ts = (now or _utc_now()).strftime("%Y%m%d_%H%M%S")
    return f"inbound_{ts}_{uuid.uuid4().hex[:8]}"


@dataclass
class LoadRunContext:
    load_run_id: str
    started_at: datetime
    run_mode: str
    source_mode: str
    year_filter: str | None
    all_years: bool
    issuer_filter: list[str] | None
    month_filter: str | None
    parser_version: str = PARSER_VERSION
    runner_version: str = RUNNER_VERSION
    git_commit: str | None = field(default_factory=resolve_git_commit)
    output_dir: Path = field(default_factory=lambda: OUTPUT_ROOT)

    @classmethod
    def create(
        cls,
        *,
        run_mode: str,
        year_filter: str | None,
        all_years: bool,
        issuer_filter: list[str] | None,
        month_filter: str | None,
        source_mode: str = "local",
    ) -> "LoadRunContext":
        started = _utc_now()
        run_id = new_load_run_id(started)
        out = OUTPUT_ROOT / run_id
        out.mkdir(parents=True, exist_ok=True)
        return cls(
            load_run_id=run_id,
            started_at=started,
            run_mode=run_mode,
            source_mode=source_mode,
            year_filter=year_filter,
            all_years=all_years,
            issuer_filter=issuer_filter,
            month_filter=month_filter,
            output_dir=out,
        )
