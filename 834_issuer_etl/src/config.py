"""
Central configuration for the 834 Issuer ETL framework.

Paths are resolved from this file's location (``834_issuer_etl/src/config.py``),
not from the terminal's current working directory. Use ``configure_paths()`` to
override roots at runtime (e.g. ``--source-root`` on the CLI).
"""

from pathlib import Path

LOG_FORMAT: str = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
LOG_LEVEL: str = "INFO"

# ---------------------------------------------------------------------------
# Project paths — anchored to package root (834_issuer_etl/), not CWD
# config.py lives in src/ → parents[1] == 834_issuer_etl/
# ---------------------------------------------------------------------------
_SRC_DIR: Path = Path(__file__).resolve().parent
PROJECT_ROOT: Path = _SRC_DIR.parent
SOURCE_DATA_DIR: Path = PROJECT_ROOT / "source_data"
ASSETS_DIR: Path = PROJECT_ROOT / "assets"

# ---------------------------------------------------------------------------
# PII handling — default is safe; set EXPORT_PII=True only for debugging
# ---------------------------------------------------------------------------
EXPORT_PII: bool = False

PII_COLUMNS: list[str] = [
    "member_ssn",
    "member_primary_phone_no",
    "member_preferred_email",
    "member_first_name",
    "member_last_name",
    "member_full_address",
]

# ---------------------------------------------------------------------------
# Required output columns (post-cleaning / standardization)
# ---------------------------------------------------------------------------
REQUIRED_COLUMNS: list[str] = [
    "source_file",
    "issuer_id",
    "source_year",
    "source_month",
    "source_period",
    "isa09",
    "isa10",
    "isa13",
    "gs04",
    "gs05",
    "gs06",
    "st02",
    "action_code",
    "insurer_tax_id_number",
    "qtyn",
    "qtyy",
    "qtyt",
    "subscriber_flag",
    "relationship_code",
    "event_type_code",
    "event_reason_code",
    "exchg_subscriber_identifier",
    "exchg_assigned_policy_id",
    "exchg_indiv_identifier",
    "issuer_subscriber_identifier",
    "issuer_indiv_identifier",
    "member_maint_effective_date",
    "member_entity_identifier_code",
    "member_gender_code",
    "member_marital_status_code",
    "member_citizenship_status_code",
    "member_tobacco_usage_code",
    "city",
    "state",
    "zip",
    "member_birth_date",
    "maintenance_type_code",
    "insurance_type_code",
    "benefit_effective_begin_date",
    "last_premium_paid_date",
    "household_or_employee_case_id",
    "class_of_contract_code",
    "health_coverage_policy_no",
    "aptc_amt",
    "health_coverage_premium_amt",
    "rating_area",
    "total_indiv_responsibility_amt",
    "total_premium_amt",
    "source_exchg_id",
    "additional_maint_reason_code",
    "load_timestamp",
    "file_date",
]

REQUIRED_ID_FIELDS: list[str] = [
    "issuer_id",
    "exchg_indiv_identifier",
    "exchg_assigned_policy_id",
]

DATE_COLUMNS: list[str] = [
    "member_maint_effective_date",
    "member_birth_date",
    "benefit_effective_begin_date",
    "last_premium_paid_date",
    "file_date",
]

NUMERIC_COLUMNS: list[str] = [
    "qtyn",
    "qtyy",
    "qtyt",
    "aptc_amt",
    "health_coverage_premium_amt",
    "total_indiv_responsibility_amt",
    "total_premium_amt",
]

VALID_SUBSCRIBER_FLAGS: set[str] = {"Y", "N"}
DEFAULT_ISSUER_EXAMPLE: str = "64357"

TABLE_ENROLLEES: str = "issuer_enrollees"
TABLE_KPIS: str = "issuer_kpis"
TABLE_VALIDATION: str = "validation_results"
TABLE_ENROLLMENT_SUMMARY: str = "enrollment_summary"
TABLE_ENROLLEES_ROLLUP: str = "issuer_enrollees_all_periods"
TABLE_KPIS_ROLLUP: str = "issuer_kpis_all_periods"
TABLE_VALIDATION_ROLLUP: str = "validation_results_all_periods"
TABLE_ENROLLMENT_SUMMARY_ROLLUP: str = "enrollment_summary_all_periods"


def configure_paths(
    source_root: Path | str | None = None,
    assets_root: Path | str | None = None,
) -> None:
    """
    Override source/assets roots at runtime (e.g. from ``--source-root`` CLI).

    When ``source_root`` ends with ``source_data``, ``PROJECT_ROOT`` is set to
    its parent so assets default to ``{project}/assets``.

    Args:
        source_root: Absolute or relative path to the source_data directory.
        assets_root: Optional override for the assets output directory.
    """
    global PROJECT_ROOT, SOURCE_DATA_DIR, ASSETS_DIR

    if source_root is not None:
        resolved = Path(source_root).resolve()
        SOURCE_DATA_DIR = resolved
        if resolved.name == "source_data":
            PROJECT_ROOT = resolved.parent
        else:
            PROJECT_ROOT = resolved

    if assets_root is not None:
        ASSETS_DIR = Path(assets_root).resolve()
    elif source_root is not None:
        ASSETS_DIR = PROJECT_ROOT / "assets"


def log_path_configuration() -> None:
    """
    Log resolved paths and source-root contents before partition discovery.

    Paths are anchored to this file's location, not the terminal CWD.
    """
    import os

    from utils.logger import get_logger

    log = get_logger(__name__)
    cwd = os.getcwd()
    alt_source = PROJECT_ROOT.parent / "source_data"

    log.info("Current working directory : %s", cwd)
    log.info("PROJECT_ROOT            : %s", PROJECT_ROOT)
    log.info("SOURCE_ROOT             : %s", SOURCE_DATA_DIR)
    log.info("ASSETS_ROOT             : %s", ASSETS_DIR)
    log.info("SOURCE_ROOT exists      : %s", SOURCE_DATA_DIR.exists())
    if alt_source.resolve() != SOURCE_DATA_DIR.resolve():
        log.info(
            "ALT_SOURCE_ROOT         : %s (exists=%s)",
            alt_source,
            alt_source.exists(),
        )

    scan_roots = [SOURCE_DATA_DIR]
    if alt_source.resolve() != SOURCE_DATA_DIR.resolve():
        scan_roots.append(alt_source)

    all_issuers: set[str] = set()
    for root in scan_roots:
        if not root.exists():
            continue
        for p in root.iterdir():
            if p.is_dir() and p.name.isdigit():
                all_issuers.add(p.name)

    if not any(r.exists() for r in scan_roots):
        log.info("Issuer folders found    : (no source_data directory)")
        return

    log.info(
        "Issuer folders found    : %s",
        sorted(all_issuers) if all_issuers else "(none)",
    )
