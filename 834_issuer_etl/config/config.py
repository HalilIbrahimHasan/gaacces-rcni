"""
Central configuration — paths, filters, .env, and processing mode.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENV_FILE = PROJECT_ROOT / ".env"
# override=True: project .env wins over pre-set shell variables (e.g. PROCESSING_MODE=local)
ENV_LOADED = load_dotenv(ENV_FILE, override=True)

_TRUTHY = frozenset({"true", "1", "yes", "y"})


def reload_env() -> bool:
    """Reload .env from project root (834_issuer_etl/.env)."""
    global ENV_LOADED
    ENV_LOADED = load_dotenv(ENV_FILE, override=True)
    return ENV_LOADED


def _env_bool(key: str, default: bool = False) -> bool:
    raw = os.getenv(key)
    if raw is None or str(raw).strip() == "":
        return default
    return str(raw).strip().lower() in _TRUTHY


def _env_present(*keys: str) -> bool:
    for key in keys:
        val = os.getenv(key)
        if val is not None and str(val).strip().strip('"').strip("'"):
            return True
    return False


def azure_startup_diagnostics() -> dict[str, str | bool | None]:
    """Log-friendly snapshot of Azure-related env after reload."""
    reload_env()
    return {
        "env_file": str(ENV_FILE),
        "env_file_exists": ENV_FILE.is_file(),
        "env_loaded": ENV_LOADED,
        "ENABLE_AZURE_raw": os.getenv("ENABLE_AZURE"),
        "azure_enabled_parsed": _env_bool("ENABLE_AZURE", False),
        "server_present": _env_present("SERVER", "AZURE_SQL_SERVER"),
        "database_present": _env_present("DATABASE", "AZURE_SQL_DATABASE"),
        "username_present": _env_present("USERNAME", "AZURE_SQL_USERNAME"),
        "driver_present": _env_present("DRIVER", "AZURE_SQL_DRIVER"),
        "driver_value": os.getenv("DRIVER") or os.getenv("AZURE_SQL_DRIVER") or "",
    }


def log_azure_startup_diagnostics() -> dict[str, str | bool | None]:
    """Print and log Azure startup diagnostics."""
    from utils.logger import get_logger

    diag = azure_startup_diagnostics()
    logger = get_logger("config")
    for key, val in diag.items():
        line = f"{key}: {val}"
        print(line)
        logger.info("Startup diagnostic — %s", line)
    return diag


def env_diagnostics() -> dict[str, str | bool | None]:
    """Return which .env path was used and whether PROCESSING_MODE was read."""
    return {
        "env_file": str(ENV_FILE),
        "env_file_exists": ENV_FILE.is_file(),
        "env_loaded": ENV_LOADED,
        "processing_mode_raw": os.getenv("PROCESSING_MODE"),
    }


def _path(key: str, default: str) -> Path:
    raw = os.getenv(key, default)
    p = Path(raw)
    return p if p.is_absolute() else PROJECT_ROOT / p


@dataclass
class Settings:
    project_root: Path = field(default_factory=lambda: PROJECT_ROOT)
    processing_mode: str = field(
        default_factory=lambda: os.getenv("PROCESSING_MODE", "local").lower()
    )
    source_data_path: Path = field(
        default_factory=lambda: _path("SOURCE_DATA_PATH", "source_data")
    )
    extracted_path: Path = field(
        default_factory=lambda: _path("EXTRACTED_PATH", "extracted")
    )
    database_path: Path = field(
        default_factory=lambda: _path("DATABASE_PATH", "data/issuer_834.db")
    )
    reports_path: Path = field(
        default_factory=lambda: _path("REPORTS_PATH", "reports")
    )
    assets_path: Path = field(
        default_factory=lambda: _path("ASSETS_PATH", "assets")
    )
    logs_path: Path = field(
        default_factory=lambda: _path("LOGS_PATH", "logs")
    )
    issuer_filter: str | None = field(
        default_factory=lambda: os.getenv("ISSUER_FILTER") or None
    )
    year_filter: str | None = field(
        default_factory=lambda: os.getenv("YEAR_FILTER") or None
    )
    month_filter: str | None = field(
        default_factory=lambda: os.getenv("MONTH_FILTER") or None
    )
    user_fee_rate: float = field(
        default_factory=lambda: float(os.getenv("USER_FEE_RATE", "0.0325"))
    )
    cancellation_window_days: int = field(
        default_factory=lambda: int(os.getenv("CANCELLATION_WINDOW_DAYS", "90"))
    )
    clean_on_start: bool = field(default_factory=lambda: _env_bool("CLEAN_ON_START", True))
    ftp_host: str = field(default_factory=lambda: os.getenv("FTP_HOST", ""))
    ftp_port: int = field(default_factory=lambda: int(os.getenv("FTP_PORT", "21")))
    ftp_user: str = field(
        default_factory=lambda: os.getenv("FTP_USERNAME") or os.getenv("FTP_USER", "")
    )
    ftp_password: str = field(default_factory=lambda: os.getenv("FTP_PASSWORD", ""))
    ftp_remote_path: str = field(
        default_factory=lambda: os.getenv("FTP_REMOTE_PATH", "/")
    )
    sftp_host: str = field(default_factory=lambda: os.getenv("SFTP_HOST", ""))
    sftp_port: int = field(default_factory=lambda: int(os.getenv("SFTP_PORT", "22")))
    sftp_user: str = field(
        default_factory=lambda: os.getenv("SFTP_USERNAME") or os.getenv("SFTP_USER", "")
    )
    sftp_password: str = field(
        default_factory=lambda: os.getenv("SFTP_PASSWORD", "")
    )
    sftp_remote_path: str = field(
        default_factory=lambda: os.getenv("SFTP_REMOTE_PATH", "/")
    )
    sftp_audit_only: bool = field(default_factory=lambda: _env_bool("SFTP_AUDIT_ONLY", False))
    force_download: bool = field(default_factory=lambda: _env_bool("FORCE_DOWNLOAD", False))
    keep_compressed: bool = field(default_factory=lambda: _env_bool("KEEP_COMPRESSED", False))
    # Azure intelligence (additive — XML pipeline unchanged when disabled)
    azure_enabled: bool = field(default_factory=lambda: _env_bool("ENABLE_AZURE", False))
    azure_reconciliation_enabled: bool = field(
        default_factory=lambda: _env_bool("ENABLE_AZURE_RECONCILIATION", False)
    )
    azure_mirror_enabled: bool = field(default_factory=lambda: _env_bool("ENABLE_AZURE_MIRROR", True))
    azure_discovery_enabled: bool = field(default_factory=lambda: _env_bool("ENABLE_AZURE_DISCOVERY", True))
    azure_lifecycle_enabled: bool = field(default_factory=lambda: _env_bool("ENABLE_AZURE_LIFECYCLE", True))
    xml_lifecycle_enabled: bool = field(default_factory=lambda: _env_bool("ENABLE_XML_LIFECYCLE", True))
    metadata_enabled: bool = field(default_factory=lambda: _env_bool("ENABLE_METADATA", True))
    business_dashboard_enabled: bool = field(default_factory=lambda: _env_bool("ENABLE_BUSINESS_DASHBOARD", True))
    sqlite_output_enabled: bool = field(default_factory=lambda: _env_bool("ENABLE_SQLITE_OUTPUT", True))
    excel_output_enabled: bool = field(default_factory=lambda: _env_bool("ENABLE_EXCEL_OUTPUT", True))
    html_output_enabled: bool = field(default_factory=lambda: _env_bool("ENABLE_HTML_OUTPUT", True))
    fast_mode: bool = field(default_factory=lambda: _env_bool("FAST_MODE", True))
    enable_full_discovery: bool = field(default_factory=lambda: _env_bool("ENABLE_FULL_DISCOVERY", False))
    use_fixed_azure_candidate: bool = field(default_factory=lambda: _env_bool("USE_FIXED_AZURE_CANDIDATE", True))
    xml_only_business_mode: bool = field(
        default_factory=lambda: _env_bool("XML_ONLY_BUSINESS_MODE", False)
    )
    enable_full_data_export: bool = field(
        default_factory=lambda: _env_bool("ENABLE_FULL_DATA_EXPORT", False)
    )
    filter_prior_year_benefit_effective: bool = field(
        default_factory=lambda: _env_bool("FILTER_PRIOR_YEAR_BENEFIT_EFFECTIVE", False)
    )
    reporting_year: str | None = field(
        default_factory=lambda: os.getenv("REPORTING_YEAR") or None
    )
    # Fast business reporting mode (lightweight outputs — does not replace full pipeline)
    fast_business_report_mode: bool = field(
        default_factory=lambda: _env_bool("FAST_BUSINESS_REPORT_MODE", False)
    )
    generate_legacy_assets_reports: bool = field(
        default_factory=lambda: _env_bool("GENERATE_LEGACY_ASSETS_REPORTS", True)
    )
    generate_html_reports: bool = field(
        default_factory=lambda: _env_bool("GENERATE_HTML_REPORTS", True)
    )
    generate_xlsx_reports: bool = field(
        default_factory=lambda: _env_bool("GENERATE_XLSX_REPORTS", True)
    )
    generate_sqlite_reports: bool = field(
        default_factory=lambda: _env_bool("GENERATE_SQLITE_REPORTS", True)
    )
    generate_debug_diagnostics: bool = field(
        default_factory=lambda: _env_bool("GENERATE_DEBUG_DIAGNOSTICS", True)
    )
    generate_business_review_package: bool = field(
        default_factory=lambda: _env_bool("GENERATE_BUSINESS_REVIEW_PACKAGE", False)
    )
    generate_filtered_reports: bool = field(
        default_factory=lambda: _env_bool("GENERATE_FILTERED_REPORTS", False)
    )
    generate_plotly_dashboards: bool = field(
        default_factory=lambda: _env_bool("GENERATE_PLOTLY_DASHBOARDS", False)
    )
    generate_storage_estimate: bool = field(
        default_factory=lambda: _env_bool("GENERATE_STORAGE_ESTIMATE", False)
    )
    relationship_min_record_match_rate: float = field(
        default_factory=lambda: float(os.getenv("RELATIONSHIP_MIN_RECORD_MATCH_RATE", "70"))
    )
    relationship_min_status_match_rate: float = field(
        default_factory=lambda: float(os.getenv("RELATIONSHIP_MIN_STATUS_MATCH_RATE", "85"))
    )
    outputs_path: Path = field(
        default_factory=lambda: _path("OUTPUTS_PATH", "outputs")
    )

    @property
    def azure_discovery_output_path(self) -> Path:
        return self.outputs_path / "azure_discovery"

    @property
    def azure_reconciliation_output_path(self) -> Path:
        return self.outputs_path / "reconciliation"

    @property
    def metadata_output_path(self) -> Path:
        return self.outputs_path / "metadata"

    @property
    def dashboard_output_path(self) -> Path:
        return self.outputs_path / "dashboard"

    @property
    def full_data_exports_path(self) -> Path:
        return self.outputs_path / "full_data_exports"

    @property
    def business_review_filtered_path(self) -> Path:
        return self.outputs_path / "business_review_filtered"

    @property
    def assets_filtered_path(self) -> Path:
        return self.project_root / "assets_filtered"

    @property
    def xml_business_reports_filtered_path(self) -> Path:
        return self.outputs_path / "xml_business_reports_filtered"

    @property
    def fast_business_reports_path(self) -> Path:
        return self.outputs_path / "fast_business_reports"

    @property
    def storage_estimates_path(self) -> Path:
        return self.outputs_path / "storage_estimates"

    def _env_bool_fast_default(self, key: str, *, fast_default: bool, normal_default: bool) -> bool:
        """When fast mode is on, use fast_default unless env explicitly sets the key."""
        raw = os.getenv(key)
        if raw is not None and str(raw).strip() != "":
            return _env_bool(key, normal_default)
        return fast_default if self.fast_business_report_mode else normal_default

    def apply_fast_business_report_mode(self, enabled: bool | None = None) -> None:
        """Apply fast reporting defaults — lightweight outputs, no legacy heavy artifacts."""
        if enabled is not None:
            self.fast_business_report_mode = enabled
        if not self.fast_business_report_mode:
            return
        self.apply_xml_only_business_mode(True)
        self.generate_legacy_assets_reports = self._env_bool_fast_default(
            "GENERATE_LEGACY_ASSETS_REPORTS", fast_default=False, normal_default=True,
        )
        self.generate_html_reports = self._env_bool_fast_default(
            "GENERATE_HTML_REPORTS", fast_default=True, normal_default=True,
        )
        self.generate_xlsx_reports = self._env_bool_fast_default(
            "GENERATE_XLSX_REPORTS", fast_default=True, normal_default=True,
        )
        self.generate_sqlite_reports = self._env_bool_fast_default(
            "GENERATE_SQLITE_REPORTS", fast_default=False, normal_default=True,
        )
        self.generate_debug_diagnostics = self._env_bool_fast_default(
            "GENERATE_DEBUG_DIAGNOSTICS", fast_default=False, normal_default=True,
        )
        self.enable_full_data_export = self._env_bool_fast_default(
            "GENERATE_FULL_DATA_EXPORTS", fast_default=False, normal_default=False,
        )
        self.generate_business_review_package = self._env_bool_fast_default(
            "GENERATE_BUSINESS_REVIEW_PACKAGE", fast_default=False, normal_default=False,
        )
        self.generate_filtered_reports = self._env_bool_fast_default(
            "GENERATE_FILTERED_REPORTS", fast_default=False, normal_default=False,
        )
        self.generate_plotly_dashboards = self._env_bool_fast_default(
            "GENERATE_PLOTLY_DASHBOARDS", fast_default=False, normal_default=False,
        )
        self.generate_storage_estimate = self._env_bool_fast_default(
            "GENERATE_STORAGE_ESTIMATE", fast_default=False, normal_default=False,
        )

    def reference_row_counts(self) -> dict[str, int]:
        raw = os.getenv("REFERENCE_ROW_COUNTS", "")
        out: dict[str, int] = {}
        if not raw:
            return out
        for part in raw.split(","):
            if "=" in part:
                k, v = part.split("=", 1)
                out[k.strip()] = int(v.strip())
        return out

    def refresh_from_env(self) -> None:
        """Re-read .env and refresh Settings fields that bind from environment."""
        reload_env()
        self.issuer_filter = os.getenv("ISSUER_FILTER") or None
        self.year_filter = os.getenv("YEAR_FILTER") or None
        self.month_filter = os.getenv("MONTH_FILTER") or None
        self.sftp_host = os.getenv("SFTP_HOST", "")
        self.sftp_port = int(os.getenv("SFTP_PORT", "22"))
        self.sftp_user = os.getenv("SFTP_USERNAME") or os.getenv("SFTP_USER", "")
        self.sftp_password = os.getenv("SFTP_PASSWORD", "")
        self.sftp_remote_path = os.getenv("SFTP_REMOTE_PATH", "/")
        self.sftp_audit_only = _env_bool("SFTP_AUDIT_ONLY", False)
        self.force_download = _env_bool("FORCE_DOWNLOAD", False)
        self.keep_compressed = _env_bool("KEEP_COMPRESSED", False)
        self.azure_enabled = _env_bool("ENABLE_AZURE", False)
        self.azure_reconciliation_enabled = _env_bool("ENABLE_AZURE_RECONCILIATION", False)
        self.azure_mirror_enabled = _env_bool("ENABLE_AZURE_MIRROR", True)
        self.azure_discovery_enabled = _env_bool("ENABLE_AZURE_DISCOVERY", True)
        self.azure_lifecycle_enabled = _env_bool("ENABLE_AZURE_LIFECYCLE", True)
        self.xml_lifecycle_enabled = _env_bool("ENABLE_XML_LIFECYCLE", True)
        self.metadata_enabled = _env_bool("ENABLE_METADATA", True)
        self.business_dashboard_enabled = _env_bool("ENABLE_BUSINESS_DASHBOARD", True)
        self.sqlite_output_enabled = _env_bool("ENABLE_SQLITE_OUTPUT", True)
        self.excel_output_enabled = _env_bool("ENABLE_EXCEL_OUTPUT", True)
        self.html_output_enabled = _env_bool("ENABLE_HTML_OUTPUT", True)
        self.fast_mode = _env_bool("FAST_MODE", True)
        self.enable_full_discovery = _env_bool("ENABLE_FULL_DISCOVERY", False)
        self.use_fixed_azure_candidate = _env_bool("USE_FIXED_AZURE_CANDIDATE", True)
        self.xml_only_business_mode = _env_bool("XML_ONLY_BUSINESS_MODE", False)
        self.enable_full_data_export = _env_bool("ENABLE_FULL_DATA_EXPORT", False)
        self.filter_prior_year_benefit_effective = _env_bool(
            "FILTER_PRIOR_YEAR_BENEFIT_EFFECTIVE", False
        )
        self.reporting_year = os.getenv("REPORTING_YEAR") or None
        self.fast_business_report_mode = _env_bool("FAST_BUSINESS_REPORT_MODE", False)
        self.generate_legacy_assets_reports = _env_bool("GENERATE_LEGACY_ASSETS_REPORTS", True)
        self.generate_html_reports = _env_bool("GENERATE_HTML_REPORTS", True)
        self.generate_xlsx_reports = _env_bool("GENERATE_XLSX_REPORTS", True)
        self.generate_sqlite_reports = _env_bool("GENERATE_SQLITE_REPORTS", True)
        self.generate_debug_diagnostics = _env_bool("GENERATE_DEBUG_DIAGNOSTICS", True)
        self.generate_business_review_package = _env_bool("GENERATE_BUSINESS_REVIEW_PACKAGE", False)
        self.generate_filtered_reports = _env_bool("GENERATE_FILTERED_REPORTS", False)
        self.generate_plotly_dashboards = _env_bool("GENERATE_PLOTLY_DASHBOARDS", False)
        self.generate_storage_estimate = _env_bool("GENERATE_STORAGE_ESTIMATE", False)
        if self.fast_business_report_mode:
            self.apply_fast_business_report_mode(True)
        self.relationship_min_record_match_rate = float(
            os.getenv("RELATIONSHIP_MIN_RECORD_MATCH_RATE", "70")
        )
        self.relationship_min_status_match_rate = float(
            os.getenv("RELATIONSHIP_MIN_STATUS_MATCH_RATE", "85")
        )

    def apply_fast_mode(self, enabled: bool = True) -> None:
        """Enable FAST_MODE — fixed 834_Inbound_test candidate, no table scan."""
        self.fast_mode = enabled
        self.use_fixed_azure_candidate = enabled
        self.enable_full_discovery = not enabled

    def apply_xml_only_business_mode(self, enabled: bool = True) -> None:
        """XML-only Chandra-like reporting — no Azure connection or discovery."""
        self.xml_only_business_mode = enabled
        if enabled:
            self.azure_enabled = False
            self.azure_reconciliation_enabled = False
            self.azure_discovery_enabled = False
            self.enable_full_discovery = False
            self.fast_mode = True
            self.use_fixed_azure_candidate = True

    @property
    def xml_business_reports_path(self) -> Path:
        return self.outputs_path / "xml_business_reports"

    def apply_cli_filters(
        self,
        issuer: str | None = None,
        year: str | None = None,
        month: str | None = None,
    ) -> None:
        if issuer:
            self.issuer_filter = issuer
        if year:
            self.year_filter = year
            self.reporting_year = year
        if month:
            self.month_filter = str(month).zfill(2)

    def ensure_dirs(self) -> None:
        for p in (
            self.source_data_path,
            self.extracted_path,
            self.database_path.parent,
            self.reports_path,
            self.logs_path,
            self.reports_path / "validation",
            self.reports_path / "kpi",
            self.assets_path,
            self.outputs_path,
            self.azure_discovery_output_path,
            self.azure_reconciliation_output_path,
            self.metadata_output_path,
            self.dashboard_output_path,
            self.outputs_path / "excel",
            self.outputs_path / "sqlite",
            self.outputs_path / "reports",
            self.outputs_path / "csv",
            self.outputs_path / "debug",
            self.outputs_path / "comparison",
            self.outputs_path / "issuer_reports",
            self.outputs_path / "xml_business_reports",
            self.outputs_path / "xml_business_reports" / "all_issuers",
            self.full_data_exports_path,
            self.business_review_filtered_path,
            self.fast_business_reports_path,
            self.storage_estimates_path,
            self.azure_reconciliation_output_path / "excel",
            self.azure_reconciliation_output_path / "dashboards",
            self.azure_reconciliation_output_path / "sqlite",
            self.azure_reconciliation_output_path / "reports",
        ):
            p.mkdir(parents=True, exist_ok=True)


# Legacy constants used by src/ exporters and validators
LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
LOG_LEVEL = "INFO"
EXPORT_PII = False
PII_COLUMNS = [
    "member_ssn", "member_primary_phone_no", "member_preferred_email",
    "member_first_name", "member_last_name", "member_full_address",
]
REQUIRED_COLUMNS = [
    "source_file", "issuer_id", "source_year", "source_month", "source_period",
    "subscriber_flag", "relationship_code", "event_type_code", "event_reason_code",
    "exchg_subscriber_identifier", "exchg_assigned_policy_id", "exchg_indiv_identifier",
    "member_maint_effective_date", "maintenance_type_code", "insurance_type_code",
    "benefit_effective_begin_date", "household_or_employee_case_id",
    "health_coverage_policy_no", "aptc_amt", "total_indiv_responsibility_amt",
    "total_premium_amt", "additional_maint_reason_code", "load_timestamp",
]
REQUIRED_ID_FIELDS = ["issuer_id", "exchg_indiv_identifier", "exchg_assigned_policy_id"]
DATE_COLUMNS = [
    "member_maint_effective_date", "member_birth_date",
    "benefit_effective_begin_date", "file_date",
]
NUMERIC_COLUMNS = [
    "aptc_amt", "total_indiv_responsibility_amt", "total_premium_amt",
]
VALID_SUBSCRIBER_FLAGS = {"Y", "N"}
TABLE_ENROLLEES = "issuer_enrollees"
TABLE_KPIS = "issuer_kpis"
TABLE_VALIDATION = "validation_results"
TABLE_ENROLLMENT_SUMMARY = "enrollment_summary"
TABLE_ENROLLEES_ROLLUP = "issuer_enrollees_all_periods"
TABLE_KPIS_ROLLUP = "issuer_kpis_all_periods"
TABLE_VALIDATION_ROLLUP = "validation_results_all_periods"
TABLE_ENROLLMENT_SUMMARY_ROLLUP = "enrollment_summary_all_periods"

settings = Settings()
