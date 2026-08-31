"""
Schema validation — verifies required columns exist in cleaned enrollee data.

Runs before data-quality rules so downstream checks can assume a stable
column contract defined in ``config.REQUIRED_COLUMNS``.
"""

import pandas as pd

from config import REQUIRED_COLUMNS
from utils.logger import get_logger

logger = get_logger(__name__)


class SchemaValidator:
    """
    Validate that a cleaned DataFrame conforms to the expected column schema.

    Produces structured pass/fail results consumed by validation reports,
    SQLite persistence, and the dashboard issue summary.
    """

    def validate(self, df: pd.DataFrame, issuer_id: str) -> list[dict]:
        """
        Check presence of all required columns.

        Args:
            df: Cleaned enrollee DataFrame.
            issuer_id: Issuer being validated (for report context).

        Returns:
            List of validation result dicts with ``check``, ``status``,
            ``message``, and ``details`` keys.
        """
        results: list[dict] = []
        missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]

        if missing:
            results.append({
                "issuer_id": issuer_id,
                "check_category": "schema",
                "check_name": "required_columns_exist",
                "status": "FAIL",
                "message": f"Missing {len(missing)} required column(s)",
                "details": ", ".join(missing),
                "affected_count": len(missing),
            })
            logger.error(
                "Schema validation FAILED for issuer %s — missing: %s",
                issuer_id,
                missing,
            )
        else:
            results.append({
                "issuer_id": issuer_id,
                "check_category": "schema",
                "check_name": "required_columns_exist",
                "status": "PASS",
                "message": f"All {len(REQUIRED_COLUMNS)} required columns present",
                "details": "",
                "affected_count": 0,
            })
            logger.info("Schema validation PASSED for issuer %s", issuer_id)

        return results
