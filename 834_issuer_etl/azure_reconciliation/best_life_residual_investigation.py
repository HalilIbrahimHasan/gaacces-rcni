"""
Best Life 2026 — residual enrollee file-level investigation.

Builds five investigation deliverables for Swathi enrollees that are still NOT
FOUND in dbo.inbound_automation (member_id, issuer_indiv_identifier,
exchg_assigned_enrollee_id).

Read-only. Does not invent file names or assume missing source files.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import bindparam, text
from sqlalchemy.engine import Engine

from azure_reconciliation.safe_export import safe_write_excel
from utils.logger import get_logger

logger = get_logger(__name__)

ISSUER = "83502"
SWATHI_STATUS_COL = "M ID in Selma's report status"
NOT_IN_SELMA = "Not in Selma's report"

SOURCE_FILE_MARKERS = (
    "source_file",
    "source file",
    "inbound_file",
    "inbound file",
    "transaction_file",
    "transaction file",
    "file_name",
    "file name",
    "834_file",
    "account_history",
    "account history",
    "file_timestamp",
    "file timestamp",
)

ENROLLED_STATUS_TOKENS = ("enroll", "confirm", "effectuat", "active")


def normalize_id(value: Any) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    s = str(value).strip()
    if s.lower() in {"", "nan", "none", "nat"}:
        return ""
    if re.fullmatch(r"-?\d+\.0", s):
        s = s[:-2]
    return s


def normalize_id_series(series: pd.Series) -> pd.Series:
    return series.map(normalize_id)


def _date_str(value: Any) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return ""
    if isinstance(value, pd.Timestamp):
        return value.strftime("%Y-%m-%d")
    s = str(value).strip()
    if s.lower() in {"", "nan", "none", "nat"}:
        return ""
    return s[:10] if len(s) >= 10 else s


def load_swathi_candidates(workbook: Path) -> pd.DataFrame:
    df = pd.read_excel(workbook, sheet_name="Swathi", dtype=str)
    df = df.fillna("")
    for col in df.columns:
        df[col] = df[col].astype(str).str.strip()
    if SWATHI_STATUS_COL not in df.columns:
        raise ValueError(f"Missing column {SWATHI_STATUS_COL!r} in Swathi sheet")
    candidates = df[df[SWATHI_STATUS_COL] == NOT_IN_SELMA].copy()
    candidates["enrollee_id_norm"] = normalize_id_series(candidates["enrollee_id"])
    candidates = candidates[candidates["enrollee_id_norm"] != ""]
    return candidates


def detect_source_file_columns(columns: list[str]) -> list[str]:
    found: list[str] = []
    for col in columns:
        low = col.lower().replace("_", " ")
        if any(marker.replace("_", " ") in low for marker in SOURCE_FILE_MARKERS):
            found.append(col)
    return found


def lookup_ids_in_azure(engine: Engine, enrollee_ids: list[str], issuer: str = ISSUER) -> pd.DataFrame:
    if not enrollee_ids:
        return pd.DataFrame(
            columns=[
                "enrollee_id",
                "db_status",
                "matched_on",
                "folder_year",
                "coverage_year",
                "file_name",
                "policy_id",
                "health_coverage_policy_no",
                "enrolleeSatatus",
                "benefit_effective_date",
                "member_maint_effective_date",
                "loaded_at",
            ]
        )

    sql = text(
        """
        WITH candidate_ids AS (
            SELECT CAST(value AS NVARCHAR(100)) AS enrollee_id
            FROM OPENJSON(:ids_json)
        ),
        normalized AS (
            SELECT NULLIF(LTRIM(RTRIM(enrollee_id)), '') AS enrollee_id
            FROM candidate_ids
        ),
        matches AS (
            SELECT
                n.enrollee_id,
                ia.folder_year,
                ia.coverage_year,
                ia.file_name,
                ia.policy_id,
                ia.health_coverage_policy_no,
                ia.enrolleeSatatus,
                ia.benefit_effective_date,
                ia.member_maint_effective_date,
                ia.loaded_at,
                CASE
                    WHEN NULLIF(LTRIM(RTRIM(ia.member_id)), '') = n.enrollee_id
                        THEN 'member_id'
                    WHEN NULLIF(LTRIM(RTRIM(ia.issuer_indiv_identifier)), '') = n.enrollee_id
                        THEN 'issuer_indiv_identifier'
                    WHEN NULLIF(LTRIM(RTRIM(ia.exchg_assigned_enrollee_id)), '') = n.enrollee_id
                        THEN 'exchg_assigned_enrollee_id'
                END AS matched_on
            FROM normalized AS n
            INNER JOIN dbo.inbound_automation AS ia
                ON ia.issuer = :issuer
               AND (
                    NULLIF(LTRIM(RTRIM(ia.member_id)), '') = n.enrollee_id
                 OR NULLIF(LTRIM(RTRIM(ia.issuer_indiv_identifier)), '') = n.enrollee_id
                 OR NULLIF(LTRIM(RTRIM(ia.exchg_assigned_enrollee_id)), '') = n.enrollee_id
               )
        )
        SELECT
            n.enrollee_id,
            CASE WHEN m.enrollee_id IS NULL THEN 'NOT_FOUND' ELSE 'FOUND' END AS db_status,
            m.matched_on,
            m.folder_year,
            m.coverage_year,
            m.file_name,
            m.policy_id,
            m.health_coverage_policy_no,
            m.enrolleeSatatus,
            m.benefit_effective_date,
            m.member_maint_effective_date,
            m.loaded_at
        FROM normalized AS n
        LEFT JOIN (
            SELECT
                enrollee_id,
                MIN(matched_on) AS matched_on,
                MIN(folder_year) AS folder_year,
                MIN(coverage_year) AS coverage_year,
                MIN(file_name) AS file_name,
                MIN(policy_id) AS policy_id,
                MIN(health_coverage_policy_no) AS health_coverage_policy_no,
                MIN(enrolleeSatatus) AS enrolleeSatatus,
                MIN(benefit_effective_date) AS benefit_effective_date,
                MIN(member_maint_effective_date) AS member_maint_effective_date,
                MIN(loaded_at) AS loaded_at
            FROM matches
            GROUP BY enrollee_id
        ) AS m
            ON m.enrollee_id = n.enrollee_id
        ORDER BY db_status DESC, n.enrollee_id
        """
    )

    # OPENJSON may not exist on all SQL Server tiers; fall back to batched OR clauses.
    try:
        import json

        payload = json.dumps(enrollee_ids)
        with engine.connect() as conn:
            return pd.read_sql(sql, conn, params={"ids_json": payload, "issuer": issuer})
    except Exception as exc:
        logger.warning("OPENJSON lookup failed (%s); using batched OR lookup", exc)
        return _lookup_ids_batched(engine, enrollee_ids, issuer)


def _lookup_ids_batched(engine: Engine, enrollee_ids: list[str], issuer: str, batch_size: int = 100) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for start in range(0, len(enrollee_ids), batch_size):
        batch = enrollee_ids[start : start + batch_size]
        stmt = text(
            """
            SELECT DISTINCT
                :enrollee_id AS enrollee_id,
                ia.folder_year,
                ia.coverage_year,
                ia.file_name,
                ia.policy_id,
                ia.health_coverage_policy_no,
                ia.enrolleeSatatus,
                ia.benefit_effective_date,
                ia.member_maint_effective_date,
                ia.loaded_at,
                CASE
                    WHEN NULLIF(LTRIM(RTRIM(ia.member_id)), '') = :enrollee_id THEN 'member_id'
                    WHEN NULLIF(LTRIM(RTRIM(ia.issuer_indiv_identifier)), '') = :enrollee_id
                        THEN 'issuer_indiv_identifier'
                    WHEN NULLIF(LTRIM(RTRIM(ia.exchg_assigned_enrollee_id)), '') = :enrollee_id
                        THEN 'exchg_assigned_enrollee_id'
                END AS matched_on
            FROM dbo.inbound_automation AS ia
            WHERE ia.issuer = :issuer
              AND (
                    NULLIF(LTRIM(RTRIM(ia.member_id)), '') = :enrollee_id
                 OR NULLIF(LTRIM(RTRIM(ia.issuer_indiv_identifier)), '') = :enrollee_id
                 OR NULLIF(LTRIM(RTRIM(ia.exchg_assigned_enrollee_id)), '') = :enrollee_id
              )
            """
        )
        with engine.connect() as conn:
            for enrollee_id in batch:
                found = pd.read_sql(
                    stmt,
                    conn,
                    params={"enrollee_id": enrollee_id, "issuer": issuer},
                )
                if found.empty:
                    rows.append({"enrollee_id": enrollee_id, "db_status": "NOT_FOUND"})
                else:
                    row = found.iloc[0].to_dict()
                    row["enrollee_id"] = enrollee_id
                    row["db_status"] = "FOUND"
                    rows.append(row)
    return pd.DataFrame(rows)


def load_lookup_results(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str) if path.suffix.lower() == ".csv" else pd.read_excel(path, dtype=str)
    df = df.fillna("")
    id_col = next((c for c in df.columns if c.lower() in {"enrollee_id", "enrollee id", "member_id"}), None)
    if not id_col:
        raise ValueError(f"Lookup results must include enrollee_id column; got {list(df.columns)}")
    status_col = next(
        (c for c in df.columns if c.lower() in {"db_status", "status", "lookup_status", "found"}),
        None,
    )
    df = df.rename(columns={id_col: "enrollee_id"})
    df["enrollee_id"] = normalize_id_series(df["enrollee_id"])
    if status_col:
        df = df.rename(columns={status_col: "db_status"})
        df["db_status"] = df["db_status"].str.upper().map(
            lambda s: "FOUND" if s in {"FOUND", "Y", "YES", "TRUE", "1"} else "NOT_FOUND"
        )
    else:
        df["db_status"] = "NOT_FOUND"
    return df


def _status_text(row: pd.Series) -> str:
    for col in ("enrollee_status_description", "enrollment_status_description", "application_status"):
        val = str(row.get(col, "")).strip()
        if val:
            return val
    return ""


def _is_enrolled_status(status: str) -> bool:
    low = status.lower()
    return any(tok in low for tok in ENROLLED_STATUS_TOKENS)


def _cluster_key(row: pd.Series) -> str:
    parts = [
        normalize_id(row.get("household_id")),
        normalize_id(row.get("enrollment_id")),
        _date_str(row.get("benefit_effective_date")),
        _date_str(row.get("benefit_end_date")),
        _date_str(row.get("GAA_Load_Datetime")),
        _status_text(row).lower(),
    ]
    return "|".join(parts)


def _recommended_lookup_key(row: pd.Series) -> str:
    keys = []
    for label, col in (
        ("enrollee_id", "enrollee_id"),
        ("enrollment_id", "enrollment_id"),
        ("household_id", "household_id"),
        ("ssap_application_id", "ssap_application_id"),
        ("external_application_id", "external_application_id"),
    ):
        val = normalize_id(row.get(col))
        if val:
            keys.append(f"{label}={val}")
    for label, col in (
        ("benefit_start", "benefit_effective_date"),
        ("benefit_end", "benefit_end_date"),
        ("GAA_load", "GAA_Load_Datetime"),
        ("enrollment_create", "enrollment_create_date"),
        ("enrollment_update", "enrollment_last_update_date"),
    ):
        val = _date_str(row.get(col))
        if val:
            keys.append(f"{label}={val}")
    return "; ".join(keys)


def _recommended_next_step(row: pd.Series, source_file: str) -> str:
    if source_file and source_file != "Not available in workbook":
        return (
            "Search dbo.inbound_automation_file_log and source_data using file_name; "
            "confirm row presence for enrollee identifiers."
        )
    if normalize_id(row.get("ssap_application_id")) or normalize_id(row.get("household_id")):
        return (
            "Georgia Access Account History lookup using recommended lookup key; "
            "retrieve inbound 834 file_name and transaction comment from UI."
        )
    return "Account History lookup by enrollee_id and enrollment_id; retrieve file_name from UI."


def _source_file_value(row: pd.Series, source_cols: list[str]) -> str:
    for col in source_cols:
        val = str(row.get(col, "")).strip()
        if val and val.lower() not in {"nan", "none"}:
            return val
    return "Not available in workbook"


@dataclass
class InvestigationOutputs:
    lookup_results: pd.DataFrame
    residual_investigation: pd.DataFrame
    cluster_summary: pd.DataFrame
    priority_list: pd.DataFrame
    search_inputs: pd.DataFrame
    final_assessment: pd.DataFrame
    readme: pd.DataFrame


def build_investigation(
    candidates: pd.DataFrame,
    lookup_results: pd.DataFrame,
    *,
    full_swathi: pd.DataFrame | None = None,
    selma_df: pd.DataFrame | None = None,
) -> InvestigationOutputs:
    source_cols = detect_source_file_columns(list(candidates.columns))
    lookup = lookup_results.copy()
    lookup["enrollee_id"] = normalize_id_series(lookup["enrollee_id"])
    lookup["db_status"] = lookup["db_status"].str.upper()

    residual_ids = set(lookup.loc[lookup["db_status"] == "NOT_FOUND", "enrollee_id"])
    residual = candidates[candidates["enrollee_id_norm"].isin(residual_ids)].copy()
    residual = residual.drop_duplicates(subset=["enrollee_id_norm"], keep="first")

    residual["cluster_key"] = residual.apply(_cluster_key, axis=1)
    cluster_sizes = residual.groupby("cluster_key")["enrollee_id_norm"].transform("count")
    residual["likely_shared_file_group"] = [
        f"cluster_{idx + 1:03d} ({size} members)"
        for idx, size in enumerate(cluster_sizes)
    ]

    # Re-number clusters deterministically by household/policy/date.
    cluster_map: dict[str, str] = {}
    for idx, key in enumerate(sorted(residual["cluster_key"].unique()), start=1):
        size = int((residual["cluster_key"] == key).sum())
        cluster_map[key] = f"cluster_{idx:03d} ({size} members)"
    residual["likely_shared_file_group"] = residual["cluster_key"].map(cluster_map)

    investigation_rows: list[dict[str, Any]] = []
    for _, row in residual.iterrows():
        source_file = _source_file_value(row, source_cols)
        investigation_rows.append(
            {
                "enrollee_id": normalize_id(row.get("enrollee_id")),
                "enrollment_id": normalize_id(row.get("enrollment_id")),
                "household_id": normalize_id(row.get("household_id")),
                "relationship_person_type": " / ".join(
                    x
                    for x in (
                        str(row.get("relationship_type", "")).strip(),
                        str(row.get("person_type", "")).strip(),
                    )
                    if x and x.lower() != "nan"
                ),
                "status": _status_text(row),
                "application_status": str(row.get("application_status", "")).strip(),
                "coverage_year": str(row.get("coverage_year", "")).strip(),
                "benefit_start": _date_str(row.get("benefit_effective_date")),
                "benefit_end": _date_str(row.get("benefit_end_date")),
                "enrollment_date": _date_str(row.get("enrollment_create_date")),
                "enrollment_confirmation_date": _date_str(row.get("enrollment_confirmation_date")),
                "enrollee_create_date": _date_str(row.get("enrollee_create_date")),
                "enrollee_last_update_date": _date_str(row.get("enrollee_last_update_date")),
                "enrollment_last_update_date": _date_str(row.get("enrollment_last_update_date")),
                "application_create_date": _date_str(row.get("application_create_date")),
                "application_last_update_date": _date_str(row.get("application_last_update_date")),
                "GAA_load_date": _date_str(row.get("GAA_Load_Datetime")),
                "ssap_application_id": normalize_id(row.get("ssap_application_id")),
                "external_application_id": normalize_id(row.get("external_application_id")),
                "source_file_name": source_file,
                "likely_shared_file_group": row["likely_shared_file_group"],
                "recommended_lookup_key": _recommended_lookup_key(row),
                "recommended_next_step": _recommended_next_step(row, source_file),
            }
        )
    investigation = pd.DataFrame(investigation_rows)

    # Household cluster summary using full Swathi sheet for household composition.
    swathi_all = full_swathi if full_swathi is not None else candidates
    swathi_all = swathi_all.copy()
    swathi_all["enrollee_id_norm"] = normalize_id_series(swathi_all["enrollee_id"])
    swathi_all["household_id_norm"] = normalize_id_series(swathi_all["household_id"])
    if selma_df is not None and not selma_df.empty:
        selma_id_cols = [
            c
            for c in selma_df.columns
            if c.lower() in {
                "enrollee id",
                "member id",
                "issuer indiv identifier",
                "exchngassigned enrollee id",
            }
        ]
        selma_ids: set[str] = set()
        for col in selma_id_cols:
            selma_ids.update(normalize_id_series(selma_df[col]).tolist())
    else:
        selma_ids = set()

    cluster_rows: list[dict[str, Any]] = []
    residual_households = sorted({normalize_id(x) for x in residual["household_id"] if normalize_id(x)})
    for hh in residual_households:
        hh_members = swathi_all[swathi_all["household_id_norm"] == hh]
        residual_members = residual[residual["household_id"].map(normalize_id) == hh]
        subscriber_rows = hh_members[
            hh_members["person_type"].str.contains("subscriber", case=False, na=False)
            | hh_members["relationship_type"].str.contains("self", case=False, na=False)
        ]
        subscriber_present_selma = any(
            normalize_id(x) in selma_ids for x in subscriber_rows["enrollee_id_norm"].tolist()
        )
        cluster_rows.append(
            {
                "household_id": hh,
                "enrollment_ids": ", ".join(
                    sorted({normalize_id(x) for x in hh_members["enrollment_id"] if normalize_id(x)})
                ),
                "residual_member_count": len(residual_members.drop_duplicates("enrollee_id_norm")),
                "household_member_count_all_swathi": len(hh_members.drop_duplicates("enrollee_id_norm")),
                "household_member_count_not_in_selma_excel": len(
                    candidates[candidates["household_id"].map(normalize_id) == hh].drop_duplicates(
                        "enrollee_id_norm"
                    )
                ),
                "residual_enrollee_ids": ", ".join(sorted(residual_members["enrollee_id_norm"].unique())),
                "subscriber_in_selma_excel": "yes" if subscriber_present_selma else "no",
                "gap_pattern": (
                    "whole_household_missing"
                    if len(residual_members.drop_duplicates("enrollee_id_norm"))
                    >= len(
                        candidates[candidates["household_id"].map(normalize_id) == hh].drop_duplicates(
                            "enrollee_id_norm"
                        )
                    )
                    and len(
                        candidates[candidates["household_id"].map(normalize_id) == hh].drop_duplicates(
                            "enrollee_id_norm"
                        )
                    )
                    > 0
                    else "partial_household_missing"
                ),
                "shared_file_groups": ", ".join(sorted(residual_members["likely_shared_file_group"].unique())),
            }
        )
    cluster_summary = pd.DataFrame(cluster_rows)

    priority = investigation.copy()
    priority["priority_score"] = 0
    priority.loc[priority["status"].map(_is_enrolled_status), "priority_score"] += 100
    hh_gap = {r["household_id"]: r["gap_pattern"] for r in cluster_rows}
    priority["gap_pattern"] = priority["household_id"].map(hh_gap).fillna("unknown")
    priority.loc[priority["gap_pattern"] == "whole_household_missing", "priority_score"] += 50
    priority.loc[priority["household_id"].duplicated(keep=False), "priority_score"] += 20
    priority.loc[priority["enrollee_id"].duplicated(keep=False), "priority_score"] += 15
    priority.loc[
        priority["source_file_name"].ne("Not available in workbook"),
        "priority_score",
    ] += 40
    priority.loc[priority["recommended_lookup_key"].str.contains("GAA_load", na=False), "priority_score"] += 10
    priority = priority.sort_values(
        ["priority_score", "status", "household_id", "enrollment_id", "enrollee_id"],
        ascending=[False, True, True, True, True],
    )
    priority["priority_rank"] = range(1, len(priority) + 1)

    search_rows: list[dict[str, str]] = []
    for _, row in investigation.iterrows():
        if row["source_file_name"] != "Not available in workbook":
            search_rows.append({"input_type": "exact_file_name", "value": row["source_file_name"]})
        for col, typ in (
            ("enrollment_id", "enrollment_id"),
            ("household_id", "household_id"),
            ("ssap_application_id", "application_id"),
            ("external_application_id", "application_id"),
        ):
            val = str(row.get(col, "")).strip()
            if val:
                search_rows.append({"input_type": typ, "value": val})
        for col, typ in (
            ("benefit_start", "benefit_start_date"),
            ("benefit_end", "benefit_end_date"),
            ("enrollment_date", "enrollment_create_date"),
            ("GAA_load_date", "GAA_load_date"),
        ):
            val = str(row.get(col, "")).strip()
            if val:
                search_rows.append({"input_type": typ, "value": val})
        enrollee = str(row.get("enrollee_id", "")).strip()
        if enrollee:
            search_rows.append({"input_type": "enrollee_id", "value": enrollee})
    search_inputs = pd.DataFrame(search_rows).drop_duplicates().sort_values(["input_type", "value"])

    # Partial filename patterns from enrollment/GAA load dates (not invented file names).
    pattern_rows: list[dict[str, str]] = []
    for val in search_inputs.loc[search_inputs["input_type"] == "GAA_load_date", "value"]:
        if len(val) >= 7:
            pattern_rows.append(
                {"input_type": "partial_file_pattern", "value": f"*83502*{val[:7].replace('-', '')}*"}
            )
    for val in search_inputs.loc[search_inputs["input_type"] == "enrollment_create_date", "value"]:
        if len(val) >= 10:
            ymd = val.replace("-", "")
            pattern_rows.append({"input_type": "partial_file_pattern", "value": f"*834*{ymd}*"})
    if pattern_rows:
        search_inputs = pd.concat([search_inputs, pd.DataFrame(pattern_rows)], ignore_index=True)
        search_inputs = search_inputs.drop_duplicates().sort_values(["input_type", "value"])

    explicit_files = investigation[investigation["source_file_name"] != "Not available in workbook"]
    ui_ready = investigation[
        (investigation["source_file_name"] == "Not available in workbook")
        & investigation["recommended_lookup_key"].str.len().gt(0)
    ]
    needs_swathi = investigation[
        (investigation["source_file_name"] == "Not available in workbook")
        & ~investigation["recommended_lookup_key"].str.contains("household_id|ssap_application_id", regex=True)
    ]
    cluster_only = (
        cluster_summary.groupby("shared_file_groups", as_index=False)
        .agg(
            residual_member_count=("residual_member_count", "sum"),
            households=("household_id", "nunique"),
        )
        .sort_values("residual_member_count", ascending=False)
    )

    assessment_rows = [
        {
            "category": "records_with_explicit_source_file_name",
            "count": len(explicit_files),
            "notes": "Workbook contains an explicit source/inbound file name.",
        },
        {
            "category": "records_without_file_name_but_ui_lookup_ready",
            "count": len(ui_ready),
            "notes": "No workbook file name; household/application/date keys support Account History lookup.",
        },
        {
            "category": "records_requiring_account_history_file_name_retrieval",
            "count": len(needs_swathi),
            "notes": "Sparse identifiers; Swathi/UI must retrieve file_name from Account History.",
        },
        {
            "category": "clusters_likely_one_missing_file",
            "count": int((cluster_only["residual_member_count"] > 1).sum()),
            "notes": "Shared-file clusters with multiple residual members — investigate once per cluster.",
        },
        {
            "category": "total_residual_not_found_in_db",
            "count": len(investigation),
            "notes": "Enrollee IDs with db_status = NOT_FOUND in member_id / issuer_indiv_identifier / exchg_assigned_enrollee_id.",
        },
    ]
    final_assessment = pd.DataFrame(assessment_rows)

    found_count = int((lookup["db_status"] == "FOUND").sum())
    not_found_count = int((lookup["db_status"] == "NOT_FOUND").sum())
    readme = pd.DataFrame(
        [
            {"item": "issuer", "value": ISSUER},
            {"item": "candidate_pool_not_in_selma_excel", "value": str(candidates["enrollee_id_norm"].nunique())},
            {"item": "db_found", "value": str(found_count)},
            {"item": "db_not_found_residual", "value": str(not_found_count)},
            {"item": "generated_at_utc", "value": datetime.now(timezone.utc).isoformat()},
            {
                "item": "source_file_columns_in_workbook",
                "value": ", ".join(source_cols) if source_cols else "none detected",
            },
            {
                "item": "note",
                "value": "Do not treat residual absence from dbo.inbound_automation as proof of a missing source file.",
            },
        ]
    )

    return InvestigationOutputs(
        lookup_results=lookup,
        residual_investigation=investigation,
        cluster_summary=cluster_summary,
        priority_list=priority,
        search_inputs=search_inputs,
        final_assessment=final_assessment,
        readme=readme,
    )


def write_investigation_workbook(outputs: InvestigationOutputs, output_path: Path) -> Path:
    sheets = {
        "README": outputs.readme,
        "DB_Lookup_Results": outputs.lookup_results,
        "1_Residual_File_Investigation": outputs.residual_investigation,
        "2_File_Household_Cluster_Summary": outputs.cluster_summary,
        "3_Manual_Validation_Priority": outputs.priority_list,
        "4_File_Log_Search_Inputs": outputs.search_inputs,
        "5_Final_Assessment": outputs.final_assessment,
    }
    safe_write_excel(output_path, sheets)
    return output_path
