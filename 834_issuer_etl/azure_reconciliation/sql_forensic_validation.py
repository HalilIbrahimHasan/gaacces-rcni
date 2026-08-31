"""
Forensic SQL validation — evidence-only diagnostics.

Read-only SELECT against Azure. Does NOT modify business logic, parsers, or comparison engines.
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from azure_reconciliation.azure_client import AZURE_TABLE_CANDIDATES, list_table_columns
from azure_reconciliation.df_utils import find_col, normalize_id, normalize_id_series
from azure_reconciliation.fixed_azure_candidate import FIXED_SCHEMA, fixed_table_name
from azure_reconciliation.partition_discovery import Partition
from azure_reconciliation.safe_export import safe_write_excel
from config.config import settings
from utils.logger import get_logger

logger = get_logger(__name__)

TABLE = fixed_table_name()
SCHEMA = FIXED_SCHEMA
FULL_TABLE = f"[{SCHEMA}].[{TABLE}]"

_ISSUER_PAT = re.compile(r"issuer|hios|carrier|exchange", re.I)
_POLICY_PAT = re.compile(r"policy|enrollment|assigned", re.I)
_MEMBER_PAT = re.compile(r"member|indiv|enrollee|subscriber|ssn", re.I)
_SUBSCRIBER_PAT = re.compile(r"subscriber", re.I)
_DATE_PAT = re.compile(r"date|effective|maint|end|load", re.I)

XML_POLICY_COLS = [
    "policy_id", "exchg_assigned_policy_id", "health_coverage_policy_no",
    "healthCoveragePolicyID", "household_or_employee_case_id",
]
XML_MEMBER_COLS = [
    "member_id", "enrollee_id", "exchg_indiv_identifier", "issuer_indiv_identifier",
    "exchg_assigned_enrollee_id",
]
XML_SUBSCRIBER_COLS = [
    "subscriber_id", "exchg_subscriber_identifier", "issuer_subscriber_identifier",
]
XML_DATE_COLS = [
    "member_maint_effective_date", "benefit_effective_date", "benefit_end_date",
    "file_name", "request_submit_timestamp",
]

AZURE_POLICY_DEFAULTS = [
    "exchgAssignedPolicyID", "healthCoveragePolicyID", "issuerPolicyID", "policy_id",
]
AZURE_MEMBER_DEFAULTS = [
    "exchgIndivIdentifier", "exchgSubscriberIdentifier", "member_id", "memberSSN",
    "subscriberIdentifier",
]
AZURE_SUBSCRIBER_DEFAULTS = [
    "exchgSubscriberIdentifier", "subscriber_id", "subscriberIdentifier",
]
AZURE_DATE_DEFAULTS = [
    "GAA_834_File_Date", "memberMaintEffectiveDate", "benefitEffectiveBeginDate",
    "benefitEffectiveEndDate", "benefitEffectiveDate",
]

EVIDENCE: list[dict[str, Any]] = []


def _out_dir() -> Path:
    d = settings.outputs_path / "sql_validation"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _record(query: str, result: Any, note: str = "") -> None:
    EVIDENCE.append({
        "query": query.strip(),
        "result": result,
        "note": note,
    })


def _bracket(col: str) -> str:
    return f"[{col.replace(']', ']]')}]"


def _cols_matching(columns: list[str], pattern: re.Pattern) -> list[str]:
    return [c for c in columns if pattern.search(c)]


def _run_scalar(engine: Engine, sql: str, params: dict | None = None) -> Any:
    with engine.connect() as conn:
        row = conn.execute(text(sql), params or {}).fetchone()
    return row[0] if row else None


def _run_df(engine: Engine, sql: str, params: dict | None = None) -> pd.DataFrame:
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params=params or {})


def _distinct_sample(engine: Engine, col: str, limit: int = 20) -> list[str]:
    sql = (
        f"SELECT DISTINCT TOP {limit} CAST({_bracket(col)} AS VARCHAR(200)) AS v "
        f"FROM {FULL_TABLE} WHERE {_bracket(col)} IS NOT NULL"
    )
    try:
        df = _run_df(engine, sql)
        return [str(x).strip() for x in df["v"].tolist() if str(x).strip()]
    except Exception as exc:
        logger.warning("distinct sample failed for %s: %s", col, exc)
        return []


def _issuer_variants(issuer: str) -> list[str]:
    base = str(issuer).strip()
    variants = {base, base.lstrip("0"), base.zfill(5), f"{base}.0"}
    if base.isdigit():
        variants.add(str(int(base)))
    return [v for v in variants if v]


def _xml_distinct_ids(xml_raw: pd.DataFrame, col_names: list[str], limit: int = 100) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    if xml_raw.empty:
        return out
    for name in col_names:
        col = find_col(xml_raw, name)
        if not col:
            continue
        vals = normalize_id_series(xml_raw[col])
        vals = vals[vals.astype(str).str.strip() != ""]
        uniq = sorted(vals.unique().tolist())[:limit]
        if uniq:
            out[col] = uniq
    return out


def _overlap_stats(xml_ids: list[str], az_ids: set[str]) -> dict[str, int]:
    xml_set = {normalize_id(x) for x in xml_ids if normalize_id(x)}
    az_norm = {normalize_id(x) for x in az_ids if normalize_id(x)}
    exact = len(xml_set & az_norm)
    partial = 0
    normalized = 0
    for x in xml_set:
        if x in az_norm:
            normalized += 1
            continue
        for a in az_norm:
            if x and a and (x in a or a in x):
                partial += 1
                break
    return {
        "exact_matches": exact,
        "partial_matches": partial,
        "normalized_matches": normalized,
        "xml_distinct": len(xml_set),
        "azure_distinct": len(az_norm),
    }


def _fetch_azure_ids(engine: Engine, col: str, issuer: str | None = None, limit: int = 50000) -> set[str]:
    where = f"WHERE {_bracket(col)} IS NOT NULL"
    params: dict[str, Any] = {}
    if issuer:
        where += " AND CAST([GAA_HIOS_ID] AS VARCHAR(20)) = :issuer"
        params["issuer"] = str(issuer)
    sql = (
        f"SELECT DISTINCT TOP {limit} CAST({_bracket(col)} AS VARCHAR(200)) AS v "
        f"FROM {FULL_TABLE} {where}"
    )
    try:
        df = _run_df(engine, sql, params)
        _record(sql, f"{len(df)} distinct values", f"azure ids for {col}")
        return {normalize_id(v) for v in df["v"].tolist() if normalize_id(v)}
    except Exception as exc:
        _record(sql, f"ERROR: {exc}", f"azure ids for {col}")
        return set()


_table_cols_cache: dict[str, list[str]] = {}


def _table_columns_cache(engine: Engine) -> list[str]:
    key = f"{SCHEMA}.{TABLE}"
    if key not in _table_cols_cache:
        _table_cols_cache[key] = list_table_columns(engine, SCHEMA, TABLE)
    return _table_cols_cache[key]


def step1_issuer_column_analysis(
    engine: Engine,
    *,
    issuer: str,
    columns: list[str],
) -> pd.DataFrame:
    """Search every issuer-like column for current partition issuer."""
    issuer_cols = _cols_matching(columns, _ISSUER_PAT)
    if "GAA_HIOS_ID" in columns and "GAA_HIOS_ID" not in issuer_cols:
        issuer_cols.insert(0, "GAA_HIOS_ID")

    rows: list[dict[str, Any]] = []
    variants = _issuer_variants(issuer)

    for col in issuer_cols:
        dist_sql = (
            f"SELECT COUNT(DISTINCT CAST({_bracket(col)} AS VARCHAR(100))) "
            f"FROM {FULL_TABLE}"
        )
        try:
            distinct_count = int(_run_scalar(engine, dist_sql) or 0)
        except Exception as exc:
            distinct_count = -1
            _record(dist_sql, f"ERROR: {exc}", col)

        match_count = 0
        contains = False
        for variant in variants:
            match_sql = (
                f"SELECT COUNT(*) FROM {FULL_TABLE} "
                f"WHERE CAST({_bracket(col)} AS VARCHAR(50)) = :v"
            )
            try:
                cnt = int(_run_scalar(engine, match_sql, {"v": variant}) or 0)
                _record(match_sql, cnt, f"{col} variant={variant}")
                if cnt > 0:
                    contains = True
                    match_count = max(match_count, cnt)
            except Exception:
                pass
            like_sql = (
                f"SELECT COUNT(*) FROM {FULL_TABLE} "
                f"WHERE CAST({_bracket(col)} AS VARCHAR(50)) LIKE :pat"
            )
            try:
                cnt = int(_run_scalar(engine, like_sql, {"pat": f"%{variant}%"}) or 0)
                _record(like_sql, cnt, f"{col} LIKE variant={variant}")
                if cnt > 0:
                    contains = True
                    match_count = max(match_count, cnt)
            except Exception:
                pass

        samples = _distinct_sample(engine, col, 20)
        rows.append({
            "column_name": col,
            "distinct_count": distinct_count,
            "contains_current_issuer": contains,
            "sample_values": "; ".join(samples[:20]),
            "matching_rows": match_count,
        })

    return pd.DataFrame(rows)


def step_overlap_matrix(
    engine: Engine,
    *,
    issuer: str,
    xml_raw: pd.DataFrame,
    xml_col_names: list[str],
    azure_col_names: list[str],
    columns: list[str],
    id_type: str,
) -> pd.DataFrame:
    """Compute XML vs Azure ID overlap for policy/member/subscriber."""
    xml_ids_map = _xml_distinct_ids(xml_raw, xml_col_names, limit=100)
    az_cols = [c for c in azure_col_names if c in columns]
    az_cols.extend([c for c in _cols_matching(columns, _POLICY_PAT if id_type == "policy" else _MEMBER_PAT if id_type == "member" else _SUBSCRIBER_PAT) if c not in az_cols])

    rows: list[dict[str, Any]] = []
    for xml_col, ids in xml_ids_map.items():
        if not ids:
            continue
        # SQL IN batch count per azure column
        for az_col in az_cols:
            if az_col not in columns:
                continue
            in_list = ids[:100]
            placeholders = ", ".join(f":p{i}" for i in range(len(in_list)))
            params = {f"p{i}": v for i, v in enumerate(in_list)}
            in_sql = (
                f"SELECT COUNT(*) FROM {FULL_TABLE} "
                f"WHERE CAST({_bracket(az_col)} AS VARCHAR(200)) IN ({placeholders})"
            )
            try:
                in_count = int(_run_scalar(engine, in_sql, params) or 0)
                _record(in_sql, in_count, f"{id_type} IN match {xml_col}->{az_col}")
            except Exception as exc:
                in_count = -1
                _record(in_sql, f"ERROR: {exc}", f"{id_type} IN match")

            az_set = _fetch_azure_ids(engine, az_col, issuer=None, limit=20000)
            stats = _overlap_stats(ids, az_set)
            rows.append({
                "id_type": id_type,
                "xml_column": xml_col,
                "azure_column": az_col,
                "exact_matches": stats["exact_matches"],
                "partial_matches": stats["partial_matches"],
                "normalized_matches": stats["normalized_matches"],
                "sql_in_clause_matches": in_count,
                "xml_distinct_sampled": stats["xml_distinct"],
                "azure_distinct_sampled": stats["azure_distinct"],
                "overlap_rank_score": (
                    stats["normalized_matches"] * 3
                    + stats["partial_matches"] * 2
                    + stats["exact_matches"]
                    + max(in_count, 0)
                ),
            })

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values("overlap_rank_score", ascending=False).reset_index(drop=True)
    return df


def step5_date_validation(
    engine: Engine,
    *,
    xml_raw: pd.DataFrame,
    columns: list[str],
) -> pd.DataFrame:
    """Compare XML date distributions to Azure date columns."""
    az_date_cols = [c for c in columns if _DATE_PAT.search(c)]
    for d in AZURE_DATE_DEFAULTS:
        if d in columns and d not in az_date_cols:
            az_date_cols.insert(0, d)

    xml_months: set[str] = set()
    if not xml_raw.empty:
        for col in XML_DATE_COLS:
            c = find_col(xml_raw, col)
            if not c:
                continue
            dates = pd.to_datetime(xml_raw[c], errors="coerce")
            for d in dates.dropna():
                xml_months.add(f"{d.year}-{int(d.month):02d}")

    rows: list[dict[str, Any]] = []
    for az_col in az_date_cols:
        sql = (
            f"SELECT YEAR({_bracket(az_col)}) AS y, MONTH({_bracket(az_col)}) AS m, COUNT(*) AS cnt "
            f"FROM {FULL_TABLE} WHERE {_bracket(az_col)} IS NOT NULL "
            f"GROUP BY YEAR({_bracket(az_col)}), MONTH({_bracket(az_col)}) "
            f"ORDER BY y, m"
        )
        try:
            df = _run_df(engine, sql)
            _record(sql, f"{len(df)} month buckets", az_col)
            az_months = {f"{int(r.y)}-{int(r.m):02d}" for r in df.itertuples() if pd.notna(r.y)}
            overlap = sorted(xml_months & az_months)
            rows.append({
                "azure_date_column": az_col,
                "azure_month_buckets": len(az_months),
                "xml_month_buckets": len(xml_months),
                "overlapping_year_months": "; ".join(overlap[:24]),
                "overlap_count": len(overlap),
                "azure_month_sample": "; ".join(sorted(az_months)[:12]),
                "xml_month_sample": "; ".join(sorted(xml_months)[:12]),
            })
        except Exception as exc:
            _record(sql, f"ERROR: {exc}", az_col)
            rows.append({
                "azure_date_column": az_col,
                "azure_month_buckets": 0,
                "xml_month_buckets": len(xml_months),
                "overlapping_year_months": "",
                "overlap_count": -1,
                "azure_month_sample": "",
                "xml_month_sample": "; ".join(sorted(xml_months)[:12]),
            })
    out = pd.DataFrame(rows)
    if not out.empty:
        out = out.sort_values("overlap_count", ascending=False).reset_index(drop=True)
    return out


def step6_issuer_existence(
    engine: Engine,
    *,
    issuer: str,
    columns: list[str],
) -> pd.DataFrame:
    """When Azure returns zero rows — prove whether issuer exists anywhere."""
    rows: list[dict[str, Any]] = []

    dist_sql = f"SELECT DISTINCT TOP 500 CAST(GAA_HIOS_ID AS VARCHAR(50)) AS v FROM {FULL_TABLE} ORDER BY v"
    try:
        issuers_df = _run_df(engine, dist_sql)
        all_issuers = issuers_df["v"].astype(str).tolist()
        _record(dist_sql, f"{len(all_issuers)} distinct GAA_HIOS_ID values", "issuer census")
        rows.append({
            "search_type": "DISTINCT GAA_HIOS_ID",
            "column_searched": "GAA_HIOS_ID",
            "variant": "(all)",
            "match_count": len(all_issuers),
            "issuer_found_in_list": issuer in all_issuers or issuer.lstrip("0") in all_issuers,
            "sample_hits": "; ".join(all_issuers[:30]),
        })
    except Exception as exc:
        _record(dist_sql, f"ERROR: {exc}", "issuer census")

    issuer_cols = _cols_matching(columns, _ISSUER_PAT)
    variants = _issuer_variants(issuer)

    for col in issuer_cols:
        for variant in variants:
            for mode, sql_tpl in (
                ("exact", f"SELECT COUNT(*) FROM {FULL_TABLE} WHERE CAST({_bracket(col)} AS VARCHAR(50)) = :v"),
                ("like", f"SELECT COUNT(*) FROM {FULL_TABLE} WHERE CAST({_bracket(col)} AS VARCHAR(50)) LIKE :v"),
                ("trim", f"SELECT COUNT(*) FROM {FULL_TABLE} WHERE LTRIM(RTRIM(CAST({_bracket(col)} AS VARCHAR(50)))) = :v"),
            ):
                pat = variant if mode != "like" else f"%{variant}%"
                try:
                    cnt = int(_run_scalar(engine, sql_tpl, {"v": pat}) or 0)
                    _record(sql_tpl, cnt, f"{col} {mode} {variant}")
                    if cnt > 0:
                        samp = _distinct_sample(engine, col, 5)
                        rows.append({
                            "search_type": mode,
                            "column_searched": col,
                            "variant": variant,
                            "match_count": cnt,
                            "issuer_found_in_list": True,
                            "sample_hits": "; ".join(samp),
                        })
                except Exception as exc:
                    _record(sql_tpl, f"ERROR: {exc}", f"{col} {mode}")

    return pd.DataFrame(rows)


def step7_record_lookup(
    engine: Engine,
    *,
    issuer: str,
    xml_raw: pd.DataFrame,
    columns: list[str],
    schema: str = SCHEMA,
) -> pd.DataFrame:
    """For 20 random XML records, attempt to locate Azure row."""
    if xml_raw.empty:
        return pd.DataFrame()

    sample = xml_raw.sample(n=min(20, len(xml_raw)), random_state=42).copy()
    policy_az = find_col(pd.DataFrame(columns=columns), "exchgAssignedPolicyID") or "exchgAssignedPolicyID"
    member_az = find_col(pd.DataFrame(columns=columns), "exchgIndivIdentifier") or "exchgIndivIdentifier"
    issuer_az = find_col(pd.DataFrame(columns=columns), "GAA_HIOS_ID") or "GAA_HIOS_ID"

    other_tables = [t for t in AZURE_TABLE_CANDIDATES if t != TABLE][:8]

    rows: list[dict[str, Any]] = []
    for idx, row in sample.iterrows():
        pol = normalize_id(row.get("policy_id", ""))
        mem = normalize_id(row.get("member_id", ""))
        if not mem:
            mem = normalize_id(row.get("exchg_indiv_identifier", "")) or normalize_id(
                row.get("issuer_indiv_identifier", "")
            ) or normalize_id(row.get("exchg_assigned_enrollee_id", ""))
        sub = normalize_id(row.get("subscriber_id", ""))
        iss = normalize_id(row.get("issuer", issuer))
        maint = str(row.get("member_maint_effective_date", ""))[:10]
        benefit = str(row.get("benefit_effective_date", ""))[:10]

        outcome = "NOT FOUND"
        evidence_sql = ""
        evidence_result = ""

        queries: list[tuple[str, str, dict]] = [
            (
                "policy+member+issuer",
                f"SELECT COUNT(*) FROM {FULL_TABLE} WHERE CAST({_bracket(issuer_az)} AS VARCHAR(20))=:iss "
                f"AND CAST({_bracket(policy_az)} AS VARCHAR(200))=:pol "
                f"AND CAST({_bracket(member_az)} AS VARCHAR(200))=:mem",
                {"iss": iss, "pol": pol, "mem": mem},
            ),
            (
                "policy+member",
                f"SELECT COUNT(*) FROM {FULL_TABLE} WHERE CAST({_bracket(policy_az)} AS VARCHAR(200))=:pol "
                f"AND CAST({_bracket(member_az)} AS VARCHAR(200))=:mem",
                {"pol": pol, "mem": mem},
            ),
            (
                "policy_only",
                f"SELECT COUNT(*) FROM {FULL_TABLE} WHERE CAST({_bracket(policy_az)} AS VARCHAR(200))=:pol",
                {"pol": pol},
            ),
            (
                "member_only",
                f"SELECT COUNT(*) FROM {FULL_TABLE} WHERE CAST({_bracket(member_az)} AS VARCHAR(200))=:mem",
                {"mem": mem},
            ),
        ]
        if sub:
            sub_col = find_col(pd.DataFrame(columns=columns), "exchgSubscriberIdentifier") or "exchgSubscriberIdentifier"
            queries.append((
                "subscriber_only",
                f"SELECT COUNT(*) FROM {FULL_TABLE} WHERE CAST({_bracket(sub_col)} AS VARCHAR(200))=:sub",
                {"sub": sub},
            ))

        for label, sql, params in queries:
            if not all(params.values()) and label != "policy_only" and label != "member_only":
                continue
            try:
                cnt = int(_run_scalar(engine, sql, params) or 0)
                _record(sql, cnt, f"record lookup {label} idx={idx}")
                if cnt > 0:
                    outcome = "FOUND"
                    evidence_sql = sql
                    evidence_result = str(cnt)
                    break
            except Exception as exc:
                _record(sql, f"ERROR: {exc}", label)

        if outcome == "NOT FOUND" and pol:
            sql_pol = f"SELECT COUNT(*) FROM {FULL_TABLE} WHERE CAST({_bracket(policy_az)} AS VARCHAR(200))=:pol"
            cnt = int(_run_scalar(engine, sql_pol, {"pol": pol}) or 0)
            if cnt > 0 and mem:
                sql_mem = f"SELECT COUNT(*) FROM {FULL_TABLE} WHERE CAST({_bracket(member_az)} AS VARCHAR(200))=:mem"
                mem_cnt = int(_run_scalar(engine, sql_mem, {"mem": mem}) or 0)
                if mem_cnt == 0:
                    outcome = "FOUND WITH DIFFERENT MEMBER"
                    evidence_sql = sql_pol
                    evidence_result = str(cnt)

        if outcome == "NOT FOUND" and mem:
            sql_mem = f"SELECT COUNT(*) FROM {FULL_TABLE} WHERE CAST({_bracket(member_az)} AS VARCHAR(200))=:mem"
            mem_cnt = int(_run_scalar(engine, sql_mem, {"mem": mem}) or 0)
            if mem_cnt > 0 and pol:
                sql_pol = f"SELECT COUNT(*) FROM {FULL_TABLE} WHERE CAST({_bracket(policy_az)} AS VARCHAR(200))=:pol"
                pol_cnt = int(_run_scalar(engine, sql_pol, {"pol": pol}) or 0)
                if pol_cnt == 0:
                    outcome = "FOUND WITH DIFFERENT POLICY"
                    evidence_sql = sql_mem
                    evidence_result = str(mem_cnt)

        if outcome == "FOUND" and (maint or benefit):
            date_col = find_col(pd.DataFrame(columns=columns), "memberMaintEffectiveDate") or "memberMaintEffectiveDate"
            if maint:
                sql_date = (
                    f"SELECT COUNT(*) FROM {FULL_TABLE} "
                    f"WHERE CAST({_bracket(policy_az)} AS VARCHAR(200))=:pol "
                    f"AND CAST({_bracket(member_az)} AS VARCHAR(200))=:mem "
                    f"AND CONVERT(VARCHAR(10), {_bracket(date_col)}, 23) = :dt"
                )
                try:
                    dt_cnt = int(_run_scalar(engine, sql_date, {"pol": pol, "mem": mem, "dt": maint}) or 0)
                    _record(sql_date, dt_cnt, "date check maint")
                    if dt_cnt == 0:
                        outcome = "FOUND WITH DIFFERENT DATE"
                        evidence_sql = sql_date
                        evidence_result = "0 rows with matching maint date"
                except Exception as exc:
                    _record(sql_date, f"ERROR: {exc}", "date check")

        if outcome == "NOT FOUND":
            for ot in other_tables:
                try:
                    ot_cols = list_table_columns(engine, schema, ot)
                except Exception:
                    continue
                if policy_az not in ot_cols:
                    continue
                ot_full = f"[{schema}].[{ot}]"
                sql = f"SELECT COUNT(*) FROM {ot_full} WHERE CAST({_bracket(policy_az)} AS VARCHAR(200))=:pol"
                try:
                    cnt = int(_run_scalar(engine, sql, {"pol": pol}) or 0)
                    if cnt > 0:
                        outcome = "FOUND IN DIFFERENT TABLE"
                        evidence_sql = sql
                        evidence_result = f"{ot}: {cnt}"
                        _record(sql, cnt, f"other table {ot}")
                        break
                except Exception:
                    pass

        rows.append({
            "xml_row_index": idx,
            "xml_issuer": iss,
            "xml_policy_id": pol,
            "xml_member_id": mem,
            "xml_subscriber_id": sub,
            "xml_maint_date": maint,
            "xml_benefit_date": benefit,
            "outcome": outcome,
            "evidence_sql": evidence_sql,
            "evidence_result": evidence_result,
        })

    return pd.DataFrame(rows)


def _write_root_cause_report(
    path: Path,
    *,
    issuer: str,
    issuer_analysis: pd.DataFrame,
    policy_overlap: pd.DataFrame,
    member_overlap: pd.DataFrame,
    subscriber_overlap: pd.DataFrame,
    date_validation: pd.DataFrame,
    issuer_existence: pd.DataFrame,
    record_lookup: pd.DataFrame,
    table_row_count: int,
    issuer_filtered_count: int,
) -> None:
    """Synthesize evidence-only root cause report."""

    def _best(df: pd.DataFrame, col: str = "overlap_rank_score") -> str:
        if df.empty or col not in df.columns:
            return "no data"
        r = df.iloc[0]
        return (
            f"{r.get('xml_column', '?')} -> {r.get('azure_column', '?')}: "
            f"normalized={r.get('normalized_matches', 0)}, "
            f"sql_in={r.get('sql_in_clause_matches', 0)}"
        )

    issuer_in_table = False
    if not issuer_analysis.empty:
        issuer_in_table = bool(issuer_analysis["contains_current_issuer"].any())

    best_issuer_col = "none"
    if not issuer_analysis.empty:
        hits = issuer_analysis[issuer_analysis["contains_current_issuer"]]
        if not hits.empty:
            best_issuer_col = str(hits.iloc[0]["column_name"])

    lookup_found = 0
    if not record_lookup.empty:
        lookup_found = int((record_lookup["outcome"] == "FOUND").sum())

    lines = [
        "# SQL Forensic Root Cause Report",
        "",
        f"**Generated:** {datetime.now(timezone.utc).isoformat(timespec='seconds')}",
        f"**Issuer partition:** {issuer}",
        f"**Table:** {SCHEMA}.{TABLE}",
        "",
        "## Evidence summary",
        "",
        f"- Total rows in `{TABLE}` (unfiltered): **{table_row_count}**",
        f"  - Query: `SELECT COUNT(*) FROM {FULL_TABLE}`",
        f"- Rows matching issuer `{issuer}` on GAA_HIOS_ID: **{issuer_filtered_count}**",
        f"  - Query: `SELECT COUNT(*) FROM {FULL_TABLE} WHERE CAST(GAA_HIOS_ID AS VARCHAR(20)) = '{issuer}'`",
        "",
        "## Answers",
        "",
        "### 1. Are XML records physically present in Azure?",
        "",
    ]

    if issuer_filtered_count > 0 and lookup_found > 0:
        lines.append(
            f"**Partially yes.** `{issuer_filtered_count}` rows match issuer filter; "
            f"{lookup_found}/20 sampled XML records found exact policy+member/issuer matches. "
            f"Evidence: record_lookup outcomes."
        )
    elif issuer_filtered_count > 0:
        lines.append(
            f"**Issuer rows exist ({issuer_filtered_count}) but sampled XML records were NOT FOUND** "
            f"by policy/member lookup. Evidence: record_lookup — "
            f"{record_lookup['outcome'].value_counts().to_dict() if not record_lookup.empty else {}}."
        )
    else:
        lines.append(
            f"**No rows for issuer `{issuer}` on GAA_HIOS_ID.** "
            f"Table has {table_row_count} total rows. Evidence: issuer filter count = 0."
        )

    lines.extend([
        "",
        "### 2. If not, why not?",
        "",
    ])
    if not issuer_in_table:
        lines.append(
            f"**Issuer `{issuer}` not found in any issuer-like column** (or only via non-GAA_HIOS_ID columns). "
            f"Best matching issuer column: `{best_issuer_col}`. "
            f"See `issuer_existence` / `issuer_column_analysis` sheets."
        )
    elif issuer_filtered_count == 0 and table_row_count > 0:
        lines.append(
            "**Date/partition filter may exclude rows** — issuer may exist under different column "
            "or GAA_HIOS_ID uses different formatting. See date_validation overlap."
        )
    else:
        lines.append("See ID overlap matrices — policy/member mapping may be wrong.")

    lines.extend([
        "",
        "### 3. Are IDs transformed?",
        "",
        f"**Best policy mapping:** {_best(policy_overlap)}",
        f"**Best member mapping:** {_best(member_overlap)}",
        f"**Best subscriber mapping:** {_best(subscriber_overlap)}",
        "",
        "### 4. Are issuers transformed?",
        "",
    ])
    if not issuer_analysis.empty:
        for _, r in issuer_analysis.head(5).iterrows():
            lines.append(
                f"- `{r['column_name']}`: distinct={r['distinct_count']}, "
                f"matches issuer={r['contains_current_issuer']}, rows={r['matching_rows']}, "
                f"samples={r['sample_values'][:80]}"
            )
    lines.extend([
        "",
        "### 5. Are dates transformed?",
        "",
    ])
    if not date_validation.empty:
        best_date = date_validation.iloc[0]
        lines.append(
            f"**Best aligning Azure date column:** `{best_date['azure_date_column']}` "
            f"with {best_date['overlap_count']} overlapping year-month buckets. "
            f"Azure months: {best_date['azure_month_sample']}. "
            f"XML months: {best_date['xml_month_sample']}."
        )
    else:
        lines.append("No date validation data.")

    lines.extend([
        "",
        "### 6. Are we reading the wrong table?",
        "",
        f"Primary table is `{TABLE}` per FAST_MODE (unchanged). "
        f"Record lookup checked alternate tables: "
        f"{record_lookup[record_lookup['outcome'] == 'FOUND IN DIFFERENT TABLE']['evidence_result'].tolist() if not record_lookup.empty else 'none'}.",
        "",
        "### 7. Are we filtering incorrectly?",
        "",
        f"Issuer filter on GAA_HIOS_ID returned **{issuer_filtered_count}** rows. "
        f"Issuer found in issuer-like columns: **{issuer_in_table}**.",
        "",
        "### 8. Is Azure missing data?",
        "",
    ])
    if table_row_count == 0:
        lines.append("**Yes — table is empty.**")
    elif issuer_filtered_count == 0:
        lines.append(f"**Azure has data ({table_row_count} rows) but none for issuer `{issuer}` on standard filter.**")
    else:
        lines.append(f"**Azure has {issuer_filtered_count} rows for issuer; data is not wholly missing.**")

    lines.extend([
        "",
        "### 9. Is XML ahead of Azure?",
        "",
    ])
    if not record_lookup.empty:
        not_found = int((record_lookup["outcome"] == "NOT FOUND").sum())
        lines.append(
            f"**{not_found}/20** sampled XML records had no Azure match — "
            f"consistent with XML ahead or ID mismatch. See record_lookup sheet."
        )

    lines.extend([
        "",
        "### 10. SQL evidence log",
        "",
        f"Full query log: `{_out_dir() / 'sql_evidence_log.json'}`",
        "",
        "Top queries executed:",
        "",
    ])
    for e in EVIDENCE[:25]:
        lines.append(f"- `{e['query'][:200]}` → **{e['result']}** ({e.get('note', '')})")

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    logger.info("Wrote root cause report: %s", path)


def run_sql_forensic_validation(
    engine: Engine,
    *,
    issuer: str,
    xml_raw: pd.DataFrame,
    partitions: list[Partition],
) -> dict[str, str]:
    """
    Execute all forensic SQL validation steps. Returns output paths.
    """
    global EVIDENCE
    EVIDENCE = []

    out = _out_dir()
    columns = _table_columns_cache(engine)
    paths: dict[str, str] = {}

    count_sql = f"SELECT COUNT(*) FROM {FULL_TABLE}"
    table_row_count = int(_run_scalar(engine, count_sql) or 0)
    _record(count_sql, table_row_count, "total rows")

    issuer_sql = (
        f"SELECT COUNT(*) FROM {FULL_TABLE} "
        f"WHERE CAST(GAA_HIOS_ID AS VARCHAR(20)) = :issuer"
    )
    issuer_filtered_count = int(_run_scalar(engine, issuer_sql, {"issuer": str(issuer)}) or 0)
    _record(issuer_sql, issuer_filtered_count, f"issuer={issuer}")

    logger.info(
        "SQL forensic validation issuer=%s table_rows=%s issuer_rows=%s",
        issuer, table_row_count, issuer_filtered_count,
    )

    issuer_analysis = step1_issuer_column_analysis(engine, issuer=issuer, columns=columns)
    policy_overlap = step_overlap_matrix(
        engine, issuer=issuer, xml_raw=xml_raw, xml_col_names=XML_POLICY_COLS,
        azure_col_names=AZURE_POLICY_DEFAULTS, columns=columns, id_type="policy",
    )
    member_overlap = step_overlap_matrix(
        engine, issuer=issuer, xml_raw=xml_raw, xml_col_names=XML_MEMBER_COLS,
        azure_col_names=AZURE_MEMBER_DEFAULTS, columns=columns, id_type="member",
    )
    subscriber_overlap = step_overlap_matrix(
        engine, issuer=issuer, xml_raw=xml_raw, xml_col_names=XML_SUBSCRIBER_COLS,
        azure_col_names=AZURE_SUBSCRIBER_DEFAULTS, columns=columns, id_type="subscriber",
    )
    date_validation = step5_date_validation(engine, xml_raw=xml_raw, columns=columns)
    issuer_existence = step6_issuer_existence(engine, issuer=issuer, columns=columns)
    record_lookup = step7_record_lookup(engine, issuer=issuer, xml_raw=xml_raw, columns=columns)

    # Step 1 xlsx (also includes other sheets)
    xlsx_policy = out / "policy_overlap.xlsx"
    safe_write_excel(xlsx_policy, {
        "policy_overlap": policy_overlap,
        "top_policy_pairs": policy_overlap.head(50) if not policy_overlap.empty else policy_overlap,
    }, drop_duplicate_value_columns=False)
    paths["policy_overlap"] = str(xlsx_policy)

    xlsx_member = out / "member_overlap.xlsx"
    safe_write_excel(xlsx_member, {
        "member_overlap": member_overlap,
        "top_member_pairs": member_overlap.head(50) if not member_overlap.empty else member_overlap,
    }, drop_duplicate_value_columns=False)
    paths["member_overlap"] = str(xlsx_member)

    xlsx_sub = out / "subscriber_overlap.xlsx"
    safe_write_excel(xlsx_sub, {
        "subscriber_overlap": subscriber_overlap,
    }, drop_duplicate_value_columns=False)
    paths["subscriber_overlap"] = str(xlsx_sub)

    xlsx_issuer = out / "issuer_column_analysis.xlsx"
    safe_write_excel(xlsx_issuer, {
        "issuer_column_analysis": issuer_analysis,
        "issuer_existence_search": issuer_existence,
    }, drop_duplicate_value_columns=False)
    paths["issuer_column_analysis"] = str(xlsx_issuer)

    xlsx_dates = out / "date_validation.xlsx"
    safe_write_excel(xlsx_dates, {"date_validation": date_validation}, drop_duplicate_value_columns=False)
    paths["date_validation"] = str(xlsx_dates)

    xlsx_lookup = out / "record_lookup.xlsx"
    safe_write_excel(xlsx_lookup, {"record_lookup": record_lookup}, drop_duplicate_value_columns=False)
    paths["record_lookup"] = str(xlsx_lookup)

    evidence_path = out / "sql_evidence_log.json"
    evidence_path.write_text(json.dumps(EVIDENCE, indent=2, default=str), encoding="utf-8")
    paths["sql_evidence_log"] = str(evidence_path)

    report_path = out / "root_cause_report.md"
    _write_root_cause_report(
        report_path,
        issuer=issuer,
        issuer_analysis=issuer_analysis,
        policy_overlap=policy_overlap,
        member_overlap=member_overlap,
        subscriber_overlap=subscriber_overlap,
        date_validation=date_validation,
        issuer_existence=issuer_existence,
        record_lookup=record_lookup,
        table_row_count=table_row_count,
        issuer_filtered_count=issuer_filtered_count,
    )
    paths["root_cause_report"] = str(report_path)

    return paths
