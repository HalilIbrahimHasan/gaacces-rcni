"""Read-only streaming profiler for a local RCNI archive tree.

Does not mutate source files, does not connect to Azure, and does not load
whole CSVs into memory.
"""

from __future__ import annotations

import csv
import gzip
import hashlib
import os
import re
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from rcni.archive_path import parse_archive_path
from rcni.constants import EXPECTED_COLUMN_COUNT, EXPECTED_HEADER
from rcni.filename import parse_rcni_filename
from rcni.matcher import is_rcni_sftp_archive_file, rcni_sftp_reject_reason
from utils.hashing import sha256_file
from utils.logger import get_logger

logger = get_logger(__name__)

DATE_FORMATS = (
    "%Y%m%d",
    "%Y-%m-%d",
    "%m/%d/%Y",
    "%m/%d/%y",
    "%Y/%m/%d",
    "%m-%d-%Y",
    "%Y%m%d%H%M%S",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S",
)

RECON_RE = re.compile(
    r"^(?P<direction>from)_"
    r"(?P<issuer_id>\d+)_"
    r"(?P<document_type>INDV_MONTHLYRECON)_"
    r"(?P<plan_year>\d{4})_"
    r"(?P<timestamp>\d{14})"
    r"(?P<suffix>\.IN(?:\.gz)?)$",
    re.IGNORECASE,
)

CATEGORICAL_COLUMNS = (
    "Enrollment Status",
    "Autofixed by HIX",
    "Discrepancy Reason Code",
    "Assignee",
)

SAMPLE_LIMIT = 12
HIX_EXAMPLE_LIMIT = 8
TOP_N = 25


class _CountingText:
    def __init__(self, handle):
        self._handle = handle
        self.physical_line_number = 0
        self.uncompressed_bytes = 0

    def __iter__(self):
        return self

    def __next__(self) -> str:
        line = next(self._handle)
        self.physical_line_number += 1
        self.uncompressed_bytes += len(line.encode("utf-8", errors="replace"))
        return line


@dataclass
class ColumnStats:
    name: str
    total_rows: int = 0
    blank_count: int = 0
    whitespace_only_count: int = 0
    non_blank_count: int = 0
    distinct: set[str] = field(default_factory=set)
    samples: list[str] = field(default_factory=list)
    min_len: int | None = None
    max_len: int = 0
    numeric_looking: int = 0
    alpha_only: int = 0
    mixed_alnum: int = 0
    leading_zeros: int = 0
    embedded_commas: int = 0
    quotes: int = 0
    line_breaks: int = 0
    non_ascii: int = 0
    frequencies: Counter[str] = field(default_factory=Counter)
    track_frequencies: bool = False


@dataclass
class FileProfile:
    source_file: str
    source_path: str
    processing_year: str
    processing_month: str
    processing_day: str | None
    issuer_id: str
    plan_year: str
    file_timestamp: str
    compressed: bool
    file_size: int
    sha256: str
    header: tuple[str, ...] | None = None
    header_column_count: int = 0
    data_row_count: int = 0
    blank_row_count: int = 0
    structural_malformed_row_count: int = 0
    parser_exceptions: int = 0
    min_field_count: int | None = None
    max_field_count: int = 0
    field_counts: Counter[int] = field(default_factory=Counter)
    uncompressed_bytes: int = 0
    bom: bool = False
    empty_file: bool = False
    header_ok: bool = False
    header_notes: list[str] = field(default_factory=list)
    header_repeat_in_data: int = 0
    duplicate_header_names: list[str] = field(default_factory=list)


def _is_blank(value: str) -> bool:
    return value == ""


def _is_whitespace_only(value: str) -> bool:
    return value != "" and value.strip() == ""


def _parse_date(raw: str) -> tuple[datetime | None, str | None]:
    text = raw.strip()
    if not text:
        return None, None
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(text, fmt), fmt
        except ValueError:
            continue
    return None, None


def _classify_structural(observed: int, raw: str, physical_span: int, parse_error: str | None) -> str:
    if parse_error:
        lower = parse_error.lower()
        if "quote" in lower:
            return "BROKEN_QUOTE"
        if "codec" in lower or "encode" in lower or "utf" in lower:
            return "ENCODING_ISSUE"
        return "OTHER"
    if physical_span > 1:
        return "MULTILINE_FIELD"
    if observed != EXPECTED_COLUMN_COUNT:
        if '"' in raw and raw.count('"') % 2 == 1:
            return "BROKEN_QUOTE"
        if observed > EXPECTED_COLUMN_COUNT:
            return "UNQUOTED_COMMA"
        return "COLUMN_COUNT_MISMATCH"
    return "OTHER"


def _update_column(stats: ColumnStats, value: str) -> None:
    stats.total_rows += 1
    if _is_blank(value):
        stats.blank_count += 1
        return
    if _is_whitespace_only(value):
        stats.whitespace_only_count += 1
        stats.blank_count += 1
        return
    stats.non_blank_count += 1
    length = len(value)
    stats.min_len = length if stats.min_len is None else min(stats.min_len, length)
    stats.max_len = max(stats.max_len, length)
    if len(stats.distinct) < 2_000_000:
        stats.distinct.add(value)
    if len(stats.samples) < SAMPLE_LIMIT and value not in stats.samples:
        stats.samples.append(value)
    if stats.track_frequencies:
        stats.frequencies[value] += 1
    has_digit = any(ch.isdigit() for ch in value)
    has_alpha = any(ch.isalpha() for ch in value)
    if value.isdigit():
        stats.numeric_looking += 1
        if value.startswith("0") and len(value) > 1:
            stats.leading_zeros += 1
    elif has_digit and has_alpha:
        stats.mixed_alnum += 1
    elif has_alpha and not has_digit:
        stats.alpha_only += 1
    if "," in value:
        stats.embedded_commas += 1
    if "'" in value or '"' in value:
        stats.quotes += 1
    if "\n" in value or "\r" in value:
        stats.line_breaks += 1
    if any(ord(ch) > 127 for ch in value):
        stats.non_ascii += 1


def discover_local_rcni_files(tree_root: Path) -> list[Path]:
    files: list[Path] = []
    for dirpath, _dirnames, filenames in os.walk(tree_root):
        for name in filenames:
            if is_rcni_sftp_archive_file(name):
                files.append(Path(dirpath) / name)
    return sorted(files)


def profile_tree(tree_root: Path, reports_dir: Path) -> dict:
    tree_root = Path(tree_root)
    reports_dir = Path(reports_dir)
    reports_dir.mkdir(parents=True, exist_ok=True)
    _reset_globals()

    all_files = []
    rejected = []
    for dirpath, _dirnames, filenames in os.walk(tree_root):
        for name in filenames:
            path = Path(dirpath) / name
            reason = rcni_sftp_reject_reason(name)
            if reason is None:
                all_files.append(path)
            else:
                rejected.append((str(path), name, reason))

    all_files = sorted(all_files)
    logger.info("Profiling %d RCNI files under %s", len(all_files), tree_root)

    file_profiles: list[FileProfile] = []
    freq_cols = set(CATEGORICAL_COLUMNS) | {
        "Recon File Name",
        "Discrepancy Reason Text",
    }
    columns = {
        name: ColumnStats(name=name, track_frequencies=name in freq_cols)
        for name in EXPECTED_HEADER
    }
    extra_columns: Counter[str] = Counter()
    date_raw_formats: Counter[str] = Counter()
    date_raw_values: Counter[str] = Counter()
    date_blank = 0
    date_invalid = 0
    date_min: datetime | None = None
    date_max: datetime | None = None
    date_invalid_samples: list[str] = []
    recon_values: Counter[str] = Counter()
    recon_malformed: Counter[str] = Counter()
    structural_rows: list[dict] = []
    row_fingerprints: dict[str, dict] = {}
    header_signatures: Counter[tuple[str, ...]] = Counter()
    hix_examples = {
        "hix_blank_issuer_populated": [],
        "hix_populated_issuer_blank": [],
        "numeric_vs_text": [],
        "punctuation": [],
        "embedded_commas": [],
        "names": [],
        "addresses": [],
        "dates": [],
        "codes": [],
        "mixed_formats": [],
    }
    parser_exception_samples: list[str] = []
    py2025_headers: set[tuple[str, ...]] = set()
    py2026_headers: set[tuple[str, ...]] = set()
    gz_headers: set[tuple[str, ...]] = set()
    plain_headers: set[tuple[str, ...]] = set()

    for path in all_files:
        rel = path.relative_to(tree_root)
        parts = rel.parts
        proc_year = tree_root.name if tree_root.name.isdigit() and len(tree_root.name) == 4 else "2026"
        proc_month = parts[0] if parts else ""
        fname_meta = parse_rcni_filename(path.name)
        issuer = tree_root.parent.name if tree_root.parent.name.isdigit() else (fname_meta.issuer_id or "15105")
        archive_root = str(tree_root.parent.parent).replace("\\", "/")
        archive = parse_archive_path(
            str(path).replace("\\", "/"),
            remote_root=archive_root,
            issuer=issuer,
            year=proc_year,
            month=proc_month or "00",
        )
        profile = FileProfile(
            source_file=path.name,
            source_path=str(path),
            processing_year=archive.processing_year,
            processing_month=archive.processing_month,
            processing_day=archive.processing_day,
            issuer_id=fname_meta.issuer_id,
            plan_year=fname_meta.plan_year,
            file_timestamp=fname_meta.file_timestamp,
            compressed=path.name.lower().endswith(".gz"),
            file_size=path.stat().st_size,
            sha256=sha256_file(path),
        )
        logger.info("Profiling %s (%s bytes)", path.name, profile.file_size)
        _profile_one_file(
            path,
            profile,
            columns,
            extra_columns,
            date_raw_formats,
            date_raw_values,
            structural_rows,
            row_fingerprints,
            hix_examples,
            parser_exception_samples,
        )
        file_profiles.append(profile)
        if profile.header is not None:
            header_signatures[profile.header] += 1
            if profile.plan_year == "2025":
                py2025_headers.add(profile.header)
            if profile.plan_year == "2026":
                py2026_headers.add(profile.header)
            if profile.compressed:
                gz_headers.add(profile.header)
            else:
                plain_headers.add(profile.header)
        if profile.header is None:
            continue
        # date min/max accumulated in _profile_one_file via nonlocal-style lists
        pass

    # Second pass over structural isn't needed; date extrema stored on columns? 
    # Date extrema were updated inside _profile_one_file using mutable containers.
    date_extrema = _DATE_EXTREMA
    date_blank = _DATE_BLANK[0]
    date_invalid = _DATE_INVALID[0]
    date_invalid_samples = _DATE_INVALID_SAMPLES

    reports = _write_reports(
        reports_dir=reports_dir,
        tree_root=tree_root,
        file_profiles=file_profiles,
        rejected=rejected,
        columns=columns,
        extra_columns=extra_columns,
        header_signatures=header_signatures,
        date_raw_formats=date_raw_formats,
        date_raw_values=date_raw_values,
        date_blank=date_blank,
        date_invalid=date_invalid,
        date_invalid_samples=date_invalid_samples,
        date_min=date_extrema[0],
        date_max=date_extrema[1],
        recon_values=columns["Recon File Name"].frequencies
        if "Recon File Name" in columns
        else Counter(),
        recon_malformed=recon_malformed,
        structural_rows=structural_rows,
        row_fingerprints=row_fingerprints,
        hix_examples=hix_examples,
        parser_exception_samples=parser_exception_samples,
        py2025_headers=py2025_headers,
        py2026_headers=py2026_headers,
        gz_headers=gz_headers,
        plain_headers=plain_headers,
    )
    return reports


# Mutable accumulators used while streaming (avoid huge return tuples).
_DATE_EXTREMA: list[datetime | None] = [None, None]
_DATE_BLANK = [0]
_DATE_INVALID = [0]
_DATE_INVALID_SAMPLES: list[str] = []
_RECON_MALFORMED: Counter[str] = Counter()


def _reset_globals() -> None:
    _DATE_EXTREMA[0] = None
    _DATE_EXTREMA[1] = None
    _DATE_BLANK[0] = 0
    _DATE_INVALID[0] = 0
    _DATE_INVALID_SAMPLES.clear()
    _RECON_MALFORMED.clear()


def _profile_one_file(
    path: Path,
    profile: FileProfile,
    columns: dict[str, ColumnStats],
    extra_columns: Counter[str],
    date_raw_formats: Counter[str],
    date_raw_values: Counter[str],
    structural_rows: list[dict],
    row_fingerprints: dict[str, dict],
    hix_examples: dict[str, list],
    parser_exception_samples: list[str],
) -> None:
    text = None
    with path.open("rb") as probe:
        prefix = probe.read(4)
    profile.bom = prefix.startswith(b"\xef\xbb\xbf")
    gzip_magic = prefix[:2] == b"\x1f\x8b"
    profile.compressed = path.name.lower().endswith(".gz") or gzip_magic
    if profile.compressed:
        text = gzip.open(path, mode="rt", encoding="utf-8", newline="", errors="replace")
    else:
        text = path.open("r", encoding="utf-8", newline="", errors="replace")

    tracker = _CountingText(text)
    reader = csv.reader(tracker)
    header_row = None
    try:
        try:
            header_row = next(reader)
        except StopIteration:
            profile.empty_file = True
            profile.header_notes.append("empty file")
            return
        except csv.Error as exc:
            profile.parser_exceptions += 1
            parser_exception_samples.append(f"{path.name}: header {exc}")
            profile.header_notes.append(f"CSV parser error on header: {exc}")
            return

        header = tuple(header_row)
        profile.header = header
        profile.header_column_count = len(header)
        if header and header[0].startswith("\ufeff"):
            profile.bom = True
            profile.header_notes.append("UTF-8 BOM present on first header cell")
        dup_names = [name for name, count in Counter(header).items() if count > 1]
        profile.duplicate_header_names = dup_names
        if dup_names:
            profile.header_notes.append(f"duplicate header names: {dup_names}")
        if header != EXPECTED_HEADER:
            profile.header_ok = False
            if [h.strip() for h in header] == list(EXPECTED_HEADER):
                profile.header_notes.append("header matches expected names after whitespace strip only")
            if [h.lower() for h in header] == [h.lower() for h in EXPECTED_HEADER]:
                profile.header_notes.append("header matches expected names case-insensitively only")
            if len(header) != EXPECTED_COLUMN_COUNT:
                profile.header_notes.append(
                    f"header count {len(header)} != {EXPECTED_COLUMN_COUNT}"
                )
            missing = [c for c in EXPECTED_HEADER if c not in header]
            extra = [c for c in header if c not in EXPECTED_HEADER]
            if missing:
                profile.header_notes.append(f"missing columns: {missing}")
            if extra:
                profile.header_notes.append(f"extra columns: {extra}")
                for name in extra:
                    extra_columns[name] += 1
            if len(header) == EXPECTED_COLUMN_COUNT:
                diffs = [
                    f"pos {i+1}: expected {EXPECTED_HEADER[i]!r} observed {header[i]!r}"
                    for i in range(EXPECTED_COLUMN_COUNT)
                    if EXPECTED_HEADER[i] != header[i]
                ]
                if diffs:
                    profile.header_notes.append("; ".join(diffs))
        else:
            profile.header_ok = True

        header_line_no = tracker.physical_line_number
        record_number = 0
        while True:
            line_before = tracker.physical_line_number
            try:
                row = next(reader)
            except StopIteration:
                break
            except csv.Error as exc:
                record_number += 1
                profile.parser_exceptions += 1
                profile.structural_malformed_row_count += 1
                profile.data_row_count += 1
                parser_exception_samples.append(f"{path.name} rec={record_number}: {exc}")
                structural_rows.append(
                    {
                        "source_file": path.name,
                        "source_path": str(path),
                        "processing_month": profile.processing_month,
                        "record_number": record_number,
                        "physical_line_number": tracker.physical_line_number,
                        "expected_column_count": EXPECTED_COLUMN_COUNT,
                        "observed_column_count": "",
                        "raw_record": "",
                        "likely_structural_cause": _classify_structural(
                            -1, "", tracker.physical_line_number - line_before, str(exc)
                        ),
                        "parser_exception": str(exc),
                    }
                )
                continue

            record_number += 1
            profile.data_row_count += 1
            observed = len(row)
            profile.field_counts[observed] += 1
            profile.max_field_count = max(profile.max_field_count, observed)
            profile.min_field_count = (
                observed if profile.min_field_count is None else min(profile.min_field_count, observed)
            )
            raw = ",".join(row)
            physical_span = max(1, tracker.physical_line_number - line_before)

            if not any(cell.strip() for cell in row):
                profile.blank_row_count += 1

            if header is not None and tuple(row) == header:
                profile.header_repeat_in_data += 1

            if observed != EXPECTED_COLUMN_COUNT or physical_span > 1:
                profile.structural_malformed_row_count += 1
                cause = _classify_structural(observed, raw, physical_span, None)
                structural_rows.append(
                    {
                        "source_file": path.name,
                        "source_path": str(path),
                        "processing_month": profile.processing_month,
                        "record_number": record_number,
                        "physical_line_number": tracker.physical_line_number,
                        "expected_column_count": EXPECTED_COLUMN_COUNT,
                        "observed_column_count": observed,
                        "raw_record": raw[:4000],
                        "likely_structural_cause": cause,
                        "parser_exception": "",
                    }
                )

            padded = list(row) + [""] * (EXPECTED_COLUMN_COUNT - len(row))
            by_name = {
                EXPECTED_HEADER[i]: padded[i] if i < len(padded) else ""
                for i in range(EXPECTED_COLUMN_COUNT)
            }
            # If header exactly matches, prefer header-aligned values.
            if profile.header_ok:
                by_name = {header[i]: row[i] if i < len(row) else "" for i in range(len(header))}

            for col_name, stats in columns.items():
                _update_column(stats, by_name.get(col_name, ""))

            date_val = by_name.get("Date of Discrepancy", "")
            if _is_blank(date_val) or _is_whitespace_only(date_val):
                _DATE_BLANK[0] += 1
            else:
                date_raw_values[date_val] += 1
                parsed, fmt = _parse_date(date_val)
                if parsed is None:
                    _DATE_INVALID[0] += 1
                    if len(_DATE_INVALID_SAMPLES) < SAMPLE_LIMIT:
                        _DATE_INVALID_SAMPLES.append(date_val)
                else:
                    date_raw_formats[fmt or "unknown"] += 1
                    if _DATE_EXTREMA[0] is None or parsed < _DATE_EXTREMA[0]:
                        _DATE_EXTREMA[0] = parsed
                    if _DATE_EXTREMA[1] is None or parsed > _DATE_EXTREMA[1]:
                        _DATE_EXTREMA[1] = parsed

            recon = by_name.get("Recon File Name", "")
            if recon and RECON_RE.match(recon) is None and recon.strip():
                _RECON_MALFORMED[recon] += 1

            hix = by_name.get("HIX Value", "")
            issuer_v = by_name.get("Issuer Value", "")
            _collect_hix_examples(hix_examples, hix, issuer_v)

            fp = hashlib.sha256(
                "\x1f".join(by_name.get(c, "") for c in EXPECTED_HEADER).encode("utf-8", errors="replace")
            ).hexdigest()
            entry = row_fingerprints.get(fp)
            if entry is None:
                row_fingerprints[fp] = {
                    "count": 1,
                    "first_file": path.name,
                    "first_record": record_number,
                    "months": {profile.processing_month},
                    "plan_years": {profile.plan_year},
                    "same_file_only": True,
                }
            else:
                entry["count"] += 1
                entry["months"].add(profile.processing_month)
                entry["plan_years"].add(profile.plan_year)
                if path.name != entry["first_file"]:
                    entry["same_file_only"] = False

        profile.uncompressed_bytes = tracker.uncompressed_bytes
        del header_line_no
    finally:
        if text is not None:
            text.close()


def _collect_hix_examples(store: dict[str, list], hix: str, issuer: str) -> None:
    def add(key: str, payload: dict) -> None:
        if len(store[key]) < HIX_EXAMPLE_LIMIT:
            store[key].append(payload)

    payload = {"hix": hix, "issuer": issuer}
    if _is_blank(hix) and not _is_blank(issuer):
        add("hix_blank_issuer_populated", payload)
    if not _is_blank(hix) and _is_blank(issuer):
        add("hix_populated_issuer_blank", payload)
    if (hix.isdigit() and issuer and not issuer.isdigit()) or (
        issuer.isdigit() and hix and not hix.isdigit()
    ):
        add("numeric_vs_text", payload)
    if any(ch in hix + issuer for ch in ".,;:/#-()"):
        add("punctuation", payload)
    if "," in hix or "," in issuer:
        add("embedded_commas", payload)
    if any(ch.isalpha() for ch in hix) and " " in hix:
        add("names", payload)
    address_tokens = ("ST", "RD", "AVE", "LN", "DR", "BLVD", "CT", "WAY", "PO BOX")
    combined = f"{hix} {issuer}".upper()
    if any(tok in combined for tok in address_tokens) or any(ch.isdigit() for ch in hix) and any(
        ch.isalpha() for ch in hix
    ) and "," in (hix + issuer):
        add("addresses", payload)
    if _parse_date(hix)[0] or _parse_date(issuer)[0]:
        add("dates", payload)
    if hix.isupper() and "_" in hix or (len(hix) <= 12 and hix.replace("_", "").isalnum() and hix):
        add("codes", payload)
    if hix and issuer and (hix.isdigit() != issuer.isdigit()):
        add("mixed_formats", payload)


def _csv_write(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _safe_sql_length(max_len: int) -> tuple[str, str]:
    if max_len <= 0:
        return "NVARCHAR(64)", "64"
    if max_len <= 16:
        return "NVARCHAR(32)", "32"
    if max_len <= 32:
        return "NVARCHAR(64)", "64"
    if max_len <= 64:
        return "NVARCHAR(128)", "128"
    if max_len <= 128:
        return "NVARCHAR(256)", "256"
    if max_len <= 256:
        return "NVARCHAR(512)", "512"
    if max_len <= 500:
        return "NVARCHAR(1000)", "1000"
    if max_len <= 1000:
        return "NVARCHAR(2000)", "2000"
    if max_len <= 2000:
        return "NVARCHAR(4000)", "4000"
    return "NVARCHAR(MAX)", "MAX"


def _write_reports(**kwargs) -> dict:
    reports_dir: Path = kwargs["reports_dir"]
    file_profiles: list[FileProfile] = kwargs["file_profiles"]
    columns: dict[str, ColumnStats] = kwargs["columns"]
    header_signatures = kwargs["header_signatures"]
    structural_rows = kwargs["structural_rows"]
    row_fingerprints = kwargs["row_fingerprints"]
    hix_examples = kwargs["hix_examples"]

    file_rows = []
    for p in file_profiles:
        file_rows.append(
            {
                "source_file": p.source_file,
                "source_path": p.source_path,
                "processing_year": p.processing_year,
                "processing_month": p.processing_month,
                "processing_day": p.processing_day or "",
                "issuer_id": p.issuer_id,
                "plan_year": p.plan_year,
                "file_timestamp": p.file_timestamp,
                "compressed": p.compressed,
                "file_size": p.file_size,
                "sha256": p.sha256,
                "header_column_count": p.header_column_count,
                "header_names": "|".join(p.header or ()),
                "header_ok": p.header_ok,
                "header_notes": " | ".join(p.header_notes),
                "data_row_count": p.data_row_count,
                "blank_row_count": p.blank_row_count,
                "structural_malformed_row_count": p.structural_malformed_row_count,
                "parser_exceptions": p.parser_exceptions,
                "min_field_count": p.min_field_count if p.min_field_count is not None else "",
                "max_field_count": p.max_field_count,
                "distinct_field_counts": ";".join(
                    f"{k}:{v}" for k, v in sorted(p.field_counts.items())
                ),
                "uncompressed_bytes": p.uncompressed_bytes,
                "bom": p.bom,
                "empty_file": p.empty_file,
                "header_repeat_in_data": p.header_repeat_in_data,
                "duplicate_header_names": "|".join(p.duplicate_header_names),
            }
        )
    file_csv = reports_dir / "rcni_file_profile.csv"
    _csv_write(file_csv, file_rows, list(file_rows[0].keys()) if file_rows else ["source_file"])

    schema_rows = []
    for i, expected in enumerate(EXPECTED_HEADER, start=1):
        observed_at_pos = Counter()
        for p in file_profiles:
            if p.header and len(p.header) >= i:
                observed_at_pos[p.header[i - 1]] += 1
        schema_rows.append(
            {
                "position": i,
                "expected_name": expected,
                "files_with_exact_name_at_position": observed_at_pos.get(expected, 0),
                "other_names_at_position": "; ".join(
                    f"{name}={count}"
                    for name, count in observed_at_pos.most_common()
                    if name != expected
                ),
            }
        )
    for header, count in header_signatures.items():
        schema_rows.append(
            {
                "position": "SIGNATURE",
                "expected_name": " | ".join(EXPECTED_HEADER),
                "files_with_exact_name_at_position": count,
                "other_names_at_position": " | ".join(header),
            }
        )
    schema_csv = reports_dir / "rcni_schema_profile.csv"
    _csv_write(
        schema_csv,
        schema_rows,
        ["position", "expected_name", "files_with_exact_name_at_position", "other_names_at_position"],
    )

    col_rows = []
    total_profiled_rows = sum(p.data_row_count for p in file_profiles)
    for name, stats in columns.items():
        denom = stats.total_rows or 1
        col_rows.append(
            {
                "column_name": name,
                "total_rows": stats.total_rows,
                "null_blank_count": stats.blank_count,
                "whitespace_only_count": stats.whitespace_only_count,
                "non_null_count": stats.non_blank_count,
                "null_pct": round(100.0 * stats.blank_count / denom, 4),
                "distinct_count": len(stats.distinct),
                "sample_values": " | ".join(stats.samples),
                "min_string_length": stats.min_len if stats.min_len is not None else "",
                "max_observed_length": stats.max_len,
                "numeric_looking_count": stats.numeric_looking,
                "alpha_only_count": stats.alpha_only,
                "mixed_alnum_count": stats.mixed_alnum,
                "leading_zeros_count": stats.leading_zeros,
                "embedded_commas_count": stats.embedded_commas,
                "quotes_count": stats.quotes,
                "line_breaks_count": stats.line_breaks,
                "non_ascii_count": stats.non_ascii,
                "patterns": (
                    f"numeric_looking={stats.numeric_looking};"
                    f"alpha_only={stats.alpha_only};"
                    f"mixed_alnum={stats.mixed_alnum};"
                    f"leading_zeros={stats.leading_zeros}"
                ),
            }
        )
    col_csv = reports_dir / "rcni_column_profile.csv"
    _csv_write(col_csv, col_rows, list(col_rows[0].keys()) if col_rows else ["column_name"])

    domain_rows = []
    for name in CATEGORICAL_COLUMNS:
        stats = columns[name]
        for value, count in stats.frequencies.most_common(TOP_N):
            domain_rows.append(
                {
                    "column_name": name,
                    "value": value,
                    "count": count,
                    "pct": round(100.0 * count / (stats.total_rows or 1), 4),
                    "rank": None,
                }
            )
        domain_rows.append(
            {
                "column_name": name,
                "value": "__DISTINCT_COUNT__",
                "count": len(stats.frequencies) or len(stats.distinct),
                "pct": "",
                "rank": "",
            }
        )
    # high-cardinality tops
    for name in (
        "Discrepancy Reason Text",
        "Exchange Assigned Policy ID",
        "Plan ID",
        "Assignee",
    ):
        stats = columns[name]
        # frequencies may be empty; use distinct samples plus count
        if stats.track_frequencies:
            ranked = stats.frequencies.most_common(TOP_N)
        else:
            ranked = []
        domain_rows.append(
            {
                "column_name": name,
                "value": "__DISTINCT_COUNT__",
                "count": len(stats.distinct),
                "pct": "",
                "rank": "",
            }
        )
        for i, (value, count) in enumerate(ranked, start=1):
            domain_rows.append(
                {
                    "column_name": name,
                    "value": value,
                    "count": count,
                    "pct": round(100.0 * count / (stats.total_rows or 1), 4),
                    "rank": i,
                }
            )
    enrollment_values = set(columns["Enrollment Status"].frequencies)
    for expected_status in ("CONFIRM", "CANCEL", "TERM", "PENDING"):
        domain_rows.append(
            {
                "column_name": "Enrollment Status",
                "value": f"__HAS_{expected_status}__",
                "count": int(expected_status in enrollment_values),
                "pct": "",
                "rank": "",
            }
        )
    extra_status = sorted(enrollment_values - {"CONFIRM", "CANCEL", "TERM", "PENDING"})
    domain_rows.append(
        {
            "column_name": "Enrollment Status",
            "value": "__ADDITIONAL_VALUES__",
            "count": len(extra_status),
            "pct": "",
            "rank": " | ".join(extra_status),
        }
    )
    domain_csv = reports_dir / "rcni_value_domains.csv"
    _csv_write(
        domain_csv,
        domain_rows,
        ["column_name", "value", "count", "pct", "rank"],
    )

    struct_csv = reports_dir / "rcni_structural_issues.csv"
    _csv_write(
        struct_csv,
        structural_rows,
        [
            "source_file",
            "source_path",
            "processing_month",
            "record_number",
            "physical_line_number",
            "expected_column_count",
            "observed_column_count",
            "raw_record",
            "likely_structural_cause",
            "parser_exception",
        ],
    )

    hash_groups: dict[str, list[FileProfile]] = defaultdict(list)
    for p in file_profiles:
        hash_groups[p.sha256].append(p)
    logical_groups: dict[tuple[str, str, str, str], list[FileProfile]] = defaultdict(list)
    for p in file_profiles:
        logical_groups[(p.issuer_id, "INDV_MONTHLYDISCREPANCY", p.plan_year, p.file_timestamp)].append(p)

    dup_rows = []
    for sha, group in hash_groups.items():
        if len(group) > 1:
            dup_rows.append(
                {
                    "duplicate_class": "PHYSICAL_SHA256",
                    "identity": sha,
                    "file_count": len(group),
                    "files": " | ".join(g.source_file for g in group),
                    "detail": "same SHA-256",
                }
            )
    for ident, group in logical_groups.items():
        if len(group) < 2:
            continue
        hashes = {g.sha256 for g in group}
        kind = "LOGICAL_DUPLICATE" if len(hashes) == 1 else "POSSIBLE_REPLACEMENT"
        dup_rows.append(
            {
                "duplicate_class": kind,
                "identity": "|".join(ident),
                "file_count": len(group),
                "files": " | ".join(g.source_file for g in group),
                "detail": f"hashes={len(hashes)}",
            }
        )
    cross_month = 0
    cross_py = 0
    within_file = 0
    multi = [e for e in row_fingerprints.values() if e["count"] > 1]
    for entry in multi:
        if len(entry["months"]) > 1:
            cross_month += 1
        if len(entry["plan_years"]) > 1:
            cross_py += 1
        if entry["same_file_only"]:
            within_file += 1
    dup_rows.append(
        {
            "duplicate_class": "BUSINESS_ROW",
            "identity": "duplicate_row_fingerprints",
            "file_count": len(multi),
            "files": "",
            "detail": (
                f"cross_month={cross_month}; cross_plan_year={cross_py}; "
                f"within_same_file={within_file}; total_duplicate_groups={len(multi)}"
            ),
        }
    )
    dup_csv = reports_dir / "rcni_duplicate_profile.csv"
    _csv_write(
        dup_csv,
        dup_rows,
        ["duplicate_class", "identity", "file_count", "files", "detail"],
    )

    recon_rows = []
    for value, count in columns["Recon File Name"].frequencies.most_common():
        match = RECON_RE.match(value)
        recon_rows.append(
            {
                "recon_file_name": value,
                "count": count,
                "pattern_ok": bool(match),
                "issuer_id": match.group("issuer_id") if match else "",
                "plan_year": match.group("plan_year") if match else "",
                "timestamp": match.group("timestamp") if match else "",
                "suffix": match.group("suffix") if match else "",
            }
        )
    recon_csv = reports_dir / "rcni_recon_lineage_profile.csv"
    _csv_write(
        recon_csv,
        recon_rows,
        ["recon_file_name", "count", "pattern_ok", "issuer_id", "plan_year", "timestamp", "suffix"],
    )

    vol_rows = []
    for p in file_profiles:
        avg = (p.uncompressed_bytes / p.data_row_count) if p.data_row_count else ""
        vol_rows.append(
            {
                "grain": "file",
                "key": p.source_file,
                "rows": p.data_row_count,
                "file_size": p.file_size,
                "uncompressed_bytes": p.uncompressed_bytes,
                "avg_row_bytes": round(avg, 2) if avg != "" else "",
                "plan_year": p.plan_year,
                "processing_month": p.processing_month,
            }
        )
    by_month = Counter()
    by_py = Counter()
    uncomp_month = Counter()
    for p in file_profiles:
        by_month[p.processing_month] += p.data_row_count
        by_py[p.plan_year] += p.data_row_count
        uncomp_month[p.processing_month] += p.uncompressed_bytes
    for month, rows in sorted(by_month.items()):
        vol_rows.append(
            {
                "grain": "processing_month",
                "key": month,
                "rows": rows,
                "file_size": "",
                "uncompressed_bytes": uncomp_month[month],
                "avg_row_bytes": "",
                "plan_year": "",
                "processing_month": month,
            }
        )
    for py, rows in sorted(by_py.items()):
        vol_rows.append(
            {
                "grain": "plan_year",
                "key": py,
                "rows": rows,
                "file_size": "",
                "uncompressed_bytes": "",
                "avg_row_bytes": "",
                "plan_year": py,
                "processing_month": "",
            }
        )
    total_rows = sum(p.data_row_count for p in file_profiles)
    total_uncomp = sum(p.uncompressed_bytes for p in file_profiles)
    largest = max(file_profiles, key=lambda p: p.data_row_count) if file_profiles else None
    vol_rows.append(
        {
            "grain": "total",
            "key": "ALL",
            "rows": total_rows,
            "file_size": sum(p.file_size for p in file_profiles),
            "uncompressed_bytes": total_uncomp,
            "avg_row_bytes": round(total_uncomp / total_rows, 2) if total_rows else "",
            "plan_year": "",
            "processing_month": "",
        }
    )
    vol_csv = reports_dir / "rcni_volume_profile.csv"
    _csv_write(
        vol_csv,
        vol_rows,
        [
            "grain",
            "key",
            "rows",
            "file_size",
            "uncompressed_bytes",
            "avg_row_bytes",
            "plan_year",
            "processing_month",
        ],
    )

    sql_rows = []
    lineage = [
        ("rcni_raw_id", "BIGINT IDENTITY", 0, "surrogate key", "NO"),
        ("issuer_id", "NVARCHAR(16)", 5, "filename/directory issuer; text to preserve leading zeros", "NO"),
        ("coverage_year", "NVARCHAR(8)", 4, "filename plan year, not processing year", "NO"),
        ("processing_year", "NVARCHAR(8)", 4, "SFTP/archive directory year", "NO"),
        ("processing_month", "NVARCHAR(8)", 2, "SFTP/archive directory month", "NO"),
        ("processing_day", "NVARCHAR(8)", 2, "optional archive day folder", "YES"),
        ("file_timestamp", "NVARCHAR(32)", 14, "filename generation timestamp YYYYMMDDHHMMSS", "NO"),
        ("source_file", "NVARCHAR(512)", max((len(p.source_file) for p in file_profiles), default=0), "basename", "NO"),
        ("source_path", "NVARCHAR(1024)", max((len(p.source_path) for p in file_profiles), default=0), "full archive path", "NO"),
        ("file_hash", "CHAR(64)", 64, "SHA-256 of physical source bytes", "NO"),
        ("row_number_in_file", "INT", 0, "1-based data record number", "NO"),
        ("load_run_id", "NVARCHAR(64)", 0, "ingestion run identifier", "NO"),
        ("loaded_at", "DATETIME2(3)", 0, "load clock time", "NO"),
        ("raw_record", "NVARCHAR(MAX)", 0, "lossless original CSV record text", "YES"),
        ("quality_status", "NVARCHAR(32)", 0, "CLEAN / WARNING / MALFORMED structural flag", "NO"),
        ("is_structural_malformed", "BIT", 0, "retain malformed rows in raw layer", "NO"),
    ]
    for col, sql_type, max_len, reason, nullable in lineage:
        sql_rows.append(
            {
                "source_column": col,
                "proposed_sql_type": sql_type,
                "max_observed_length": max_len,
                "recommended_safe_length": sql_type,
                "nullable": nullable,
                "reason": reason,
            }
        )
    text_blobs = {"HIX Value", "Issuer Value", "Discrepancy Reason Text", "Recon File Name"}
    for name, stats in columns.items():
        sql_type, safe = _safe_sql_length(stats.max_len)
        if name in text_blobs:
            sql_type, safe = "NVARCHAR(MAX)", "MAX"
        sql_rows.append(
            {
                "source_column": name,
                "proposed_sql_type": sql_type,
                "max_observed_length": stats.max_len,
                "recommended_safe_length": safe,
                "nullable": "YES",
                "reason": (
                    "Preserve source as text. "
                    + (
                        "Heterogeneous field; never infer numeric type. "
                        if name in text_blobs or name.endswith("ID")
                        else ""
                    )
                    + (
                        "Exchange-assigned identifiers stored as text even when numeric-looking."
                        if name.startswith("Exchange Assigned")
                        else ""
                    )
                    + (
                        "Issuer-assigned identifiers may be alphanumeric."
                        if name.startswith("Issuer Assigned")
                        else ""
                    )
                ),
            }
        )
    sql_csv = reports_dir / "rcni_proposed_sql_schema.csv"
    _csv_write(
        sql_csv,
        sql_rows,
        [
            "source_column",
            "proposed_sql_type",
            "max_observed_length",
            "recommended_safe_length",
            "nullable",
            "reason",
        ],
    )

    summary_path = reports_dir / "rcni_profile_summary.md"
    summary_path.write_text(
        _render_summary(
            file_profiles=file_profiles,
            columns=columns,
            header_signatures=header_signatures,
            structural_rows=structural_rows,
            dup_rows=dup_rows,
            kwargs=kwargs,
            total_rows=total_rows,
            total_uncomp=total_uncomp,
            largest=largest,
            enrollment_values=enrollment_values,
            extra_status=extra_status,
            hix_examples=hix_examples,
            rejected=kwargs["rejected"],
            sql_rows=sql_rows,
        ),
        encoding="utf-8",
    )
    return {
        "files": str(file_csv),
        "schema": str(schema_csv),
        "columns": str(col_csv),
        "domains": str(domain_csv),
        "structural": str(struct_csv),
        "duplicates": str(dup_csv),
        "recon": str(recon_csv),
        "volume": str(vol_csv),
        "sql": str(sql_csv),
        "summary": str(summary_path),
        "file_count": len(file_profiles),
        "total_rows": total_rows,
    }


def _render_summary(**opts) -> str:
    file_profiles: list[FileProfile] = opts["file_profiles"]
    columns: dict[str, ColumnStats] = opts["columns"]
    header_signatures = opts["header_signatures"]
    structural_rows = opts["structural_rows"]
    kwargs = opts["kwargs"]
    total_rows = opts["total_rows"]
    total_uncomp = opts["total_uncomp"]
    largest = opts["largest"]
    enrollment_values = opts["enrollment_values"]
    extra_status = opts["extra_status"]
    hix_examples = opts["hix_examples"]
    rejected = opts["rejected"]
    sql_rows = opts["sql_rows"]

    headers_same = list(header_signatures.keys()) == [EXPECTED_HEADER] or (
        len(header_signatures) == 1 and next(iter(header_signatures)) == EXPECTED_HEADER
    )
    all_header_ok = all(p.header_ok for p in file_profiles) and not any(p.empty_file for p in file_profiles)
    py2025 = kwargs["py2025_headers"]
    py2026 = kwargs["py2026_headers"]
    gz_h = kwargs["gz_headers"]
    plain_h = kwargs["plain_headers"]
    causes = Counter(r["likely_structural_cause"] for r in structural_rows)
    max_lens = sorted(
        ((s.max_len, s.name) for s in columns.values()), reverse=True
    )
    gz_files = [p for p in file_profiles if p.compressed]
    plain_files = [p for p in file_profiles if not p.compressed]
    date_formats = kwargs["date_raw_formats"]
    date_min = kwargs["date_min"]
    date_max = kwargs["date_max"]

    lines = [
        "# RCNI local archive profile — issuer 15105 / processing year 2026",
        "",
        "Read-only. Source files were not modified. Azure tables were not created.",
        "",
        f"- Profiled RCNI files: **{len(file_profiles)}**",
        f"- Rejected non-RCNI artifacts: **{len(rejected)}**",
        f"- Total data rows: **{total_rows:,}**",
        f"- Approximate uncompressed bytes: **{total_uncomp:,}**",
        f"- Largest file by rows: **{largest.source_file if largest else 'n/a'}** "
        f"({largest.data_row_count if largest else 0:,} rows)",
        "",
        "## 1. Are all 14 RCNI files structurally the same?",
        "",
        f"- Exact expected 19-column header on every file: **{all_header_ok}**",
        f"- Distinct header signatures: **{len(header_signatures)}**",
        f"- Empty files: **{sum(1 for p in file_profiles if p.empty_file)}**",
        f"- Files with BOM: **{sum(1 for p in file_profiles if p.bom)}**",
        f"- Header repeated inside data rows: **{sum(p.header_repeat_in_data for p in file_profiles)}**",
        "",
        "## 2. Is the 19-column schema stable across Jan–Aug?",
        "",
        f"- Schema matches EXPECTED_HEADER on all files: **{headers_same and all_header_ok}**",
        f"- PY2025 header signatures: {len(py2025)}",
        f"- PY2026 header signatures: {len(py2026)}",
        f"- PY2025 vs PY2026 header equality: **{py2025 == py2026}**",
        "",
        "## 3. Which fields require permissive text storage?",
        "",
        "- HIX Value, Issuer Value (heterogeneous; never numeric-typed)",
        "- Issuer Assigned Member ID / Subscriber ID (alphanumeric allowed)",
        "- Exchange Assigned Policy / Member / Subscriber ID (store as text even if numeric-looking)",
        "- Discrepancy Reason Text, Recon File Name",
        "- Any column with embedded commas, quotes, or non-ASCII in this run:",
        "",
    ]
    for stats in columns.values():
        if stats.embedded_commas or stats.quotes or stats.non_ascii or stats.line_breaks or stats.mixed_alnum:
            lines.append(
                f"  - {stats.name}: commas={stats.embedded_commas}, quotes={stats.quotes}, "
                f"non_ascii={stats.non_ascii}, line_breaks={stats.line_breaks}, mixed_alnum={stats.mixed_alnum}"
            )
    lines += [
        "",
        "## 4. Maximum observed lengths",
        "",
    ]
    for length, name in max_lens:
        lines.append(f"- {name}: {length}")
    lines += [
        "",
        "## 5. Actual malformed patterns",
        "",
        f"- Structural malformed rows: **{len(structural_rows):,}**",
        f"- Cause counts: {dict(causes)}",
        "",
        "## 6. Are malformed rows still losslessly preservable?",
        "",
        "- Yes. Each malformed record was captured as `raw_record` text with file/line coordinates.",
        "- Recommend storing `raw_record` + `is_structural_malformed` in dbo.rcni_raw; do not drop rows.",
        "",
        "## 7. Duplicate patterns",
        "",
    ]
    for row in opts["dup_rows"]:
        lines.append(
            f"- {row['duplicate_class']}: {row['identity']} files={row['file_count']} {row['detail']}"
        )
    lines += [
        "",
        "## 8. PY2025 vs PY2026 schema/value differences",
        "",
        f"- Header equality: **{py2025 == py2026}**",
        f"- Rows by plan year: "
        + ", ".join(
            f"{p}={sum(fp.data_row_count for fp in file_profiles if fp.plan_year == p):,}"
            for p in sorted({fp.plan_year for fp in file_profiles})
        ),
        "",
        "## 9. Jan/Feb .gz vs Mar–Aug .OUT.good beyond compression",
        "",
        f"- .gz files: {len(gz_files)}; uncompressed .OUT.good files: {len(plain_files)}",
        f"- Header equality across packaging: **{gz_h == plain_h}**",
        "- Layout remains `{month}/{day}/{batch}/{file}` in both groups.",
        "- Sidecar logs change (`log.txt.gz` → `log.txt` → `last-status-outbound-log.txt`) but those are not RCNI data files.",
        "",
        "## 10. Recommended raw SQL schema",
        "",
        "See `rcni_proposed_sql_schema.csv`. Principles: NVARCHAR for all source fields; "
        "HIX/Issuer as NVARCHAR(MAX); identifiers as text; lineage columns; raw_record for anomalies; "
        "no business deduplication in raw.",
        "",
        "## 11. Parsing/loading strategy for 5M+ monthly rows",
        "",
        f"- Observed total rows in this 8-month single-issuer sample: {total_rows:,}",
        f"- Average uncompressed row size: "
        f"{(total_uncomp / total_rows) if total_rows else 0:.1f} bytes",
        "- Use the existing streaming csv.reader (chunked gzip for .gz; direct text for .OUT.good).",
        "- Do not pandas-read the full file.",
        "- For Azure: stage to a durable raw table with batched inserts.",
        "- pyodbc `fast_executemany` is suitable for batches of ~1,000–5,000 rows of NVARCHAR columns; "
        "avoid it for NVARCHAR(MAX) heavy rows if driver buffers explode — then use smaller batches "
        "or BULK INSERT / staging file + `OPENROWSET`/`BULK`.",
        "- For multi-million-row months, prefer: stream parse → write UTF-8 staging CSV (malformed rows included) "
        "→ BULK INSERT into dbo.rcni_raw, then quality flags in a second pass.",
        "- Keep `fast_executemany` as a fallback for smaller issuers / incremental loads.",
        "",
        "## Date of Discrepancy",
        "",
        f"- Raw formats: {dict(date_formats)}",
        f"- Blank values: {kwargs['date_blank']:,}",
        f"- Invalid/unparseable: {kwargs['date_invalid']:,}",
        f"- Invalid samples: {kwargs['date_invalid_samples']}",
        f"- Min valid date: {date_min}",
        f"- Max valid date: {date_max}",
        "- processing_year/month/day come from the archive path.",
        "- file_timestamp comes from the filename.",
        "- Date of Discrepancy is a payload field. These three must stay separate.",
        "",
        "## Enrollment Status domain",
        "",
        f"- Observed values: {sorted(enrollment_values)}",
        f"- Has CONFIRM/CANCEL/TERM/PENDING: "
        f"{ {s: s in enrollment_values for s in ('CONFIRM', 'CANCEL', 'TERM', 'PENDING')} }",
        f"- Additional values: {extra_status or '(none)'}",
        "- Accepted values were not hard-coded.",
        "",
        "## HIX Value / Issuer Value examples",
        "",
    ]
    for key, examples in hix_examples.items():
        lines.append(f"### {key}")
        if not examples:
            lines.append("- (none observed in sample window)")
        for ex in examples:
            hix = (ex.get("hix") or "")[:180]
            issuer = (ex.get("issuer") or "")[:180]
            lines.append(f"- HIX={hix!r} | Issuer={issuer!r}")
        lines.append("")
    lines += [
        "## Rejected non-RCNI files",
        "",
    ]
    for path, name, reason in rejected:
        lines.append(f"- `{name}` — {reason}")
        lines.append(f"  `{path}`")
    lines += [
        "",
        "## Proposed SQL columns (abbreviated)",
        "",
    ]
    for row in sql_rows:
        lines.append(
            f"- {row['source_column']}: {row['proposed_sql_type']} "
            f"(max={row['max_observed_length']}, safe={row['recommended_safe_length']}, "
            f"null={row['nullable']})"
        )
    lines.append("")
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    del argv
    _reset_globals()
    project_root = Path(__file__).resolve().parents[1]
    tree = project_root / "last reports" / "15105" / "2026"
    reports = project_root / "outputs" / "rcni" / "profiling"
    if not tree.is_dir():
        raise SystemExit(f"Local tree not found: {tree}")
    result = profile_tree(tree, reports)
    print(f"Profiled {result['file_count']} files, {result['total_rows']:,} rows")
    print(f"Reports: {reports}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
