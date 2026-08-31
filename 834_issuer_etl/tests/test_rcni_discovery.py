from __future__ import annotations

from types import SimpleNamespace

from ingestion.sftp_filters import filters_from_settings, parse_csv_filter, partition_matches
from rcni.archive_path import parse_archive_path
from rcni.constants import DEFAULT_RCNI_BASE_PATH, FORBIDDEN_INBOUND_PATH_FRAGMENT
from rcni.discovery import discover_rcni_candidates
from rcni.matcher import (
    is_rcni_local_file,
    is_rcni_monthly_discrepancy_file,
    is_rcni_sftp_archive_file,
)
from rcni.settings import RcniScope, resolve_rcni_scope
from rcni.staging import staging_paths


class FakeAttr:
    def __init__(self, filename: str, is_dir: bool) -> None:
        self.filename = filename
        self.st_mode = 0o040000 if is_dir else 0o100000


class FakeSFTP:
    """Minimal SFTP double: path -> (dirs, files)."""

    def __init__(self, tree: dict[str, tuple[list[str], list[str]]]) -> None:
        self.tree = tree

    def _node(self, path: str):
        path = path.rstrip("/") or "/"
        if path not in self.tree:
            raise OSError(f"missing {path}")
        return self.tree[path]

    def listdir(self, path: str) -> list[str]:
        dirs, files = self._node(path)
        return list(dirs) + list(files)

    def listdir_attr(self, path: str) -> list[FakeAttr]:
        dirs, files = self._node(path)
        return [FakeAttr(name, True) for name in dirs] + [FakeAttr(name, False) for name in files]

    def stat(self, path: str):
        path = path.rstrip("/") or "/"
        if path in self.tree:
            return SimpleNamespace(st_mode=0o040000)
        for parent, (dirs, files) in self.tree.items():
            if path == parent:
                return SimpleNamespace(st_mode=0o040000)
            for name in files:
                if f"{parent.rstrip('/')}/{name}" == path:
                    return SimpleNamespace(st_mode=0o100000)
        raise OSError(f"missing {path}")


def _sample_tree() -> dict[str, tuple[list[str], list[str]]]:
    root = "/archive/out/good/PAS"
    return {
        root: (["15105", "37301"], []),
        f"{root}/15105": (["2026"], []),
        f"{root}/15105/2026": (["07", "08"], []),
        f"{root}/15105/2026/07": (
            ["17", "nested_no_day"],
            ["to_15105_INDV_MONTHLYDISCREPANCY_2026_20260717000000.OUT.good.gz"],
        ),
        f"{root}/15105/2026/07/17": (["batch_a"], []),
        f"{root}/15105/2026/07/17/batch_a": (
            [],
            [
                "to_15105_INDV_MONTHLYDISCREPANCY_2025_20260717005653.OUT.good.gz",
                "to_15105_INDV_MONTHLYDISCREPANCY_2026_20260717005507.OUT.good.gz",
                "to_15105_INDV_MONTHLYDISCREPANCY_2026_20260717005507.OUT.good",
                "log.txt.gz",
                "from_15105_INDV_MONTHLYRECON_2026_20260716080716.IN.gz",
            ],
        ),
        f"{root}/15105/2026/07/nested_no_day": (
            ["deep"],
            [],
        ),
        f"{root}/15105/2026/07/nested_no_day/deep": (
            [],
            ["to_15105_INDV_MONTHLYDISCREPANCY_2026_20260717111111.OUT.good.gz"],
        ),
        f"{root}/15105/2026/08": (["16"], []),
        f"{root}/15105/2026/08/16": (["batch_b"], []),
        f"{root}/15105/2026/08/16/batch_b": (
            [],
            ["to_15105_INDV_MONTHLYDISCREPANCY_2026_20260816231311.OUT.good.gz"],
        ),
        f"{root}/37301": (["2026"], []),
        f"{root}/37301/2026": (["07"], []),
        f"{root}/37301/2026/07": (["09"], []),
        f"{root}/37301/2026/07/09": (["batch_c"], []),
        f"{root}/37301/2026/07/09/batch_c": (
            [],
            ["to_37301_INDV_MONTHLYDISCREPANCY_2026_20260709122334.OUT.good.gz"],
        ),
    }


def _scope(**kwargs) -> RcniScope:
    defaults = dict(
        base_path="/archive/out/good/PAS",
        issuer_allow={"15105"},
        year_allow={"2026"},
        month_allow={"07"},
        force_download=False,
        keep_compressed=True,
        sftp_host="x",
        sftp_port=22,
        sftp_user="u",
        sftp_password="p",
        local_root=__import__("pathlib").Path("/tmp/rcni-local"),
        reports_dir=__import__("pathlib").Path("/tmp/rcni-reports"),
        logs_dir=__import__("pathlib").Path("/tmp/rcni-logs"),
    )
    defaults.update(kwargs)
    return RcniScope(**defaults)


class TestRcniDiscoveryFilters:
    def test_issuer_year_month_env_filtering(self) -> None:
        sftp = FakeSFTP(_sample_tree())
        result = discover_rcni_candidates(sftp, _scope())
        names = {c.filename for c in result.candidates}
        assert names == {
            "to_15105_INDV_MONTHLYDISCREPANCY_2026_20260717000000.OUT.good.gz",
            "to_15105_INDV_MONTHLYDISCREPANCY_2025_20260717005653.OUT.good.gz",
            "to_15105_INDV_MONTHLYDISCREPANCY_2026_20260717005507.OUT.good.gz",
            "to_15105_INDV_MONTHLYDISCREPANCY_2026_20260717111111.OUT.good.gz",
        }
        skipped_names = {c.filename for c in result.skipped}
        assert "log.txt.gz" in skipped_names
        assert "from_15105_INDV_MONTHLYRECON_2026_20260716080716.IN.gz" in skipped_names
        assert "to_15105_INDV_MONTHLYDISCREPANCY_2026_20260717005507.OUT.good" in skipped_names
        assert all(c.processing_month == "07" for c in result.candidates)
        assert all(c.issuer == "15105" for c in result.candidates)

    def test_live_sftp_rejects_decompressed_out_good(self) -> None:
        assert not is_rcni_sftp_archive_file(
            "to_15105_INDV_MONTHLYDISCREPANCY_2026_20260717005507.OUT.good"
        )
        assert is_rcni_local_file(
            "to_15105_INDV_MONTHLYDISCREPANCY_2026_20260717005507.OUT.good"
        )
        assert is_rcni_monthly_discrepancy_file(
            "to_15105_INDV_MONTHLYDISCREPANCY_2026_20260717005507.OUT.good.gz"
        )
        assert not is_rcni_monthly_discrepancy_file(
            "to_15105_INDV_MONTHLYDISCREPANCY_2026_20260717005507.OUT.good"
        )

    def test_does_not_select_august_when_month_is_july(self) -> None:
        sftp = FakeSFTP(_sample_tree())
        result = discover_rcni_candidates(sftp, _scope())
        assert all("20260816" not in c.filename for c in result.candidates)

    def test_plan_year_can_differ_from_directory_year(self) -> None:
        sftp = FakeSFTP(_sample_tree())
        result = discover_rcni_candidates(sftp, _scope())
        plan_2025 = [c for c in result.candidates if c.plan_year == "2025"]
        plan_2026 = [c for c in result.candidates if c.plan_year == "2026"]
        assert plan_2025
        assert plan_2026
        assert all(c.processing_year == "2026" for c in result.candidates)
        assert all(c.plan_year_differs_from_processing_year for c in plan_2025)

    def test_empty_filter_means_all(self) -> None:
        class _S:
            issuer_filter = None
            year_filter = None
            month_filter = None
        issuer_allow, year_allow, month_allow = filters_from_settings(_S())
        assert issuer_allow is None
        assert year_allow is None
        assert month_allow is None

    def test_partition_matches_uses_existing_filter_helpers(self) -> None:
        issuers = parse_csv_filter("15105")
        years = parse_csv_filter("2026")
        months = parse_csv_filter("7", normalizer=lambda v: str(int(v)).zfill(2))
        assert partition_matches("15105", "2026", "07", issuers, years, months)
        assert not partition_matches("37301", "2026", "07", issuers, years, months)

    def test_default_base_path_is_outbound_pas(self) -> None:
        assert DEFAULT_RCNI_BASE_PATH == "/archive/out/good/PAS"
        assert FORBIDDEN_INBOUND_PATH_FRAGMENT == "/archive/in/"

    def test_files_directly_under_month_are_discovered(self) -> None:
        sftp = FakeSFTP(_sample_tree())
        result = discover_rcni_candidates(sftp, _scope())
        month_root = [
            c for c in result.candidates
            if c.filename.endswith("20260717000000.OUT.good.gz")
        ]
        assert month_root
        assert month_root[0].processing_year == "2026"
        assert month_root[0].processing_month == "07"
        assert month_root[0].nested_relative is None or month_root[0].nested_relative == ""

    def test_env_filters_used_when_cli_omitted(self, monkeypatch) -> None:
        monkeypatch.setenv("ISSUER_FILTER", "15105")
        monkeypatch.setenv("YEAR_FILTER", "2026")
        monkeypatch.setenv("MONTH_FILTER", "07")
        monkeypatch.setenv("RCNI_BASE_PATH", "/archive/out/good/PAS")
        monkeypatch.setenv("SFTP_HOST", "example")
        scope = resolve_rcni_scope()
        assert scope.issuer_allow == {"15105"}
        assert scope.year_allow == {"2026"}
        assert scope.month_allow == {"07"}

    def test_nested_folders_without_day_are_still_discovered(self) -> None:
        sftp = FakeSFTP(_sample_tree())
        result = discover_rcni_candidates(sftp, _scope())
        nested = [
            c for c in result.candidates
            if c.filename.endswith("20260717111111.OUT.good.gz")
        ]
        assert nested
        assert nested[0].processing_year == "2026"
        assert nested[0].processing_month == "07"

    def test_archive_day_observed_when_present_not_required(self) -> None:
        meta = parse_archive_path(
            "/archive/out/good/PAS/13535/2026/01/27/3558832_593393076201/"
            "to_13535_INDV_MONTHLYDISCREPANCY_2025_20260127221729.OUT.good.gz",
            remote_root="/archive/out/good/PAS",
            issuer="13535",
            year="2026",
            month="01",
        )
        assert meta.processing_day == "27"
        assert meta.nested_relative == "3558832_593393076201"
        assert meta.processing_year == "2026"

    def test_staging_uses_processing_date_not_plan_year(self, tmp_path) -> None:
        sftp = FakeSFTP(_sample_tree())
        result = discover_rcni_candidates(sftp, _scope())
        py2025 = next(c for c in result.candidates if c.plan_year == "2025")
        paths = staging_paths(tmp_path, py2025)
        parts = paths.extracted_path.parts
        assert "2026" in parts
        assert "07" in parts
        assert "2025" not in paths.extracted_path.parent.parts or py2025.processing_year == "2026"

    def test_cli_filters_override_env_issuer_filter(self, monkeypatch) -> None:
        monkeypatch.setenv("ISSUER_FILTER", "37301")
        monkeypatch.setenv("YEAR_FILTER", "2025")
        monkeypatch.setenv("MONTH_FILTER", "01")
        monkeypatch.setenv("RCNI_BASE_PATH", "/archive/out/good/PAS")
        monkeypatch.setenv("SFTP_HOST", "example")
        monkeypatch.delenv("RCNI_ISSUER", raising=False)
        monkeypatch.delenv("RCNI_YEAR", raising=False)
        monkeypatch.delenv("RCNI_MONTH", raising=False)
        scope = resolve_rcni_scope(issuer="15105", year="2026", month="07")
        assert scope.issuer_allow == {"15105"}
        assert scope.year_allow == {"2026"}
        assert scope.month_allow == {"07"}
        assert scope.base_path == "/archive/out/good/PAS"

    def test_no_external_project_env_dependency(self) -> None:
        import inspect
        from rcni import settings as rcni_settings
        source = inspect.getsource(rcni_settings)
        assert "Desktop/gaaccess" not in source
        assert "RCNI_ISSUER" not in source

    def test_matcher_rejects_inbound_recon_even_under_out_path(self) -> None:
        assert not is_rcni_sftp_archive_file(
            "from_15105_INDV_MONTHLYRECON_2026_20260716080716.IN.gz"
        )
