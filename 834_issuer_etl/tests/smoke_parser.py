#!/usr/bin/env python3
"""Technical smoke test for parser optional fields (not business validation)."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "src"))

from parsers.parser_834 import Parser834  # noqa: E402
from validation.parser_field_report import build_parser_field_report  # noqa: E402
from transform.identifier_comparison import build_identifier_comparison  # noqa: E402

FIXTURE = ROOT / "tests" / "fixtures" / "smoke_sample.xml"


def main() -> int:
    parser = Parser834()
    records = parser.parse_file(
        FIXTURE.read_bytes(),
        issuer="99999",
        year="2026",
        month="01",
        file_name="smoke_sample.xml",
        file_path=str(FIXTURE),
    )
    assert len(records) == 2, f"expected 2 records, got {len(records)}"

    r0 = records[0]
    assert r0["enrollment_action_code"] == "2"
    assert r0["enrollee_event_type_code"] == "ADD"
    assert r0["enrollee_event_reason_code"] == "EC"
    assert r0["action_code"] == "ADD"
    assert r0["exchg_assigned_enrollee_id"] == "E100"
    assert r0["request_submit_timestamp"] == "20260201120000"
    assert r0["qtyt"] == "2"
    assert r0["issuer_subscriber_identifier"] == "ISS-S100"

    r1 = records[1]
    assert r1["enrollment_action_code"] == "2"
    assert r1["enrollee_event_type_code"] == "CHANGE"
    assert r1["enrollee_event_reason_code"] is None

    import pandas as pd

    df = pd.DataFrame(records)
    report = build_parser_field_report(df, "99999")
    assert report["distinct_policy_ids"] == 1
    assert report["distinct_member_ids"] == 2
    assert report["distinct_enrollee_ids"] == 2
    assert report["enrollee_id_available"]

    from validation.json_sanitize import dumps_json

    dumps_json(report)  # numpy.bool_ values must serialize

    comparison = build_identifier_comparison(df, "99999")
    assert not comparison["summary"].empty

    print("smoke test passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
