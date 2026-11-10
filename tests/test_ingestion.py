import json
from pathlib import Path

import pytest
import requests

from ingestion.ingest import build_records, load_config, to_jsonl
from ingestion.usaspending_client import USAspendingClient, flatten_award

CONFIG = Path(__file__).resolve().parents[1] / "config" / "sources.yml"


class FakeResponse:
    def __init__(self, status, body):
        self.status_code = status
        self._body = body

    def json(self):
        return self._body

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"status {self.status_code}", response=self)


class FakeSession:
    """Returns queued responses and records the payloads it was sent."""

    def __init__(self, responses):
        self.responses = list(responses)
        self.payloads = []

    def post(self, url, json=None, timeout=None):
        self.payloads.append(json)
        return self.responses.pop(0)


def page(results, has_next):
    return FakeResponse(200, {"results": results, "page_metadata": {"hasNext": has_next}})


def client(responses):
    return USAspendingClient(session=FakeSession(responses), backoff_seconds=0)


def test_flatten_handles_nested_and_plain_codes():
    nested = flatten_award({"Award ID": "A1", "NAICS": {"code": "541511", "description": "Custom Programming"}})
    plain = flatten_award({"Award ID": "A2", "PSC": "D302"})
    assert nested == {"award_id": "A1", "naics_code": "541511", "naics_description": "Custom Programming"}
    assert plain == {"award_id": "A2", "psc_code": "D302", "psc_description": None}


def test_pagination_follows_has_next_and_increments_page():
    c = client([page([{"Award ID": "1"}], True), page([{"Award ID": "2"}], False)])
    out = list(c.iter_awards(endpoint="/x", award_type_codes=["A"], fields=["Award ID"],
                             start_date="2026-01-01", end_date="2026-01-02"))
    assert [r["Award ID"] for r in out] == ["1", "2"]
    assert [p["page"] for p in c.session.payloads] == [1, 2]
    assert "date_type" not in c.session.payloads[0]["filters"]["time_period"][0]
    assert c.session.payloads[0]["sort"] == "Award Amount"


def test_max_pages_caps_requests():
    c = client([page([{"Award ID": "1"}], True), page([{"Award ID": "2"}], True)])
    out = list(c.iter_awards(endpoint="/x", award_type_codes=["A"], fields=[], start_date="a",
                             end_date="b", max_pages=1))
    assert len(out) == 1 and len(c.session.payloads) == 1


def test_retries_on_server_error_then_succeeds():
    c = client([FakeResponse(503, {}), FakeResponse(429, {}), page([{"Award ID": "1"}], False)])
    out = list(c.iter_awards(endpoint="/x", award_type_codes=["A"], fields=[], start_date="a", end_date="b"))
    assert len(out) == 1 and len(c.session.payloads) == 3


def test_bad_request_is_not_retried():
    c = client([FakeResponse(400, {"detail": "bad filter"})])
    with pytest.raises(requests.HTTPError):
        list(c.iter_awards(endpoint="/x", award_type_codes=["A"], fields=[], start_date="a", end_date="b"))
    assert len(c.session.payloads) == 1


def test_build_records_adds_lineage_columns_and_writes_jsonl():
    source = load_config(CONFIG)["sources"][0]
    c = client([page([{"Award ID": "1", "Award Amount": 1000.5}], False)])
    recs = build_records(c, source, "2026-10-01", "2026-10-07", "RUN1")
    assert recs[0]["_source"] == "contracts" and recs[0]["_run_id"] == "RUN1"
    lines = to_jsonl(recs).decode().splitlines()
    assert json.loads(lines[0])["award_amount"] == 1000.5


def test_date_type_and_sort_are_sent_when_configured():
    c = client([page([], False)])
    list(c.iter_awards(endpoint="/x", award_type_codes=["A"], fields=[], start_date="a", end_date="b",
                       date_type="last_modified_date", sort="Transaction Amount"))
    sent = c.session.payloads[0]
    assert sent["filters"]["time_period"][0]["date_type"] == "last_modified_date"
    assert sent["sort"] == "Transaction Amount"


def test_transactions_source_uses_its_own_endpoint_sort_and_flattens_fields():
    source = next(s for s in load_config(CONFIG)["sources"] if s["name"] == "transactions")
    c = client([page([{"Award ID": "X1", "Mod": "P00003", "Action Date": "2026-10-02",
                       "Transaction Amount": -2500.0, "generated_internal_id": "CONT_AWD_X1",
                       "NAICS": {"code": "541511", "description": "Custom Programming"}}], False)])
    recs = build_records(c, source, "2026-10-01", "2026-10-07", "RUN1")
    sent = c.session.payloads[0]
    assert sent["sort"] == "Transaction Amount" and "date_type" not in sent["filters"]["time_period"][0]
    r = recs[0]
    assert (r["mod"], r["action_date"], r["transaction_amount"]) == ("P00003", "2026-10-02", -2500.0)
    assert r["naics_code"] == "541511" and r["_source"] == "transactions"
