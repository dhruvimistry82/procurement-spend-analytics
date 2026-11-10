import json
from pathlib import Path

import pytest
import requests

from ingestion.ingest import LocalTarget, bisect, dedupe, fetch_day, ingest, is_day_file, load_config
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
    """Returns queued responses (or calls a handler) and records the payloads it was sent."""

    def __init__(self, responses=None, handler=None):
        self.responses = list(responses or [])
        self.handler = handler
        self.payloads = []

    def post(self, url, json=None, timeout=None):
        self.payloads.append(json)
        return self.handler(json) if self.handler else self.responses.pop(0)


def page(results, has_next):
    return FakeResponse(200, {"results": results, "page_metadata": {"hasNext": has_next}})


def client(responses=None, handler=None):
    return USAspendingClient(session=FakeSession(responses, handler), backoff_seconds=0)


def fetch(c, **kw):
    args = dict(endpoint="/x", award_type_codes=["A"], fields=[], start_date="a", end_date="b")
    args.update(kw)
    return c.fetch(**args)


SOURCE = {"name": "contracts", "endpoint": "/x", "award_type_codes": ["A"], "fields": [],
          "page_size": 1, "dedup_keys": ["generated_internal_id"]}


# --- client -----------------------------------------------------------------

def test_flatten_handles_nested_and_plain_codes():
    nested = flatten_award({"Award ID": "A1", "NAICS": {"code": "541511", "description": "Custom Programming"}})
    plain = flatten_award({"Award ID": "A2", "PSC": "D302"})
    assert nested == {"award_id": "A1", "naics_code": "541511", "naics_description": "Custom Programming"}
    assert plain == {"award_id": "A2", "psc_code": "D302", "psc_description": None}


def test_pagination_follows_has_next():
    c = client([page([{"id": 1}], True), page([{"id": 2}], False)])
    res = fetch(c)
    assert [r["id"] for r in res.records] == [1, 2] and not res.capped
    assert [p["page"] for p in c.session.payloads] == [1, 2]
    sent = c.session.payloads[0]
    assert "date_type" not in sent["filters"]["time_period"][0] and sent["sort"] == "Award Amount"


def test_optional_filters_are_sent():
    c = client([page([], False)])
    fetch(c, date_type="last_modified_date", sort="Transaction Amount", amount_range=(0, 1000),
          extra_filters={"agencies": [{"name": "NASA"}]})
    f = c.session.payloads[0]["filters"]
    assert f["time_period"][0]["date_type"] == "last_modified_date"
    assert f["award_amounts"] == [{"lower_bound": 0, "upper_bound": 1000}]
    assert f["agencies"] == [{"name": "NASA"}]
    assert c.session.payloads[0]["sort"] == "Transaction Amount"


def test_open_ended_amount_range_omits_missing_bound():
    c = client([page([], False)])
    fetch(c, amount_range=(None, 0))
    assert c.session.payloads[0]["filters"]["award_amounts"] == [{"upper_bound": 0}]


def test_detects_result_cap():
    # max_results=2 with page_size=1 -> cap after page 2 while API says more exist
    c = client([page([{"id": 1}], True), page([{"id": 2}], True)])
    res = fetch(c, page_size=1, max_results=2)
    assert res.capped and len(res.records) == 2 and len(c.session.payloads) == 2


def test_retries_on_server_error_then_succeeds():
    c = client([FakeResponse(503, {}), FakeResponse(429, {}), page([{"id": 1}], False)])
    assert len(fetch(c).records) == 1 and len(c.session.payloads) == 3


def test_bad_request_is_not_retried():
    c = client([FakeResponse(400, {"detail": "bad filter"})])
    with pytest.raises(requests.HTTPError):
        fetch(c)
    assert len(c.session.payloads) == 1


# --- splitting and dedup ----------------------------------------------------

def test_bisect_splits_finite_bands_only():
    assert bisect((0, 100)) == [(0, 50.0), (50.0, 100)]
    assert bisect((None, 0)) is None and bisect((5, 5.5)) is None


def test_dedupe_by_keys_keeps_first():
    rows = [{"generated_internal_id": "a", "Mod": "0", "v": 1},
            {"generated_internal_id": "a", "Mod": "0", "v": 2},
            {"generated_internal_id": "a", "Mod": "P01", "v": 3}]
    out = dedupe(rows, ["generated_internal_id", "Mod"])
    assert [r["v"] for r in out] == [1, 3]


def make_api(rows, cap):
    """Fake API over `rows` (dicts with 'amt'), honoring award_amounts, page size 1, cap on results."""
    def handler(payload):
        f = payload["filters"]
        sel = rows
        if "award_amounts" in f:
            b = f["award_amounts"][0]
            sel = [r for r in rows if b.get("lower_bound", -1e18) <= r["amt"] <= b.get("upper_bound", 1e18)]
        i = payload["page"] - 1
        return page(sel[i:i + 1], i + 1 < len(sel))
    return handler


def test_fetch_day_splits_capped_day_by_amount_and_recovers_every_row(monkeypatch):
    import ingestion.ingest as ing
    monkeypatch.setattr(ing, "DEFAULT_BANDS", [None, 0, 100, None])
    rows = [{"generated_internal_id": f"r{i}", "amt": a}
            for i, a in enumerate([-5, 10, 20, 30, 40, 50, 60, 70, 500])]
    c = client(handler=make_api(rows, cap=3))
    # cap = 3 results per query (page_size 1 x 3 pages)
    orig = c.fetch
    c.fetch = lambda **kw: orig(**{**kw, "max_results": 3})
    raw, incomplete = fetch_day(c, SOURCE, "2026-10-01")
    got = {r["generated_internal_id"] for r in dedupe(raw, ["generated_internal_id"])}
    assert got == {r["generated_internal_id"] for r in rows} and incomplete == 0


class MemTarget:
    def __init__(self, existing=()):
        self.files = {}
        self.existing = set(existing)

    def exists(self, source, day):
        return (source, day) in self.existing

    def write(self, data, rel):
        self.files[rel] = data
        return rel


def day_of(payload):
    return payload["filters"]["time_period"][0]["start_date"]


def test_ingest_writes_one_file_per_source_per_day_with_lineage():
    cfg = {"sources": [dict(SOURCE, page_size=100)]}
    c = client(handler=lambda p: page([{"generated_internal_id": day_of(p)}], False))
    t = MemTarget()
    summary = ingest(c, cfg, "2026-10-01", "2026-10-02", t, "20261110T000000Z")
    assert sorted(t.files) == ["contracts/2026-10-01_20261110T000000Z.jsonl",
                               "contracts/2026-10-02_20261110T000000Z.jsonl"]
    rec = json.loads(t.files["contracts/2026-10-01_20261110T000000Z.jsonl"].decode().splitlines()[0])
    assert rec["_window_start"] == "2026-10-01" and rec["_possibly_incomplete"] is False
    assert summary["rows"] == {"contracts": 2} and summary["failed"] == []


def test_failed_day_is_recorded_and_other_days_continue():
    cfg = {"sources": [dict(SOURCE, page_size=100)]}

    def handler(p):
        if day_of(p) == "2026-10-02":
            return FakeResponse(503, {})
        return page([{"generated_internal_id": day_of(p)}], False)
    c = USAspendingClient(session=FakeSession(handler=handler), backoff_seconds=0, max_retries=2)
    t = MemTarget()
    summary = ingest(c, cfg, "2026-10-01", "2026-10-03", t, "R")
    assert summary["failed"] == ["contracts 2026-10-02"]
    assert len(t.files) == 2  # days 1 and 3 still landed


def test_resume_skips_days_already_landed():
    cfg = {"sources": [dict(SOURCE, page_size=100)]}
    c = client(handler=lambda p: page([{"generated_internal_id": day_of(p)}], False))
    t = MemTarget(existing={("contracts", "2026-10-01")})
    summary = ingest(c, cfg, "2026-10-01", "2026-10-02", t, "R")
    assert summary["skipped"] == ["contracts 2026-10-01"]
    assert [day_of(p) for p in c.session.payloads] == ["2026-10-02"]


def test_local_target_only_counts_this_scripts_day_files(tmp_path):
    (tmp_path / "contracts").mkdir()
    (tmp_path / "contracts" / "2026-10-01_2026-10-07_20261110T190933Z.jsonl").write_text("{}")  # old format
    t = LocalTarget(tmp_path)
    assert not t.exists("contracts", "2026-10-01")
    (tmp_path / "contracts" / "2026-10-01_20261110T221758Z.jsonl").write_text("{}")
    assert t.exists("contracts", "2026-10-01")
    assert is_day_file("2026-10-01_20261110T221758Z.jsonl", "2026-10-01")
    assert not is_day_file("2026-10-01_20261110T221758Z.jsonl", "2026-10-02")


def test_backoff_is_capped():
    c = USAspendingClient(session=FakeSession([FakeResponse(503, {})] * 3 + [page([], False)]),
                          backoff_seconds=0, max_backoff_seconds=0)
    assert fetch(c).records == []


def test_config_sources_are_valid():
    cfg = load_config(CONFIG)
    names = {s["name"] for s in cfg["sources"]}
    assert names == {"contracts", "transactions"}
    tx = next(s for s in cfg["sources"] if s["name"] == "transactions")
    assert tx["dedup_keys"] == ["generated_internal_id", "Mod"] and tx["sort"] == "Transaction Amount"
    assert all(s["filters"]["agencies"] for s in cfg["sources"])
