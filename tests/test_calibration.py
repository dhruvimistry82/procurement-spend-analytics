"""Calibrated synthetic data should mimic the real sample's shape, keep every name
fictional, and stay tagged so it never blends silently with real data."""
import json
import statistics
from collections import Counter, defaultdict
from datetime import date

import pytest

from ingestion.calibrate import profile, profile_dir
from ingestion.ingest import daterange
from ingestion.synthetic import Generator, lineage, main


def fake_real_sample():
    """Stand-in for real files: realistic shape, with 'real-looking' agency and vendor names."""
    g = Generator(seed=1, rows_per_day=400)
    tx, ct = [], []
    for d in daterange("2026-07-01", "2026-07-03"):
        c, t = g.day(date.fromisoformat(d))
        tx += [dict(r, awarding_agency="Real Agency X", recipient_name="REAL VENDOR CO") for r in t]
        ct += c
    return tx, ct


def test_profile_keeps_only_aggregates():
    tx, ct = fake_real_sample()
    p = profile(tx, ct)
    assert p["transactions"] == len(tx) and p["weekday_transactions_per_day"] > 300
    assert 0 < p["modification_share"] < 1 and p["categories"]
    assert abs(sum(c["weight"] for c in p["categories"]) - 1) < 0.01
    text = json.dumps(p)
    assert "REAL VENDOR CO" not in text and "Real Agency X" not in text and "award_id" not in text


def test_profile_ignores_synthetic_rows(tmp_path):
    tx, ct = fake_real_sample()
    for src, rows in (("transactions", tx), ("contracts", ct)):
        (tmp_path / src).mkdir()
        fake = lineage(rows[:50], src, "2026-07-01", "R")  # tagged _synthetic
        (tmp_path / src / "a.jsonl").write_text("\n".join(json.dumps(r) for r in rows + fake))
    assert profile_dir(str(tmp_path))["transactions"] == len(tx)


def test_profile_rejects_empty_input():
    with pytest.raises(ValueError):
        profile([], [])


def calibrated(days=("2026-07-06", "2026-09-30"), scale=0.25):
    tx, ct = fake_real_sample()
    cal = profile(tx, ct)
    g = Generator(seed=3, calibration=cal, scale=scale)
    out_tx = []
    for d in daterange(*days):
        _, t = g.day(date.fromisoformat(d))
        out_tx += t
    return cal, g, out_tx


def test_calibrated_data_follows_profile_and_uses_fictional_names():
    cal, g, tx = calibrated()
    names = {t["recipient_name"] for t in tx} | {t["awarding_agency"] for t in tx}
    assert "REAL VENDOR CO" not in names and "Real Agency X" not in names
    real_codes = {c["psc_code"] for c in cal["categories"]}
    assert {t["psc_code"] for t in tx} <= real_codes
    july_weekdays = defaultdict(int)
    for t in tx:
        if t["action_date"] < "2026-08-01" and date.fromisoformat(t["action_date"]).weekday() < 5:
            july_weekdays[t["action_date"]] += 1
    mean = statistics.mean(july_weekdays.values())
    assert 0.6 * cal["weekday_transactions_per_day"] * 0.25 < mean < 1.6 * cal["weekday_transactions_per_day"] * 0.25
    mod_share = sum(t["mod"] != "0" for t in tx if t["mod"]) / len(tx)
    assert abs(mod_share - cal["modification_share"]) < 0.15


def test_planted_splits_still_detectable_in_calibrated_mode():
    _, g, tx = calibrated()
    uniq = {(t["generated_internal_id"], t["mod"]): t for t in tx
            if t["awarding_agency"] and t["transaction_amount"] is not None and t["action_date"] <= "2026-11-10"}
    groups = Counter()
    for t in uniq.values():
        if t["mod"] == "0" and 9000 <= t["transaction_amount"] < 10000:
            groups[(t["awarding_agency"], t["recipient_uei"], t["action_date"])] += 1
    flagged = {k for k, n in groups.items() if n >= 2}
    planted = {(c["agency"], c["vendor_uei"], c["date"]) for c in g.truth["split_cases"]}
    assert planted and planted <= flagged


def test_cli_into_real_landing_tags_rows(tmp_path, monkeypatch):
    import ingestion.synthetic as syn
    tx, ct = fake_real_sample()
    cal_file = tmp_path / "cal.json"
    cal_file.write_text(json.dumps(profile(tx, ct)))
    config = str(syn.REPO_ROOT / "config" / "sources.yml")
    monkeypatch.setattr(syn, "REPO_ROOT", tmp_path)
    assert main(["--start", "2026-07-04", "--end", "2026-07-06", "--config", config,
                 "--calibration", str(cal_file), "--into-real-landing"]) == 0
    files = list((tmp_path / "data" / "landing" / "transactions").glob("2026-07-06_*.jsonl"))
    assert files
    rows = [json.loads(l) for l in files[0].read_text().splitlines()]
    assert rows and all(r["_synthetic"] is True for r in rows)
