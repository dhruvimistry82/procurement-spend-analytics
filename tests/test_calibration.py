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


# --- dollar scale (heavy tail) ------------------------------------------------

def heavy_tail_sample():
    """Fake 'real' sample where a few awards are enormous, as in real federal data."""
    import random
    rng = random.Random(5)
    tx, ct = fake_real_sample()
    for t in tx:
        if t["mod"] == "0" and isinstance(t["transaction_amount"], float) and rng.random() < 0.004:
            t["transaction_amount"] = round(t["transaction_amount"] * 5000, 2)
    return tx, ct


def test_sample_quantiles_stays_within_range_and_hits_tail():
    import random
    from ingestion.calibrate import quantiles
    from ingestion.synthetic import sample_quantiles
    vals = [float(i) for i in range(1, 10001)]
    q = quantiles(vals)
    rng = random.Random(0)
    draws = [sample_quantiles(rng, q) for _ in range(20000)]
    assert min(draws) >= 1 and max(draws) <= 10000 * 1.5
    assert sum(d > 9900 for d in draws) / len(draws) > 0.003  # top tail reproduced
    assert abs(statistics.mean(draws) / statistics.mean(vals) - 1) < 0.03  # average preserved
    # a lone giant keeps its full weight
    vals2 = [1.0] * 123 + [1_000_000.0]
    q2 = quantiles(vals2, points=101)  # uneven bucket sizes
    weighted = sum(a * n for a, n in zip(q2["avg"], q2["n"])) / sum(q2["n"])
    assert abs(weighted - statistics.mean(vals2)) < 1e-6


def test_synthetic_dollars_match_real_daily_scale():
    from ingestion.synthetic import run_generation
    tx, ct = heavy_tail_sample()
    cal = profile(tx, ct)
    gen = Generator(seed=11, calibration=cal, scale=1.0)
    stats = run_generation(gen, "2026-07-06", "2026-08-28", lambda data, rel: rel, "R", today="2026-11-10")
    weekdays = sum(1 for d in daterange("2026-07-06", "2026-08-28") if date.fromisoformat(d).weekday() < 5)
    synthetic_daily = sum(stats["net_obligations_by_month"].values()) / weekdays
    ratio = synthetic_daily / cal["weekday_net_obligations_per_day"]
    assert 0.6 < ratio < 1.6, ratio  # heavy tail makes single runs noisy; mean over seeds is ~1.0


def test_run_generation_expectations_match_brute_force():
    from ingestion.synthetic import run_generation, tx_valid, contract_valid
    tx, ct = fake_real_sample()
    cal = profile(tx, ct)
    written = {}
    stats = run_generation(Generator(seed=4, calibration=cal, scale=0.2), "2026-07-06", "2026-07-20",
                           lambda data, rel: written.setdefault(rel, data), "R", today="2026-11-10")
    rows = {"contracts": [], "transactions": []}
    for rel, data in written.items():
        rows[rel.split("/")[0]] += [json.loads(l) for l in data.decode().splitlines()]
    T, C = rows["transactions"], rows["contracts"]
    assert stats["bronze_transactions"] == len(T) and stats["bronze_contracts"] == len(C)
    assert stats["quarantine_transactions"] == sum(not tx_valid(t, "2026-11-10") for t in T)
    assert stats["silver_transactions"] == len({(t["generated_internal_id"], t["mod"]) for t in T if tx_valid(t, "2026-11-10")})
    assert stats["silver_contracts"] == len({c["generated_internal_id"] for c in C if contract_valid(c)})
