"""The synthetic generator must look like real data and its planted anomalies must be
detectable with the same rules the pipeline uses."""
import json
from collections import defaultdict
from datetime import date

from ingestion.ingest import daterange
from ingestion.synthetic import Generator, main

# Keys the real API produced (checked against live landing files).
REAL_TX_KEYS = {"award_id", "mod", "recipient_name", "recipient_uei", "action_date", "transaction_amount",
                "transaction_description", "awarding_agency", "awarding_sub_agency", "award_type",
                "naics_code", "naics_description", "psc_code", "psc_description", "generated_internal_id"}
REAL_CONTRACT_KEYS = {"award_id", "generated_internal_id", "recipient_name", "recipient_uei", "awarding_agency",
                      "awarding_sub_agency", "funding_agency", "award_amount", "total_outlays",
                      "contract_award_type", "naics_code", "naics_description", "psc_code", "psc_description",
                      "description", "start_date", "end_date", "last_modified_date",
                      "place_of_performance_state_code"}


def generate(start="2026-07-01", end="2026-09-30", **kw):
    g = Generator(**kw)
    contracts, txs = [], []
    for d in daterange(start, end):
        c, t = g.day(date.fromisoformat(d))
        contracts += c
        txs += t
    return g, contracts, txs


def tx_valid(t):  # mirrors TRANSACTION_RULES in pipelines/silver.py
    return (t["generated_internal_id"] and t["mod"] and t["awarding_agency"]
            and t["transaction_amount"] is not None and t["action_date"] and t["action_date"] <= "2026-11-10")


def contract_valid(c):  # mirrors AWARD_RULES
    return (c["generated_internal_id"] and c["awarding_agency"] and c["award_amount"] is not None
            and c["start_date"] <= c["end_date"] and c["last_modified_date"])


def test_schema_matches_real_api_output():
    _, contracts, txs = generate(end="2026-07-07")
    assert all(set(t) == REAL_TX_KEYS for t in txs)
    assert all(set(c) == REAL_CONTRACT_KEYS for c in contracts)


def test_same_seed_same_data():
    _, c1, t1 = generate(end="2026-07-10", seed=7)
    _, c2, t2 = generate(end="2026-07-10", seed=7)
    assert t1 == t2 and c1 == c2


def test_planted_split_purchases_are_found_by_gold_rule():
    g, _, txs = generate()
    threshold = 10_000
    unique = {(t["generated_internal_id"], t["mod"]): t for t in txs if tx_valid(t)}  # Silver dedup
    groups = defaultdict(list)
    for t in unique.values():  # same logic as gold_split_purchase_flags
        if t["mod"] == "0" and threshold * 0.9 <= t["transaction_amount"] < threshold:
            groups[(t["awarding_agency"], t["recipient_uei"], t["action_date"])].append(t["award_id"])
    flagged = {k for k, v in groups.items() if len(v) >= 2}
    planted = {(c["agency"], c["vendor_uei"], c["date"]) for c in g.truth["split_cases"]}
    assert planted and planted <= flagged


def test_invalid_rows_match_truth_counts():
    g, contracts, txs = generate()
    assert sum(not contract_valid(c) for c in contracts) == g.truth["invalid_contracts"] > 0
    # duplicates copy rows as-is, so a duplicated broken row counts twice in the raw data
    distinct_bad = {json.dumps(t, sort_keys=True) for t in txs if not tx_valid(t)}
    assert len(distinct_bad) == g.truth["invalid_transactions"] > 0


def test_has_deobligations_and_fiscal_year_end_surge():
    g, _, txs = generate()
    assert g.truth["deobligations"] > 0 and any((t["transaction_amount"] or 0) < 0 for t in txs)
    by_month = defaultdict(int)
    for t in txs:
        by_month[t["action_date"][:7]] += 1
    assert by_month["2026-09"] > 1.8 * by_month["2026-07"]


def test_cli_writes_files_and_truth(tmp_path, monkeypatch):
    import ingestion.synthetic as syn
    config = str(syn.REPO_ROOT / "config" / "sources.yml")
    monkeypatch.setattr(syn, "REPO_ROOT", tmp_path)
    assert main(["--start", "2026-07-01", "--end", "2026-07-03", "--config", config]) == 0
    files = sorted(p.relative_to(tmp_path).as_posix() for p in tmp_path.rglob("*.jsonl"))
    assert any(f.startswith("data/landing_synthetic/transactions/2026-07-01_") for f in files)
    row = json.loads((tmp_path / files[0]).read_text().splitlines()[0])
    assert row["_synthetic"] is True
    assert "split_cases" in json.loads((tmp_path / "data/synthetic_truth/truth.json").read_text())
