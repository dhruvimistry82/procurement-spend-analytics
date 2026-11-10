"""Learn a statistical profile from real landing files, for calibrating synthetic data.

Only aggregate statistics are kept (category mix, typical amounts, daily volume,
modification and de-obligation rates). No individual records, vendors or award IDs.

In a Databricks notebook (files in a Volume are readable as normal paths):
    from ingestion.calibrate import profile_dir
    import json; print(json.dumps(profile_dir("/Volumes/procurement/raw/landing"), indent=1))

Locally:
    python -m ingestion.calibrate --input data/landing --out config/calibration.json
"""
from __future__ import annotations

import argparse
import glob
import json
import math
import os
import statistics
from collections import Counter, defaultdict
from datetime import date
from typing import Any, Iterable

TOP_CATEGORIES = 200
MIN_ROWS_FOR_OWN_QUANTILES = 30
# Amount distributions are stored as equal-count buckets: sort the real amounts, split them
# into K groups of equal size and keep each group's average. Sampling a bucket at random
# reproduces the real average exactly, heavy tail included (federal spending is dominated
# by a few huge awards). Only bucket averages are kept, never linked to any award or vendor.
GLOBAL_POINTS, CATEGORY_POINTS, MOD_POINTS = 1001, 101, 201


def _read_jsonl(paths: Iterable[str]) -> list[dict[str, Any]]:
    rows = []
    for p in paths:
        with open(p) as f:
            rows.extend(json.loads(line) for line in f if line.strip())
    return [r for r in rows if not r.get("_synthetic")]  # learn from real rows only


def _lognormal(values: list[float]) -> tuple[float, float]:
    """Median and log-space standard deviation of positive values."""
    logs = [math.log(v) for v in values if v and v > 0]
    if not logs:
        return 1.0, 1.0
    sigma = statistics.pstdev(logs) if len(logs) > 1 else 1.0
    return round(math.exp(statistics.median(logs)), 2), round(max(sigma, 0.2), 3)


def quantiles(values: list[float], points: int = GLOBAL_POINTS) -> dict[str, list] | None:
    """Equal-count buckets of the positive values: {"avg": [...], "n": [...]}, ascending.

    Sampling a bucket with probability n/total and returning its average reproduces the
    real mean exactly, including lone outliers.
    """
    v = sorted(x for x in values if x and x > 0)
    if not v:
        return None
    k = min(points, len(v))
    avg, n = [], []
    for b in range(k):
        chunk = v[b * len(v) // k:(b + 1) * len(v) // k]
        avg.append(round(sum(chunk) / len(chunk), 2))
        n.append(len(chunk))
    return {"avg": avg, "n": n}


def profile(transactions: list[dict[str, Any]], contracts: list[dict[str, Any]]) -> dict[str, Any]:
    if not transactions:
        raise ValueError("no real transactions found to calibrate from")

    # Count volume by the day each file covers; fall back to action_date, ignoring
    # impossible future dates that would otherwise show up as near-empty "days".
    today = date.today().isoformat()
    per_day = Counter(t.get("_window_start") or t["action_date"] for t in transactions
                      if t.get("_window_start") or (t.get("action_date") and t["action_date"] <= today))
    weekday_counts = [n for d, n in per_day.items() if date.fromisoformat(d).weekday() < 5]

    amounts = [t for t in transactions if isinstance(t.get("transaction_amount"), (int, float))]
    is_base = [str(t.get("mod")).strip() in ("0", "00", "000", "0000") for t in amounts]
    base = [t for t, b in zip(amounts, is_base) if b]
    mods = [t for t, b in zip(amounts, is_base) if not b]
    deobs = [t for t in mods if t["transaction_amount"] < 0]

    groups: dict[tuple, list[float]] = defaultdict(list)
    for t in base:
        if t["transaction_amount"] > 0:
            key = (t.get("naics_code"), t.get("naics_description"), t.get("psc_code"), t.get("psc_description"))
            groups[key].append(t["transaction_amount"])
    top = sorted(groups.items(), key=lambda kv: len(kv[1]), reverse=True)[:TOP_CATEGORIES]
    total = sum(len(v) for _, v in top) or 1
    categories = []
    for (naics, naics_d, psc, psc_d), vals in top:
        median, sigma = _lognormal(vals)
        cat = {"naics_code": naics, "naics_description": naics_d, "psc_code": psc,
               "psc_description": psc_d, "weight": round(len(vals) / total, 5),
               "median": median, "sigma": sigma}
        if len(vals) >= MIN_ROWS_FOR_OWN_QUANTILES:
            cat["quantiles"] = quantiles(vals, CATEGORY_POINTS)
        categories.append(cat)

    deob_median, deob_sigma = _lognormal([-t["transaction_amount"] for t in deobs])
    award_types = Counter(t.get("award_type") for t in transactions if t.get("award_type"))
    vendors_per_day = statistics.mean(
        len({t.get("recipient_uei") for t in transactions
             if (t.get("_window_start") or t.get("action_date")) == d}) for d in per_day)

    daily_net = defaultdict(float)
    for t in amounts:
        d = t.get("_window_start") or t.get("action_date")
        if d in per_day:
            daily_net[d] += t["transaction_amount"]
    weekday_net = [v for d, v in daily_net.items() if date.fromisoformat(d).weekday() < 5] or list(daily_net.values())

    return {
        "source": "USAspending.gov real sample",
        "base_amount_quantiles": quantiles([t["transaction_amount"] for t in base]),
        "modification_amount_quantiles": quantiles([t["transaction_amount"] for t in mods if t["transaction_amount"] > 0], MOD_POINTS),
        "deobligation_amount_quantiles": quantiles([-t["transaction_amount"] for t in deobs], MOD_POINTS),
        "weekday_net_obligations_per_day": round(statistics.mean(weekday_net), 2) if weekday_net else 0.0,
        "days": sorted(per_day),
        "transactions": len(transactions),
        "contracts": len(contracts),
        "weekday_transactions_per_day": round(statistics.mean(weekday_counts or per_day.values()), 1),
        "contracts_per_transaction": round(len(contracts) / len(transactions), 3),
        "modification_share": round(len(mods) / max(len(amounts), 1), 4),
        "deobligation_share_of_modifications": round(len(deobs) / max(len(mods), 1), 4),
        "deobligation_median": deob_median,
        "deobligation_sigma": deob_sigma,
        "vendors_per_day": round(vendors_per_day, 1),
        "award_types": {k: round(v / sum(award_types.values()), 4) for k, v in award_types.most_common()},
        "categories": categories,
    }


def profile_dir(landing_dir: str) -> dict[str, Any]:
    tx = _read_jsonl(sorted(glob.glob(os.path.join(landing_dir, "transactions", "*.jsonl"))))
    ct = _read_jsonl(sorted(glob.glob(os.path.join(landing_dir, "contracts", "*.jsonl"))))
    return profile(tx, ct)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--input", required=True, help="landing folder containing contracts/ and transactions/")
    parser.add_argument("--out", default="config/calibration.json")
    args = parser.parse_args(argv)
    p = profile_dir(args.input)
    with open(args.out, "w") as f:
        json.dump(p, f, indent=1)
    print(f"wrote {args.out}: {p['transactions']} transactions, {len(p['categories'])} categories")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
