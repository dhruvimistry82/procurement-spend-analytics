"""Generate synthetic procurement data in exactly the same format as the real ingestion.

Use it to test the pipeline end to end without calling the API, and to prove the
pipeline catches problems: it plants known anomalies and writes the ground truth
so you can check the Gold and quarantine tables against it.

    python -m ingestion.synthetic --start 2026-07-01 --end 2026-07-31
    python -m ingestion.synthetic --start 2026-07-01 --end 2026-07-31 --target volume

Calibrated mode extends a real sample: it learns category mix, amounts, volume and
modification / de-obligation rates from config/calibration.json (see calibrate.py)
and writes into the real landing folder, every row tagged `_synthetic: true`:

    python -m ingestion.synthetic --start 2026-07-04 --end 2026-09-30 \
        --calibration config/calibration.json --into-real-landing --target volume

Planted on purpose (counts in data/synthetic_truth/truth.json):
  * split purchases: 2-3 new awards to one vendor, same agency, same day, each just
    under the threshold  -> should appear in gold_split_purchase_flags
  * invalid rows (missing agency, missing amount, end before start, future date)
    -> should appear in quarantine_contracts / quarantine_transactions
  * de-obligations (negative transactions) -> valid, reported as deobligations
  * re-landed duplicates of transactions -> Silver should keep one copy

Agency and vendor names are always fictional, so synthetic rows can never be
mistaken for records about a real organization. Without --into-real-landing, files
go to a separate folder (landing_synthetic).
"""
from __future__ import annotations

import argparse
import json
import logging
import math
import random
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from ingestion.ingest import REPO_ROOT, LocalTarget, VolumeTarget, daterange, load_config, to_jsonl

log = logging.getLogger("synthetic")

AGENCIES = {
    "Department of Space Exploration": ["Office of Launch Systems", "Office of Science Missions"],
    "Department of Public Buildings": ["Federal Facilities Service", "Fleet Management Service"],
    "Department of Clean Water": ["Office of Water Quality", "Office of Research"],
}
CATEGORIES = [  # (NAICS code, NAICS description, PSC code, PSC description, typical amount)
    ("541511", "CUSTOM COMPUTER PROGRAMMING SERVICES", "DA01", "IT AND TELECOM - APPLICATION DEVELOPMENT", 250_000),
    ("541330", "ENGINEERING SERVICES", "R425", "SUPPORT- PROFESSIONAL: ENGINEERING/TECHNICAL", 400_000),
    ("236220", "COMMERCIAL AND INSTITUTIONAL BUILDING CONSTRUCTION", "Z2AA", "REPAIR OR ALTERATION OF OFFICE BUILDINGS", 900_000),
    ("336411", "AIRCRAFT MANUFACTURING", "1550", "UNMANNED AIRCRAFT", 3_000_000),
    ("423430", "COMPUTER EQUIPMENT MERCHANT WHOLESALERS", "7021", "ADP CENTRAL PROCESSING UNIT", 40_000),
    ("561720", "JANITORIAL SERVICES", "S201", "HOUSEKEEPING- CUSTODIAL JANITORIAL", 60_000),
    ("541380", "TESTING LABORATORIES", "H234", "EQUIPMENT AND MATERIALS TESTING", 30_000),
    ("339113", "SURGICAL APPLIANCE AND SUPPLIES MANUFACTURING", "6515", "MEDICAL AND SURGICAL INSTRUMENTS", 12_000),
]
AWARD_TYPES = ["DEFINITIVE CONTRACT", "PURCHASE ORDER", "DELIVERY ORDER", "BPA CALL"]
NAME_A = ["Apex", "Blue Ridge", "Cedar", "Delta", "Evergreen", "Falcon", "Granite", "Harbor", "Ironwood", "Juniper",
          "Keystone", "Lakeside", "Meridian", "Northstar", "Orion", "Pinnacle", "Quarry", "Redwood", "Summit", "Titan"]
NAME_B = ["Systems", "Solutions", "Engineering", "Logistics", "Technologies", "Services", "Labs", "Builders"]
STATES = ["AZ", "CA", "TX", "VA", "MD", "FL", "CO", "WA", "OH", "AL"]


def make_vendors(rng: random.Random, n: int = 60) -> list[dict[str, str]]:
    vendors, seen = [], set()
    while len(vendors) < n:
        name = f"{rng.choice(NAME_A)} {rng.choice(NAME_B)} {rng.choice(['Inc.', 'LLC', 'Corp.'])}"
        if name in seen:
            continue
        seen.add(name)
        uei = "".join(rng.choice("ABCDEFGHJKLMNPQRSTUVWXYZ0123456789") for _ in range(12))
        vendors.append({"name": name.upper(), "uei": uei, "state": rng.choice(STATES)})
    return vendors


class Generator:
    def __init__(self, seed: int = 42, rows_per_day: int = 120, split_threshold: float = 10_000,
                 split_cases_per_week: int = 3, invalid_rate: float = 0.01,
                 calibration: dict[str, Any] | None = None, scale: float = 0.25):
        self.rng = random.Random(seed)
        self.cal = calibration
        if calibration:
            # Real-looking mix and volume; enough vendors that chance coincidences are rare.
            self.categories = [dict(c) for c in calibration["categories"]]
            rows_per_day = max(10, int(calibration["weekday_transactions_per_day"] * scale))
            self.mod_share = calibration["modification_share"]
            self.deob_share = calibration["deobligation_share_of_modifications"]
            self.award_types = calibration["award_types"]
            n_vendors = max(60, int(calibration.get("vendors_per_day", 60) * scale * 2))
        else:
            self.categories = [{"naics_code": n, "naics_description": nd, "psc_code": p, "psc_description": pd,
                                "weight": 1.0, "median": typical, "sigma": 1.1}
                               for n, nd, p, pd, typical in CATEGORIES]
            self.mod_share, self.deob_share = 0.35, 0.12
            self.award_types = {t: 1.0 for t in AWARD_TYPES}
            n_vendors = 60
        self.vendors = make_vendors(self.rng, n_vendors)
        # A few vendors win most work, like real procurement (drives the concentration metric).
        top = max(5, n_vendors // 20)
        self.weights = [8 if i < top else 1 for i in range(len(self.vendors))]
        self.n_top = top
        self.rows_per_day = rows_per_day
        self.threshold = split_threshold
        self.split_cases_per_week = split_cases_per_week
        self.invalid_rate = invalid_rate
        self.awards: dict[str, dict[str, Any]] = {}
        self.next_id = 1
        self.truth: dict[str, Any] = {"split_cases": [], "invalid_contracts": 0,
                                      "invalid_transactions": 0, "deobligations": 0, "duplicates": 0}

    # --- building blocks -----------------------------------------------------
    def _new_award(self, day: date, amount: float | None = None, vendor=None, agency=None,
                   category=None, award_type=None) -> dict[str, Any]:
        rng = self.rng
        agency = agency or rng.choice(list(AGENCIES))
        vendor = vendor or rng.choices(self.vendors, weights=self.weights)[0]
        cat = category or rng.choices(self.categories, weights=[c["weight"] for c in self.categories])[0]
        if amount is None:
            amount = round(max(100.0, rng.lognormvariate(math.log(cat["median"]), cat["sigma"])), 2)
        piid = f"SYN{day:%y}{self.next_id:07d}"
        self.next_id += 1
        award = {
            "award_id": piid, "generated_internal_id": f"CONT_AWD_{piid}_SYN",
            "recipient_name": vendor["name"], "recipient_uei": vendor["uei"], "state": vendor["state"],
            "awarding_agency": agency, "awarding_sub_agency": rng.choice(AGENCIES[agency]),
            "naics_code": cat["naics_code"], "naics_description": cat["naics_description"],
            "psc_code": cat["psc_code"], "psc_description": cat["psc_description"],
            "award_type": award_type or rng.choices(list(self.award_types), weights=list(self.award_types.values()))[0],
            "start_date": day, "end_date": day + timedelta(days=rng.choice([30, 90, 180, 365, 730])),
            "obligated": 0.0, "mods": 0, "base_amount": amount,
            "ceiling": round(amount * rng.choice([1.0, 1.0, 1.5, 2.0, 3.0]), 2),
            "description": f"{(cat['psc_description'] or 'general').split('- ')[-1].title()} support",
        }
        self.awards[award["generated_internal_id"]] = award
        return award

    def _transaction(self, award: dict[str, Any], day: date, amount: float, mod: str) -> dict[str, Any]:
        award["obligated"] = round(award["obligated"] + amount, 2)
        award["last_modified"] = datetime.combine(day, datetime.min.time()) + timedelta(
            hours=self.rng.randint(8, 18), minutes=self.rng.randint(0, 59))
        return {
            "award_id": award["award_id"], "mod": mod, "recipient_name": award["recipient_name"],
            "recipient_uei": award["recipient_uei"], "action_date": day.isoformat(),
            "transaction_amount": amount, "transaction_description": award["description"].upper(),
            "awarding_agency": award["awarding_agency"], "awarding_sub_agency": award["awarding_sub_agency"],
            "award_type": award["award_type"], "naics_code": award["naics_code"],
            "naics_description": award["naics_description"], "psc_code": award["psc_code"],
            "psc_description": award["psc_description"], "generated_internal_id": award["generated_internal_id"],
        }

    def _contract(self, award: dict[str, Any]) -> dict[str, Any]:
        return {
            "award_id": award["award_id"], "generated_internal_id": award["generated_internal_id"],
            "recipient_name": award["recipient_name"], "recipient_uei": award["recipient_uei"],
            "awarding_agency": award["awarding_agency"], "awarding_sub_agency": award["awarding_sub_agency"],
            "funding_agency": award["awarding_agency"], "award_amount": award["ceiling"],
            "total_outlays": round(award["obligated"] * self.rng.uniform(0.2, 0.9), 2),
            "contract_award_type": award["award_type"], "naics_code": award["naics_code"],
            "naics_description": award["naics_description"], "psc_code": award["psc_code"],
            "psc_description": award["psc_description"], "description": award["description"].upper(),
            "start_date": award["start_date"].isoformat(), "end_date": award["end_date"].isoformat(),
            "last_modified_date": award["last_modified"].strftime("%Y-%m-%d %H:%M:%S"),
            "place_of_performance_state_code": award["state"],
        }

    def _break(self, row: dict[str, Any], kind: str) -> dict[str, Any]:
        """Make a row invalid in one specific way."""
        row = dict(row)
        choice = self.rng.choice(["no_agency", "no_amount", "bad_dates" if kind == "contract" else "future_date"])
        if choice == "no_agency":
            row["awarding_agency"] = None
        elif choice == "no_amount":
            row["award_amount" if kind == "contract" else "transaction_amount"] = None
        elif choice == "bad_dates":
            row["end_date"] = "2000-01-01"
        else:
            row["action_date"] = "2099-01-01"
        return row

    # --- one day -------------------------------------------------------------
    def day(self, day: date) -> tuple[list[dict], list[dict]]:
        rng = self.rng
        if day.weekday() >= 5:  # weekends are quiet
            n = rng.randint(0, self.rows_per_day // 10)
        else:
            # Fiscal year ends Sep 30: obligations surge through September.
            surge = 2.5 if day.month == 9 else 1.0
            n = int(rng.gauss(self.rows_per_day, self.rows_per_day * 0.15) * surge)
        txs: list[dict] = []
        touched: set[str] = set()

        open_awards = [a for a in self.awards.values() if a["end_date"] >= day]
        for _ in range(max(0, n)):
            if open_awards and rng.random() < self.mod_share:  # modification of an existing award
                award = rng.choice(open_awards)
                award["mods"] += 1
                if rng.random() < self.deob_share:
                    amount = -round(min(award["obligated"], award["base_amount"]) * rng.uniform(0.05, 0.4), 2)
                    self.truth["deobligations"] += 1
                else:
                    amount = round(award["base_amount"] * rng.uniform(0.05, 0.6), 2)
                txs.append(self._transaction(award, day, amount, f"P{award['mods']:05d}"))
            else:
                award = self._new_award(day)
                txs.append(self._transaction(award, day, award["base_amount"], "0"))
            touched.add(award["generated_internal_id"])

        # Invalid transactions (regular rows only, so planted split cases stay intact).
        for i in range(len(txs)):
            if rng.random() < self.invalid_rate:
                txs[i] = self._break(txs[i], "transaction")
                self.truth["invalid_transactions"] += 1

        # Planted split purchases (weekdays only).
        if day.weekday() < 5 and rng.random() < self.split_cases_per_week / 5:
            vendor = rng.choice(self.vendors[self.n_top:])
            agency = rng.choice(list(AGENCIES))
            k = rng.choice([2, 2, 3])
            ids = []
            for _ in range(k):
                amt = round(rng.uniform(self.threshold * 0.91, self.threshold * 0.995), 2)
                a = self._new_award(day, amount=amt, vendor=vendor, agency=agency,
                                    category=min(self.categories, key=lambda c: c["median"]),
                                    award_type="PURCHASE ORDER")
                txs.append(self._transaction(a, day, amt, "0"))
                touched.add(a["generated_internal_id"])
                ids.append(a["award_id"])
            self.truth["split_cases"].append({"date": day.isoformat(), "agency": agency,
                                              "vendor_uei": vendor["uei"], "award_ids": ids})

        contracts = [self._contract(self.awards[i]) for i in sorted(touched)]
        for i in range(len(contracts)):
            if rng.random() < self.invalid_rate:
                contracts[i] = self._break(contracts[i], "contract")
                self.truth["invalid_contracts"] += 1

        dupes = [dict(t) for t in txs if rng.random() < 0.01]
        self.truth["duplicates"] += len(dupes)
        txs.extend(dupes)
        return contracts, txs


def lineage(rows: list[dict], source: str, day: str, run_id: str) -> list[dict]:
    return [{**r, "_source": source, "_window_start": day, "_window_end": day, "_run_id": run_id,
             "_possibly_incomplete": False, "_synthetic": True} for r in rows]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--target", choices=["local", "volume"], default="local")
    parser.add_argument("--rows-per-day", type=int, default=120)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--config", default=str(REPO_ROOT / "config" / "sources.yml"))
    parser.add_argument("--calibration", help="profile from ingestion.calibrate, to mimic a real sample")
    parser.add_argument("--scale", type=float, default=0.25, help="fraction of the real daily volume (calibrated mode)")
    parser.add_argument("--into-real-landing", action="store_true",
                        help="write into the real landing folder (rows stay tagged _synthetic)")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    cfg = load_config(Path(args.config))
    landing = cfg["landing"] if args.into_real_landing else cfg.get("synthetic_landing", {})
    if args.target == "volume":
        target = VolumeTarget(landing.get("volume_dir", "/Volumes/procurement/raw/landing_synthetic"))
    else:
        target = LocalTarget(REPO_ROOT / landing.get("local_dir", "data/landing_synthetic"))

    calibration = json.loads(Path(args.calibration).read_text()) if args.calibration else None
    gen = Generator(seed=args.seed, rows_per_day=args.rows_per_day, calibration=calibration, scale=args.scale)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    totals: dict[str, int] = defaultdict(int)
    for day in daterange(args.start, args.end):
        contracts, txs = gen.day(date.fromisoformat(day))
        for source, rows in (("contracts", contracts), ("transactions", txs)):
            if rows:
                target.write(to_jsonl(lineage(rows, source, day, run_id)), f"{source}/{day}_{run_id}.jsonl")
                totals[source] += len(rows)

    truth_dir = REPO_ROOT / "data" / "synthetic_truth"
    truth_dir.mkdir(parents=True, exist_ok=True)
    (truth_dir / "truth.json").write_text(json.dumps(gen.truth, indent=2))
    log.info("rows written: %s", dict(totals))
    log.info("planted: %s split cases, %s invalid contracts, %s invalid transactions, %s deobligations, %s duplicates",
             len(gen.truth["split_cases"]), gen.truth["invalid_contracts"], gen.truth["invalid_transactions"],
             gen.truth["deobligations"], gen.truth["duplicates"])
    log.info("ground truth saved to %s", truth_dir / "truth.json")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
