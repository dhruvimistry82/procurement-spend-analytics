# Databricks notebook source
# MAGIC %md
# MAGIC # Synthetic extension of the real sample
# MAGIC 1. Profile the real landing files (aggregate statistics only).
# MAGIC 2. Remove any earlier synthetic files, keeping the real ones.
# MAGIC 3. Generate Jul 4 - Sep 30 synthetic data into the landing folder, every row tagged `_synthetic`.
# MAGIC 4. **Run the pipeline with Full refresh**, then run the validation cell at the bottom.
# MAGIC
# MAGIC All synthetic agency and vendor names are fictional.

# COMMAND ----------

import glob, json, os, sys
from datetime import datetime, timezone

# Make the repo importable from wherever this notebook lives.
here = os.getcwd()
root = next(c for c in [here, os.path.dirname(here)] + glob.glob("/Workspace/Users/*/procurement-spend-analytics")
            if os.path.exists(os.path.join(c, "ingestion", "synthetic.py")))
sys.path.insert(0, root)

from ingestion.calibrate import profile_dir
from ingestion.synthetic import Generator, run_generation

LANDING = "/Volumes/procurement/raw/landing"
META = f"{LANDING}/_meta"
FIRST_SYNTHETIC_DAY, LAST_DAY = "2026-07-04", "2026-09-30"
SCALE = 1.0  # fraction of the real daily volume
print("repo:", root)

# COMMAND ----------

# 1-2. Remove earlier synthetic files (by date), then profile the real files that remain.
removed = 0
for src in ("contracts", "transactions"):
    for f in dbutils.fs.ls(f"{LANDING}/{src}"):
        if f.name[:10] >= FIRST_SYNTHETIC_DAY:
            dbutils.fs.rm(f.path)
            removed += 1
print("removed", removed, "old synthetic files")

profile = profile_dir(LANDING)
print({k: profile[k] for k in ("days", "transactions", "weekday_transactions_per_day",
                               "weekday_net_obligations_per_day", "modification_share")})

# COMMAND ----------

# 3. Generate and land synthetic data; compute what every table should contain.
def write(data: bytes, rel: str) -> str:
    path = f"{LANDING}/{rel}"
    with open(path, "wb") as f:
        f.write(data)
    return path

gen = Generator(seed=42, calibration=profile, scale=SCALE)
run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
expected = run_generation(gen, FIRST_SYNTHETIC_DAY, LAST_DAY, write, run_id)

os.makedirs(META, exist_ok=True)
for name, obj in (("calibration", profile), ("truth", gen.truth), ("expected", expected)):
    with open(f"{META}/{name}.json", "w") as f:
        json.dump(obj, f, indent=1)

print(json.dumps({k: v for k, v in expected.items() if k != "net_obligations_by_month"}, indent=1))
print("synthetic net obligations by month ($M):",
      {m: round(v / 1e6, 1) for m, v in expected["net_obligations_by_month"].items()})
print("real sample, per weekday ($M):", round(profile["weekday_net_obligations_per_day"] / 1e6, 1))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Now run the pipeline: **Run pipeline ▾ → Full refresh all**. When it finishes, run the cell below.

# COMMAND ----------

# 4. Validate: synthetic part against the generator's expectations, real part against the real-only run.
exp = json.load(open(f"{META}/expected.json"))
REAL = {"bronze_transactions": 11902, "bronze_contracts": 11373, "silver_transactions": 11902,
        "silver_contracts": 11373, "split_flags": 4, "deobligation_rows": 243}
T = "procurement.analytics"
q = lambda sql: spark.sql(sql).collect()[0][0]
checks = [
    ("bronze_transactions",     f"SELECT COUNT(*) FROM {T}.bronze_transactions WHERE _synthetic",  f"SELECT COUNT(*) FROM {T}.bronze_transactions WHERE _synthetic IS NOT TRUE"),
    ("bronze_contracts",        f"SELECT COUNT(*) FROM {T}.bronze_contracts WHERE _synthetic",     f"SELECT COUNT(*) FROM {T}.bronze_contracts WHERE _synthetic IS NOT TRUE"),
    ("quarantine_transactions", f"SELECT COUNT(*) FROM {T}.quarantine_transactions WHERE is_synthetic", None),
    ("quarantine_contracts",    f"SELECT COUNT(*) FROM {T}.quarantine_contracts WHERE is_synthetic",    None),
    ("silver_transactions",     f"SELECT COUNT(*) FROM {T}.silver_transactions WHERE is_synthetic", f"SELECT COUNT(*) FROM {T}.silver_transactions WHERE NOT is_synthetic"),
    ("silver_contracts",        f"SELECT COUNT(*) FROM {T}.silver_contracts WHERE is_synthetic",    f"SELECT COUNT(*) FROM {T}.silver_contracts WHERE NOT is_synthetic"),
    ("split_flags",             f"SELECT COUNT(*) FROM {T}.gold_split_purchase_flags WHERE data_source = 'synthetic'", f"SELECT COUNT(*) FROM {T}.gold_split_purchase_flags WHERE data_source = 'real'"),
    ("deobligation_rows",       f"SELECT COUNT(*) FROM {T}.silver_transactions WHERE is_synthetic AND transaction_amount < 0", f"SELECT COUNT(*) FROM {T}.silver_transactions WHERE NOT is_synthetic AND transaction_amount < 0"),
]
rows = []
for name, syn_sql, real_sql in checks:
    rows.append((name, "synthetic", exp.get(name, 0), q(syn_sql)))
    if real_sql:
        rows.append((name, "real", REAL[name], q(real_sql)))
df = spark.createDataFrame([(n, s, int(e), int(a), "✅" if int(e) == int(a) else "❌") for n, s, e, a in rows],
                           "check string, data_source string, expected long, actual long, ok string")
display(df)
print("ALL MATCH" if all(int(e) == int(a) for _, _, e, a in rows) else "MISMATCHES ABOVE")
print("planted split cases:", exp["planted_split_cases"], "| synthetic flags (planted + chance):", exp["split_flags"])
