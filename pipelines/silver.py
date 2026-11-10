# Lakeflow Declarative Pipeline — SILVER
# For each source: type and standardize Bronze, check data quality rules, send bad
# rows to a quarantine table (nothing is silently dropped), and keep the latest
# version of each record. Vendor history is tracked as SCD Type 2.
from pyspark import pipelines as dp
from pyspark.sql import functions as F

AWARD_RULES = {
    "has_award_key": "award_key IS NOT NULL",
    "has_vendor": "recipient_uei IS NOT NULL OR recipient_name IS NOT NULL",
    "has_agency": "awarding_agency IS NOT NULL",
    "amount_is_number": "award_amount IS NOT NULL",
    "valid_dates": "start_date IS NULL OR end_date IS NULL OR start_date <= end_date",
    "has_last_modified": "last_modified_ts IS NOT NULL",
}

# Negative amounts are valid: they are de-obligations (money taken back).
TRANSACTION_RULES = {
    "has_award_key": "award_key IS NOT NULL",
    "has_mod": "mod IS NOT NULL",
    "has_vendor": "recipient_uei IS NOT NULL OR recipient_name IS NOT NULL",
    "has_agency": "awarding_agency IS NOT NULL",
    "amount_is_number": "transaction_amount IS NOT NULL",
    "has_action_date": "action_date IS NOT NULL",
    "action_date_not_in_future": "action_date <= current_date()",
}

COMMON = [
    F.upper(F.trim("recipient_uei")).alias("recipient_uei"),
    F.initcap(F.trim("recipient_name")).alias("recipient_name"),
    F.trim("awarding_agency").alias("awarding_agency"),
    F.trim("awarding_sub_agency").alias("awarding_sub_agency"),
    F.col("naics_code"), F.col("naics_description"),
    F.col("psc_code"), F.col("psc_description"),
    F.col("_ingested_at"), F.col("_source_file"), F.col("_run_id"),
]


def typed_contracts(df):
    return df.select(
        F.coalesce("generated_internal_id", "award_id").alias("award_key"),
        F.col("award_id"),
        F.trim("funding_agency").alias("funding_agency"),
        F.col("award_amount").cast("decimal(18,2)").alias("award_amount"),
        F.col("total_outlays").cast("decimal(18,2)").alias("total_outlays"),
        F.col("contract_award_type"),
        F.col("description"),
        F.to_date("start_date").alias("start_date"),
        F.to_date("end_date").alias("end_date"),
        F.to_timestamp("last_modified_date").alias("last_modified_ts"),
        F.upper("place_of_performance_state_code").alias("pop_state"),
        *COMMON,
    )


def typed_transactions(df):
    return df.select(
        F.coalesce("generated_internal_id", "award_id").alias("award_key"),
        F.col("award_id"),
        F.upper(F.trim("mod")).alias("mod"),
        # Mod "0" is the base award; anything else is a modification.
        (F.trim("mod").isin("0", "00", "000", "0000")).alias("is_base_award"),
        F.to_date("action_date").alias("action_date"),
        F.col("transaction_amount").cast("decimal(18,2)").alias("transaction_amount"),
        F.col("transaction_description"),
        F.col("award_type"),
        *COMMON,
    )


def quality_tables(source: str, typer, rules: dict, keys: list, sequence_by: str):
    """Typed view -> quarantine table + valid view -> latest-version Silver table."""
    all_rules = " AND ".join(f"({c})" for c in rules.values())

    @dp.view(name=f"{source}_typed")
    def _typed():
        return typer(spark.readStream.table(f"bronze_{source}")).withColumn(  # noqa: F821
            "_is_valid", F.coalesce(F.expr(all_rules), F.lit(False)))

    @dp.table(name=f"quarantine_{source}", comment=f"{source} rows that failed a rule, with the failed rules.")
    def _quarantine():
        failed = F.filter(
            F.array(*[F.when(~F.coalesce(F.expr(c), F.lit(False)), F.lit(n)) for n, c in rules.items()]),
            lambda x: x.isNotNull(),
        )
        return (spark.readStream.table(f"{source}_typed")  # noqa: F821
                .where("NOT _is_valid").withColumn("failed_rules", failed))

    @dp.view(name=f"{source}_valid")
    @dp.expect_all_or_drop(rules)  # also shows pass/fail counts per rule in the pipeline UI
    def _valid():
        return spark.readStream.table(f"{source}_typed").where("_is_valid")  # noqa: F821

    dp.create_streaming_table(f"silver_{source}", comment=f"Latest version of each {source} record.")
    dp.create_auto_cdc_flow(
        target=f"silver_{source}",
        source=f"{source}_valid",
        keys=keys,
        sequence_by=F.col(sequence_by),
        except_column_list=["_is_valid"],
        stored_as_scd_type=1,
    )


# Awards change over their life: keep the most recently modified version.
quality_tables("contracts", typed_contracts, AWARD_RULES, ["award_key"], "last_modified_ts")
# A transaction is one modification of one award; if it is re-landed, the newest copy wins.
quality_tables("transactions", typed_transactions, TRANSACTION_RULES, ["award_key", "mod"], "_ingested_at")


@dp.view
def vendor_changes():
    return (
        spark.readStream.table("contracts_valid")  # noqa: F821
        .where("recipient_uei IS NOT NULL")
        .select("recipient_uei", "recipient_name", "pop_state", "last_modified_ts")
    )


# Vendor history: a new version whenever a vendor's name or state changes.
dp.create_streaming_table("silver_vendors_scd2", comment="Vendor dimension with full history (SCD Type 2).")
dp.create_auto_cdc_flow(
    target="silver_vendors_scd2",
    source="vendor_changes",
    keys=["recipient_uei"],
    sequence_by=F.col("last_modified_ts"),
    stored_as_scd_type=2,
    track_history_column_list=["recipient_name", "pop_state"],
)
