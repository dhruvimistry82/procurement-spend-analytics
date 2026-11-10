# Lakeflow Declarative Pipeline — GOLD
# Business-ready tables for procurement analytics.
#
# Every table carries data_source ('real' or 'synthetic') so dashboards can filter
# and real figures are never blended with synthetic ones unknowingly.
#
# Spend over time comes from TRANSACTIONS (money obligated on a date), never from
# award totals: an award's amount is its lifetime value, so summing it by start
# month would put years of spend into one month.
from pyspark import pipelines as dp
from pyspark.sql import Window
from pyspark.sql import functions as F

# New awards just under an approval threshold can signal split purchasing.
# Configurable because procurement thresholds change over time.
SPLIT_THRESHOLD = float(spark.conf.get("split_threshold", "10000"))  # noqa: F821
SPLIT_BAND = 0.10  # flag base awards within 10% below the threshold


def with_source(df):
    return df.withColumn("data_source", F.when(F.col("is_synthetic"), "synthetic").otherwise("real"))


def transactions():
    return with_source(spark.read.table("silver_transactions"))  # noqa: F821


@dp.materialized_view(comment="Monthly obligations per agency: new awards vs modifications, and de-obligations.")
def gold_spend_by_agency_month():
    return (
        transactions()
        .withColumn("month", F.date_trunc("month", "action_date"))
        .groupBy("data_source", "awarding_agency", "month")
        .agg(
            F.sum("transaction_amount").alias("net_obligations"),
            F.sum(F.when(F.col("is_base_award"), F.col("transaction_amount"))).alias("new_award_obligations"),
            F.sum(F.when(~F.col("is_base_award"), F.col("transaction_amount"))).alias("modification_obligations"),
            F.sum(F.when(F.col("transaction_amount") < 0, F.col("transaction_amount"))).alias("deobligations"),
            F.count("*").alias("transaction_count"),
            F.countDistinct("recipient_uei").alias("vendor_count"),
        )
    )


@dp.materialized_view(comment="Net obligations by product/service category (PSC) and industry (NAICS).")
def gold_spend_by_category():
    return (
        transactions()
        .groupBy("data_source", "psc_code", "psc_description", "naics_code", "naics_description")
        .agg(F.sum("transaction_amount").alias("net_obligations"),
             F.countDistinct("award_key").alias("award_count"))
    )


@dp.materialized_view(comment="Vendors ranked by net obligations within each agency, with share of agency spend.")
def gold_top_vendors():
    by_vendor = (
        transactions()
        .groupBy("data_source", "awarding_agency", "recipient_uei", "recipient_name")
        .agg(F.sum("transaction_amount").alias("vendor_obligations"),
             F.countDistinct("award_key").alias("award_count"))
        .where("vendor_obligations > 0")
    )
    agency = Window.partitionBy("data_source", "awarding_agency")
    return (
        by_vendor
        .withColumn("share_of_agency_spend", F.col("vendor_obligations") / F.sum("vendor_obligations").over(agency))
        .withColumn("rank_in_agency", F.dense_rank().over(agency.orderBy(F.desc("vendor_obligations"))))
    )


@dp.materialized_view(
    comment="Vendor concentration per agency (Herfindahl-Hirschman Index, 0-10,000). "
            "Above 2,500 means spend is concentrated in few vendors."
)
def gold_vendor_concentration():
    return (
        spark.read.table("gold_top_vendors")  # noqa: F821
        .groupBy("data_source", "awarding_agency")
        .agg(F.round(F.sum(F.pow(F.col("share_of_agency_spend") * 100, 2)), 0).alias("hhi"),
             F.count("*").alias("vendor_count"))
        .withColumn("concentration",
                    F.when(F.col("hhi") > 2500, "high")
                     .when(F.col("hhi") > 1500, "moderate")
                     .otherwise("competitive"))
    )


@dp.materialized_view(
    comment="Possible split purchases: 2+ new awards to the same vendor from the same agency on the "
            "same day, each just under the approval threshold. A lead for review, not proof."
)
def gold_split_purchase_flags():
    near = transactions().where(
        F.col("is_base_award")
        & (F.col("transaction_amount") < SPLIT_THRESHOLD)
        & (F.col("transaction_amount") >= SPLIT_THRESHOLD * (1 - SPLIT_BAND))
    )
    return (
        near.groupBy("data_source", "awarding_agency", "recipient_uei", "recipient_name", "action_date")
        .agg(F.count("*").alias("award_count"),
             F.sum("transaction_amount").alias("combined_amount"),
             F.collect_list("award_id").alias("award_ids"))
        .where("award_count >= 2")
    )


@dp.materialized_view(comment="Largest contracts by lifetime value, with obligations seen in the loaded period.")
def gold_contract_portfolio():
    obligated = (spark.read.table("silver_transactions").groupBy("award_key")  # noqa: F821
                 .agg(F.sum("transaction_amount").alias("obligations_in_period"),
                      F.max("action_date").alias("last_action_date")))
    return (
        with_source(spark.read.table("silver_contracts"))  # noqa: F821
        .select("data_source", "award_key", "award_id", "recipient_name", "awarding_agency", "award_amount",
                "start_date", "end_date", "psc_description")
        .join(obligated, "award_key", "left")
    )
