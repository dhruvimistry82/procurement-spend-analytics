# Lakeflow Declarative Pipeline — BRONZE
# Incrementally ingests landed JSON Lines files with Auto Loader, one Bronze table
# per source. Raw values are kept as strings plus lineage columns, so Silver can
# always be rebuilt.
from pyspark import pipelines as dp
from pyspark.sql import functions as F

LANDING = spark.conf.get("landing_path", "/Volumes/procurement/raw/landing")  # noqa: F821

SOURCES = {
    "contracts": "Raw contract awards (one row per award per landed file).",
    "transactions": "Raw contract transactions: base awards and modifications with their obligated amounts.",
}


def bronze_table(source: str, comment: str):
    @dp.table(name=f"bronze_{source}", comment=comment, table_properties={"quality": "bronze"})
    def _bronze():
        return (
            spark.readStream.format("cloudFiles")  # noqa: F821
            .option("cloudFiles.format", "json")
            .option("cloudFiles.inferColumnTypes", "false")      # keep raw values as strings
            .option("cloudFiles.schemaEvolutionMode", "rescue")   # unexpected fields -> _rescued_data
            .load(f"{LANDING}/{source}/")
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source_file", F.col("_metadata.file_path"))
        )


for name, description in SOURCES.items():
    bronze_table(name, description)
