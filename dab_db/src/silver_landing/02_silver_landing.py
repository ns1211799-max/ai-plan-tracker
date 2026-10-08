# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 02 - Silver: Landing
# MAGIC Reads new Parquet snapshot files from Bronze (Auto Loader tracks which
# MAGIC files it's already processed via its checkpoint, so a rerun never
# MAGIC double-loads a file) and appends them into a Delta table.
# MAGIC
# MAGIC **Partitioning**: `load_date` + `load_hour`. With 4 full loads/day this
# MAGIC gives exactly one partition per run - the right granularity. `load_year`
# MAGIC and `load_month` are kept as plain (non-partition) columns for easy
# MAGIC filtering/reporting without fragmenting the table into tiny partitions.
# MAGIC `load_min` is intentionally NOT included as a partition or even a
# MAGIC separate column here - `_load_ts` (full timestamp, already present from
# MAGIC Bronze) covers that level of detail if you ever need it.

# COMMAND ----------

from pyspark.sql.functions import year, month, dayofmonth, hour, date_format, col

CATALOG = "poc_fullload"
LANDING_PATH = "/Volumes/poc_fullload/bronze/raw_landing"

TABLES = ["customers", "products", "suppliers", "orders"]

# COMMAND ----------

def run_landing_for_table(table_name: str):
    source_folder = f"{LANDING_PATH}/{table_name}"
    landing_table = f"{CATALOG}.silver_landing.{table_name}"
    schema_location = f"{LANDING_PATH}/_schemas/landing_{table_name}"
    checkpoint = f"{LANDING_PATH}/_checkpoints/landing_{table_name}"

    print(f"Landing: {source_folder} -> {landing_table}")

    raw_stream = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", schema_location)
        .load(source_folder)
    )

    landing_df = (
        raw_stream
        .withColumn("load_year",  year(col("_load_ts")))
        .withColumn("load_month", month(col("_load_ts")))
        .withColumn("load_date",  date_format(col("_load_ts"), "yyyy-MM-dd"))
        .withColumn("load_hour",  hour(col("_load_ts")))
    )

    (
        landing_df.writeStream
        .format("delta")
        .option("checkpointLocation", checkpoint)
        .option("mergeSchema", "true")
        .partitionBy("load_date", "load_hour")
        .outputMode("append")
        .trigger(availableNow=True)
        .toTable(landing_table)
        .awaitTermination()
    )

    print(f"  done -> {spark.table(landing_table).count()} total rows in Landing")

# COMMAND ----------

for t in TABLES:
    run_landing_for_table(t)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sanity check: see the partitions created

# COMMAND ----------

display(spark.sql(f"""
SELECT load_date, load_hour, count(*) AS row_count
FROM {CATALOG}.silver_landing.orders
GROUP BY load_date, load_hour
ORDER BY load_date, load_hour
"""))