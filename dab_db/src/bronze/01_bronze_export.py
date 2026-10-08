# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 01 - Bronze export
# MAGIC Snapshots the connector's current full-load table into its own folder
# MAGIC in the Bronze Volume, as Parquet. This must run BEFORE the connector's
# MAGIC next 6-hour full load overwrites the table - schedule this job to run
# MAGIC shortly after the connector's own refresh completes.
# MAGIC
# MAGIC **Increment logic**: a small control table remembers the max value of
# MAGIC each source table's `updated_at` column that we last exported. If the
# MAGIC connector table hasn't produced anything newer than that watermark,
# MAGIC we skip the export - this makes the job safe to run more than once
# MAGIC without creating duplicate snapshot files.

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit, max as spark_max

CATALOG = "poc_fullload"
LANDING_VOLUME = f"{CATALOG}.bronze.raw_landing"
LANDING_PATH = "/Volumes/poc_fullload/bronze/raw_landing"
CONTROL_TABLE = f"{CATALOG}.bronze._export_control"

TABLES = [
    {"name": "customers", "date_col": "updated_at"},
    {"name": "products",  "date_col": "updated_at"},
    {"name": "suppliers", "date_col": "updated_at"},
    {"name": "orders",    "date_col": "updated_at"},
]

# COMMAND ----------

spark.sql(f"""
CREATE VOLUME IF NOT EXISTS {LANDING_VOLUME}
COMMENT 'Bronze - one folder per table, Parquet snapshots, one file per full load'
""")

for t in TABLES:
    dbutils.fs.mkdirs(f"{LANDING_PATH}/{t['name']}")

if not spark.catalog.tableExists(CONTROL_TABLE):
    spark.sql(f"""
        CREATE TABLE {CONTROL_TABLE} (
            table_name STRING,
            last_exported_watermark STRING,
            last_exported_at TIMESTAMP
        )
    """)

# COMMAND ----------

def get_last_watermark(table_name: str):
    row = (
        spark.table(CONTROL_TABLE)
        .filter(f"table_name = '{table_name}'")
        .select("last_exported_watermark")
        .collect()
    )
    return row[0]["last_exported_watermark"] if row else None

def set_last_watermark(table_name: str, watermark: str):
    spark.sql(f"DELETE FROM {CONTROL_TABLE} WHERE table_name = '{table_name}'")
    spark.sql(f"""
        INSERT INTO {CONTROL_TABLE} VALUES
        ('{table_name}', '{watermark}', current_timestamp())
    """)

# COMMAND ----------

def export_bronze_snapshot(table_name: str, date_col: str):
    source_table = f"{CATALOG}.connector_landed.{table_name}"
    df = spark.table(source_table)

    current_max = df.select(spark_max(date_col).alias("m")).collect()[0]["m"]
    last_watermark = get_last_watermark(table_name)

    if last_watermark is not None and current_max is not None and str(current_max) <= last_watermark:
        print(f"{table_name}: no new load since last export ({last_watermark}) - skipping.")
        return

    load_ts = current_timestamp()
    export_df = df.withColumn("_load_ts", load_ts)

    # filename carries the load time so files sort chronologically and are
    # easy to reason about when archived later
    file_stamp = spark.sql("SELECT date_format(current_timestamp(), 'yyyy-MM-dd_HHmmss') AS s").collect()[0]["s"]
    out_path = f"{LANDING_PATH}/{table_name}/{table_name}_{file_stamp}"

    export_df.coalesce(1).write.format("parquet").mode("overwrite").save(out_path)
    set_last_watermark(table_name, str(current_max))

    print(f"{table_name}: exported {df.count()} rows -> {out_path}")

# COMMAND ----------

for t in TABLES:
    export_bronze_snapshot(t["name"], t["date_col"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check the control table

# COMMAND ----------

display(spark.table(CONTROL_TABLE))