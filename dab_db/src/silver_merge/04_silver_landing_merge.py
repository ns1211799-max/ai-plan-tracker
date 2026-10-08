# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 04 - Silver: Landing Merge
# MAGIC Builds one row per business key, always reflecting the latest full
# MAGIC load. Three things happen here, in order:
# MAGIC
# MAGIC 1. **Deduplication** - the newest Landing partition could in theory
# MAGIC    contain more than one row for the same key (bad export, retry,
# MAGIC    etc.) - keep only the latest by `updated_at`.
# MAGIC 2. **Upsert** - `MERGE ... WHEN MATCHED THEN UPDATE`, `WHEN NOT MATCHED
# MAGIC    THEN INSERT` for anything new or changed.
# MAGIC 3. **Delete detection** - `WHEN NOT MATCHED BY SOURCE` catches business
# MAGIC    keys that exist in the merge table but are ABSENT from the newest
# MAGIC    full load - meaning they were deleted at source. Depending on
# MAGIC    `HARD_DELETE_ENABLED`, either soft-delete (flag + keep) or hard-delete
# MAGIC    (physically remove) those rows.

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable

CATALOG = "poc_fullload"

TABLES = [
    {"name": "customers", "business_key": "customer_id"},
    {"name": "products",  "business_key": "product_id"},
    {"name": "suppliers", "business_key": "supplier_id"},
    {"name": "orders",    "business_key": "order_id"},
]
ORDER_BY_COL = "updated_at"

# Per-table override: True = physically delete missing rows, False = soft delete (flag + keep).
# Default to soft delete everywhere - safer, reversible, keeps audit history.
HARD_DELETE_ENABLED = {
    "customers": False,
    "products":  False,
    "suppliers": False,
    "orders":    False,   # e.g. flip to True only if you have a real reason to purge orders permanently
}

# COMMAND ----------

def run_merge_for_table(table_name: str, business_key: str, hard_delete: bool):
    landing_table = f"{CATALOG}.silver_landing.{table_name}"
    merge_table = f"{CATALOG}.silver_merge.{table_name}"

    print(f"Merge: {landing_table} -> {merge_table} (key={business_key}, hard_delete={hard_delete})")

    # Only merge the MOST RECENT full load's worth of Landing data - not the
    # entire history table - since a full load already represents "everything
    # that currently exists at source." Merging older loads too would just
    # re-process rows that later loads already superseded.
    latest_load_date, latest_load_hour = (
        spark.table(landing_table)
        .agg(F.max("load_date").alias("d"), F.max("load_hour").alias("h"))
        .collect()[0]
    )
    latest_batch = spark.table(landing_table).filter(
        (F.col("load_date") == latest_load_date) & (F.col("load_hour") == latest_load_hour)
    )

    # Step 1: dedup within this batch, keep latest per key
    deduped = (
        latest_batch.groupBy(business_key)
        .agg(F.max_by(F.struct("*"), ORDER_BY_COL).alias("latest"))
        .select("latest.*")
        .withColumn("is_deleted", F.lit(False))
        .withColumn("deleted_at", F.lit(None).cast("timestamp"))
    )

    # Create the target table on first run, with the extra delete-tracking columns
    if not spark.catalog.tableExists(merge_table):
        deduped.limit(0).write.format("delta").saveAsTable(merge_table)

    target = DeltaTable.forName(spark, merge_table)

    merge_builder = (
        target.alias("t")
        .merge(deduped.alias("s"), f"t.{business_key} = s.{business_key}")
        .whenMatchedUpdateAll(condition=f"s.{ORDER_BY_COL} >= t.{ORDER_BY_COL}")
        .whenNotMatchedInsertAll()
    )

    # Step 2 (delete detection): key exists in target but NOT in this load
    if hard_delete:
        merge_builder = merge_builder.whenNotMatchedBySourceDelete()
    else:
        merge_builder = merge_builder.whenNotMatchedBySourceUpdate(set={
            "is_deleted": "true",
            "deleted_at": "current_timestamp()"
        })

    merge_builder.execute()

    total = spark.table(merge_table).count()
    active = spark.table(merge_table).filter("is_deleted = false").count() if not hard_delete else total
    print(f"  done -> {total} total rows in {merge_table} ({active} active)")

# COMMAND ----------

for t in TABLES:
    run_merge_for_table(t["name"], t["business_key"], HARD_DELETE_ENABLED[t["name"]])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Try it yourself
# MAGIC After running `00_connector_landed_tables.py`'s `simulate_next_full_load()`
# MAGIC and rerunning 01 -> 02 -> 04:
# MAGIC ```sql
# MAGIC SELECT order_id, is_deleted, deleted_at FROM poc_fullload.silver_merge.orders
# MAGIC WHERE order_id = 1;
# MAGIC ```
# MAGIC `order_id = 1` was removed from the simulated source - you should see
# MAGIC `is_deleted = true` with a `deleted_at` timestamp, NOT a missing row.
# MAGIC (If you'd set `HARD_DELETE_ENABLED["orders"] = True`, the row would be
# MAGIC gone entirely instead.)