# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 05 - Silver: Unification
# MAGIC Builds the conformed dims/facts from `silver_merge.*`. Since Merge now
# MAGIC carries `is_deleted`, every query here filters `is_deleted = false` -
# MAGIC Unification should only ever represent the CURRENT, non-deleted state.
# MAGIC (Rows that were soft-deleted are still recoverable/auditable in
# MAGIC `silver_merge.*` directly, just excluded from this layer onward.)

# COMMAND ----------

CATALOG = "poc_fullload"
spark.sql(f"USE CATALOG {CATALOG}")

# COMMAND ----------

# MAGIC %md ## dim_customer

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.silver_unified.dim_customer AS
SELECT
  monotonically_increasing_id() AS customer_sk,
  customer_id, name, email, city, segment, updated_at
FROM {CATALOG}.silver_merge.customers
WHERE is_deleted = false
""")

# COMMAND ----------

# MAGIC %md ## dim_product

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.silver_unified.dim_product AS
SELECT
  monotonically_increasing_id() AS product_sk,
  product_id, product_name, category, price, updated_at
FROM {CATALOG}.silver_merge.products
WHERE is_deleted = false
""")

# COMMAND ----------

# MAGIC %md ## dim_supplier

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.silver_unified.dim_supplier AS
SELECT
  monotonically_increasing_id() AS supplier_sk,
  supplier_id, supplier_name, country, updated_at
FROM {CATALOG}.silver_merge.suppliers
WHERE is_deleted = false
""")

# COMMAND ----------

# MAGIC %md ## unified_orders
# MAGIC Grain: one row per order. `LEFT JOIN` on purpose - see note below.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.silver_unified.unified_orders AS
SELECT
  o.order_id, o.order_date, o.status AS order_status, o.updated_at AS order_updated_at,
  c.customer_sk, c.customer_id, c.name AS customer_name,
  c.city AS customer_city, c.segment AS customer_segment
FROM {CATALOG}.silver_merge.orders o
LEFT JOIN {CATALOG}.silver_unified.dim_customer c ON o.customer_id = c.customer_id
WHERE o.is_deleted = false
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sanity check

# COMMAND ----------

for t in ["dim_customer", "dim_product", "dim_supplier", "unified_orders"]:
    print(f"{t}: {spark.table(f'{CATALOG}.silver_unified.{t}').count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Why `WHERE o.is_deleted = false` but still `LEFT JOIN` on customer
# MAGIC We deliberately filter deleted ORDERS out (they're not "current" data
# MAGIC anymore), but we still `LEFT JOIN` to `dim_customer` rather than
# MAGIC `INNER JOIN` - if a customer record is late-arriving or was itself
# MAGIC soft-deleted, we still want to see the order (with `customer_sk = NULL`)
# MAGIC rather than silently dropping it.