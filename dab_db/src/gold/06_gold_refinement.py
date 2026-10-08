# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 06 - Gold: Refinement
# MAGIC Small, purpose-built marts built from Silver Unification. Orchestrated
# MAGIC by the Databricks Workflow in `workflow_job.json`, not DLT this time -
# MAGIC plain PySpark, same as every other step in this pipeline.

# COMMAND ----------

CATALOG = "poc_fullload"
spark.sql(f"USE CATALOG {CATALOG}")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.gold.mart_customer_order_summary AS
SELECT
  customer_id, customer_name, customer_city, customer_segment,
  COUNT(*) AS total_orders,
  SUM(CASE WHEN order_status = 'delivered' THEN 1 ELSE 0 END) AS delivered_orders,
  MAX(order_date) AS most_recent_order_date
FROM {CATALOG}.silver_unified.unified_orders
GROUP BY customer_id, customer_name, customer_city, customer_segment
""")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.gold.mart_orders_by_status_daily AS
SELECT DATE(order_date) AS order_day, order_status, COUNT(*) AS order_count
FROM {CATALOG}.silver_unified.unified_orders
GROUP BY DATE(order_date), order_status
ORDER BY order_day, order_status
""")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.gold.mart_product_catalog_by_category AS
SELECT category, COUNT(*) AS product_count, ROUND(AVG(price), 2) AS avg_price
FROM {CATALOG}.silver_unified.dim_product
GROUP BY category
""")

# COMMAND ----------

for t in ["mart_customer_order_summary", "mart_orders_by_status_daily", "mart_product_catalog_by_category"]:
    print(f"{t}: {spark.table(f'{CATALOG}.gold.{t}').count()} rows")