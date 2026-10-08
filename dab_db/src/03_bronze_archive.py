# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 03 - Bronze archive
# MAGIC Runs AFTER Landing (step 02) has successfully consumed the Bronze
# MAGIC files. Moves those files out of the "raw" folder into a matching
# MAGIC "archive" folder, so:
# MAGIC - Bronze's raw folder stays small (only ever holds unprocessed files)
# MAGIC - the archive folder keeps a full historical trail of every load, in
# MAGIC   case you ever need to reprocess Landing from scratch
# MAGIC
# MAGIC We move (not copy+delete-later) so a file is never counted twice, and
# MAGIC we only move files whose modification time is before we started this
# MAGIC run - so a file that Bronze export is still writing right now (from a
# MAGIC concurrent run) never gets swept up prematurely.

# COMMAND ----------

from datetime import datetime

CATALOG = "poc_fullload"
ARCHIVE_VOLUME = f"{CATALOG}.bronze.archive"
RAW_PATH = "/Volumes/poc_fullload/bronze/raw_landing"
ARCHIVE_PATH = "/Volumes/poc_fullload/bronze/archive"

TABLES = ["customers", "products", "suppliers", "orders"]

run_started_at_ms = int(datetime.utcnow().timestamp() * 1000)

# COMMAND ----------

spark.sql(f"""
CREATE VOLUME IF NOT EXISTS {ARCHIVE_VOLUME}
COMMENT 'Historical trail of every Bronze snapshot file, moved here once Landing has consumed it'
""")

for t in TABLES:
    dbutils.fs.mkdirs(f"{ARCHIVE_PATH}/{t}")

# COMMAND ----------

def archive_processed_files(table_name: str):
    raw_folder = f"{RAW_PATH}/{table_name}"
    archive_folder = f"{ARCHIVE_PATH}/{table_name}"

    files = dbutils.fs.ls(raw_folder)
    moved = 0

    for f in files:
        if f.modificationTime < run_started_at_ms:
            dest = f"{archive_folder}/{f.name}"
            dbutils.fs.mv(f.path, dest, recurse=True)
            moved += 1

    print(f"{table_name}: archived {moved} file(s) -> {archive_folder}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Confirm raw folders are now empty (or only contain the newest,
# MAGIC not-yet-loaded file, if Bronze export ran again since step 02)

# COMMAND ----------

for t in TABLES:
    archive_processed_files(t)

# COMMAND ----------

for t in TABLES:
    print(f"\n=== {ARCHIVE_PATH}/{t} ===")
    for f in dbutils.fs.ls(f"{ARCHIVE_PATH}/{t}"):
        print(" ", f.path)

# COMMAND ----------

for t in TABLES:
    print(f"\n=== {RAW_PATH}/{t} ===")
    for f in dbutils.fs.ls(f"{RAW_PATH}/{t}"):
        print(" ", f.path)