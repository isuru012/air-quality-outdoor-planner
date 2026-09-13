# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Create storage for the individual project
# MAGIC Run this in the workspace selected for your individual project.
# MAGIC A separate schema organizes data but does not establish private access:
# MAGIC workspace administrators and inherited grants may still apply.
# MAGIC
# MAGIC Creates `workspace.air_quality_outdoor_isuru` and a managed Volume.
# MAGIC Uses no old team tables or volumes. Existing project objects are preserved.
# MAGIC Creates a unique verification table and file, reads them, then removes only
# MAGIC those verification objects. It does not change users, grants, or permissions.

# COMMAND ----------

import json
import uuid
from datetime import datetime, timezone

CATALOG = "workspace"
SCHEMA = "air_quality_outdoor_isuru"
VOLUME = "project_files"
PROJECT_SCHEMA = f"{CATALOG}.{SCHEMA}"
VOLUME_ROOT = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

visible_catalogs = {row[0] for row in spark.sql("SHOW CATALOGS").collect()}
if CATALOG not in visible_catalogs:
    raise RuntimeError("The workspace catalog is unavailable here. Share SHOW CATALOGS output before changing the configuration.")

spark.sql("SET TIME ZONE 'UTC'")
print("Target schema:", PROJECT_SCHEMA)
print("Target managed Volume:", f"{PROJECT_SCHEMA}.{VOLUME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create containers
# MAGIC IF NOT EXISTS makes reruns preserve existing objects. Databricks chooses
# MAGIC managed storage, so an AWS account or bucket is not needed for this step.

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {PROJECT_SCHEMA} COMMENT 'Individual Air Quality Outdoor Planner project'")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {PROJECT_SCHEMA}.{VOLUME} COMMENT 'Raw data, logs and dashboard exports for the individual project'")

folders = [
    "raw/historical/air_quality",
    "raw/historical/weather",
    "raw/forecast/air_quality",
    "raw/forecast/weather",
    "metadata",
    "logs",
    "quarantine",
    "exports",
]
for folder in folders:
    dbutils.fs.mkdirs(f"{VOLUME_ROOT}/{folder}")

print("Project folders created or already present.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify real write and read access
# MAGIC Catalog visibility alone is insufficient. These two small probes verify
# MAGIC Delta table storage and managed Volume files. Only unique probe objects
# MAGIC created by this run are removed afterwards.

# COMMAND ----------

probe_id = uuid.uuid4().hex
probe_table = f"{PROJECT_SCHEMA}._storage_check_{probe_id}"
table_created = False
try:
    spark.sql(f"CREATE TABLE {probe_table} (check_id STRING) USING DELTA")
    table_created = True
    spark.sql(f"INSERT INTO {probe_table} VALUES ('{probe_id}')")
    actual = spark.sql(f"SELECT check_id FROM {probe_table}").collect()
    if len(actual) != 1 or actual[0]["check_id"] != probe_id:
        raise RuntimeError("Delta write/read verification returned unexpected data")
    print("PASS | Delta table write/read")
finally:
    if table_created:
        spark.sql(f"DROP TABLE {probe_table}")

probe_path = f"{VOLUME_ROOT}/logs/storage_check_{probe_id}.json"
probe_data = {"check_id": probe_id, "checked_at_utc": datetime.now(timezone.utc).isoformat()}
file_created = False
try:
    dbutils.fs.put(probe_path, json.dumps(probe_data), overwrite=False)
    file_created = True
    if json.loads(dbutils.fs.head(probe_path, 4096)) != probe_data:
        raise RuntimeError("Volume write/read verification returned unexpected data")
    print("PASS | Managed Volume file write/read")
finally:
    if file_created:
        dbutils.fs.rm(probe_path, recurse=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Results
# MAGIC Share the two PASS lines and this directory listing. City metadata and
# MAGIC ingestion follow this setup; no pollution or weather data are fetched here.

# COMMAND ----------

print("STORAGE SETUP COMPLETE")
print("Catalog:", CATALOG)
print("Schema:", PROJECT_SCHEMA)
print("Volume path:", VOLUME_ROOT)
display(dbutils.fs.ls(VOLUME_ROOT))