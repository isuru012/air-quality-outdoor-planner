# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Build Bronze and Silver for the verified pilot
# MAGIC Uses the saved 1-7 January 2025 responses; makes no API requests.
# MAGIC Bronze preserves hourly source values and provenance. Silver joins weather
# MAGIC and pollution on city + UTC hour, adds local time, and separates invalid rows.
# MAGIC Missing values remain null, with explicit quality flags.
# MAGIC
# MAGIC This pilot validates a modest dataset, not a claim of large data volume.
# MAGIC Python validates the 24 raw envelopes; Spark performs table construction,
# MAGIC joins, quality rules and Delta writes. Forecast data remain separate.

# COMMAND ----------

import hashlib
import json
import math
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo
from pyspark.sql import functions as F, types as T
from delta.tables import DeltaTable

SCHEMA = "workspace.air_quality_outdoor_isuru"
ROOT = Path("/Volumes/workspace/air_quality_outdoor_isuru/project_files")
INGESTION_RUN_ID = "1db303f788ef4725b392d14e9bc7a9be"
START_DATE, END_DATE = "2025-01-01", "2025-01-07"
BUILD_ID = uuid.uuid4().hex
START = datetime.fromisoformat(START_DATE).replace(tzinfo=timezone.utc)
STOP = datetime.fromisoformat(END_DATE).replace(tzinfo=timezone.utc) + timedelta(days=1)
TIMES = [START + timedelta(hours=h) for h in range(int((STOP - START).total_seconds() / 3600))]
TIME_STRINGS = [t.strftime("%Y-%m-%dT%H:%M") for t in TIMES]
AIR = ["pm2_5", "pm10", "nitrogen_dioxide", "ozone", "sulphur_dioxide", "carbon_monoxide", "european_aqi"]
WEATHER = ["temperature_2m", "relative_humidity_2m", "precipitation", "surface_pressure", "cloud_cover", "wind_speed_10m", "wind_direction_10m", "shortwave_radiation"]
KEYS = ["city", "event_time_utc"]
EXPECTED_CITIES = {"Colombo", "Kandy", "Galle", "Jaffna", "Kurunegala", "Anuradhapura", "Delhi", "Beijing", "Singapore", "Dubai", "London", "Sydney"}
EXPECTED_UNITS = {**{v: "μg/m³" for v in AIR if v != "european_aqi"}, "european_aqi": "EAQI", "temperature_2m": "°C", "relative_humidity_2m": "%", "precipitation": "mm", "surface_pressure": "hPa", "cloud_cover": "%", "wind_speed_10m": "m/s", "wind_direction_10m": "°", "shortwave_radiation": "W/m²"}
spark.sql("SET TIME ZONE 'UTC'")

city_df = spark.table(f"{SCHEMA}.city_metadata")
cities = {r["city"]: r.asDict() for r in city_df.collect()}
if city_df.count() != 12 or set(cities) != EXPECTED_CITIES:
    raise ValueError("Expected the agreed 12 unique cities in city_metadata")
for city in cities.values():
    ZoneInfo(city["timezone"])
EXPECTED_ROWS = len(cities) * len(TIMES)

# Read this exact successful run, not an arbitrary latest or old cached batch.
manifest = (spark.table(f"{SCHEMA}.ingestion_log")
    .filter((F.col("run_id") == INGESTION_RUN_ID)
            & (F.col("start_date") == START_DATE) & (F.col("end_date") == END_DATE)))
entries = [r.asDict() for r in manifest.collect()]
if len(entries) != 24 or {(r["city"], r["source"]) for r in entries} != {(c, s) for c in cities for s in ("air_quality", "weather")}:
    raise ValueError("Expected exactly 24 city/source log rows for the verified pilot run. Share the error; do not guess a replacement run ID.")
if any(r["status"] not in ("SUCCESS", "WARNING") or r["row_count"] != len(TIMES) for r in entries):
    raise ValueError("The selected run contains failed or incomplete source responses")
if len({r["file_path"] for r in entries}) != 24:
    raise ValueError("A raw file was assigned to more than one city/source")
print("Verified ingestion run:", INGESTION_RUN_ID)
print("Expected rows per Bronze table:", EXPECTED_ROWS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check raw provenance and flatten hourly arrays
# MAGIC Check source identity, requested coordinates, units, UTC dates and array
# MAGIC lengths before writing tables. Reject duplicate hours instead of silently
# MAGIC choosing one. Preserve nulls and negative readings for quality assessment.

# COMMAND ----------

def flatten_envelope(envelope, city, source, path):
    meta, payload = envelope["metadata"], envelope["response"]
    params = meta["request_params"]
    expected_url = ("https://air-quality-api.open-meteo.com/v1/air-quality"
                    if source == "air_quality" else "https://archive-api.open-meteo.com/v1/archive")
    variables = AIR if source == "air_quality" else WEATHER
    if (meta.get("format_version") != 1 or meta.get("data_kind") != "historical"
        or meta.get("city") != city["city"] or meta.get("source") != source
        or meta.get("geoname_id") != city["geoname_id"] or meta.get("request_url") != expected_url
        or meta.get("city_timezone") != city["timezone"]):
        raise ValueError(f"Raw metadata mismatch: {path}")
    for key, value in {"start_date": START_DATE, "end_date": END_DATE, "timezone": "UTC"}.items():
        if params.get(key) != value:
            raise ValueError(f"Wrong request {key}: {path}")
    for key in ("latitude", "longitude"):
        if not math.isclose(float(params[key]), float(city[key]), rel_tol=0, abs_tol=1e-8):
            raise ValueError(f"City coordinate mismatch: {path}")
    if params.get("hourly", "").split(",") != variables:
        raise ValueError(f"Unexpected variable list: {path}")
    fingerprint = hashlib.sha256(json.dumps({"url": expected_url, "params": params}, sort_keys=True).encode()).hexdigest()[:20]
    if meta.get("request_fingerprint") != fingerprint:
        raise ValueError(f"Request fingerprint mismatch: {path}")
    if payload.get("utc_offset_seconds") != 0:
        raise ValueError(f"Response is not UTC: {path}")
    hourly = payload.get("hourly", {})
    if hourly.get("time") != TIME_STRINGS:
        raise ValueError(f"Missing, duplicate, out-of-order or unexpected hour: {path}")
    units = payload.get("hourly_units", {})
    for variable in variables:
        actual_unit = str(units.get(variable, "")).replace("µ", "μ")
        if actual_unit != EXPECTED_UNITS[variable]:
            raise ValueError(f"Unexpected unit for {variable}: {actual_unit!r}; expected {EXPECTED_UNITS[variable]!r}")
        values = hourly.get(variable)
        if not isinstance(values, list) or len(values) != len(TIMES):
            raise ValueError(f"Misaligned array: {variable}")
        if any(v is not None and (isinstance(v, bool) or not isinstance(v, (int, float))) for v in values):
            raise ValueError(f"Non-numeric source value: {variable}")
    retrieved = datetime.fromisoformat(meta["retrieved_at_utc"].replace("Z", "+00:00"))
    if retrieved.tzinfo is None:
        raise ValueError("Raw retrieval timestamp has no timezone")
    retrieved = retrieved.astimezone(timezone.utc)
    return [{
        "city": city["city"], "event_time_utc": timestamp,
        **{v: None if hourly[v][i] is None else float(hourly[v][i]) for v in variables},
        "raw_file_path": str(path), "source_retrieved_at_utc": retrieved,
        "source_run_id": meta["run_id"], "ingestion_run_id": INGESTION_RUN_ID,
        "request_fingerprint": fingerprint, "source_url": expected_url,
        "source_grid_latitude": float(payload["latitude"]),
        "source_grid_longitude": float(payload["longitude"]),
        "units_json": json.dumps({v: units[v] for v in variables}, ensure_ascii=False, sort_keys=True),
    } for i, timestamp in enumerate(TIMES)]


def bronze_schema(variables):
    return T.StructType([
        T.StructField("city", T.StringType(), False),
        T.StructField("event_time_utc", T.TimestampType(), False),
        *[T.StructField(v, T.DoubleType(), True) for v in variables],
        T.StructField("raw_file_path", T.StringType(), False),
        T.StructField("source_retrieved_at_utc", T.TimestampType(), False),
        *[T.StructField(v, T.StringType(), False) for v in ("source_run_id", "ingestion_run_id", "request_fingerprint", "source_url")],
        T.StructField("source_grid_latitude", T.DoubleType(), False),
        T.StructField("source_grid_longitude", T.DoubleType(), False),
        T.StructField("units_json", T.StringType(), False),
    ])

rows = {"air_quality": [], "weather": []}
for entry in entries:
    source, city_name = entry["source"], entry["city"]
    path = Path(entry["file_path"])
    required_parent = ROOT / "raw" / "historical" / source
    if path.parent != required_parent or path.suffix != ".json":
        raise ValueError(f"Unexpected raw file location: {path}")
    envelope = json.loads(path.read_text(encoding="utf-8"))
    rows[source].extend(flatten_envelope(envelope, cities[city_name], source, path))
    print("VALIDATED |", source, "|", city_name)

# Prove the exact city/hour grid before writing. Equal row totals alone are insufficient.
expected_keys = {(city, timestamp) for city in cities for timestamp in TIMES}
for source in rows:
    keys = [(r["city"], r["event_time_utc"]) for r in rows[source]]
    if len(keys) != EXPECTED_ROWS or len(set(keys)) != len(keys) or set(keys) != expected_keys:
        raise ValueError(f"Wrong or duplicate city/hour keys in {source}")
bronze_air = spark.createDataFrame(rows["air_quality"], bronze_schema(AIR))
bronze_weather = spark.createDataFrame(rows["weather"], bronze_schema(WEATHER))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Bronze Delta tables
# MAGIC MERGE updates existing city/hour records and inserts new ones. Synchronization
# MAGIC is restricted to these 12 cities and the pilot period; other dates are retained.
# MAGIC Run one instance at a time. Invalid source values remain visible in Bronze.

# COMMAND ----------

city_sql = ",".join("'" + c.replace("'", "''") + "'" for c in sorted(cities))
period_sql = (f"event_time_utc >= TIMESTAMP '{START_DATE} 00:00:00' AND "
              f"event_time_utc < TIMESTAMP '{STOP:%Y-%m-%d} 00:00:00' AND city IN ({city_sql})")
target_period_sql = period_sql.replace("event_time_utc", "target.event_time_utc").replace("city IN", "target.city IN")

def write_period(df, table_name):
    full_name = f"{SCHEMA}.{table_name}"
    if not spark.catalog.tableExists(full_name):
        df.write.format("delta").mode("error").saveAsTable(full_name)
    else:
        target = spark.table(full_name)
        if sorted((f.name, f.dataType.simpleString()) for f in target.schema.fields) != sorted((f.name, f.dataType.simpleString()) for f in df.schema.fields):
            raise ValueError(f"Schema mismatch in {full_name}; existing table was not overwritten")
        if target.filter(period_sql).groupBy(*KEYS).count().filter("count > 1").limit(1).count():
            raise ValueError(f"Existing duplicate keys in {full_name}; inspect before modifying")
        (DeltaTable.forName(spark, full_name).alias("target")
            .merge(df.alias("source"), "target.city = source.city AND target.event_time_utc = source.event_time_utc")
            .whenMatchedUpdateAll().whenNotMatchedInsertAll()
            .whenNotMatchedBySourceDelete(condition=target_period_sql)
            .execute())
    return spark.table(full_name).filter(period_sql)

bronze_air = write_period(bronze_air, "bronze_air_quality")
bronze_weather = write_period(bronze_weather, "bronze_weather")
print("Bronze tables saved.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join and apply quality rules
# MAGIC Valid ranges: non-negative pollutants, AQI, precipitation, wind speed and
# MAGIC solar radiation; humidity/cloud cover 0-100%; wind direction 0-360 degrees;
# MAGIC surface pressure >0. No arbitrary temperature threshold is imposed.
# MAGIC Values outside these rules are flagged, not corrected or replaced with zero.
# MAGIC Missing source fields remain null and are reported separately.

# COMMAND ----------

def prepare_source(df, variables, prefix):
    return df.select(*KEYS, *variables,
        F.col("raw_file_path").alias(f"{prefix}_raw_file_path"),
        F.col("source_retrieved_at_utc").alias(f"{prefix}_retrieved_at_utc"),
        F.col("source_grid_latitude").alias(f"{prefix}_grid_latitude"),
        F.col("source_grid_longitude").alias(f"{prefix}_grid_longitude"),
        F.lit(True).alias(f"has_{prefix}"))

joined = (prepare_source(bronze_air, AIR, "air")
    .join(prepare_source(bronze_weather, WEATHER, "weather"), KEYS, "full_outer")
    .join(city_df, "city", "left"))

rules = [
    ("missing_air_source", F.col("has_air").isNull()),
    ("missing_weather_source", F.col("has_weather").isNull()),
    ("missing_city_metadata", F.col("timezone").isNull()),
]
for variable in AIR + WEATHER:
    col = F.col(variable)
    rules.append((f"{variable}:non_finite", col.isNotNull() & (F.isnan(col) | (F.abs(col) == F.lit(float("inf"))))))
for variable in AIR + ["precipitation", "wind_speed_10m", "shortwave_radiation"]:
    rules.append((f"{variable}:negative", F.col(variable) < 0))
for variable in ["relative_humidity_2m", "cloud_cover"]:
    rules.append((f"{variable}:outside_0_100", ~F.col(variable).between(0, 100)))
rules.append(("wind_direction_10m:outside_0_360", ~F.col("wind_direction_10m").between(0, 360)))
rules.append(("surface_pressure:non_positive", F.col("surface_pressure") <= 0))

joined = (joined
    .withColumn("invalid_reasons", F.filter(F.array(*[
        F.when(F.coalesce(condition, F.lit(False)), F.lit(name)) for name, condition in rules
    ]), lambda x: x.isNotNull()))
    .withColumn("missing_variables", F.filter(F.array(*[
        F.when(F.col(v).isNull(), F.lit(v)) for v in AIR + WEATHER
    ]), lambda x: x.isNotNull()))
    .withColumn("missing_variable_count", F.size("missing_variables"))
    .withColumn("is_valid", F.size("invalid_reasons") == 0)
    .withColumn("data_quality_status", F.when(~F.col("is_valid"), "INVALID")
                .when(F.col("missing_variable_count") > 0, "MISSING_VALUES").otherwise("COMPLETE"))
    .withColumn("ingestion_run_id", F.lit(INGESTION_RUN_ID)))

# Local clock is an intermediate shifted timestamp. Store its human-readable string
# and calendar features; retain event_time_utc as the actual timestamp/join key.
joined = (joined
    .withColumn("_local_clock", F.from_utc_timestamp(F.col("event_time_utc"), F.col("timezone")))
    .withColumn("local_time", F.date_format("_local_clock", "yyyy-MM-dd HH:mm:ss"))
    .withColumn("local_date", F.to_date("_local_clock"))
    .withColumn("local_hour", F.hour("_local_clock"))
    .withColumn("local_minute", F.minute("_local_clock"))
    .withColumn("local_year", F.year("_local_clock"))
    .withColumn("local_month", F.month("_local_clock"))
    .withColumn("local_day_of_week", F.pmod(F.dayofweek("_local_clock") + 5, F.lit(7)) + 1)
    .withColumn("is_weekend", F.col("local_day_of_week").isin(6, 7))
    .withColumn("event_date_utc", F.to_date("event_time_utc"))
    .drop("_local_clock", "has_air", "has_weather"))

# Review before publishing Silver. Invalid rows remain in quarantine with all values.
joined_count = joined.count()
if joined_count != EXPECTED_ROWS:
    raise ValueError(f"Unexpected join size: {joined_count}; expected {EXPECTED_ROWS}")
if joined.groupBy(*KEYS).count().filter("count > 1").limit(1).count():
    raise ValueError("Duplicate keys after joining; Silver has not been written")
valid = joined.filter("is_valid")
invalid = joined.filter("NOT is_valid")
silver = write_period(valid, "silver_city_hourly")
quarantine = write_period(invalid, "silver_quarantine")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate stored tables and save a quality report
# MAGIC A missing-free pilot can still contain out-of-range values. Report actual
# MAGIC quarantine results instead of assuming zero. Full-year ingestion remains
# MAGIC gated on these results. Local dates at the UTC pilot boundaries are partial.

# COMMAND ----------

counts = {"bronze_air_quality": bronze_air.count(), "bronze_weather": bronze_weather.count(),
          "silver_city_hourly": silver.count(), "silver_quarantine": quarantine.count()}
if counts["bronze_air_quality"] != EXPECTED_ROWS or counts["bronze_weather"] != EXPECTED_ROWS:
    raise ValueError("Persisted Bronze counts are incorrect")
if counts["silver_city_hourly"] + counts["silver_quarantine"] != EXPECTED_ROWS:
    raise ValueError("Silver and quarantine do not account for all expected city-hours")
all_results = silver.unionByName(quarantine)
expected_grid = city_df.select("city").crossJoin(spark.createDataFrame([(t,) for t in TIMES], "event_time_utc TIMESTAMP"))
if expected_grid.join(all_results.select(*KEYS), KEYS, "left_anti").limit(1).count() or all_results.select(*KEYS).join(expected_grid, KEYS, "left_anti").limit(1).count():
    raise ValueError("Stored outputs have missing or unexpected city-hour keys")
if all_results.groupBy(*KEYS).count().filter("count > 1").limit(1).count():
    raise ValueError("Stored outputs have duplicate city-hour keys")

missing = all_results.agg(*[F.sum(F.col(v).isNull().cast("long")).alias(v) for v in AIR + WEATHER]).first().asDict()
# Check every stored local clock against Python's timezone database, including
# Colombo's half-hour offset and daylight-saving rules in international cities.
local_errors = []
for row in all_results.select("city", "event_time_utc", "timezone", "local_time").collect():
    instant = row["event_time_utc"]
    instant = instant.replace(tzinfo=timezone.utc) if instant.tzinfo is None else instant.astimezone(timezone.utc)
    expected_local = instant.astimezone(ZoneInfo(row["timezone"])).strftime("%Y-%m-%d %H:%M:%S")
    if expected_local != row["local_time"]:
        local_errors.append(row["city"])
if local_errors:
    raise ValueError(f"Local timezone conversion mismatch for {sorted(set(local_errors))}")

quality_report = {
    "build_id": BUILD_ID, "ingestion_run_id": INGESTION_RUN_ID,
    "checked_at_utc": datetime.now(timezone.utc).isoformat(),
    "start_date": START_DATE, "end_date": END_DATE,
    "expected_city_hours": EXPECTED_ROWS, "counts": counts,
    "missing_values_by_variable": missing, "duplicate_keys": 0,
    "missing_city_hour_keys": 0, "local_timezone_check": "PASS",
    "status": "PASS" if counts["silver_quarantine"] == 0 and sum(missing.values()) == 0 else "REVIEW_REQUIRED",
}
report_path = ROOT / "logs" / f"bronze_silver_quality_{BUILD_ID}.json"
with report_path.open("x", encoding="utf-8") as handle:
    json.dump(quality_report, handle, indent=2)
print("BRONZE AND SILVER SUMMARY")
print(json.dumps(quality_report, indent=2))
print("Quality report:", report_path)
display(all_results.groupBy("city").agg(F.count("*").alias("total_hours"),
    F.sum(F.col("is_valid").cast("int")).alias("valid_hours"),
    F.sum((~F.col("is_valid")).cast("int")).alias("quarantined_hours")).orderBy("city"))
display(silver.select("city", "event_time_utc", "local_time", "pm2_5", "european_aqi", "temperature_2m", "precipitation", "data_quality_status").orderBy(*KEYS).limit(12))
if counts["silver_quarantine"]:
    display(quarantine.select("city", "event_time_utc", "invalid_reasons").orderBy(*KEYS).limit(30))
print("NEXT: share this summary. A PASS allows the full-year ingestion stage; REVIEW_REQUIRED means inspect quality issues first.")