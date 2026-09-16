# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Build annual Bronze, Silver and Gold, then export dashboard data
# MAGIC Uses the 288 monthly files in the verified 2025 manifest; makes no API requests.
# MAGIC Bronze preserves hourly source values and provenance. Silver joins weather
# MAGIC and pollution on city + UTC hour, adds local time, and separates invalid rows.
# MAGIC Missing values remain null, with explicit quality flags.
# MAGIC
# MAGIC This study has 105,120 city-hours, a modest dataset, not a claim of large volume.
# MAGIC Python validates the monthly envelopes; Spark performs table construction,
# MAGIC joins, quality rules and Delta writes. Forecast data remain separate.

# COMMAND ----------

import calendar
import hashlib
import json
import math
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo
from pyspark.sql import functions as F, types as T, Window
from delta.tables import DeltaTable

SCHEMA = "workspace.air_quality_outdoor_isuru"
ROOT = Path("/Volumes/workspace/air_quality_outdoor_isuru/project_files")
INGESTION_RUN_ID = "6aed579412324c3886313a3b97d2c97b"
MANIFEST_PATH = ROOT / "metadata" / "backfill_2025_6aed579412324c3886313a3b97d2c97b.json"
START_DATE, END_DATE = "2025-01-01", "2025-12-31"
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

# Use the exact manifest, including its frozen city metadata and monthly file list.
manifest = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
summary = manifest.get("summary", {})
if (manifest.get("format_version") != 1 or summary.get("run_id") != INGESTION_RUN_ID
    or summary.get("status") != "PASS" or summary.get("year") != 2025):
    raise ValueError("Expected the verified successful full-year manifest")
entries = manifest.get("files", [])
expected_inputs = {
    (city, source, f"2025-{month:02d}-01", f"2025-{month:02d}-{calendar.monthrange(2025, month)[1]:02d}")
    for city in cities for source in ("air_quality", "weather") for month in range(1, 13)
}
if len(entries) != 288 or {(e["city"], e["source"], e["start_date"], e["end_date"]) for e in entries} != expected_inputs:
    raise ValueError("Manifest does not identify exactly the 288 expected monthly responses")
if len({e["file_path"] for e in entries}) != 288:
    raise ValueError("Duplicate raw file paths in full-year manifest")
for e in entries:
    hours = (datetime.fromisoformat(e["end_date"]) - datetime.fromisoformat(e["start_date"])).days * 24 + 24
    if e["run_id"] != INGESTION_RUN_ID or e["status"] != "SUCCESS" or e["row_count"] != hours:
        raise ValueError("Manifest includes an incomplete or unreviewed response")
frozen = manifest.get("city_metadata", [])
if len(frozen) != 12 or {c["city"] for c in frozen} != set(cities):
    raise ValueError("Manifest city metadata is incomplete")
for c in frozen:
    for field in ("country_code", "geoname_id", "latitude", "longitude", "timezone"):
        if c[field] != cities[c["city"]][field]:
            raise ValueError(f"City metadata changed since ingestion: {c['city']} {field}")
print("Verified full-year ingestion:", INGESTION_RUN_ID)
print("Monthly raw files:", len(entries))
print("Expected annual rows per source:", EXPECTED_ROWS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check raw provenance and flatten hourly arrays
# MAGIC Check source identity, requested coordinates, units, UTC dates and array
# MAGIC lengths before writing tables. Reject duplicate hours instead of silently
# MAGIC choosing one. Preserve nulls and negative readings for quality assessment.

# COMMAND ----------

def flatten_envelope(envelope, city, source, path, start_date, end_date):
    batch_start = datetime.fromisoformat(start_date).replace(tzinfo=timezone.utc)
    batch_stop = datetime.fromisoformat(end_date).replace(tzinfo=timezone.utc) + timedelta(days=1)
    batch_times = [batch_start + timedelta(hours=h) for h in range(int((batch_stop - batch_start).total_seconds() // 3600))]
    batch_strings = [t.strftime("%Y-%m-%dT%H:%M") for t in batch_times]
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
    for key, value in {"start_date": start_date, "end_date": end_date, "timezone": "UTC"}.items():
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
    if hourly.get("time") != batch_strings:
        raise ValueError(f"Missing, duplicate, out-of-order or unexpected hour: {path}")
    units = payload.get("hourly_units", {})
    for variable in variables:
        actual_unit = str(units.get(variable, "")).replace("µ", "μ")
        if actual_unit != EXPECTED_UNITS[variable]:
            raise ValueError(f"Unexpected unit for {variable}: {actual_unit!r}; expected {EXPECTED_UNITS[variable]!r}")
        values = hourly.get(variable)
        if not isinstance(values, list) or len(values) != len(batch_times):
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
    } for i, timestamp in enumerate(batch_times)]


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
    rows[source].extend(flatten_envelope(envelope, cities[city_name], source, path, entry["start_date"], entry["end_date"]))
    if len(rows[source]) % 20000 < 750:
        print("VALIDATED |", source, "|", len(rows[source]), "hours so far", flush=True)

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
# MAGIC is restricted to these 12 cities and 2025; other dates are retained.
# MAGIC Run one instance at a time. Invalid source values remain visible in Bronze.

# COMMAND ----------

city_sql = ",".join("'" + c.replace("'", "''") + "'" for c in sorted(cities))
period_sql = (f"event_time_utc >= TIMESTAMP '{START_DATE} 00:00:00' AND "
              f"event_time_utc < TIMESTAMP '{STOP:%Y-%m-%d} 00:00:00' AND city IN ({city_sql})")
target_period_sql = period_sql.replace("event_time_utc", "target.event_time_utc").replace("city IN", "target.city IN")

def write_period(df, table_name):
    full_name = f"{SCHEMA}.{table_name}"
    if not spark.catalog.tableExists(full_name):
        # Spark Connect accepts "error"; some versions reject "errorifexists".
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
# MAGIC quarantine results instead of assuming zero. Gold generation is gated on
# MAGIC these results. Daily/monthly Gold use UTC; hourly patterns use local hour.

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
report_path = ROOT / "logs" / f"annual_bronze_silver_quality_{BUILD_ID}.json"
with report_path.open("x", encoding="utf-8") as handle:
    json.dump(quality_report, handle, indent=2)
print("ANNUAL BRONZE AND SILVER SUMMARY")
print(json.dumps(quality_report, indent=2))
print("Quality report:", report_path)
display(all_results.groupBy("city").agg(F.count("*").alias("total_hours"),
    F.sum(F.col("is_valid").cast("int")).alias("valid_hours"),
    F.sum((~F.col("is_valid")).cast("int")).alias("quarantined_hours")).orderBy("city"))
display(silver.select("city", "event_time_utc", "local_time", "pm2_5", "european_aqi", "temperature_2m", "precipitation", "data_quality_status").orderBy(*KEYS).limit(12))
if counts["silver_quarantine"]:
    display(quarantine.select("city", "event_time_utc", "invalid_reasons").orderBy(*KEYS).limit(30))
if quality_report["status"] != "PASS":
    raise RuntimeError("Annual Silver needs review. Share the summary and quarantine reasons; Gold/export have not run.")
print("Annual Silver gate: PASS. Building Gold summaries next.", flush=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold summaries
# MAGIC Daily/monthly summaries use UTC calendar periods for consistent city coverage.
# MAGIC Hour-of-day patterns use each city's local clock, including half-hour offsets.
# MAGIC Rankings describe the 12 selected case-study cities, ordered by annual mean
# MAGIC PM2.5 (lower = rank 1). They do not rank all world cities.
# MAGIC AQI categories are applied to hourly provider values, not daily mean values.
# MAGIC No correlation between wind-direction degrees and pollution is calculated.

# COMMAND ----------

analysis = silver.withColumn("month_utc", F.month("event_time_utc"))
average_variables = AIR + [v for v in WEATHER if v not in ("precipitation", "wind_direction_10m")]

def aggregate_period(group_columns):
    return (analysis.groupBy(*group_columns).agg(
        F.count("*").alias("hours_available"),
        F.count("european_aqi").alias("aqi_hours_available"),
        *[F.avg(v).alias(f"average_{v}") for v in average_variables],
        F.max("european_aqi").alias("maximum_european_aqi"),
        F.max("pm2_5").alias("maximum_pm2_5"),
        F.sum("precipitation").alias("total_precipitation_mm"),
        F.sum(F.when(F.col("european_aqi") > 60, 1).otherwise(0)).alias("poor_or_worse_hours"),
    ).withColumn("poor_or_worse_percent", F.when(F.col("aqi_hours_available") > 0,
        100.0 * F.col("poor_or_worse_hours") / F.col("aqi_hours_available")))
     .withColumn("analysis_year", F.lit(2025)))

daily = aggregate_period(["city", "event_date_utc"]).withColumn("time_basis", F.lit("UTC"))
monthly = aggregate_period(["city", "month_utc"]).withColumn("time_basis", F.lit("UTC"))
ranking = (aggregate_period(["city"])
    .withColumn("pm2_5_rank", F.dense_rank().over(Window.orderBy(F.col("average_pm2_5").asc()))))
# Precipitation totals across repeated local hours are not useful as a planning
# expectation: report average hourly precipitation for this view instead.
hourly_pattern = (analysis.groupBy("city", "local_hour").agg(
    F.count("*").alias("hours_available"),
    F.avg("pm2_5").alias("average_pm2_5"),
    F.avg("pm10").alias("average_pm10"),
    F.avg("european_aqi").alias("average_european_aqi"),
    F.avg("temperature_2m").alias("average_temperature_2m"),
    F.avg("precipitation").alias("average_precipitation_mm"),
    F.avg("wind_speed_10m").alias("average_wind_speed_10m"),
).withColumn("analysis_year", F.lit(2025)).withColumn("time_basis", F.lit("city local time")))

# European AQI category boundaries. Values at an upper bound stay in that band.
# Source: https://open-meteo.com/en/docs/air-quality-api
category_names = ["Good", "Fair", "Moderate", "Poor", "Very poor", "Extremely poor"]
aqi = F.col("european_aqi")
classified = analysis.withColumn("category", F.when(aqi <= 20, "Good")
    .when(aqi <= 40, "Fair").when(aqi <= 60, "Moderate")
    .when(aqi <= 80, "Poor").when(aqi <= 100, "Very poor").otherwise("Extremely poor"))
category_dimension = spark.createDataFrame([(i + 1, name) for i, name in enumerate(category_names)], "category_order INT, category STRING")
category_grid = city_df.select("city").crossJoin(category_dimension)
category_counts = classified.groupBy("city", "category").agg(F.count("*").alias("hours"))
category_totals = classified.groupBy("city").agg(F.count("*").alias("total_hours"))
aqi_distribution = (category_grid.join(category_counts, ["city", "category"], "left")
    .fillna({"hours": 0}).join(category_totals, "city")
    .withColumn("percentage", F.col("hours") * 100.0 / F.col("total_hours"))
    .withColumn("analysis_year", F.lit(2025)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exploratory correlations
# MAGIC Six pollutant concentrations x four weather variables x 12 cities.
# MAGIC Pearson correlation is computed within each city from paired hourly values.
# MAGIC Sample counts and an undefined-correlation reason are included. A constant
# MAGIC variable gives null, not zero. These are descriptive associations, not causal
# MAGIC effects or significance tests; seasonality and serial dependence remain.

# COMMAND ----------

pollutants = [v for v in AIR if v != "european_aqi"]
weather_predictors = ["temperature_2m", "relative_humidity_2m", "precipitation", "wind_speed_10m"]
pair_structs = [F.struct(F.lit(p).alias("pollutant"), F.lit(w).alias("weather_variable"),
                        F.col(p).alias("x"), F.col(w).alias("y"))
                for p in pollutants for w in weather_predictors]
paired = (analysis.select("city", F.explode(F.array(*pair_structs)).alias("pair"))
    .select("city", "pair.*").filter(F.col("x").isNotNull() & F.col("y").isNotNull()))
pair_stats = paired.groupBy("city", "pollutant", "weather_variable").agg(
    F.count("*").alias("paired_hours"), F.stddev_pop("x").alias("std_x"),
    F.stddev_pop("y").alias("std_y"), F.covar_pop("x", "y").alias("covariance"))
correlations = (pair_stats.withColumn("pearson_r",
    F.when((F.col("paired_hours") >= 3) & (F.col("std_x") > 0) & (F.col("std_y") > 0),
           F.col("covariance") / (F.col("std_x") * F.col("std_y"))))
    .withColumn("undefined_reason", F.when(F.col("paired_hours") < 3, "fewer_than_3_pairs")
        .when((F.col("std_x") == 0) | (F.col("std_y") == 0), "constant_variable")
        .otherwise(F.lit(None).cast("string")))
    .select("city", "pollutant", "weather_variable", "paired_hours", "pearson_r", "undefined_reason")
    .withColumn("analysis_year", F.lit(2025)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate and save Gold
# MAGIC Gold tables are derived outputs for this 2025-only study. Rerunning replaces
# MAGIC these six summaries with results from the current validated annual Silver.
# MAGIC The source Bronze/Silver tables are updated by city/hour MERGE above.

# COMMAND ----------

products = {
    "gold_city_daily": (daily, ["city", "event_date_utc"], 4380),
    "gold_city_monthly": (monthly, ["city", "month_utc"], 144),
    "gold_city_ranking": (ranking, ["pm2_5_rank", "city"], 12),
    "gold_hourly_pattern": (hourly_pattern, ["city", "local_hour"], 288),
    "gold_aqi_distribution": (aqi_distribution, ["city", "category_order"], 72),
    "gold_correlations": (correlations, ["city", "pollutant", "weather_variable"], 288),
}
if daily.filter(F.col("hours_available") != 24).limit(1).count():
    raise ValueError("A daily summary lacks its expected 24 UTC hours")
if ranking.filter(F.col("hours_available") != 8760).limit(1).count():
    raise ValueError("A city ranking does not use the full 8760-hour year")
if correlations.filter(F.col("pearson_r").isNotNull() & (F.isnan("pearson_r") | (F.abs("pearson_r") > 1.00000001))).limit(1).count():
    raise ValueError("Invalid correlation coefficient")
if aqi_distribution.groupBy("city").agg(F.sum("hours").alias("n")).filter("n != 8760").limit(1).count():
    raise ValueError("AQI category counts do not cover the year")

# Validate every product before publishing any of them.
gold_counts = {}
for name, (df, order_columns, expected_count) in products.items():
    count = df.count()
    if count != expected_count:
        raise ValueError(f"{name}: expected {expected_count} rows, found {count}")
    if df.groupBy(*order_columns).count().filter("count > 1").limit(1).count():
        raise ValueError(f"{name}: duplicate summary keys")
    gold_counts[name] = count
for name, (df, _, _) in products.items():
    df.write.format("delta").mode("overwrite").saveAsTable(f"{SCHEMA}.{name}")
    print("GOLD SAVED |", name, "|", gold_counts[name], "rows", flush=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Export the dashboard bundle
# MAGIC Exports only compact summaries and metadata. The dashboard does not need
# MAGIC to download the entire Silver dataset. This is historical analysis only;
# MAGIC live weather, air-quality forecasts and time-window comparison come next.

# COMMAND ----------

def json_ready(value):
    if isinstance(value, dict):
        return {str(k): json_ready(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_ready(v) for v in value]
    if isinstance(value, datetime):
        return value.isoformat()
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError("Non-finite number encountered while exporting dashboard data")
    return value

bundle = {
    "schema_version": 1,
    "metadata": {
        "project": "Air Quality and Outdoor Planning", "study_year": 2025,
        "start_date_utc": START_DATE, "end_date_utc": END_DATE,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "ingestion_run_id": INGESTION_RUN_ID, "build_id": BUILD_ID,
        "source_manifest": str(MANIFEST_PATH), "city_count": 12,
        "hourly_record_count": counts["silver_city_hourly"],
        "daily_monthly_time_basis": "UTC", "hourly_pattern_time_basis": "city local time",
        "ranking_metric": "annual mean PM2.5; lower concentrations rank first",
        "poor_or_worse_definition": "hourly European AQI > 60",
        "units": EXPECTED_UNITS,
        "sources": [
            {"name": "Open-Meteo Air Quality API / CAMS", "url": "https://open-meteo.com/en/docs/air-quality-api"},
            {"name": "Open-Meteo Historical Weather API", "url": "https://open-meteo.com/en/docs/historical-weather-api"},
            {"name": "Open-Meteo Geocoding / GeoNames", "url": "https://open-meteo.com/en/docs/geocoding-api"},
        ],
        "limitations": [
            "Air quality represents regional model estimates, not city street-level sensor observations.",
            "The 12 purposively selected cities do not represent all world cities.",
            "One year describes 2025 patterns; it does not establish a long-term trend.",
            "Historical averages do not forecast today's or tomorrow's conditions.",
            "Correlation is descriptive; seasonality and serial dependence are not controlled, and causation is not established.",
            "Air-quality model coverage and grid resolution differ across regions.",
            "UTC daily/monthly boundaries differ from local calendar periods. Local-hour patterns include timezone and daylight-saving effects.",
        ],
    },
    "cities": [r.asDict() for r in city_df.orderBy("city").collect()],
    "quality": quality_report,
    "gold_row_counts": gold_counts,
}
export_names = {"gold_city_daily": "daily", "gold_city_monthly": "monthly", "gold_city_ranking": "ranking",
                "gold_hourly_pattern": "hourly_pattern", "gold_aqi_distribution": "aqi_distribution", "gold_correlations": "correlations"}
for table_name, (_, order_columns, _) in products.items():
    bundle[export_names[table_name]] = [r.asDict(recursive=True) for r in spark.table(f"{SCHEMA}.{table_name}").orderBy(*order_columns).collect()]

export_path = ROOT / "exports" / f"dashboard_2025_{BUILD_ID}.json"
with export_path.open("x", encoding="utf-8") as handle:
    json.dump(json_ready(bundle), handle, ensure_ascii=False, allow_nan=False, separators=(",", ":"))
# Verify the actual file, not only the in-memory bundle.
reopened = json.loads(export_path.read_text(encoding="utf-8"))
if reopened["metadata"]["ingestion_run_id"] != INGESTION_RUN_ID:
    raise ValueError("Export run identity mismatch")
for table_name, export_name in export_names.items():
    if len(reopened[export_name]) != gold_counts[table_name]:
        raise ValueError(f"Exported row count mismatch for {export_name}")

print("\nANNUAL DATA AND GOLD SUMMARY")
print(json.dumps({"status": "PASS", "ingestion_run_id": INGESTION_RUN_ID, "build_id": BUILD_ID,
    "bronze_silver_counts": counts, "gold_counts": gold_counts,
    "dashboard_export_path": str(export_path), "dashboard_export_bytes": export_path.stat().st_size}, indent=2))
print("NEXT: share this summary and download/upload the dashboard JSON file so the web application can use your actual results.")
display(spark.table(f"{SCHEMA}.gold_city_ranking").select("city", "pm2_5_rank", "average_pm2_5", "average_european_aqi", "poor_or_worse_percent").orderBy("pm2_5_rank", "city"))