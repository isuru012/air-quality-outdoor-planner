# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Register cities and ingest the seven-day historical pilot
# MAGIC **Period:** 1-7 January 2025, inclusive, requested in UTC.
# MAGIC **Scope:** 12 cities, seven air-quality variables and eight weather variables.
# MAGIC Each source should return 168 hourly timestamps per city (2,016 per source).
# MAGIC This notebook stores raw responses and ingestion logs. Bronze/Silver tables
# MAGIC come next. Modelled source values are not local sensor observations.
# MAGIC
# MAGIC API references: https://open-meteo.com/en/docs/geocoding-api,
# MAGIC https://open-meteo.com/en/docs/air-quality-api,
# MAGIC https://open-meteo.com/en/docs/historical-weather-api.
# MAGIC Attribution: Open-Meteo; GeoNames for locations; CAMS for air-quality data.

# COMMAND ----------

import hashlib
import json
import math
import time
import unicodedata
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

SCHEMA = "workspace.air_quality_outdoor_isuru"
ROOT = Path("/Volumes/workspace/air_quality_outdoor_isuru/project_files")
START_DATE = "2025-01-01"
END_DATE = "2025-01-07"
RUN_ID = uuid.uuid4().hex
AIR_VARIABLES = [
    "pm2_5", "pm10", "nitrogen_dioxide", "ozone", "sulphur_dioxide",
    "carbon_monoxide", "european_aqi",
]
WEATHER_VARIABLES = [
    "temperature_2m", "relative_humidity_2m", "precipitation", "surface_pressure",
    "cloud_cover", "wind_speed_10m", "wind_direction_10m", "shortwave_radiation",
]
CITY_SPECS = [
    ("Colombo", "LK"), ("Kandy", "LK"), ("Galle", "LK"),
    ("Jaffna", "LK"), ("Kurunegala", "LK"), ("Anuradhapura", "LK"),
    ("Delhi", "IN"), ("Beijing", "CN"), ("Singapore", "SG"),
    ("Dubai", "AE"), ("London", "GB"), ("Sydney", "AU"),
]
# Verified against Open-Meteo geocoding responses on 2026-09-13.
# These names also match small settlements. Pin the intended metropolitan city:
# Delhi in the National Capital Territory and Beijing in Beijing Municipality.
GEOCODING_ID_OVERRIDES = {"Delhi": 1273294, "Beijing": 1816670}

spark.sql("SET TIME ZONE 'UTC'")
if not ROOT.is_dir():
    raise RuntimeError("Project Volume not found. Run 01_project_storage_setup first.")

start = datetime.fromisoformat(START_DATE)
stop = datetime.fromisoformat(END_DATE) + timedelta(days=1)
EXPECTED_TIMES = [
    (start + timedelta(hours=h)).strftime("%Y-%m-%dT%H:%M")
    for h in range(int((stop - start).total_seconds() // 3600))
]
print("Run ID:", RUN_ID)
print("Schema:", SCHEMA)
print("Expected hours per city and source:", len(EXPECTED_TIMES))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reusable request and validation functions
# MAGIC Retry only connection failures, rate limits and server failures. Keep
# MAGIC missing values as null. A response with missing values is flagged WARNING;
# MAGIC a missing variable, all-null variable or wrong time coverage fails.

# COMMAND ----------

def fetch_json(url, params, attempts=3):
    request_url = url + "?" + urlencode(params)
    for attempt in range(attempts):
        try:
            request = Request(request_url, headers={"User-Agent": "AirQualityOutdoorPlanner-StudentProject/1.0"})
            with urlopen(request, timeout=45) as response:
                payload = json.loads(response.read().decode("utf-8"))
            if not isinstance(payload, dict):
                raise ValueError("Expected one JSON response object")
            if payload.get("error"):
                raise ValueError(str(payload.get("reason", "API returned an error")))
            return payload
        except HTTPError as exc:
            if exc.code != 429 and not 500 <= exc.code < 600:
                detail = exc.read().decode("utf-8", errors="replace")[:250]
                raise RuntimeError(f"HTTP {exc.code}: {detail}") from exc
            if attempt == attempts - 1:
                raise
        except (URLError, TimeoutError, ConnectionError):
            if attempt == attempts - 1:
                raise
        time.sleep(2 ** (attempt + 1))


def normalized(text):
    return "".join(
        c for c in unicodedata.normalize("NFKD", text.casefold())
        if not unicodedata.combining(c)
    ).strip()


def validate_hourly(payload, variables, expected_times):
    if payload.get("utc_offset_seconds") != 0:
        raise ValueError("API response is not using UTC")
    hourly = payload.get("hourly", {})
    times = hourly.get("time", [])
    if times != expected_times:
        raise ValueError(f"Expected {len(expected_times)} exact UTC hours; received {len(times)} timestamps. Check coverage, order and duplicates.")
    units = payload.get("hourly_units", {})
    missing = {}
    for variable in variables:
        values = hourly.get(variable)
        if not isinstance(values, list) or len(values) != len(expected_times):
            raise ValueError(f"Missing or misaligned variable: {variable}")
        if variable not in units:
            raise ValueError(f"Missing unit metadata: {variable}")
        missing[variable] = sum(v is None for v in values)
        if missing[variable] == len(expected_times):
            raise ValueError(f"All values are missing: {variable}")
        if any(v is not None and (isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v)) for v in values):
            raise ValueError(f"Non-finite or non-numeric value: {variable}")
    return missing


def save_new_json(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    # Unique or content-addressed filenames preserve earlier raw responses.
    with path.open("x", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, allow_nan=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resolve and store the agreed cities
# MAGIC Select only an exact normalized city-name match, matching country, and a
# MAGIC populated-place feature. Ambiguous results stop here and print candidates.
# MAGIC No city is silently replaced by the first search result.
# MAGIC Existing metadata is reused after checking its city list and coordinates.

# COMMAND ----------

CITY_DDL = """city STRING, country_code STRING, country STRING,
comparison_group STRING, latitude DOUBLE, longitude DOUBLE, timezone STRING,
geoname_id BIGINT, source_name STRING, admin1 STRING"""
city_table = f"{SCHEMA}.city_metadata"
spark.sql(f"CREATE TABLE IF NOT EXISTS {city_table} ({CITY_DDL}) USING DELTA")
cities = [r.asDict() for r in spark.table(city_table).collect()]

if not cities:
    candidates_by_city = []
    resolved = []
    resolution_errors = []
    for city_name, country_code in CITY_SPECS:
        try:
            payload = fetch_json("https://geocoding-api.open-meteo.com/v1/search", {
                "name": city_name, "countryCode": country_code, "count": 100,
                "language": "en", "format": "json",
            })
            candidates = payload.get("results", [])
            candidates_by_city.append({"city": city_name, "country_code": country_code, "response": payload})
            matches = [r for r in candidates if
                r.get("country_code") == country_code
                and normalized(r.get("name", "")) == normalized(city_name)
                and r.get("feature_code", "").startswith("PPL")]
            if city_name in GEOCODING_ID_OVERRIDES:
                matches = [r for r in candidates if r.get("id") == GEOCODING_ID_OVERRIDES[city_name]
                           and r.get("country_code") == country_code
                           and r.get("feature_code", "").startswith("PPL")]
            if len(matches) != 1:
                summary = [{k: r.get(k) for k in ("id", "name", "country_code", "admin1", "feature_code", "latitude", "longitude")} for r in candidates]
                print(city_name, "CANDIDATES:", json.dumps(summary, ensure_ascii=False))
                raise ValueError(f"Expected one unambiguous city match, found {len(matches)}")
            r = matches[0]
            resolved.append({
                "city": city_name, "country_code": country_code, "country": r["country"],
                "comparison_group": "Sri Lanka" if country_code == "LK" else "International",
                "latitude": float(r["latitude"]), "longitude": float(r["longitude"]),
                "timezone": r["timezone"], "geoname_id": int(r["id"]),
                "source_name": r["name"], "admin1": r.get("admin1", ""),
            })
            print("RESOLVED |", city_name, "|", r["latitude"], r["longitude"], "|", r["timezone"])
        except Exception as exc:
            resolution_errors.append(f"{city_name}: {type(exc).__name__}: {exc}")
        time.sleep(0.15)
    save_new_json(ROOT / "metadata" / f"geocoding_{RUN_ID}.json", candidates_by_city)
    if resolution_errors:
        raise RuntimeError("City registration stopped. Share these errors/candidates:\n" + "\n".join(resolution_errors))
    cities = resolved

if len(cities) != 12 or {(c["city"], c["country_code"]) for c in cities} != set(CITY_SPECS):
    raise ValueError("City metadata must contain exactly the agreed 12 city-country pairs; existing data were not overwritten")
for c in cities:
    if not (math.isfinite(c["latitude"]) and -90 <= c["latitude"] <= 90
            and math.isfinite(c["longitude"]) and -180 <= c["longitude"] <= 180):
        raise ValueError(f"Invalid coordinates for {c['city']}")
    ZoneInfo(c["timezone"])

if spark.table(city_table).count() == 0:
    spark.createDataFrame(cities, schema=CITY_DDL).write.mode("append").saveAsTable(city_table)
# Ingestion always reads the registered Delta metadata.
cities = [r.asDict() for r in spark.table(city_table).orderBy("city").collect()]
if len(cities) != 12:
    raise RuntimeError("Unexpected city count after registration; do not run this notebook concurrently")
display(spark.table(city_table).orderBy("city"))
print("PASS | City metadata | 12 cities")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fetch and retain raw pilot responses
# MAGIC Each JSON file contains a metadata envelope and the complete API response
# MAGIC under `response`. Filenames include a hash of endpoint and request parameters.
# MAGIC Matching cached files are revalidated and reused; malformed cache is flagged.
# MAGIC Each attempt has a separate JSON log. Failed responses remain available for
# MAGIC diagnosis. Historical air data are archived model products, not ground truth.

# COMMAND ----------

LOG_DDL = """run_id STRING, source STRING, city STRING, start_date STRING,
end_date STRING, status STRING, row_count BIGINT, from_cache BOOLEAN,
missing_values_json STRING, file_path STRING, error_message STRING,
logged_at_utc TIMESTAMP"""
log_table = f"{SCHEMA}.ingestion_log"
spark.sql(f"CREATE TABLE IF NOT EXISTS {log_table} ({LOG_DDL}) USING DELTA")
SOURCES = [
    ("air_quality", "https://air-quality-api.open-meteo.com/v1/air-quality", AIR_VARIABLES, {}),
    ("weather", "https://archive-api.open-meteo.com/v1/archive", WEATHER_VARIABLES,
     {"temperature_unit": "celsius", "wind_speed_unit": "ms", "precipitation_unit": "mm"}),
]
logs = []
for city in cities:
    for source, url, variables, extra in SOURCES:
        params = {
            "latitude": city["latitude"], "longitude": city["longitude"],
            "start_date": START_DATE, "end_date": END_DATE, "timezone": "UTC",
            "hourly": ",".join(variables), **extra,
        }
        fingerprint = hashlib.sha256(json.dumps({"url": url, "params": params}, sort_keys=True).encode()).hexdigest()[:20]
        file_path = ROOT / "raw" / "historical" / source / f"{city['geoname_id']}_{START_DATE}_{END_DATE}_{fingerprint}.json"
        log = {
            "run_id": RUN_ID, "source": source, "city": city["city"],
            "start_date": START_DATE, "end_date": END_DATE, "status": "FAILED",
            "row_count": 0, "from_cache": False, "missing_values_json": "{}",
            "file_path": str(file_path), "error_message": "",
            "logged_at_utc": datetime.now(timezone.utc).replace(tzinfo=None),
        }
        try:
            if file_path.exists():
                envelope = json.loads(file_path.read_text(encoding="utf-8"))
                log["from_cache"] = True
                if (envelope["metadata"]["request_url"] != url
                    or envelope["metadata"]["request_params"] != params
                    or envelope["metadata"]["city"] != city["city"]):
                    raise ValueError("Cached metadata does not match this request")
            else:
                payload = fetch_json(url, params)
                envelope = {
                    "metadata": {
                        "format_version": 1, "run_id": RUN_ID, "source": source,
                        "data_kind": "historical", "city": city["city"],
                        "geoname_id": city["geoname_id"], "city_timezone": city["timezone"],
                        "request_url": url, "request_params": params,
                        "retrieved_at_utc": datetime.now(timezone.utc).isoformat(),
                        "request_fingerprint": fingerprint,
                    },
                    "response": payload,
                }
                save_new_json(file_path, envelope)
            missing = validate_hourly(envelope["response"], variables, EXPECTED_TIMES)
            log["row_count"] = len(EXPECTED_TIMES)
            log["missing_values_json"] = json.dumps(missing, sort_keys=True)
            log["status"] = "WARNING" if any(missing.values()) else "SUCCESS"
        except Exception as exc:
            log["error_message"] = f"{type(exc).__name__}: {str(exc)[:800]}"
        logs.append(log)
        # Persist progress without requiring a Spark job for every request.
        durable_log = {**log, "logged_at_utc": log["logged_at_utc"].isoformat() + "Z"}
        save_new_json(ROOT / "logs" / f"pilot_{RUN_ID}_{source}_{city['geoname_id']}.json", durable_log)
        print(f"{log['status']} | {source} | {city['city']} | hours={log['row_count']} | cache={log['from_cache']} | {log['error_message']}")
        time.sleep(0.15)

log_df = spark.createDataFrame(logs, schema=LOG_DDL)
log_df.write.mode("append").saveAsTable(log_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pilot result
# MAGIC There are 2,016 air-quality rows and 2,016 weather rows when complete.
# MAGIC They describe the same city-hours, so the eventual joined table should have
# MAGIC 2,016 rows, not 4,032. Raw files are not Bronze or Silver tables yet.
# MAGIC Range checks and join checks will be added in the next stage.

# COMMAND ----------

display(log_df.select("source", "city", "status", "row_count", "from_cache", "missing_values_json", "error_message").orderBy("source", "city"))
print("PILOT SUMMARY | Run ID:", RUN_ID)
print("Registered cities:", len(cities))
print("Requests checked:", len(logs))
for source, _, _, _ in SOURCES:
    source_rows = sum(r["row_count"] for r in logs if r["source"] == source)
    print(f"{source}: {source_rows} hourly records; expected {12 * len(EXPECTED_TIMES)}")
print("Reused cached responses:", sum(r["from_cache"] for r in logs))
failed = [r for r in logs if r["status"] == "FAILED"]
warnings = [r for r in logs if r["status"] == "WARNING"]
print("Failed requests:", len(failed))
print("Requests with missing values:", len(warnings))
if failed:
    raise RuntimeError("PILOT INCOMPLETE: share the failed rows above. Full-year ingestion has not started.")
if warnings:
    print("PILOT NEEDS REVIEW: all timestamps exist, but missing values need review before full-year ingestion.")
else:
    print("PILOT INGESTION PASSED: 12 cities, 24 source responses, complete requested timestamps and variables.")
print("NEXT: build and validate Bronze and Silver using these raw files.")