# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Full-year historical ingestion: 2025
# MAGIC Downloads January-December in monthly batches for the registered 12 cities.
# MAGIC Expected: 288 city/month/source responses and 105,120 hourly records per source.
# MAGIC Reuses validated monthly raw files on rerun. Logs each response immediately
# MAGIC and checkpoints each month in Delta. Stops on the first failed response.
# MAGIC This notebook does not alter the validated pilot Bronze/Silver tables.
# MAGIC Full-year Bronze/Silver processing follows this ingestion stage.
# MAGIC
# MAGIC Sources: Open-Meteo Air Quality and Historical Weather APIs.
# MAGIC https://open-meteo.com/en/docs/air-quality-api
# MAGIC https://open-meteo.com/en/docs/historical-weather-api
# MAGIC Monthly requests can count as multiple API calls under Open-Meteo quotas.
# MAGIC https://open-meteo.com/en/pricing
# MAGIC Keep one notebook run active at a time.

# COMMAND ----------

import calendar
import hashlib
import json
import math
import time
import uuid
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

SCHEMA = "workspace.air_quality_outdoor_isuru"
ROOT = Path("/Volumes/workspace/air_quality_outdoor_isuru/project_files")
YEAR = 2025
RUN_ID = uuid.uuid4().hex
PILOT_REPORT = ROOT / "logs" / "bronze_silver_quality_1b80d380684a4b439cda4d8eb29d2396.json"
AIR = ["pm2_5", "pm10", "nitrogen_dioxide", "ozone", "sulphur_dioxide", "carbon_monoxide", "european_aqi"]
WEATHER = ["temperature_2m", "relative_humidity_2m", "precipitation", "surface_pressure", "cloud_cover", "wind_speed_10m", "wind_direction_10m", "shortwave_radiation"]
EXPECTED_CITIES = {
    "Colombo": "LK", "Kandy": "LK", "Galle": "LK", "Jaffna": "LK", "Kurunegala": "LK", "Anuradhapura": "LK",
    "Delhi": "IN", "Beijing": "CN", "Singapore": "SG", "Dubai": "AE", "London": "GB", "Sydney": "AU",
}
UNITS = {**{v: "μg/m³" for v in AIR if v != "european_aqi"}, "european_aqi": "EAQI",
         "temperature_2m": "°C", "relative_humidity_2m": "%", "precipitation": "mm",
         "surface_pressure": "hPa", "cloud_cover": "%", "wind_speed_10m": "m/s",
         "wind_direction_10m": "°", "shortwave_radiation": "W/m²"}
SOURCES = [
    ("air_quality", "https://air-quality-api.open-meteo.com/v1/air-quality", AIR, {}),
    ("weather", "https://archive-api.open-meteo.com/v1/archive", WEATHER,
     {"temperature_unit": "celsius", "wind_speed_unit": "ms", "precipitation_unit": "mm"}),
]
spark.sql("SET TIME ZONE 'UTC'")

pilot = json.loads(PILOT_REPORT.read_text(encoding="utf-8"))
if (pilot.get("status") != "PASS" or pilot.get("build_id") != "1b80d380684a4b439cda4d8eb29d2396"
    or pilot.get("start_date") != "2025-01-01" or pilot.get("end_date") != "2025-01-07"
    or pilot.get("counts", {}).get("silver_city_hourly") != 2016):
    raise RuntimeError("The verified pilot quality report is missing or does not match the successful pilot")

cities = [r.asDict() for r in spark.table(f"{SCHEMA}.city_metadata").orderBy("city").collect()]
if len(cities) != 12 or {c["city"]: c["country_code"] for c in cities} != EXPECTED_CITIES:
    raise ValueError("Expected the registered 12 city-country pairs")
for c in cities:
    if not (-90 <= c["latitude"] <= 90 and -180 <= c["longitude"] <= 180):
        raise ValueError(f"Invalid coordinates for {c['city']}")
    ZoneInfo(c["timezone"])
print("Full-year run ID:", RUN_ID)
print("Pilot quality gate: PASS")
print("City count:", len(cities))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Request, validation and checkpoint helpers
# MAGIC Retry temporary network/server errors at most three times. Respect a short
# MAGIC Retry-After delay; stop if the provider requests a longer wait. Source nulls
# MAGIC remain null and produce warnings; invalid structure, all-null variables,
# MAGIC unexpected units or missing hours stop ingestion for review.

# COMMAND ----------

def month_window(year, month):
    start = datetime(year, month, 1)
    last = calendar.monthrange(year, month)[1]
    end = datetime(year, month, last)
    times = [(start + timedelta(hours=i)).strftime("%Y-%m-%dT%H:%M") for i in range(last * 24)]
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"), times


def save_new(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x", encoding="utf-8") as handle:
        json.dump(data, handle, ensure_ascii=False, allow_nan=False, indent=2)


def request_json(url, params, attempts=3):
    request = Request(url + "?" + urlencode(params), headers={"User-Agent": "AirQualityOutdoorPlanner-StudentProject/1.0"})
    for attempt in range(attempts):
        wait = 2 ** (attempt + 1)
        try:
            with urlopen(request, timeout=45) as response:
                data = json.loads(response.read().decode("utf-8"))
            if not isinstance(data, dict) or data.get("error"):
                raise ValueError(f"Invalid API response: {str(data)[:250]}")
            return data
        except HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")[:300]
            if exc.code != 429 and not 500 <= exc.code < 600:
                raise RuntimeError(f"HTTP {exc.code}: {detail}") from exc
            retry_after = exc.headers.get("Retry-After")
            if retry_after:
                try:
                    requested_wait = float(retry_after)
                except ValueError:
                    requested_wait = (parsedate_to_datetime(retry_after) - datetime.now(timezone.utc)).total_seconds()
                if requested_wait > 30:
                    raise RuntimeError(f"HTTP {exc.code}: provider asks to wait {requested_wait:.0f} seconds. Stop and rerun later; completed files will be reused. {detail}") from exc
                wait = max(wait, max(0, requested_wait))
            if attempt == attempts - 1:
                raise RuntimeError(f"HTTP {exc.code} after {attempts} attempts: {detail}") from exc
        except (URLError, TimeoutError, ConnectionError):
            if attempt == attempts - 1:
                raise
        print(f"Temporary request failure. Retry {attempt + 2}/{attempts} in {wait:.0f}s.", flush=True)
        time.sleep(wait)


def validate_response(data, variables, expected_times):
    if data.get("utc_offset_seconds") != 0:
        raise ValueError("Response is not in UTC")
    hourly = data.get("hourly", {})
    if hourly.get("time") != expected_times:
        raise ValueError("Response does not contain exactly the requested monthly UTC hours")
    missing = {}
    for v in variables:
        actual_unit = str(data.get("hourly_units", {}).get(v, "")).replace("µ", "μ")
        if actual_unit != UNITS[v]:
            raise ValueError(f"Unexpected unit for {v}: {actual_unit!r}")
        values = hourly.get(v)
        if not isinstance(values, list) or len(values) != len(expected_times):
            raise ValueError(f"Missing or misaligned variable {v}")
        missing[v] = sum(x is None for x in values)
        if missing[v] == len(expected_times):
            raise ValueError(f"All values are missing for {v}")
        if any(x is not None and (isinstance(x, bool) or not isinstance(x, (int, float)) or not math.isfinite(x)) for x in values):
            raise ValueError(f"Non-numeric or non-finite value in {v}")
    return missing


def validate_cache(envelope, city, source, url, params, fingerprint):
    meta = envelope.get("metadata", {})
    if (meta.get("format_version") != 1 or meta.get("data_kind") != "historical"
        or meta.get("city") != city["city"] or meta.get("geoname_id") != city["geoname_id"]
        or meta.get("city_timezone") != city["timezone"] or meta.get("source") != source
        or meta.get("request_url") != url or meta.get("request_params") != params
        or meta.get("request_fingerprint") != fingerprint):
        raise ValueError("Cached raw metadata does not match the requested city, source and month. File preserved for review.")
    stamp = datetime.fromisoformat(meta["retrieved_at_utc"].replace("Z", "+00:00"))
    if stamp.tzinfo is None:
        raise ValueError("Missing timezone in raw retrieval timestamp")

LOG_DDL = """run_id STRING, source STRING, city STRING, start_date STRING,
end_date STRING, status STRING, row_count BIGINT, from_cache BOOLEAN,
missing_values_json STRING, file_path STRING, error_message STRING,
logged_at_utc TIMESTAMP"""
log_table = f"{SCHEMA}.ingestion_log"
spark.sql(f"CREATE TABLE IF NOT EXISTS {log_table} ({LOG_DDL}) USING DELTA")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest monthly batches
# MAGIC Files use the same envelope and hashed request naming as the pilot.
# MAGIC January's full-month response is separate from the seven-day pilot.
# MAGIC A frozen manifest identifies the exact monthly files to process next, so
# MAGIC overlapping pilot files will not be counted again in the annual dataset.

# COMMAND ----------

all_logs = []
month_summaries = []
aborted = False
for month in range(1, 13):
    start_date, end_date, expected_times = month_window(YEAR, month)
    print(f"\nMONTH {month}/12 | {start_date} to {end_date} | {len(expected_times)} hours per city/source", flush=True)
    month_logs = []
    for city in cities:
        for source, url, variables, extra in SOURCES:
            params = {"latitude": city["latitude"], "longitude": city["longitude"],
                      "start_date": start_date, "end_date": end_date,
                      "timezone": "UTC", "hourly": ",".join(variables), **extra}
            fingerprint = hashlib.sha256(json.dumps({"url": url, "params": params}, sort_keys=True).encode()).hexdigest()[:20]
            path = ROOT / "raw" / "historical" / source / f"{city['geoname_id']}_{start_date}_{end_date}_{fingerprint}.json"
            log = {"run_id": RUN_ID, "source": source, "city": city["city"],
                   "start_date": start_date, "end_date": end_date, "status": "FAILED",
                   "row_count": 0, "from_cache": False, "missing_values_json": "{}",
                   "file_path": str(path), "error_message": "", "logged_at_utc": datetime.now(timezone.utc)}
            try:
                if path.exists():
                    envelope = json.loads(path.read_text(encoding="utf-8"))
                    log["from_cache"] = True
                else:
                    data = request_json(url, params)
                    envelope = {"metadata": {
                        "format_version": 1, "run_id": RUN_ID, "source": source,
                        "data_kind": "historical", "city": city["city"],
                        "geoname_id": city["geoname_id"], "city_timezone": city["timezone"],
                        "request_url": url, "request_params": params,
                        "retrieved_at_utc": datetime.now(timezone.utc).isoformat(),
                        "request_fingerprint": fingerprint}, "response": data}
                    # Preserve the original response even if later validation fails.
                    save_new(path, envelope)
                validate_cache(envelope, city, source, url, params, fingerprint)
                missing = validate_response(envelope["response"], variables, expected_times)
                log["missing_values_json"] = json.dumps(missing, sort_keys=True)
                log["row_count"] = len(expected_times)
                log["status"] = "WARNING" if any(missing.values()) else "SUCCESS"
            except Exception as exc:
                log["error_message"] = f"{type(exc).__name__}: {str(exc)[:1000]}"
                aborted = True
            month_logs.append(log)
            all_logs.append(log)
            durable_log = {**log, "logged_at_utc": log["logged_at_utc"].isoformat()}
            save_new(ROOT / "logs" / f"backfill_{RUN_ID}_{month:02d}_{source}_{city['geoname_id']}.json", durable_log)
            print(f"{log['status']} | {source} | {city['city']} | {log['row_count']} hours | cache={log['from_cache']} | {log['error_message']}", flush=True)
            if aborted:
                break
            time.sleep(0.4)
        if aborted:
            break
    spark.createDataFrame(month_logs, schema=LOG_DDL).write.mode("append").saveAsTable(log_table)
    month_summary = {"month": month, "start_date": start_date, "end_date": end_date,
                     "responses_checked": len(month_logs),
                     "air_hours": sum(r["row_count"] for r in month_logs if r["source"] == "air_quality"),
                     "weather_hours": sum(r["row_count"] for r in month_logs if r["source"] == "weather"),
                     "failures": sum(r["status"] == "FAILED" for r in month_logs),
                     "warnings": sum(r["status"] == "WARNING" for r in month_logs)}
    month_summaries.append(month_summary)
    print("MONTH CHECKPOINT:", json.dumps(month_summary), flush=True)
    if aborted:
        print("Stopped after a failure. Completed responses are retained for the next run.", flush=True)
        break

# COMMAND ----------

# MAGIC %md
# MAGIC ## Full-year summary and input manifest
# MAGIC The next transformation step must use this manifest's 288 monthly files,
# MAGIC rather than globbing all historical files (which would include the pilot).
# MAGIC Partial runs are marked INCOMPLETE and cannot qualify as a full-year dataset.

# COMMAND ----------

expected_hours = (datetime(YEAR + 1, 1, 1) - datetime(YEAR, 1, 1)).days * 24 * len(cities)
failures = [r for r in all_logs if r["status"] == "FAILED"]
warnings = [r for r in all_logs if r["status"] == "WARNING"]
totals = {source: sum(r["row_count"] for r in all_logs if r["source"] == source) for source, _, _, _ in SOURCES}
unique_inputs = {(r["city"], r["source"], r["start_date"], r["end_date"]) for r in all_logs}
complete = (not failures and len(all_logs) == 288 and len(unique_inputs) == 288
            and all(total == expected_hours for total in totals.values()))
summary = {
    "run_id": RUN_ID, "year": YEAR, "registered_cities": len(cities),
    "responses_checked": len(all_logs), "expected_responses": 288,
    "completed_months": sum(m["responses_checked"] == 24 and m["failures"] == 0 for m in month_summaries),
    "hourly_records_by_source": totals, "expected_hours_per_source": expected_hours,
    "reused_cached_responses": sum(r["from_cache"] for r in all_logs),
    "failed_responses": len(failures), "responses_with_missing_values": len(warnings),
    "status": "INCOMPLETE" if not complete else ("REVIEW_REQUIRED" if warnings else "PASS"),
    "checked_at_utc": datetime.now(timezone.utc).isoformat(),
}
manifest_path = ROOT / "metadata" / f"backfill_{YEAR}_{RUN_ID}.json"
manifest = {"format_version": 1, "summary": summary, "city_metadata": cities,
            "months": month_summaries,
            "files": [{**r, "logged_at_utc": r["logged_at_utc"].isoformat()} for r in all_logs]}
save_new(manifest_path, manifest)
print("\nFULL-YEAR INGESTION SUMMARY")
print(json.dumps(summary, indent=2))
print("MANIFEST PATH:", manifest_path)
display(spark.createDataFrame(month_summaries).orderBy("month"))
if failures:
    display(spark.createDataFrame(failures, schema=LOG_DDL).select("city", "source", "start_date", "error_message"))
if warnings:
    display(spark.createDataFrame(warnings, schema=LOG_DDL).select("city", "source", "start_date", "missing_values_json"))
if not complete:
    raise RuntimeError("Full-year ingestion is incomplete. Share the summary and failed response. Successful monthly files will be reused on rerun.")
print("NEXT: share the full-year summary and manifest path before building annual Bronze/Silver tables.")