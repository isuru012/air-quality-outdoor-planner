# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Air Quality Outdoor Planner: environment check
# MAGIC Run all cells on Databricks serverless compute.
# MAGIC This notebook lists visible catalogs and makes five small API requests.
# MAGIC It does not create tables, modify permissions, or save raw data.
# MAGIC PASS means the requested sample has a valid structure and non-null values.
# MAGIC It does not establish full-year coverage or storage write permissions.

# COMMAND ----------

import requests
from datetime import datetime, timezone

results = []

def record(check, status, details):
    results.append((check, status, str(details)))
    print(f"{status} | {check} | {details}")

print("Checked at:", datetime.now(timezone.utc).isoformat())

try:
    context = spark.sql(
        "SELECT current_catalog() AS catalog, current_schema() AS schema"
    ).first()
    print("Current catalog:", context["catalog"])
    print("Current schema:", context["schema"])
    catalogs = [row[0] for row in spark.sql("SHOW CATALOGS").collect()]
    record("Catalog visibility", "PASS", ", ".join(catalogs))
except Exception as exc:
    record("Catalog visibility", "FAIL", f"{type(exc).__name__}: {str(exc)[:300]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resolve the pilot city
# MAGIC Geocoding supplies the coordinates. The country filter selects Colombo,
# MAGIC Sri Lanka. These are city reference coordinates, not sensor coordinates.

# COMMAND ----------

def fetch_json(url, params):
    response = requests.get(url, params=params, timeout=(10, 30))
    if not response.ok:
        raise RuntimeError(f"HTTP {response.status_code}: {response.text[:240]}")
    payload = response.json()
    if payload.get("error"):
        raise RuntimeError(str(payload.get("reason", "API returned an error")))
    return payload

city = None
try:
    payload = fetch_json(
        "https://geocoding-api.open-meteo.com/v1/search",
        {"name": "Colombo", "count": 10, "language": "en", "format": "json", "countryCode": "LK"},
    )
    candidates = [
        row for row in payload.get("results", [])
        if row.get("country_code") == "LK"
        and row.get("name", "").casefold() == "colombo"
    ]
    if len(candidates) != 1:
        raise ValueError(f"Expected one exact Colombo, LK match; found {len(candidates)}. Review geocoding before proceeding.")
    city = candidates[0]
    if not all(key in city for key in ("latitude", "longitude", "timezone")):
        raise ValueError("City response is missing coordinates or timezone")
    record("Geocoding API", "PASS", f"{city['name']}, {city['country_code']}; latitude={city['latitude']}; longitude={city['longitude']}; timezone={city['timezone']}")
except Exception as exc:
    record("Geocoding API", "FAIL", f"{type(exc).__name__}: {str(exc)[:300]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test current forecasts and historical samples
# MAGIC Both historical tests request 1 January 2025 in UTC: exactly 24 hours.
# MAGIC Forecast samples request 48 hours. Data are model estimates and forecasts.
# MAGIC Historical and forecast records will be kept separate in the pipeline.

# COMMAND ----------

checks = [
    (
        "Weather forecast API",
        "https://api.open-meteo.com/v1/forecast",
        {"hourly": "temperature_2m,precipitation,wind_speed_10m", "forecast_days": 2},
        ["temperature_2m", "precipitation", "wind_speed_10m"],
        48,
    ),
    (
        "Air-quality forecast API",
        "https://air-quality-api.open-meteo.com/v1/air-quality",
        {"hourly": "pm2_5,pm10,european_aqi", "forecast_days": 2},
        ["pm2_5", "pm10", "european_aqi"],
        48,
    ),
    (
        "Historical weather API",
        "https://archive-api.open-meteo.com/v1/archive",
        {"hourly": "temperature_2m,precipitation,wind_speed_10m", "start_date": "2025-01-01", "end_date": "2025-01-01"},
        ["temperature_2m", "precipitation", "wind_speed_10m"],
        24,
    ),
    (
        "Historical air-quality sample",
        "https://air-quality-api.open-meteo.com/v1/air-quality",
        {"hourly": "pm2_5,pm10,european_aqi", "start_date": "2025-01-01", "end_date": "2025-01-01"},
        ["pm2_5", "pm10", "european_aqi"],
        24,
    ),
]

for name, url, parameters, variables, expected_hours in checks:
    if city is None:
        record(name, "SKIP", "Geocoding must succeed first; no coordinates were assumed")
        continue
    try:
        payload = fetch_json(url, {
            "latitude": city["latitude"],
            "longitude": city["longitude"],
            "timezone": "UTC",
            **parameters,
        })
        hourly = payload.get("hourly", {})
        times = hourly.get("time", [])
        if len(times) != expected_hours or len(set(times)) != expected_hours:
            raise ValueError(f"Expected {expected_hours} unique hours; received {len(times)} rows and {len(set(times))} unique timestamps")
        parsed = [datetime.fromisoformat(t) for t in times]
        if any((b - a).total_seconds() != 3600 for a, b in zip(parsed, parsed[1:])):
            raise ValueError("Timestamps are not consecutive hourly values")
        if "start_date" in parameters and (
            times[0] != "2025-01-01T00:00" or times[-1] != "2025-01-01T23:00"
        ):
            raise ValueError(f"Wrong historical period: {times[0]} to {times[-1]}")
        missing = {}
        for variable in variables:
            values = hourly.get(variable)
            if not isinstance(values, list) or len(values) != expected_hours:
                raise ValueError(f"Missing or misaligned variable: {variable}")
            missing[variable] = sum(value is None for value in values)
            if missing[variable] == expected_hours:
                raise ValueError(f"All values are missing for {variable}")
        status = "WARN" if any(missing.values()) else "PASS"
        record(name, status, f"{len(times)} hours; {times[0]} to {times[-1]} UTC; missing={missing}")
    except Exception as exc:
        record(name, "FAIL", f"{type(exc).__name__}: {str(exc)[:300]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary to share
# MAGIC Send this table and the catalog output for the next setup step.
# MAGIC If any test fails, its response determines the fix. Do not change account
# MAGIC permissions or assume an identity-verification step is required from a failure alone.

# COMMAND ----------

print("\nENVIRONMENT CHECK SUMMARY")
for name, status, details in results:
    print(f"{status:4} | {name} | {details}")

try:
    display(spark.createDataFrame(results, ["check", "status", "details"]))
except Exception:
    print("The text summary above remains available if table display is unavailable.")

if all(status == "PASS" for _, status, _ in results):
    print("NEXT: create project storage and run the seven-day pilot with all agreed variables.")
else:
    print("NEXT: review FAIL, WARN, or SKIP rows before starting the full ingestion.")