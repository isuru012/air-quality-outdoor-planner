# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Requested historical analytics: any supported location and dates
# MAGIC Add alongside notebooks 00-05. Uses NEW explorer_* tables in your existing
# MAGIC schema and preserves the original study. First run: Colombo, 1-7 Jan 2024.
# MAGIC Inputs are UTC calendar dates; dates are inclusive. Run one instance at a time.
# MAGIC This notebook does not yet connect the website to Databricks.
# MAGIC Raw ingestion is Python; joins, quality flags and daily aggregates use Spark.

# COMMAND ----------

import hashlib
import json
import math
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from urllib.error import HTTPError
from zoneinfo import ZoneInfo
from pyspark.sql import functions as F, types as T
from delta.tables import DeltaTable

# Editable request inputs. Existing widget values are retained on rerun.
DEFAULTS = {
    'city': 'Colombo', 'country_code': 'LK',
    'latitude': '6.93548', 'longitude': '79.84868', 'timezone': 'Asia/Colombo',
    'start_date': '2024-01-01', 'end_date': '2024-01-07',
}
for name, value in DEFAULTS.items():
    dbutils.widgets.text(name, value)
PARAMS = {name: dbutils.widgets.get(name).strip() for name in DEFAULTS}
SCHEMA = 'workspace.air_quality_outdoor_isuru'
ROOT = Path('/Volumes/workspace/air_quality_outdoor_isuru/project_files')
MAX_REQUEST_DAYS = 366
CHUNK_DAYS = 7
AIR_START = date(2022, 8, 1)
RUN_ID = uuid.uuid4().hex
spark.sql("SET TIME ZONE 'UTC'")

# COMMAND ----------

lat, lon = float(PARAMS['latitude']), float(PARAMS['longitude'])
if not math.isfinite(lat) or not math.isfinite(lon) or not -90 <= lat <= 90 or not -180 <= lon <= 180:
    raise ValueError('Coordinates are invalid')
lat, lon = round(lat, 8), round(lon, 8)
ZoneInfo(PARAMS['timezone'])
START, END = date.fromisoformat(PARAMS['start_date']), date.fromisoformat(PARAMS['end_date'])
if PARAMS['start_date'] != START.isoformat() or PARAMS['end_date'] != END.isoformat():
    raise ValueError('Dates must use YYYY-MM-DD')
DAYS = (END - START).days + 1
if not 1 <= DAYS <= MAX_REQUEST_DAYS or START < date(1940, 1, 1):
    raise ValueError('Choose 1-366 days starting on or after 1940-01-01')
if END >= datetime.now(timezone.utc).date():
    raise ValueError('Choose complete past UTC dates; recent archive data may still be unavailable')
if not PARAMS['city'] or len(PARAMS['city']) > 200:
    raise ValueError('Provide a city label up to 200 characters')
if not ROOT.is_dir():
    raise RuntimeError('Run notebook 01 storage setup first in this workspace')
# Coordinates define identity. City names are labels and need not be globally unique.
LOCATION_ID = hashlib.sha256(f'{lat:.8f},{lon:.8f}'.encode()).hexdigest()[:24]
REQUEST_ID = hashlib.sha256(f'v1|{LOCATION_ID}|{START}|{END}'.encode()).hexdigest()[:24]
EXPECTED = DAYS * 24
AIR = ['pm2_5', 'pm10', 'european_aqi']
WEATHER = ['temperature_2m', 'precipitation', 'wind_speed_10m']
UNITS = {'pm2_5': 'μg/m³', 'pm10': 'μg/m³', 'european_aqi': 'EAQI',
         'temperature_2m': '°C', 'precipitation': 'mm', 'wind_speed_10m': 'm/s'}
print('Request:', REQUEST_ID, 'Location:', PARAMS['city'], 'Dates:', START, END)
print('Expected city-hours:', EXPECTED)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Immutable raw responses and bounded source requests
# MAGIC Cache keys include endpoint and full request parameters. Reruns reuse raw
# MAGIC files. A 429 stops this run so no retry loop consumes the provider allowance.
# MAGIC API budgets in this notebook and the laptop gateway are separate.

# COMMAND ----------

raw_files = []
source_rows = {'air': [], 'weather': []}
cache_hits = 0

def request_source(kind, chunk_start, chunk_end):
    global cache_hits
    keys = AIR if kind == 'air' else WEATHER
    url = ('https://air-quality-api.open-meteo.com/v1/air-quality' if kind == 'air'
           else 'https://archive-api.open-meteo.com/v1/archive')
    # Weather needs the following midnight to align precipitation to [hour,hour+1).
    fetch_end = chunk_end + timedelta(days=1) if kind == 'weather' else chunk_end
    params = {'latitude': lat, 'longitude': lon, 'start_date': str(chunk_start),
              'end_date': str(fetch_end), 'timezone': 'UTC', 'timeformat': 'unixtime',
              'hourly': ','.join(keys)}
    if kind == 'air':
        params['domains'] = 'cams_global'
    else:
        params.update(temperature_unit='celsius', precipitation_unit='mm', wind_speed_unit='ms')
    fingerprint = hashlib.sha256(json.dumps([url, params], sort_keys=True).encode()).hexdigest()
    path = ROOT / 'raw' / 'explorer' / kind / f'{fingerprint}.json'
    if path.exists():
        envelope = json.loads(path.read_text(encoding='utf-8'))
        if envelope.get('request_url') != url or envelope.get('request_params') != params:
            raise ValueError('Cached metadata mismatch; raw file preserved for inspection')
        cache_hits += 1
    else:
        req = Request(url + '?' + urlencode(params), headers={'User-Agent': 'Airwise-CourseProject/1.0'})
        try:
            with urlopen(req, timeout=45) as response:
                payload = json.load(response)
        except HTTPError as exc:
            if exc.code == 429:
                raise RuntimeError(f'Provider rate limit: stop and retry later. Retry-After={exc.headers.get("Retry-After", "not supplied")}') from exc
            raise RuntimeError(f'{kind} provider HTTP {exc.code}; completed raw files remain cached') from exc
        envelope = {'format_version': 1, 'request_url': url, 'request_params': params,
                    'retrieved_at_utc': datetime.now(timezone.utc).isoformat(), 'response': payload}
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open('x', encoding='utf-8') as handle:
            json.dump(envelope, handle, ensure_ascii=False, allow_nan=False)
    data = envelope['response']
    if data.get('error') or data.get('utc_offset_seconds') != 0:
        raise ValueError(f'{kind}: invalid response or non-UTC timestamps')
    first = int(datetime.combine(chunk_start, datetime.min.time(), timezone.utc).timestamp())
    stop = int(datetime.combine(fetch_end + timedelta(days=1), datetime.min.time(), timezone.utc).timestamp())
    times = list(range(first, stop, 3600))
    hourly = data.get('hourly', {})
    if hourly.get('time') != times:
        raise ValueError(f'{kind}: missing, duplicated or unexpected hourly timestamps')
    for key in keys:
        if str(data.get('hourly_units', {}).get(key, '')).replace('µ', 'μ') != UNITS[key]:
            raise ValueError(f'{kind}: unexpected unit for {key}')
        values = hourly.get(key)
        if not isinstance(values, list) or len(values) != len(times):
            raise ValueError(f'{kind}: wrong array length for {key}')
        if any(x is not None and (isinstance(x, bool) or not isinstance(x, (float, int)) or not math.isfinite(x)) for x in values):
            raise ValueError(f'{kind}: invalid numeric data for {key}')
    retrieved = datetime.fromisoformat(envelope['retrieved_at_utc'].replace('Z', '+00:00'))
    if retrieved.tzinfo is None:
        raise ValueError('Raw retrieval timestamp must include a timezone')
    # Keep the extra weather midnight in Bronze for a verifiable rain join.
    keep = ((chunk_end - chunk_start).days + 1) * 24 + (1 if kind == 'weather' else 0)
    for i, stamp in enumerate(times[:keep]):
        row = {'location_id': LOCATION_ID, 'event_time_utc': datetime.fromtimestamp(stamp, timezone.utc),
               'raw_path': str(path), 'retrieved_at_utc': retrieved, 'run_id': RUN_ID}
        row.update({key: None if hourly[key][i] is None else float(hourly[key][i]) for key in keys})
        source_rows[kind].append(row)
    raw_files.append({'source': kind, 'path': str(path), 'fingerprint': fingerprint})

cursor = START
while cursor <= END:
    last = min(cursor + timedelta(days=CHUNK_DAYS-1), END)
    request_source('weather', cursor, last)
    if last >= AIR_START:
        request_source('air', max(cursor, AIR_START), last)
    cursor = last + timedelta(days=1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Delta tables and Spark processing
# MAGIC Merge by coordinates-derived location ID and UTC hour. The existing study
# MAGIC tables remain untouched. Extra weather boundary hours are deduplicated by
# MAGIC retrieval time before merging. At most one notebook/job run should execute.

# COMMAND ----------

from pyspark.sql.window import Window

def source_frame(kind):
    fields = [T.StructField('location_id', T.StringType()), T.StructField('event_time_utc', T.TimestampType()),
              T.StructField('raw_path', T.StringType()), T.StructField('retrieved_at_utc', T.TimestampType()),
              T.StructField('run_id', T.StringType())]
    fields += [T.StructField(k, T.DoubleType()) for k in (AIR if kind == 'air' else WEATHER)]
    df = spark.createDataFrame(source_rows[kind], T.StructType(fields))
    window = Window.partitionBy('location_id', 'event_time_utc').orderBy(F.col('retrieved_at_utc').desc(), F.col('raw_path'))
    return df.withColumn('_rank', F.row_number().over(window)).filter('_rank = 1').drop('_rank')

def merge_table(df, name, keys):
    full = f'{SCHEMA}.{name}'
    view = f'_airwise_{uuid.uuid4().hex}'
    df.createOrReplaceTempView(view)
    try:
        spark.sql(f'CREATE TABLE IF NOT EXISTS {full} USING DELTA AS SELECT * FROM {view} WHERE 1 = 0')
        actual = sorted((x.name, x.dataType.simpleString()) for x in spark.table(full).schema.fields)
        expected = sorted((x.name, x.dataType.simpleString()) for x in df.schema.fields)
        if actual != expected:
            raise ValueError(f'Schema mismatch in {full}; no overwrite performed')
        if df.groupBy(*keys).count().filter('count > 1').limit(1).count():
            raise ValueError(f'Duplicate incoming keys for {full}')
        # Inspect only keys affected by this write; unrelated locations stay intact.
        affected = spark.table(full).join(df.select(*keys), keys, 'inner')
        if affected.groupBy(*keys).count().filter('count > 1').limit(1).count():
            raise ValueError(f'Existing duplicate keys in {full}')
        condition = ' AND '.join(f'target.{key} = source.{key}' for key in keys)
        (DeltaTable.forName(spark, full).alias('target').merge(df.alias('source'), condition)
            .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
    finally:
        spark.catalog.dropTempView(view)

bronze_air, bronze_weather = source_frame('air'), source_frame('weather')
merge_table(bronze_air, 'explorer_bronze_air', ['location_id', 'event_time_utc'])
merge_table(bronze_weather, 'explorer_bronze_weather', ['location_id', 'event_time_utc'])
start_epoch = int(datetime.combine(START, datetime.min.time(), timezone.utc).timestamp())
grid = spark.range(EXPECTED).select(F.lit(LOCATION_ID).alias('location_id'),
    F.timestamp_seconds(F.lit(start_epoch) + F.col('id') * 3600).alias('event_time_utc'))
keys = ['location_id', 'event_time_utc']
# Weather precipitation is the preceding hour's accumulation. Shift only rain.
rain = bronze_weather.select('location_id', (F.col('event_time_utc') - F.expr('INTERVAL 1 HOUR')).alias('event_time_utc'), F.col('precipitation').alias('precipitation'))
weather = bronze_weather.select(*keys, 'temperature_2m', 'wind_speed_10m', F.col('raw_path').alias('weather_raw_path'))
air = bronze_air.select(*keys, *AIR, F.col('raw_path').alias('air_raw_path'))
joined = grid.join(weather, keys, 'left').join(air, keys, 'left').join(rain, keys, 'left')
joined = (joined.withColumn('city', F.lit(PARAMS['city'])).withColumn('country_code', F.lit(PARAMS['country_code']))
    .withColumn('latitude', F.lit(lat)).withColumn('longitude', F.lit(lon)).withColumn('timezone', F.lit(PARAMS['timezone']))
    .withColumn('local_time', F.from_utc_timestamp('event_time_utc', PARAMS['timezone']))
    .withColumn('date_utc', F.to_date('event_time_utc')).withColumn('run_id', F.lit(RUN_ID)))
invalid = F.lit(False)
for key in AIR + ['precipitation', 'wind_speed_10m']:
    invalid = invalid | F.coalesce(F.col(key) < 0, F.lit(False))
joined = joined.withColumn('invalid_value', invalid)
joined = joined.withColumn('missing_variable_count', sum(F.col(k).isNull().cast('int') for k in AIR + WEATHER))
merge_table(joined.filter('invalid_value'), 'explorer_quarantine_events', keys + ['run_id'])
# Preserve negative source values in Bronze/quarantine; expose nulls in Silver.
silver = joined
for key in AIR + ['precipitation', 'wind_speed_10m']:
    silver = silver.withColumn(key, F.when(F.col(key) >= 0, F.col(key)))
merge_table(silver, 'explorer_silver_hourly', keys)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold daily summaries, coverage and export
# MAGIC UTC daily boundaries are explicit. Rain totals require 24 valid hours.
# MAGIC Missing pollution before August 2022 is not filled with zeros.

# COMMAND ----------

aggregations = [F.count('*').alias('hour_count')]
for key in AIR + WEATHER:
    aggregations.append(F.count(key).alias(f'{key}_valid_hours'))
    if key != 'precipitation':
        aggregations.append(F.avg(key).alias(f'{key}_mean'))
aggregations.append(F.when(F.count('precipitation') == 24, F.sum('precipitation')).alias('precipitation_total_mm'))
gold = silver.groupBy('location_id', 'date_utc').agg(*aggregations).withColumn('run_id', F.lit(RUN_ID))
merge_table(gold, 'explorer_gold_daily', ['location_id', 'date_utc'])
actual = silver.count()
if actual != EXPECTED or silver.select(*keys).distinct().count() != EXPECTED:
    raise RuntimeError('Unexpected or duplicated Silver keys; inspect before using export')
coverage = silver.agg(*[F.count(k).alias(k) for k in AIR + WEATHER]).first().asDict()
invalid_count = joined.filter('invalid_value').count()
quality = {'run_id': RUN_ID, 'request_id': REQUEST_ID, 'location_id': LOCATION_ID,
           'expected_hours': EXPECTED, 'silver_hours': actual, 'invalid_rows': invalid_count,
           'valid_hours_by_variable': coverage, 'raw_responses': len(raw_files), 'cached_responses': cache_hits,
           'status': 'PASS' if invalid_count == 0 and all(n == EXPECTED for n in coverage.values()) else 'PASS_WITH_GAPS',
           'checked_at_utc': datetime.now(timezone.utc).isoformat()}
export = {'format_version': 1, 'request': PARAMS, 'location_id': LOCATION_ID,
          'time_basis': 'UTC', 'precipitation_interval': '[hour,hour+1hour)', 'quality': quality,
          'daily': [{**row.asDict(), 'date_utc': row['date_utc'].isoformat()}
                    for row in gold.orderBy('date_utc').collect()]}
# Only at most 366 daily rows are collected for the website, not all hourly rows.
folder = ROOT / 'exports' / 'explorer' / REQUEST_ID
folder.mkdir(parents=True, exist_ok=True)
output_path = folder / f'{RUN_ID}.json'
output_path.write_text(json.dumps(export, ensure_ascii=False, allow_nan=False), encoding='utf-8')
log_path = ROOT / 'logs' / f'explorer_quality_{RUN_ID}.json'
log_path.write_text(json.dumps(quality, indent=2), encoding='utf-8')
manifest_path = ROOT / 'metadata' / f'explorer_manifest_{RUN_ID}.json'
manifest_path.write_text(json.dumps({'request': PARAMS, 'files': raw_files, 'quality': quality}, indent=2), encoding='utf-8')
print('REQUESTED HISTORY SUMMARY')
print(json.dumps(quality, indent=2))
print('EXPORT PATH:', output_path)
print('QUALITY REPORT:', log_path)
display(gold.orderBy('date_utc'))