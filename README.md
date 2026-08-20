# NYC TLC Pipeline

A PySpark data pipeline for ingesting and processing NYC Taxi & Limousine Commission (TLC) trip record data. The pipeline reads raw Parquet files for Yellow Taxi, Green Taxi, and For-Hire Vehicle (FHV) trips, cleans and transforms them, and writes date-partitioned Parquet output to both local storage and S3.

The environment runs fully containerized — Spark 4.2 on Java 17, with a Jupyter Notebook interface for interactive development.

## Tech Stack

- **PySpark 4.2** — distributed data processing
- **Java 17** (Eclipse Temurin) — Spark runtime
- **Python 3.11** — managed by [uv](https://github.com/astral-sh/uv)
- **Jupyter Notebook** — interactive job development
- **Docker Compose** — container orchestration
- **Snowflake** — cloud data warehouse (RAW landing + dbt target)
- **dbt (dbt-fusion 2.0)** — transformation layer (staging → dims → fct → marts)

## Project Structure

```
NYC_TLC_PIPELINE/
├── data/
│   ├── input/
│   │   ├── yellow/               # raw Yellow Taxi parquet files
│   │   ├── green/                # raw Green Taxi parquet files
│   │   ├── fhv/                  # raw FHV parquet files
│   │   └── hvfhv/                # raw High-Volume FHV (Uber, Lyft, etc.) parquet files
│   ├── output/
│   │   ├── yellow/               # processed output, partitioned by pickup_date
│   │   ├── green/                # processed output, partitioned by pickup_date
│   │   ├── fhv/                  # processed output, partitioned by pickup_date
│   │   └── hvfhv/                # processed output, partitioned by pickup_date
│   └── logs/                     # pipeline run logs per taxi type
├── spark/
│   └── jobs/
│       ├── ingest_yellow.ipynb   # Yellow Taxi ingestion notebook
│       ├── ingest_green.ipynb    # Green Taxi ingestion notebook
│       ├── ingest_fhv.ipynb      # FHV ingestion notebook
│       └── ingest_fhvhv.ipynb    # High-Volume FHV ingestion notebook
├── snowflake/
│   └── nyc_tlc_setup.sql         # Snowflake environment setup and raw table definitions
├── nyc_tlc/                      # dbt project
│   ├── models/
│   │   ├── staging/              # views over RAW tables (rename, cast, add trip_type)
│   │   │   ├── source.yml
│   │   │   ├── stg_models.yml        # tests + column docs for all staging models
│   │   │   ├── stg_yellow_trips.sql
│   │   │   ├── stg_green_trips.sql
│   │   │   ├── stg_fhv_trips.sql
│   │   │   └── stg_hvfhv_trips.sql
│   │   ├── intermediate/         # UNION ALL of all trip types
│   │   │   ├── int_models.yml        # tests + column docs
│   │   │   └── int_all_trips_unioned.sql
│   │   ├── dim/                  # dimension tables from seeds
│   │   │   ├── dim_models.yml        # tests + column docs
│   │   │   ├── dim_taxi_zones.sql
│   │   │   ├── dim_vendors.sql
│   │   │   ├── dim_rate_codes.sql
│   │   │   ├── dim_payment_types.sql
│   │   │   └── dim_hvfhv_bases.sql
│   │   ├── facts/                # incremental fact table
│   │   │   ├── fct_models.yml        # tests + column docs
│   │   │   └── fct_trips.sql
│   │   └── marts/                # aggregated BI-ready tables
│   │       ├── mart_models.yml       # tests + column docs
│   │       ├── mart_daily_summary.sql
│   │       ├── mart_zone_performance.sql
│   │       └── mart_revenue_breakdown.sql
│   ├── macros/
│   │   └── safe_divide.sql               # null-safe division macro
│   ├── seeds/
│   │   ├── taxi_zone_lookup.csv          # 265 NYC taxi zones
│   │   ├── vendors.csv                   # taxi vendor codes
│   │   ├── rate_codes.csv                # rate code labels
│   │   ├── payment_types.csv             # payment type labels
│   │   └── hvfhv_bases.csv               # Uber/Lyft/Via/Juno platform codes
│   ├── packages.yml                      # dbt_utils dependency
│   └── dbt_project.yml                   # project config and materializations
├── main.py
├── Dockerfile
├── docker-compose.yml
├── pyproject.toml                # project dependencies
├── .env                          # local environment variables (git-ignored)
├── .env.example                  # environment variable template (committed)
└── uv.lock
```

## Getting Started

### Prerequisites

- [Docker](https://docs.docker.com/get-docker/) and Docker Compose

### 1. Configure environment variables

Copy the example file and fill in your values:

```bash
cp .env.example .env
```

Open `.env` and set your AWS credentials and any other values. The file is git-ignored and will never be committed.

### 2. Run the pipeline

Build and start the container:

```bash
docker compose up --build
```

Then open Jupyter in your browser at **http://localhost:8888** (token: `tlc`).

The Spark UI is available at **http://localhost:4041** while a job is running.

### Volume mounts

The following host paths are mounted into the container:

| Host        | Container               | Purpose                        |
|-------------|-------------------------|--------------------------------|
| `./spark`   | `/app/spark`            | Notebooks & jobs (live reload) |
| `./data`    | `/app/data`             | Raw & processed data           |

> **Note:** Always read/write data under `/app/data` inside the container so output is visible on the host and persists across container restarts.

## Data

Input data is the NYC TLC trip records in Parquet format, available from the
[TLC Trip Record Data page](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).

| Taxi Type | Input Path | Output Path | S3 Path |
|-----------|-----------|-------------|---------|
| Yellow | `data/input/yellow/` | `data/output/yellow/` | `s3a://<bucket>/yellow` |
| Green | `data/input/green/` | `data/output/green/` | `s3a://<bucket>/green` |
| FHV | `data/input/fhv/` | `data/output/fhv/` | `s3a://<bucket>/fhv` |
| HVFHV | `data/input/hvfhv/` | `data/output/hvfhv/` | `s3a://<bucket>/hvfhv` |

All output is partitioned by `pickup_date`.

## Pipeline Steps

Each notebook follows the same structure:

1. **Load environment** — `python-dotenv` loads `.env` at startup
2. **Configure logging** — file handler writes to `data/logs/<type>_taxi_trips.log`
3. **Start Spark session** — `local[*]`, 4 GB driver memory, AQE enabled, S3 connector loaded via `spark.jars.packages`
4. **Read parquet** — explicit schema enforced on read
5. **Data quality checks** — see below
6. **Derive columns** — see below
7. **Write to S3** — overwrite mode, partitioned by `pickup_date`

## Data Quality Checks

The following filters are applied before writing output:

**Yellow & Green Taxi**

| Check | Action |
|-------|--------|
| Cash payment (`payment_type == 2`) with `tip_amount > 0` | Drop — invalid combination |
| Pickup month outside the file's target month | Drop — out-of-range records |
| `passenger_count == 0` | Drop |
| `fare_amount <= 0` | Drop |
| `trip_distance < 0` | Verified absent; filter is a guard |
| Pickup time after dropoff time | Verified absent; logged as a check |

**FHV**

| Check | Action |
|-------|--------|
| Pickup month outside the file's target month | Drop — out-of-range records |
| Both `PUlocationID` and `DOlocationID` are null | Drop — no location data |
| `pickup_datetime >= dropOff_datetime` | Count check only — if any exist, drops rows where `pickup_datetime == dropOff_datetime` |
| `trip_duration_minutes == 0` (after derivation) | Drop |

**HVFHV (High-Volume FHV)**

| Check | Action |
|-------|--------|
| Pickup month outside the file's target month | Drop — out-of-range records |
| Both `PULocationID` and `DOLocationID` are null | Drop — no location data |
| `pickup_datetime >= dropoff_datetime` | Count check only — if any exist, drops rows where `pickup_datetime == dropoff_datetime` |
| `trip_duration_minutes == 0` (after derivation) | Drop |

## Derived Columns

The following columns are added during transformation:

**Yellow & Green Taxi**

| Column | Description |
|--------|-------------|
| `trip_duration_minutes` | Duration from pickup to dropoff in minutes |
| `avg_speed_mph` | Average speed in mph (`trip_distance / (trip_duration_minutes / 60)`); `0` for zero-duration trips |
| `source_file` | Literal name of the source parquet file |
| `ingested_at_timestamp` | Timestamp when the record was ingested |
| `pickup_date` | Date portion of the pickup datetime; used as the partition key |

**FHV**

| Column | Description |
|--------|-------------|
| `trip_duration_minutes` | Duration from `pickup_datetime` to `dropOff_datetime` in minutes |
| `source_file` | Literal name of the source parquet file |
| `ingestion_timestamp` | Timestamp when the record was ingested |
| `pickup_date` | Date portion of `pickup_datetime`; used as the partition key |

**HVFHV**

| Column | Description |
|--------|-------------|
| `trip_duration_minutes` | Duration from `pickup_datetime` to `dropoff_datetime` in minutes |
| `source_file` | Literal name of the source parquet file |
| `ingestion_timestamp` | Timestamp when the record was ingested |
| `pickup_date` | Date portion of `pickup_datetime`; used as the partition key |

## Schema Notes

Yellow and Green Taxi schemas differ in a few columns:

| Field | Yellow | Green |
|-------|--------|-------|
| Pickup datetime | `tpep_pickup_datetime` | `lpep_pickup_datetime` |
| Dropoff datetime | `tpep_dropoff_datetime` | `lpep_dropoff_datetime` |
| Airport fee | `airport_fee` | — |
| E-hail fee | — | `ehail_fee` |
| Trip type | — | `trip_type` |

HVFHV has a distinct schema from FHV, reflecting the richer data reported by app-based providers (Uber, Lyft, Via):

| Field | Description |
|-------|-------------|
| `hvfhs_license_num` | TLC license number of the HVFHV company |
| `dispatching_base_num` | TLC base number of the dispatching entity |
| `originating_base_num` | TLC base that originally accepted the trip request |
| `request_datetime` | When the passenger requested the trip |
| `on_scene_datetime` | When the driver arrived at pickup |
| `pickup_datetime` / `dropoff_datetime` | Trip start and end times |
| `trip_miles` | Distance travelled in miles |
| `trip_time` | Trip duration in seconds (raw field) |
| `base_passenger_fare` | Base fare before surcharges and tips |
| `tolls`, `bcf`, `sales_tax`, `congestion_surcharge`, `airport_fee`, `cbd_congestion_fee` | Itemised surcharges |
| `tips` | Tip amount |
| `driver_pay` | Total driver compensation |
| `shared_request_flag` / `shared_match_flag` | Whether passenger requested / was matched into a shared ride |
| `access_a_ride_flag` | MTA Access-A-Ride trip indicator |
| `wav_request_flag` / `wav_match_flag` | Wheelchair-accessible vehicle request and match flags |

## Logging

Each job writes a log file to `/app/data/logs/`:

| Job | Log file |
|-----|----------|
| Yellow | `yellow_taxi_trips.log` |
| Green | `green_taxi_trips.log` |
| FHV | `fhv_trips.log` |
| HVFHV | `fhvhv_trips.log` |

Logged events: raw row count, post-validation row count, rows written to S3.

## S3 Staging

Processed data is written to S3 partitioned by `pickup_date`. The target bucket is set via the `S3_BUCKET` environment variable:

```
s3a://<S3_BUCKET>/yellow/
s3a://<S3_BUCKET>/green/
s3a://<S3_BUCKET>/fhv/
s3a://<S3_BUCKET>/hvfhv/
```

### AWS SDK

S3 access uses the `hadoop-aws` connector with the AWS SDK v2 bundle. The required JARs are pulled automatically by Spark at session startup via the `spark.jars.packages` config — no manual download needed:

```python
.config(
    "spark.jars.packages",
    "org.apache.hadoop:hadoop-aws:3.5.0,"
    "software.amazon.awssdk:bundle:2.31.54"
)
```

On first run Spark downloads these JARs from Maven Central and caches them in `~/.ivy2/`. Subsequent runs use the cache.

### Credentials

AWS credentials are loaded from `.env` via `python-dotenv` at notebook startup and injected into the Spark session:

```python
from dotenv import load_dotenv
load_dotenv()

spark = (
    SparkSession.builder
    ...
    .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
    .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
    .config("spark.hadoop.fs.s3a.endpoint",   os.environ.get("S3_ENDPOINT", "s3.amazonaws.com"))
    .getOrCreate()
)
```

Never commit real credentials. Keep them in `.env` (git-ignored) or use a secrets manager.

## Snowflake Setup

[snowflake/nyc_tlc_setup.sql](snowflake/nyc_tlc_setup.sql) provisions the entire Snowflake environment for loading the processed S3 data into raw tables. Run the script once against your Snowflake account using `ACCOUNTADMIN` or equivalent. The steps are:

### Step 1 — Role

Creates a `TRANSFORM` role and grants it to `ACCOUNTADMIN`.

### Step 2 — Warehouse

Creates an `XSMALL` warehouse (`COMPUTE_WH`) that auto-suspends after 5 minutes of inactivity and resumes automatically on query.

### Step 3 — Service user

Creates a `dbt_tlc` legacy-service user with `TRANSFORM` as its default role and `TLC.RAW` as its default namespace. This account is intended for dbt or other programmatic access — not for human login.

### Step 4 — Database & schema

Creates the `TLC` database and a `RAW` schema inside it. All raw tables live in `TLC.RAW`.

### Step 5 — Permissions

Grants the `TRANSFORM` role full privileges on the warehouse, database, all existing schemas and tables in `TLC`, and future schemas and tables in `TLC.RAW` so the role never needs manual re-granting as new objects are added.

### Step 6 — File format

Defines a `PARQUET` file format (`ff_parquet`) used by the external stage and `INFER_SCHEMA` calls.

### Step 7 — External stage

Creates an external stage (`nyc_tlc_stage`) pointing at the S3 bucket where the Spark pipeline writes its output. The stage uses AWS key-based authentication and the parquet file format so Snowflake can read the partitioned output directly.

### Step 8 — Raw tables & data load

Creates one raw table per taxi type, mirroring the schema written by the Spark pipeline (including the derived columns added during ingestion):

| Table | Source S3 prefix | Notes |
|-------|-----------------|-------|
| `TLC.RAW.YELLOW_TAXI_TRIPS` | `s3://nyc-tlc-pipeline/yellow` | Includes `avg_speed_mph`, `ingested_at_timestamp` |
| `TLC.RAW.GREEN_TAXI_TRIPS` | `s3://nyc-tlc-pipeline/green` | Uses `lpep_*` datetime columns; includes `ehail_fee`, `trip_type` |
| `TLC.RAW.FHV_TRIPS` | `s3://nyc-tlc-pipeline/fhv` | Dispatching-base schema; no fare fields |
| `TLC.RAW.FHVHV_TRIPS` | `s3://nyc-tlc-pipeline/fhvhv` | Full HVFHV schema with itemised surcharges and ride-share flags |

Each table is loaded with `COPY INTO ... MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE` so column ordering in the parquet files does not matter. Files that fail to load are skipped (`ON_ERROR = 'SKIP_FILE'`) rather than aborting the entire copy. After loading, a `UPDATE ... SET pickup_date = TO_DATE(...)` backfills any rows where the partition date column arrived as `NULL`.

`INFER_SCHEMA` queries are included for FHV and HVFHV as a diagnostic aid — they print the column names and types that Snowflake detects directly from the parquet files, useful for verifying schema alignment before or after a load.

## dbt Transformation Layer

The dbt project lives in [`nyc_tlc/`](nyc_tlc/) and transforms raw Snowflake tables through four layers: staging → intermediate → dimensions/facts → marts.

### Snowflake Schemas (dbt target: `NYC`)

| dbt folder | Snowflake schema | Materialization |
|---|---|---|
| `staging/` | `TLC.NYC_STAGING` | view |
| `intermediate/` | `TLC.NYC_INTERMEDIATE` | view |
| `dim/` | `TLC.NYC_DIMENSIONS` | table |
| `facts/` | `TLC.NYC_FACTS` | table |
| `marts/` | `TLC.NYC_MARTS` | table |
| `seeds/` | `TLC.NYC_LOOKUP` | table |

### Seeds

Five static reference tables loaded into `TLC.NYC_LOOKUP`:

| File | Rows | Purpose |
|---|---|---|
| `taxi_zone_lookup.csv` | 265 | Maps LocationID to zone name and borough |
| `vendors.csv` | 3 | Maps VendorID 1/2/7 to vendor names (7=Unknown) |
| `rate_codes.csv` | 6 | Maps RatecodeID 1-6 to labels |
| `payment_types.csv` | 6 | Maps payment_type 1-6 to labels |
| `hvfhv_bases.csv` | 4 | Maps hvfhs_license_num to platform name (Uber/Lyft/Via/Juno) |

### Macros

| Macro | Purpose |
|---|---|
| `safe_divide(numerator, denominator)` | Null-safe division using `NULLIF` — prevents division-by-zero errors |

### Models — Current Status

| Model | Status | Notes |
|---|---|---|
| `staging/source.yml` | ✅ Done | Declares 4 RAW source tables |
| `staging/stg_models.yml` | ✅ Done | Tests + docs for all 4 staging models |
| `staging/stg_yellow_trips.sql` | ✅ Done | Renames tpep_ columns, adds trip_type='yellow' |
| `staging/stg_green_trips.sql` | ✅ Done | Renames lpep_ columns, green_trip_type, adds trip_type='green' |
| `staging/stg_fhv_trips.sql` | ✅ Done | No fare columns, fixes PUlocationID casing |
| `staging/stg_hvfhv_trips.sql` | ✅ Done | Renames base_passenger_fare, tips, tolls |
| `intermediate/int_models.yml` | ✅ Done | Tests + docs for int_all_trips_unioned |
| `intermediate/int_all_trips_unioned.sql` | ✅ Done | UNION ALL of yellow + green + hvfhv |
| `dim/dim_models.yml` | ✅ Done | Tests + docs for all 5 dimension models |
| `dim/dim_taxi_zones.sql` | ✅ Done | From seed |
| `dim/dim_vendors.sql` | ✅ Done | From seed |
| `dim/dim_rate_codes.sql` | ✅ Done | From seed |
| `dim/dim_payment_types.sql` | ✅ Done | From seed |
| `dim/dim_hvfhv_bases.sql` | ✅ Done | From seed |
| `facts/fct_models.yml` | ✅ Done | unique + not_null on trip_id, accepted_values on trip_type |
| `facts/fct_trips.sql` | ✅ Done | Incremental, surrogate key via dbt_utils, ROW_NUMBER() dedup |
| `marts/mart_models.yml` | ✅ Done | Tests + docs for all 3 mart models |
| `marts/mart_daily_summary.sql` | ✅ Done | Aggregates by pickup_date |
| `marts/mart_zone_performance.sql` | ✅ Done | Aggregates by pickup zone |
| `marts/mart_revenue_breakdown.sql` | ✅ Done | Revenue split by trip_type |

### Mart Tables

| Mart | Schema | Description |
|---|---|---|
| `mart_daily_summary` | `TLC.NYC_MARTS` | Daily trip metrics aggregated by `pickup_date` and `trip_type`. Tracks trip count, average fare, average distance, average duration, and total revenue. Primary table for daily trend dashboards. |
| `mart_zone_performance` | `TLC.NYC_MARTS` | Pickup zone performance aggregated by `pu_location_id`, `zone`, and `borough`. Shows trip volume and revenue per zone — used to identify top and bottom performing pickup areas across NYC. |
| `mart_revenue_breakdown` | `TLC.NYC_MARTS` | Revenue components split by `trip_type` and `pickup_date`. Breaks down fare, tips, tolls, congestion surcharge, and airport fees into separate totals — used for revenue attribution analysis across Yellow, Green, and HVFHV trips. |

### Running dbt

```bash
cd nyc_tlc

# load seed tables
dbt seed

# run all models
dbt run

# run a specific layer
dbt run --select staging
dbt run --select intermediate
dbt run --select dim
dbt run --select fct
dbt run --select marts

# run tests
dbt test

# generate and serve docs
dbt docs generate
dbt docs serve
```

## Development

Dependencies are managed with `uv` and defined in [pyproject.toml](pyproject.toml). To add a dependency, update the file and rebuild the image:

```bash
docker compose up --build
```
