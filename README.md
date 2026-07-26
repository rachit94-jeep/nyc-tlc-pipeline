# NYC TLC Pipeline

A PySpark data pipeline for ingesting and processing NYC Taxi & Limousine Commission (TLC) trip record data. The pipeline reads raw Parquet files for Yellow Taxi, Green Taxi, and For-Hire Vehicle (FHV) trips, cleans and transforms them, and writes date-partitioned Parquet output to both local storage and S3.

The environment runs fully containerized — Spark 4.2 on Java 17, with a Jupyter Notebook interface for interactive development.

## Tech Stack

- **PySpark 4.2** — distributed data processing
- **Java 17** (Eclipse Temurin) — Spark runtime
- **Python 3.11** — managed by [uv](https://github.com/astral-sh/uv)
- **Jupyter Notebook** — interactive job development
- **Docker Compose** — container orchestration

## Project Structure

```
NYC_TLC_PIPELINE/
├── data/
│   ├── input/
│   │   ├── yellow/         # raw Yellow Taxi parquet files
│   │   ├── green/          # raw Green Taxi parquet files
│   │   ├── fhv/            # raw FHV parquet files
│   │   └── hvfhv/          # raw High-Volume FHV parquet files
│   ├── output/
│   │   ├── yellow/         # processed output, partitioned by pickup_date
│   │   ├── green/          # processed output, partitioned by pickup_date
│   │   └── fhv/            # processed output, partitioned by pickup_date
│   └── logs/               # pipeline run logs per taxi type
├── spark/
│   └── jobs/
│       ├── ingest_yellow.ipynb   # Yellow Taxi ingestion notebook
│       ├── ingest_green.ipynb    # Green Taxi ingestion notebook
│       └── ingest_fhv.ipynb      # FHV ingestion notebook
├── main.py
├── Dockerfile
├── docker-compose.yml
├── pyproject.toml          # project dependencies
├── .env                    # local environment variables (git-ignored)
├── .env.example            # environment variable template (committed)
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
| `pickup_datetime >= dropOff_datetime` | Drop — zero or negative duration trips |
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

## Schema Notes

Yellow and Green Taxi schemas differ in a few columns:

| Field | Yellow | Green |
|-------|--------|-------|
| Pickup datetime | `tpep_pickup_datetime` | `lpep_pickup_datetime` |
| Dropoff datetime | `tpep_dropoff_datetime` | `lpep_dropoff_datetime` |
| Airport fee | `airport_fee` | — |
| E-hail fee | — | `ehail_fee` |
| Trip type | — | `trip_type` |

## Logging

Each job writes a log file to `/app/data/logs/`:

| Job | Log file |
|-----|----------|
| Yellow | `yellow_taxi_trips.log` |
| Green | `green_taxi_trips.log` |
| FHV | `fhv_trips.log` |

Logged events: raw row count, post-validation row count, rows written to S3.

## S3 Staging

Processed data is written to S3 partitioned by `pickup_date`. The target bucket is set via the `S3_BUCKET` environment variable:

```
s3a://<S3_BUCKET>/yellow/
s3a://<S3_BUCKET>/green/
s3a://<S3_BUCKET>/fhv/
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

## Development

Dependencies are managed with `uv` and defined in [pyproject.toml](pyproject.toml). To add a dependency, update the file and rebuild the image:

```bash
docker compose up --build
```
