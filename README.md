# NYC TLC Pipeline

A PySpark data pipeline for ingesting and processing NYC Taxi & Limousine Commission (TLC) trip record data. The pipeline reads raw Yellow Taxi Parquet files, cleans and transforms them, and writes date-partitioned Parquet output.

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
│   ├── input/yellow/       # raw Yellow Taxi parquet files
│   └── output/yellow/      # processed output, partitioned by pickup_date
├── spark/
│   └── jobs/
│       └── ingest_yellow.ipynb   # Yellow Taxi ingestion notebook
├── Dockerfile
├── docker-compose.yml
├── pyproject.toml          # project dependencies
└── uv.lock
```

## Getting Started

### Prerequisites

- [Docker](https://docs.docker.com/get-docker/) and Docker Compose

### Run the pipeline

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

Input data is the NYC TLC Yellow Taxi trip records in Parquet format, available from the
[TLC Trip Record Data page](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).

Place raw files under `data/input/yellow/` (e.g. `yellow_tripdata_2026-04.parquet`).

Processed output is written to `data/output/yellow/`, partitioned by `pickup_date`.

## Development

Dependencies are managed with `uv` and defined in [pyproject.toml](pyproject.toml). To add a dependency, update the file and rebuild the image:

```bash
docker compose up --build
```
