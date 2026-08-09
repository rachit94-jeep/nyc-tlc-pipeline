--Snowflake user creation
--Step 1: Use an admin role
USE ROLE ACCOUNTADMIN;

--Step 2: Create the 'TRANSFORM' role and assign it to ACCOUNTADMIN
CREATE ROLE IF NOT EXISTS TRANSFORM;
GRANT ROLE TRANSFORM TO ROLE ACCOUNTADMIN;

--Step 3: Create a default warehouse
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH 
WITH 
WAREHOUSE_SIZE = 'XSMALL'
AUTO_SUSPEND = 300
AUTO_RESUME = TRUE;
GRANT OPERATE ON WAREHOUSE COMPUTE_WH TO ROLE TRANSFORM;

-- Step 4: Create a 'dbt' user and assign to the transform role
CREATE USER IF NOT EXISTS dbt_tlc
PASSWORD = 'Welcome@123'
LOGIN_NAME = 'dbt_tlc'
MUST_CHANGE_PASSWORD = FALSE
DEFAULT_WAREHOUSE = 'COMPUTE_WH'
DEFAULT_ROLE = TRANSFORM
DEFAULT_NAMESPACE = 'TLC.RAW'
COMMENT = 'DBT user used for data transformation';
ALTER USER dbt_tlc SET TYPE = LEGACY_SERVICE;
GRANT ROLE TRANSFORM TO USER dbt_tlc;

-- Step 5: Create a database and schema for the TLC project
CREATE DATABASE IF NOT EXISTS TLC;
CREATE SCHEMA IF NOT EXISTS TLC.RAW;

--Step 6: Grant permissions to the 'TRANSFORM' rule
GRANT ALL ON WAREHOUSE COMPUTE_WH TO ROLE TRANSFORM;
GRANT ALL ON DATABASE TLC TO ROLE TRANSFORM;
GRANT ALL ON ALL SCHEMAS IN DATABASE TLC TO ROLE TRANSFORM;
GRANT ALL ON FUTURE SCHEMAS IN DATABASE TLC TO ROLE TRANSFORM;
GRANT ALL ON ALL TABLES IN SCHEMA TLC.RAW TO ROLE TRANSFORM;
GRANT ALL ON FUTURE TABLES IN SCHEMA TLC.RAW TO ROLE TRANSFORM;


--Step 7: Create File Format
CREATE OR REPLACE FILE FORMAT ff_parquet
TYPE = PARQUET;


--Step 8: Create stage to connect to AWS S3
CREATE or REPLACE STAGE nyc_tlc_stage
    URL = 's3://nyc-tlc-pipeline'
    CREDENTIALS=(AWS_KEY_ID='<your_aws_access_key_id>' AWS_SECRET_KEY='<your_aws_secret_access_key>')
    FILE_FORMAT = ff_parquet;

--Step 9: Create Raw Table : Yellow_Taxi_Trips

CREATE OR REPLACE TABLE TLC.RAW.YELLOW_TAXI_TRIPS (
    VendorID                  INTEGER,
    tpep_pickup_datetime      TIMESTAMP_NTZ,
    tpep_dropoff_datetime     TIMESTAMP_NTZ,
    passenger_count           BIGINT,
    trip_distance             NUMBER(10,2),
    RatecodeID                BIGINT,
    store_and_fwd_flag        STRING,
    PULocationID              INTEGER,
    DOLocationID              INTEGER,
    payment_type              BIGINT,
    fare_amount               NUMBER(10,2),
    extra                     NUMBER(10,2),
    mta_tax                   NUMBER(10,2),
    tip_amount                NUMBER(10,2),
    tolls_amount              NUMBER(10,2),
    improvement_surcharge     NUMBER(10,2),
    total_amount              NUMBER(10,2),
    congestion_surcharge      NUMBER(10,2),
    airport_fee               NUMBER(10,2),
    cbd_congestion_fee        NUMBER(10,2),
    trip_duration_minutes     INTEGER,
    avg_speed_mph             NUMBER(10,2),
    source_file               STRING,
    ingested_at_timestamp     TIMESTAMP_NTZ,
    pickup_date               DATE

);

--Copying data from S3 buckets to raw tables
COPY INTO TLC.RAW.YELLOW_TAXI_TRIPS
FROM @nyc_tlc_stage/yellow
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
PATTERN = '.*\.parquet'
ON_ERROR = 'SKIP_FILE';

--Checking the count from yellow_taxi_trips
SELECT COUNT(1) FROM YELLOW_TAXI_TRIPS;
SELECT * FROM TLC.RAW.YELLOW_TAXI_TRIPS;

--Update pickup_date column
UPDATE TLC.RAW.YELLOW_TAXI_TRIPS
SET pickup_date = TO_DATE(tpep_pickup_datetime)
WHERE pickup_date IS NULL;


--Create Raw Table: FHV Vehicles

CREATE OR REPLACE TABLE TLC.RAW.FHV_TRIPS (
    dispatching_base_num      VARCHAR,
    pickup_datetime           TIMESTAMP_NTZ,
    dropOff_datetime          TIMESTAMP_NTZ,
    PUlocationID              BIGINT,
    DOlocationID              BIGINT,
    SR_Flag                   BIGINT,
    Affiliated_base_number    VARCHAR,
    trip_duration_minutes     INTEGER,
    source_file               STRING,
    ingestion_timestamp     TIMESTAMP_NTZ,
    pickup_date               DATE    
);

--checking the column name and type from parquet files in fhv
SELECT COLUMN_NAME, TYPE
FROM TABLE(INFER_SCHEMA(
  LOCATION => '@TLC.RAW.nyc_tlc_stage/fhv',
  FILE_FORMAT => 'TLC.RAW.ff_parquet'
));

--Copying data from S3 buckets to raw tables
COPY INTO TLC.RAW.FHV_TRIPS
FROM @nyc_tlc_stage/fhv
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
PATTERN = '.*\.parquet'
ON_ERROR = 'SKIP_FILE';

--Checking the count from fhv [OPTIONAL]
--SELECT COUNT(1) FROM TLC.RAW.FHV_TRIPS;
--SELECT * FROM TLC.RAW.FHV_TRIPS;

--Update pickup_date column
UPDATE TLC.RAW.FHV_TRIPS
SET pickup_date = TO_DATE(pickup_datetime)
WHERE pickup_date IS NULL;

--Creating Raw Table: FHVHV Vehicles

CREATE OR REPLACE TABLE TLC.RAW.FHVHV_TRIPS (
    hvfhs_license_num       VARCHAR,
    dispatching_base_num    VARCHAR,
    originating_base_num    VARCHAR,

    request_datetime        TIMESTAMP_NTZ,
    on_scene_datetime       TIMESTAMP_NTZ,
    pickup_datetime         TIMESTAMP_NTZ,
    dropoff_datetime        TIMESTAMP_NTZ,

    PULocationID            INTEGER,
    DOLocationID            INTEGER,

    trip_miles              DOUBLE,
    trip_time               BIGINT,

    base_passenger_fare     DOUBLE,
    tolls                   DOUBLE,
    bcf                     DOUBLE,
    sales_tax               DOUBLE,
    congestion_surcharge    DOUBLE,
    airport_fee             DOUBLE,
    tips                    DOUBLE,
    driver_pay              DOUBLE,

    shared_request_flag     VARCHAR,
    shared_match_flag       VARCHAR,
    access_a_ride_flag      VARCHAR,
    wav_request_flag        VARCHAR,
    wav_match_flag          VARCHAR,

    cbd_congestion_fee      DOUBLE,
    trip_duration_minutes   INTEGER,
    source_file             STRING,
    ingestion_timestamp     TIMESTAMP_NTZ,
    pickup_date             DATE    
);

--Copying data from S3 buckets to raw tables
COPY INTO TLC.RAW.FHVHV_TRIPS
FROM @nyc_tlc_stage/fhvhv
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
PATTERN = '.*\.parquet'
ON_ERROR = 'SKIP_FILE';

--Update pickup_date column
UPDATE TLC.RAW.FHVHV_TRIPS
SET pickup_date = TO_DATE(pickup_datetime)
WHERE pickup_date IS NULL;

SELECT COLUMN_NAME, TYPE
FROM TABLE(INFER_SCHEMA(
  LOCATION => '@TLC.RAW.nyc_tlc_stage/fhvhv',
  FILE_FORMAT => 'TLC.RAW.ff_parquet'
));

SELECT * FROM TLC.RAW.FHVHV_TRIPS;

--Create Raw table : green taxi trips
CREATE OR REPLACE TABLE TLC.RAW.GREEN_TAXI_TRIPS (
    VendorID                  INTEGER,
    lpep_pickup_datetime      TIMESTAMP_NTZ,
    lpep_dropoff_datetime     TIMESTAMP_NTZ,
    store_and_fwd_flag        VARCHAR,
    RatecodeID                BIGINT,
    PULocationID              INTEGER,
    DOLocationID              INTEGER,
    passenger_count           BIGINT,
    trip_distance             DOUBLE,
    fare_amount               DOUBLE,
    extra                     DOUBLE,
    mta_tax                    DOUBLE,
    tip_amount                DOUBLE,
    tolls_amount              DOUBLE,
    ehail_fee                  DOUBLE,
    improvement_surcharge     DOUBLE,
    total_amount              DOUBLE,
    payment_type              BIGINT,
    trip_type                 BIGINT,
    congestion_surcharge      DOUBLE,
    cbd_congestion_fee        DOUBLE,
    trip_duration_minutes     INTEGER,
    avg_speed_mph             NUMBER(10,2),
    source_file               STRING,
    ingested_at_timestamp     TIMESTAMP_NTZ,
    pickup_date               DATE
);

--Copying data from S3 buckets to raw tables
COPY INTO TLC.RAW.GREEN_TAXI_TRIPS
FROM @nyc_tlc_stage/green
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
PATTERN = '.*\.parquet'
ON_ERROR = 'SKIP_FILE';

--update pickup_date
UPDATE TLC.RAW.GREEN_TAXI_TRIPS
SET pickup_date = TO_DATE(lpep_pickup_datetime)
WHERE pickup_date IS NULL;