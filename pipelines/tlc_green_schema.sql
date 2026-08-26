-- The NYC TLC Green Taxi trip record schema at the two shapes this backfill spans.
--
-- No TLC Parquet is committed here, for the licence reason in scripts/fetch_tlc.py,
-- and the test suite may not open a socket. So the schema is checked in instead and
-- tests/test_tlc_backfill.py builds three partitions from it. What that gives up is
-- the real values; what it keeps is the only thing the schema-change work is about,
-- which is that one partition has a column the others do not.
--
-- Read from the files themselves with DESCRIBE on 2026-08-26, not transcribed from
-- the PDF, because what DuckDB makes of the Parquet is what the pipeline sees.
-- RatecodeID is BIGINT and PULocationID is INTEGER in the same file, which is the
-- kind of thing a hand transcription gets wrong.
--
-- Data dictionary, updated 2025-03-18 to document cbd_congestion_fee:
-- https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_green.pdf
-- Source page: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

-- Every month up to and including 2024-12. Twenty columns.
CREATE TABLE green_tripdata_before_2025 (
    "VendorID" INTEGER,
    "lpep_pickup_datetime" TIMESTAMP,
    "lpep_dropoff_datetime" TIMESTAMP,
    "store_and_fwd_flag" VARCHAR,
    "RatecodeID" BIGINT,
    "PULocationID" INTEGER,
    "DOLocationID" INTEGER,
    "passenger_count" BIGINT,
    "trip_distance" DOUBLE,
    "fare_amount" DOUBLE,
    "extra" DOUBLE,
    "mta_tax" DOUBLE,
    "tip_amount" DOUBLE,
    "tolls_amount" DOUBLE,
    "ehail_fee" DOUBLE,
    "improvement_surcharge" DOUBLE,
    "total_amount" DOUBLE,
    "payment_type" BIGINT,
    "trip_type" BIGINT,
    "congestion_surcharge" DOUBLE
);

-- 2025-01 onward. The same twenty plus cbd_congestion_fee, which carries the
-- Congestion Relief Zone charge that took effect on 5 January 2025.
CREATE TABLE green_tripdata_from_2025 (
    "VendorID" INTEGER,
    "lpep_pickup_datetime" TIMESTAMP,
    "lpep_dropoff_datetime" TIMESTAMP,
    "store_and_fwd_flag" VARCHAR,
    "RatecodeID" BIGINT,
    "PULocationID" INTEGER,
    "DOLocationID" INTEGER,
    "passenger_count" BIGINT,
    "trip_distance" DOUBLE,
    "fare_amount" DOUBLE,
    "extra" DOUBLE,
    "mta_tax" DOUBLE,
    "tip_amount" DOUBLE,
    "tolls_amount" DOUBLE,
    "ehail_fee" DOUBLE,
    "improvement_surcharge" DOUBLE,
    "total_amount" DOUBLE,
    "payment_type" BIGINT,
    "trip_type" BIGINT,
    "congestion_surcharge" DOUBLE,
    "cbd_congestion_fee" DOUBLE
);
