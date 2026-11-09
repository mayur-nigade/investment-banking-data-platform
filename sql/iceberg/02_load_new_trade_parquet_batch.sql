-- Iceberg Bronze: manually ingest the five NEW 2026-11-03 Parquet files.
--
-- STATUS: PLANNED; not yet run. Historical backfill has been verified:
-- NATIVE_RAW=1704, ICEBERG_BRONZE=1704, missing EVENT_IDs=0, extra=0.
-- The five new staged Parquet files contain 79+79+78+78+78=392 events.
-- Expected after successful first-time load: 1704+392=2096.
--
-- Why COPY with SELECT rather than MATCH_BY_COLUMN_NAME?
-- Incoming Parquet has 14 lowercase source fields; Bronze has 17 columns,
-- including SOURCE_FILE, FILE_ROW_NUMBER, INGESTED_AT metadata.
-- COPY transformations explicitly map all fields and attach metadata.
-- Snowflake does NOT allow MATCH_BY_COLUMN_NAME with COPY SELECT transforms.
--
-- FULL_INGEST scans the raw Parquet and writes Iceberg-managed Parquet + metadata.
-- This is NOT ADD_FILES_COPY (which requires matching-compatible raw files,
-- and does not allow the transformations used here).
--
-- Guardrails: never use FORCE=TRUE; PURGE=FALSE keeps source files immutable.
-- Run only when the existing Iceberg table is confirmed at 1704 rows.

USE WAREHOUSE COMPUTE_WH;

-- 1) Read-only baseline counts: both should report 1704.
SELECT 'NATIVE_RAW' AS LAYER, COUNT(*) AS RECORDS
FROM IB_LAB.RAW.RAW_TRADE_EVENTS
UNION ALL
SELECT 'ICEBERG_BRONZE', COUNT(*)
FROM IB_LAB.BRONZE.RAW_TRADE_EVENTS_ICEBERG;

-- 2) Optional preflight: inspect first few Parquet event IDs read-only.
SELECT t.$1:event_id::VARCHAR AS EVENT_ID,
       t.$1:trade_id::VARCHAR AS TRADE_ID,
       METADATA$FILENAME AS SOURCE_FILE,
       METADATA$FILE_ROW_NUMBER AS FILE_ROW_NUMBER
FROM @IB_LAB.RAW.IB_S3_STAGE/parquet/batches/batch_2026-11-03/
    (FILE_FORMAT => 'IB_LAB.RAW.IB_PARQUET_FORMAT') t
LIMIT 10;

-- 3) ONE-TIME first load of the five November 3 files.
-- Every target column is explicitly named in the same order as SELECT.
COPY INTO IB_LAB.BRONZE.RAW_TRADE_EVENTS_ICEBERG
(
    EVENT_ID,
    TRADE_ID,
    EVENT_TYPE,
    SOURCE_SEQUENCE,
    CLIENT_ID,
    ACCOUNT_ID,
    INSTRUMENT_ID,
    SIDE,
    QUANTITY,
    PRICE,
    TRADE_CURRENCY,
    TRADE_DATE,
    EVENT_TS,
    SOURCE_SYSTEM,
    SOURCE_FILE,
    FILE_ROW_NUMBER,
    INGESTED_AT
)
FROM (
    SELECT
        t.$1:event_id::VARCHAR,
        t.$1:trade_id::VARCHAR,
        t.$1:event_type::VARCHAR,
        t.$1:source_sequence::VARCHAR,
        t.$1:client_id::VARCHAR,
        t.$1:account_id::VARCHAR,
        t.$1:instrument_id::VARCHAR,
        t.$1:side::VARCHAR,
        t.$1:quantity::VARCHAR,
        t.$1:price::VARCHAR,
        t.$1:trade_currency::VARCHAR,
        t.$1:trade_date::VARCHAR,
        t.$1:event_ts::VARCHAR,
        t.$1:source_system::VARCHAR,
        METADATA$FILENAME,
        METADATA$FILE_ROW_NUMBER,
        METADATA$START_SCAN_TIME::TIMESTAMP_LTZ(6)
    FROM @IB_LAB.RAW.IB_S3_STAGE/parquet/batches/batch_2026-11-03/ t
)
FILE_FORMAT = (FORMAT_NAME = 'IB_LAB.RAW.IB_PARQUET_FORMAT')
LOAD_MODE = FULL_INGEST
ON_ERROR = ABORT_STATEMENT
PURGE = FALSE;

-- 4) Read-only verification after successful load:
SELECT COUNT(*) AS ICEBERG_BRONZE_COUNT
FROM IB_LAB.BRONZE.RAW_TRADE_EVENTS_ICEBERG;
-- Expected: 2096; not observed yet.

-- 5) Group only new source files:
SELECT SOURCE_FILE, COUNT(*) AS RECORD_COUNT
FROM IB_LAB.BRONZE.RAW_TRADE_EVENTS_ICEBERG
WHERE SOURCE_FILE LIKE '%parquet/batches/batch_2026-11-03/%'
GROUP BY SOURCE_FILE
ORDER BY SOURCE_FILE;
-- Expected: 79,79,78,78,78 for files 001..005, not observed yet.

-- 6) Check event key duplication in Bronze; should return no rows
-- if source EVENT_IDs are unique (verify; Iceberg does not enforce this here).
SELECT EVENT_ID, COUNT(*) AS C
FROM IB_LAB.BRONZE.RAW_TRADE_EVENTS_ICEBERG
GROUP BY EVENT_ID
HAVING COUNT(*) > 1
ORDER BY C DESC;
