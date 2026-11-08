-- PLAN: DO NOT RUN BEFORE AWS POLICY/ROLE ARE CREATED.
-- Substitute AWS account ID. Run each step in order.
-- This script is not verified in the lab yet. Original native RAW must remain untouched.

USE ROLE ACCOUNTADMIN;

-- The role must exist in AWS and have scoped read/write/delete/list permissions.
-- The External ID below must match the AWS role's trust policy.
CREATE EXTERNAL VOLUME IF NOT EXISTS OMERA_IB_ICEBERG_VOL
    STORAGE_LOCATIONS = (
        (
            NAME = 'omera-iceberg-bronze'
            STORAGE_PROVIDER = 'S3'
            STORAGE_BASE_URL = 's3://omera-ib-trade-lake/iceberg/bronze/'
            STORAGE_AWS_ROLE_ARN =
              'arn:aws:iam::<AWS_ACCOUNT_ID>:role/OMERA_SNOWFLAKE_ICEBERG_ROLE'
            STORAGE_AWS_EXTERNAL_ID = 'OMERA_ICEBERG_BRONZE_2026'
        )
    )
    ALLOW_WRITES = TRUE;

-- Inspect STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID,
-- then update the AWS role TRUST POLICY before verification.
DESC EXTERNAL VOLUME OMERA_IB_ICEBERG_VOL;

-- Requires correct S3 IAM role trust, object permissions and possibly KMS.
SELECT SYSTEM$VERIFY_EXTERNAL_VOLUME('OMERA_IB_ICEBERG_VOL');

-- Run below only if verification returned success=true.
USE WAREHOUSE COMPUTE_WH;
CREATE SCHEMA IF NOT EXISTS IB_LAB.BRONZE;

-- CTAS is a ONE-TIME historical backfill from our already-ingested 1704 RAW events.
-- IF NOT EXISTS protects an existing table, but does NOT fill an empty existing table.
CREATE ICEBERG TABLE IF NOT EXISTS IB_LAB.BRONZE.RAW_TRADE_EVENTS_ICEBERG
    CATALOG = 'SNOWFLAKE'
    EXTERNAL_VOLUME = 'OMERA_IB_ICEBERG_VOL'
    BASE_LOCATION = 'trade_events'
AS
SELECT *
FROM IB_LAB.RAW.RAW_TRADE_EVENTS;

-- Inspect table type and physical table file location.
SHOW ICEBERG TABLES LIKE 'RAW_TRADE_EVENTS_ICEBERG' IN SCHEMA IB_LAB.BRONZE;
DESC ICEBERG TABLE IB_LAB.BRONZE.RAW_TRADE_EVENTS_ICEBERG;

-- Row preservation / backfill verification. Expected baseline 1704 on each side.
SELECT 'NATIVE_RAW' AS SOURCE, COUNT(*) AS EVENT_COUNT
FROM IB_LAB.RAW.RAW_TRADE_EVENTS
UNION ALL
SELECT 'ICEBERG_BRONZE', COUNT(*)
FROM IB_LAB.BRONZE.RAW_TRADE_EVENTS_ICEBERG;

-- Events missing from Iceberg. Expected: no rows (assuming EVENT_ID is populated).
SELECT EVENT_ID
FROM IB_LAB.RAW.RAW_TRADE_EVENTS
MINUS
SELECT EVENT_ID
FROM IB_LAB.BRONZE.RAW_TRADE_EVENTS_ICEBERG;

-- Reverse check. Expected: no rows.
SELECT EVENT_ID
FROM IB_LAB.BRONZE.RAW_TRADE_EVENTS_ICEBERG
MINUS
SELECT EVENT_ID
FROM IB_LAB.RAW.RAW_TRADE_EVENTS;

-- Optional inspection: one event sample. No writes.
SELECT EVENT_ID, TRADE_ID, EVENT_TYPE, SOURCE_SEQUENCE, SOURCE_FILE
FROM IB_LAB.BRONZE.RAW_TRADE_EVENTS_ICEBERG
LIMIT 10;

-- Do NOT COPY the converted older Parquet batches into Bronze again.
-- The separate 392-record 2026-11-03 batch remains staged for the NEXT milestone.
