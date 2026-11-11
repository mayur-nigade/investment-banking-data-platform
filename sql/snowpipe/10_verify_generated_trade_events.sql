-- Final synthetic test: new trade-events Parquet via Snowpipe to Iceberg & Gold.
-- STATUS: PREPARED; not yet executed in the user's Snowflake account.
-- Filename: trade_events_snowpipe_20261111_001.parquet (6 rows, 14 fields).
-- Contains 5 fresh I business trades and one U event for the first trade.
-- Synthetic client/account/instrument combinations from original starter data.
-- Do NOT manually COPY the file: upload to S3 parquet/auto/ and let Snowpipe do it.

-- 1) Verify that exactly our new file reached the Bronze Iceberg target.
SELECT COUNT(*) AS INGESTED_EVENTS
FROM IB_LAB.BRONZE.RAW_TRADE_EVENTS_ICEBERG
WHERE SOURCE_FILE LIKE '%parquet/auto/trade_events_snowpipe_20261111_001.parquet';
-- Expected: 6, not yet observed.

-- 2) Verify business validation for exactly this file.
SELECT 'VALID' AS STATUS, COUNT(*) AS RECORDS
FROM IB_LAB.DBT_DEV.INT_VALID_TRADES
WHERE SOURCE_FILE LIKE '%parquet/auto/trade_events_snowpipe_20261111_001.parquet'
UNION ALL
SELECT 'REJECTED', COUNT(*)
FROM IB_LAB.DBT_DEV.INT_REJECTED_TRADES
WHERE SOURCE_FILE LIKE '%parquet/auto/trade_events_snowpipe_20261111_001.parquet';
-- Expected: VALID 6, REJECTED 0, provided the referenced client IDs
-- match this lab's STG_CLIENTS records (they came from synthetic starter).

-- 3) Gold CDC current-state arrival. Tasks must be RESUMED to run.
SELECT TRADE_ID, LAST_SOURCE_SEQUENCE, LAST_EVENT_TYPE, PRICE, IS_DELETED
FROM IB_LAB.GOLD.CURRENT_TRADES
WHERE TRADE_ID LIKE 'LABTRD20261111%'
ORDER BY TRADE_ID;
-- Expected: 5 TRADE_IDs. For LABTRD202611110001, latest seq=2,
-- event type U, price 71.5000, IS_DELETED=FALSE.

-- 4) Gold PHYSICAL SCD2 (if the independent SCD2 Task is enabled).
SELECT TRADE_ID, VERSION_NO, VALID_FROM_SEQUENCE,
       VALID_TO_SEQUENCE_EXCLUSIVE, PRICE, IS_CURRENT
FROM IB_LAB.GOLD.TRADE_SCD2_HISTORY
WHERE TRADE_ID = 'LABTRD202611110001'
ORDER BY VERSION_NO;
-- Expected: 2 rows (insert + price-changing update), one current.
-- If empty, verify that independent SCD2 stream/task is STARTED and
-- procedure was created, not just that bootstrap ran.

-- Important: Earlier settlement Parquet files are NOT valid trade events;
-- they lack quantity and failed DQ. Do not reload/rename them.
