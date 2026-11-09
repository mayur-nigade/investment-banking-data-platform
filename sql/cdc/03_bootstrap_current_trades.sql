-- Step 1: ONE-TIME bootstrap of current business trade state.
-- STATUS: PLANNED; not yet executed in Snowflake.
-- Prerequisite: dbt has been rebuilt against IB_LAB.BRONZE.RAW_TRADE_EVENTS_ICEBERG.
-- This backfills the history that was present before we created the append-only stream.
-- We preserve delete (D) events as TOMBSTONES, not physical DELETEs, so that
-- out-of-order older updates cannot recreate a deleted trade.
--
-- Important: Expected shape is "one latest event per TRADE_ID", NOT one row per event.

USE WAREHOUSE COMPUTE_WH;
CREATE SCHEMA IF NOT EXISTS IB_LAB.GOLD;

-- CREATE TABLE IF NOT EXISTS AS SELECT is one-time initialization only.
-- If table already exists, inspect before attempting any backfill.
CREATE TABLE IF NOT EXISTS IB_LAB.GOLD.CURRENT_TRADES
AS
SELECT
    TRADE_ID,
    CLIENT_ID,
    ACCOUNT_ID,
    INSTRUMENT_ID,
    SIDE,
    QUANTITY,
    PRICE,
    TRADE_CURRENCY,
    TRADE_DATE,
    SOURCE_SEQUENCE AS LAST_SOURCE_SEQUENCE,
    EVENT_TS AS LAST_EVENT_TS,
    EVENT_ID AS LAST_EVENT_ID,
    EVENT_TYPE AS LAST_EVENT_TYPE,
    (EVENT_TYPE = 'D') AS IS_DELETED,
    SOURCE_FILE,
    CURRENT_TIMESTAMP()::TIMESTAMP_LTZ(6) AS UPDATED_AT
FROM IB_LAB.DBT_DEV.INT_VALID_TRADES
WHERE TRADE_ID IS NOT NULL
  AND SOURCE_SEQUENCE IS NOT NULL
  AND EVENT_TS IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY TRADE_ID
    ORDER BY SOURCE_SEQUENCE DESC,
             EVENT_TS DESC,
             EVENT_ID DESC
) = 1;

-- One quick acceptance check, not an expectation that rows equal event count.
SELECT
    COUNT(*) AS DISTINCT_TRADE_STATES,
    COUNT_IF(IS_DELETED) AS DELETED_TOMBSTONES,
    COUNT_IF(NOT IS_DELETED) AS ACTIVE_TRADES
FROM IB_LAB.GOLD.CURRENT_TRADES;

-- Verify no duplicated business state; should return no rows.
SELECT TRADE_ID, COUNT(*)
FROM IB_LAB.GOLD.CURRENT_TRADES
GROUP BY TRADE_ID
HAVING COUNT(*) > 1;
