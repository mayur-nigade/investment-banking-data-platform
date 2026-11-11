-- SCD Type 2-style history derived from immutable synthetic trade events.
-- STATUS: PLANNED; not yet executed in Snowflake.
--
-- Why a VIEW first? Our Iceberg Bronze already stores every I/U/D event,
-- so version history can be reconstructed on every SELECT without replaying
-- the already-consumed CDC stream or running a second ETL task.
-- This is a derived SCD2-style history READ MODEL, not a separately persisted
-- SCD2 history table. New valid events appear automatically on view queries.
--
-- Uses SOURCE_SEQUENCE as authoritative per-trade ordering. EVENT_TS
-- provides display timestamps; for AS-OF time joins we must guarantee that
-- timestamps are monotonic with source sequence. Otherwise use sequence.
-- The "D" event creates the latest tombstone row (IS_DELETED = TRUE).
--
-- Dedupe exact EVENT_ID replays; note this is an example without a formal
-- quality contract for duplicate SOURCE_SEQUENCE with different EVENT_IDs.

USE WAREHOUSE COMPUTE_WH;
CREATE SCHEMA IF NOT EXISTS IB_LAB.GOLD;

CREATE VIEW IF NOT EXISTS IB_LAB.GOLD.V_TRADE_SCD2_HISTORY AS
WITH source_events AS (
    SELECT
        EVENT_ID,
        TRADE_ID,
        EVENT_TYPE,
        SOURCE_SEQUENCE,
        EVENT_TS,
        CLIENT_ID,
        ACCOUNT_ID,
        INSTRUMENT_ID,
        SIDE,
        QUANTITY,
        PRICE,
        TRADE_CURRENCY,
        TRADE_DATE,
        SOURCE_FILE,
        FILE_ROW_NUMBER,
        INGESTED_AT,
        ROW_NUMBER() OVER (
            PARTITION BY EVENT_ID
            ORDER BY INGESTED_AT DESC,
                     SOURCE_FILE DESC,
                     FILE_ROW_NUMBER DESC
        ) AS EVENT_DEDUPE_RN
    FROM IB_LAB.DBT_DEV.INT_VALID_TRADES
    WHERE EVENT_ID IS NOT NULL
      AND TRADE_ID IS NOT NULL
      AND SOURCE_SEQUENCE IS NOT NULL
      AND EVENT_TS IS NOT NULL
),
ordered_versions AS (
    SELECT
        EVENT_ID,
        TRADE_ID,
        EVENT_TYPE,
        SOURCE_SEQUENCE,
        EVENT_TS,
        CLIENT_ID,
        ACCOUNT_ID,
        INSTRUMENT_ID,
        SIDE,
        QUANTITY,
        PRICE,
        TRADE_CURRENCY,
        TRADE_DATE,
        SOURCE_FILE,
        INGESTED_AT,
        ROW_NUMBER() OVER (
            PARTITION BY TRADE_ID
            ORDER BY SOURCE_SEQUENCE ASC, EVENT_TS ASC, EVENT_ID ASC
        ) AS VERSION_NO,
        LEAD(SOURCE_SEQUENCE) OVER (
            PARTITION BY TRADE_ID
            ORDER BY SOURCE_SEQUENCE ASC, EVENT_TS ASC, EVENT_ID ASC
        ) AS NEXT_SOURCE_SEQUENCE,
        LEAD(EVENT_TS) OVER (
            PARTITION BY TRADE_ID
            ORDER BY SOURCE_SEQUENCE ASC, EVENT_TS ASC, EVENT_ID ASC
        ) AS NEXT_EVENT_TS
    FROM source_events
    WHERE EVENT_DEDUPE_RN = 1
)
SELECT
    TRADE_ID,
    VERSION_NO,
    EVENT_ID,
    EVENT_TYPE,
    CLIENT_ID,
    ACCOUNT_ID,
    INSTRUMENT_ID,
    SIDE,
    QUANTITY,
    PRICE,
    TRADE_CURRENCY,
    TRADE_DATE,
    SOURCE_SEQUENCE AS VALID_FROM_SEQUENCE,
    NEXT_SOURCE_SEQUENCE AS VALID_TO_SEQUENCE_EXCLUSIVE,
    EVENT_TS AS VALID_FROM_TS,
    NEXT_EVENT_TS AS VALID_TO_TS_EXCLUSIVE,
    (NEXT_SOURCE_SEQUENCE IS NULL) AS IS_CURRENT,
    (EVENT_TYPE = 'D') AS IS_DELETED,
    SOURCE_FILE,
    INGESTED_AT
FROM ordered_versions;

-- One purposeful check: show a trade's version chain (if found).
SELECT
    TRADE_ID, VERSION_NO, EVENT_TYPE, PRICE,
    VALID_FROM_SEQUENCE, VALID_TO_SEQUENCE_EXCLUSIVE,
    VALID_FROM_TS, VALID_TO_TS_EXCLUSIVE,
    IS_CURRENT, IS_DELETED
FROM IB_LAB.GOLD.V_TRADE_SCD2_HISTORY
QUALIFY COUNT(*) OVER (PARTITION BY TRADE_ID) > 1
ORDER BY TRADE_ID, VERSION_NO
LIMIT 20;

-- The previously created smoke-test trade may initially have only one
-- version; add a new U event on the next lesson if you want to see version 2.
SELECT TRADE_ID, VERSION_NO, EVENT_TYPE, IS_CURRENT, IS_DELETED
FROM IB_LAB.GOLD.V_TRADE_SCD2_HISTORY
WHERE TRADE_ID = 'LAB_CDC_TRD_20261110_001'
ORDER BY VERSION_NO;

-- Optional assertion: should return no rows (one current version per trade).
SELECT TRADE_ID, COUNT_IF(IS_CURRENT) AS CURRENT_VERSION_COUNT
FROM IB_LAB.GOLD.V_TRADE_SCD2_HISTORY
GROUP BY TRADE_ID
HAVING COUNT_IF(IS_CURRENT) <> 1;
