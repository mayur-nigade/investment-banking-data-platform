-- PRODUCTION-STYLE physical SCD Type 2: SCHEMA + HISTORICAL BOOTSTRAP.
-- STATUS: PLANNED, not executed in the user's Snowflake account.
-- Use after our learning-only event-version view (script 06).
-- This creates a *separate physical history table*; it DOES NOT replace the
-- current-state GOLD.CURRENT_TRADES or the event-history view.
--
-- DEPENDENCY: IB_LAB.DBT_DEV.INT_VALID_TRADES is a live dbt view fed by
-- IB_LAB.BRONZE.RAW_TRADE_EVENTS_ICEBERG, and SOURCE_SEQUENCE is stable,
-- increasing for each TRADE_ID. D events carry full validated row images.
--
-- CRITICAL ORDER: create an INDEPENDENT append-only Iceberg stream FIRST,
-- then backfill. Using the existing CURRENT_TRADES stream would let
-- consumers interfere with each other's offsets.
--
-- This lab's physical history creates a new SCD2 row only when one of our
-- TRACKED business attributes changes (including IS_DELETED).
-- Identical U/retry events are not new physical SCD2 versions.
-- SHA-256 hashes a stable ordered array of tracked typed values, including
-- the soft-delete indicator. This is a change-detection signature, NOT a key.
-- Event ID replays are deduplicated by latest INGESTED_AT / file tie-break.
-- SOURCE_SEQUENCE is the ordering authority; EVENT_TS intervals are
-- meaningful as business-valid time only if timestamps are monotonic.

USE WAREHOUSE COMPUTE_WH;
CREATE SCHEMA IF NOT EXISTS IB_LAB.GOLD;

-- Independent stream: create this BEFORE the historical backfill.
CREATE STREAM IF NOT EXISTS IB_LAB.BRONZE.TRADE_EVENTS_SCD2_STREAM
ON TABLE IB_LAB.BRONZE.RAW_TRADE_EVENTS_ICEBERG
APPEND_ONLY = TRUE;

-- Canonical desired state: recomputable view over all valid Iceberg events.
-- It is a read-only transformation, not the production PHYSICAL SCD2 table.
CREATE VIEW IF NOT EXISTS IB_LAB.GOLD.V_TRADE_SCD2_DESIRED AS
WITH deduplicated_events AS (
    SELECT
        EVENT_ID, TRADE_ID, EVENT_TYPE,
        SOURCE_SEQUENCE, EVENT_TS,
        CLIENT_ID, ACCOUNT_ID, INSTRUMENT_ID, SIDE,
        QUANTITY, PRICE, TRADE_CURRENCY, TRADE_DATE,
        SOURCE_FILE, FILE_ROW_NUMBER, INGESTED_AT,
        (EVENT_TYPE = 'D') AS IS_DELETED,
        SHA2_HEX(
            TO_JSON(
                ARRAY_CONSTRUCT(
                    CLIENT_ID, ACCOUNT_ID, INSTRUMENT_ID, SIDE,
                    QUANTITY, PRICE, TRADE_CURRENCY, TRADE_DATE,
                    (EVENT_TYPE = 'D')
                )
            ),
            256
        ) AS ATTR_HASH
    FROM IB_LAB.DBT_DEV.INT_VALID_TRADES
    WHERE EVENT_ID IS NOT NULL
      AND TRADE_ID IS NOT NULL
      AND SOURCE_SEQUENCE IS NOT NULL
      AND EVENT_TS IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY EVENT_ID
        ORDER BY INGESTED_AT DESC,
                 SOURCE_FILE DESC,
                 FILE_ROW_NUMBER DESC
    ) = 1
),
ordered_events AS (
    SELECT
        *,
        LAG(ATTR_HASH) OVER (
            PARTITION BY TRADE_ID
            ORDER BY SOURCE_SEQUENCE, EVENT_TS, EVENT_ID
        ) AS PREV_ATTR_HASH,
        ROW_NUMBER() OVER (
            PARTITION BY TRADE_ID
            ORDER BY SOURCE_SEQUENCE, EVENT_TS, EVENT_ID
        ) AS EVENT_ORDINAL
    FROM deduplicated_events
),
changed_states AS (
    SELECT *
    FROM ordered_events
    WHERE EVENT_ORDINAL = 1
       OR ATTR_HASH <> PREV_ATTR_HASH
),
versions AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY TRADE_ID
            ORDER BY SOURCE_SEQUENCE, EVENT_TS, EVENT_ID
        ) AS VERSION_NO,
        LEAD(SOURCE_SEQUENCE) OVER (
            PARTITION BY TRADE_ID
            ORDER BY SOURCE_SEQUENCE, EVENT_TS, EVENT_ID
        ) AS NEXT_SOURCE_SEQUENCE,
        LEAD(EVENT_TS) OVER (
            PARTITION BY TRADE_ID
            ORDER BY SOURCE_SEQUENCE, EVENT_TS, EVENT_ID
        ) AS NEXT_EVENT_TS
    FROM changed_states
)
SELECT
    TRADE_ID, VERSION_NO,
    EVENT_ID, EVENT_TYPE,
    CLIENT_ID, ACCOUNT_ID, INSTRUMENT_ID, SIDE,
    QUANTITY, PRICE, TRADE_CURRENCY, TRADE_DATE,
    SOURCE_SEQUENCE AS VALID_FROM_SEQUENCE,
    NEXT_SOURCE_SEQUENCE AS VALID_TO_SEQUENCE_EXCLUSIVE,
    EVENT_TS AS VALID_FROM_TS,
    NEXT_EVENT_TS AS VALID_TO_TS_EXCLUSIVE,
    (NEXT_SOURCE_SEQUENCE IS NULL) AS IS_CURRENT,
    IS_DELETED,
    ATTR_HASH,
    SOURCE_FILE,
    INGESTED_AT
FROM versions;

-- Explicit storage schema is a separate NATIVE GOLD Snowflake table.
CREATE TABLE IF NOT EXISTS IB_LAB.GOLD.TRADE_SCD2_HISTORY (
    TRADE_ID VARCHAR,
    VERSION_NO NUMBER(38,0),
    EVENT_ID VARCHAR,
    EVENT_TYPE VARCHAR,
    CLIENT_ID VARCHAR,
    ACCOUNT_ID VARCHAR,
    INSTRUMENT_ID VARCHAR,
    SIDE VARCHAR,
    QUANTITY NUMBER(38,0),
    PRICE NUMBER(18,4),
    TRADE_CURRENCY VARCHAR,
    TRADE_DATE DATE,
    VALID_FROM_SEQUENCE NUMBER(38,0),
    VALID_TO_SEQUENCE_EXCLUSIVE NUMBER(38,0),
    VALID_FROM_TS TIMESTAMP_NTZ,
    VALID_TO_TS_EXCLUSIVE TIMESTAMP_NTZ,
    IS_CURRENT BOOLEAN,
    IS_DELETED BOOLEAN,
    ATTR_HASH VARCHAR(64),
    SOURCE_FILE VARCHAR,
    INGESTED_AT TIMESTAMP_LTZ,
    PUBLISHED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Persistent exception/audit tables; failures are also visible in TASK_HISTORY.
CREATE TABLE IF NOT EXISTS IB_LAB.GOLD.SCD2_REJECT_AUDIT (
    RUN_ID VARCHAR,
    RECORDED_AT TIMESTAMP_LTZ,
    EVENT_ID VARCHAR,
    TRADE_ID VARCHAR,
    SOURCE_FILE VARCHAR,
    REASON VARCHAR
);

CREATE TABLE IF NOT EXISTS IB_LAB.GOLD.SCD2_APPLY_AUDIT (
    RUN_ID VARCHAR,
    APPLIED_AT TIMESTAMP_LTZ,
    INPUT_EVENT_COUNT NUMBER,
    CHANGED_TRADE_COUNT NUMBER,
    STATUS VARCHAR
);

-- ONE-TIME bootstrap. The guard avoids duplicate history if script reruns.
-- If the table is partially populated, investigate rather than auto-overwrite.
INSERT INTO IB_LAB.GOLD.TRADE_SCD2_HISTORY (
    TRADE_ID, VERSION_NO, EVENT_ID, EVENT_TYPE,
    CLIENT_ID, ACCOUNT_ID, INSTRUMENT_ID, SIDE,
    QUANTITY, PRICE, TRADE_CURRENCY, TRADE_DATE,
    VALID_FROM_SEQUENCE, VALID_TO_SEQUENCE_EXCLUSIVE,
    VALID_FROM_TS, VALID_TO_TS_EXCLUSIVE,
    IS_CURRENT, IS_DELETED, ATTR_HASH, SOURCE_FILE, INGESTED_AT
)
SELECT
    TRADE_ID, VERSION_NO, EVENT_ID, EVENT_TYPE,
    CLIENT_ID, ACCOUNT_ID, INSTRUMENT_ID, SIDE,
    QUANTITY, PRICE, TRADE_CURRENCY, TRADE_DATE,
    VALID_FROM_SEQUENCE, VALID_TO_SEQUENCE_EXCLUSIVE,
    VALID_FROM_TS, VALID_TO_TS_EXCLUSIVE,
    IS_CURRENT, IS_DELETED, ATTR_HASH, SOURCE_FILE, INGESTED_AT
FROM IB_LAB.GOLD.V_TRADE_SCD2_DESIRED
WHERE NOT EXISTS (
    SELECT 1 FROM IB_LAB.GOLD.TRADE_SCD2_HISTORY
);

-- One acceptance test: zero records means every TRADE_ID has one current row.
SELECT TRADE_ID, COUNT_IF(IS_CURRENT) AS CURRENT_VERSIONS
FROM IB_LAB.GOLD.TRADE_SCD2_HISTORY
GROUP BY TRADE_ID
HAVING COUNT_IF(IS_CURRENT) <> 1;

-- Show a short historical example; avoid repeated raw row reconciliation.
SELECT
    TRADE_ID, VERSION_NO, EVENT_TYPE, PRICE,
    VALID_FROM_SEQUENCE, VALID_TO_SEQUENCE_EXCLUSIVE,
    VALID_FROM_TS, VALID_TO_TS_EXCLUSIVE,
    IS_CURRENT, IS_DELETED
FROM IB_LAB.GOLD.TRADE_SCD2_HISTORY
QUALIFY COUNT(*) OVER (PARTITION BY TRADE_ID) > 1
ORDER BY TRADE_ID, VERSION_NO
LIMIT 15;

-- IMPORTANT: no TASK yet. Run script 08 only after bootstrap succeeds.
