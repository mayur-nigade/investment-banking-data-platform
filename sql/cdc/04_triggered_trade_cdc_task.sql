-- Step 2: triggered CDC task on the existing append-only Iceberg stream.
-- STATUS: user reports task CREATED (2026-11-10).
-- Whether ALTER TASK ... RESUME succeeded / task is STARTED is not confirmed.
-- No observed task execution or successful CDC MERGE has been shared.
--
-- PREREQUISITES
-- 1) IB_LAB.BRONZE.TRADE_EVENTS_APPEND_STREAM exists and is append-only.
-- 2) IB_LAB.GOLD.CURRENT_TRADES has been bootstrapped by script 03.
-- 3) IB_LAB.DBT_DEV.STG_CLIENTS exists with clean client IDs.
-- 4) The event feed carries FULL I/U/D row images (including quantity/price),
--    and SOURCE_SEQUENCE increases for each TRADE_ID. For partial updates,
--    use a different field-level apply strategy.
--
-- The stream starts at its creation offset; historical data is not in it.
-- A task with WHEN SYSTEM$STREAM_HAS_DATA is TRIGGERED, not scheduled polling.
-- It consumes the stream via the MERGE DML transaction when triggered.
-- Invalid records are still available in Iceberg Bronze but are skipped
-- here; add separate reject/audit persistence before production use.
-- No "DELETE FROM current" is used: D events become tombstones.

USE WAREHOUSE COMPUTE_WH;

CREATE TASK IF NOT EXISTS IB_LAB.GOLD.APPLY_TRADE_CDC_TASK
    WAREHOUSE = COMPUTE_WH
    WHEN SYSTEM$STREAM_HAS_DATA(
        'IB_LAB.BRONZE.TRADE_EVENTS_APPEND_STREAM'
    )
AS
MERGE INTO IB_LAB.GOLD.CURRENT_TRADES AS tgt
USING (
    WITH typed AS (
        SELECT
            NULLIF(TRIM(EVENT_ID), '') AS EVENT_ID,
            NULLIF(TRIM(TRADE_ID), '') AS TRADE_ID,
            UPPER(NULLIF(TRIM(EVENT_TYPE), '')) AS EVENT_TYPE,
            TRY_TO_NUMBER(SOURCE_SEQUENCE) AS SOURCE_SEQUENCE,
            NULLIF(TRIM(CLIENT_ID), '') AS CLIENT_ID,
            NULLIF(TRIM(ACCOUNT_ID), '') AS ACCOUNT_ID,
            NULLIF(TRIM(INSTRUMENT_ID), '') AS INSTRUMENT_ID,
            UPPER(NULLIF(TRIM(SIDE), '')) AS SIDE,
            TRY_TO_NUMBER(QUANTITY) AS QUANTITY,
            TRY_TO_DECIMAL(PRICE, 18, 4) AS PRICE,
            UPPER(NULLIF(TRIM(TRADE_CURRENCY), '')) AS TRADE_CURRENCY,
            TRY_TO_DATE(TRADE_DATE) AS TRADE_DATE,
            TRY_TO_TIMESTAMP_NTZ(EVENT_TS) AS EVENT_TS,
            SOURCE_FILE
        FROM IB_LAB.BRONZE.TRADE_EVENTS_APPEND_STREAM
        WHERE METADATA$ACTION = 'INSERT'
    ),
    valid AS (
        SELECT t.*
        FROM typed AS t
        WHERE t.EVENT_ID IS NOT NULL
          AND t.TRADE_ID IS NOT NULL
          AND t.EVENT_TYPE IN ('I', 'U', 'D')
          AND t.SOURCE_SEQUENCE IS NOT NULL
          AND t.EVENT_TS IS NOT NULL
          AND t.QUANTITY > 0
          AND t.PRICE > 0
          AND t.TRADE_DATE IS NOT NULL
          AND EXISTS (
              SELECT 1
              FROM IB_LAB.DBT_DEV.STG_CLIENTS AS c
              WHERE c.CLIENT_ID = t.CLIENT_ID
          )
    )
    SELECT *
    FROM valid
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY TRADE_ID
        ORDER BY SOURCE_SEQUENCE DESC,
                 EVENT_TS DESC,
                 EVENT_ID DESC
    ) = 1
) AS src
ON tgt.TRADE_ID = src.TRADE_ID

-- Only a strictly newer business event can change the stored trade state.
WHEN MATCHED AND (
       src.SOURCE_SEQUENCE > tgt.LAST_SOURCE_SEQUENCE
    OR (
        src.SOURCE_SEQUENCE = tgt.LAST_SOURCE_SEQUENCE
        AND (
               src.EVENT_TS > tgt.LAST_EVENT_TS
            OR (src.EVENT_TS = tgt.LAST_EVENT_TS
                AND src.EVENT_ID > tgt.LAST_EVENT_ID)
        )
    )
) THEN UPDATE SET
    tgt.CLIENT_ID            = src.CLIENT_ID,
    tgt.ACCOUNT_ID           = src.ACCOUNT_ID,
    tgt.INSTRUMENT_ID        = src.INSTRUMENT_ID,
    tgt.SIDE                 = src.SIDE,
    tgt.QUANTITY             = src.QUANTITY,
    tgt.PRICE                = src.PRICE,
    tgt.TRADE_CURRENCY       = src.TRADE_CURRENCY,
    tgt.TRADE_DATE           = src.TRADE_DATE,
    tgt.LAST_SOURCE_SEQUENCE = src.SOURCE_SEQUENCE,
    tgt.LAST_EVENT_TS        = src.EVENT_TS,
    tgt.LAST_EVENT_ID        = src.EVENT_ID,
    tgt.LAST_EVENT_TYPE      = src.EVENT_TYPE,
    tgt.IS_DELETED           = (src.EVENT_TYPE = 'D'),
    tgt.SOURCE_FILE          = src.SOURCE_FILE,
    tgt.UPDATED_AT           = CURRENT_TIMESTAMP()::TIMESTAMP_LTZ(6)

WHEN NOT MATCHED THEN INSERT (
    TRADE_ID, CLIENT_ID, ACCOUNT_ID, INSTRUMENT_ID,
    SIDE, QUANTITY, PRICE, TRADE_CURRENCY, TRADE_DATE,
    LAST_SOURCE_SEQUENCE, LAST_EVENT_TS, LAST_EVENT_ID,
    LAST_EVENT_TYPE, IS_DELETED, SOURCE_FILE, UPDATED_AT
)
VALUES (
    src.TRADE_ID, src.CLIENT_ID, src.ACCOUNT_ID, src.INSTRUMENT_ID,
    src.SIDE, src.QUANTITY, src.PRICE, src.TRADE_CURRENCY, src.TRADE_DATE,
    src.SOURCE_SEQUENCE, src.EVENT_TS, src.EVENT_ID,
    src.EVENT_TYPE, (src.EVENT_TYPE = 'D'), src.SOURCE_FILE,
    CURRENT_TIMESTAMP()::TIMESTAMP_LTZ(6)
);

-- Tasks are SUSPENDED by default; enable only after verifying the target
-- bootstrap and inspecting the task's definition.
ALTER TASK IB_LAB.GOLD.APPLY_TRADE_CDC_TASK RESUME;

-- Single check: ensure the task exists and is STARTED.
SHOW TASKS LIKE 'APPLY_TRADE_CDC_TASK' IN SCHEMA IB_LAB.GOLD;

-- To pause while learning / avoid warehouse usage:
-- ALTER TASK IB_LAB.GOLD.APPLY_TRADE_CDC_TASK SUSPEND;
