-- End-to-end CDC smoke test: Iceberg -> append-only Stream -> triggered Task -> Gold.
-- STATUS: PLANNED (not executed). Run after user has created and bootstrapped
-- IB_LAB.GOLD.CURRENT_TRADES and created APPLY_TRADE_CDC_TASK.
-- Uses a brand-new synthetic TRADE_ID and EVENT_ID. Safe for public portfolio.
-- One-time test guard: do not replay same EVENT_ID.

USE WAREHOUSE COMPUTE_WH;

-- 1) Ensure triggered task is enabled.
ALTER TASK IB_LAB.GOLD.APPLY_TRADE_CDC_TASK RESUME;

-- 2) Insert a brand-new I business event into Snowflake-managed Iceberg.
-- Seed with known-good non-key fields from one ACTIVE synthetic Gold record.
-- If Gold has no suitable active row, INSERT returns 0 rows; bootstrap first.
INSERT INTO IB_LAB.BRONZE.RAW_TRADE_EVENTS_ICEBERG
(
    EVENT_ID, TRADE_ID, EVENT_TYPE, SOURCE_SEQUENCE,
    CLIENT_ID, ACCOUNT_ID, INSTRUMENT_ID, SIDE,
    QUANTITY, PRICE, TRADE_CURRENCY, TRADE_DATE,
    EVENT_TS, SOURCE_SYSTEM, SOURCE_FILE,
    FILE_ROW_NUMBER, INGESTED_AT
)
SELECT
    'LAB_CDC_EVT_20261110_001',
    'LAB_CDC_TRD_20261110_001',
    'I',
    '1',
    s.CLIENT_ID,
    s.ACCOUNT_ID,
    s.INSTRUMENT_ID,
    s.SIDE,
    TO_VARCHAR(s.QUANTITY),
    TO_VARCHAR(s.PRICE),
    s.TRADE_CURRENCY,
    TO_VARCHAR(s.TRADE_DATE, 'YYYY-MM-DD'),
    TO_VARCHAR(
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ,
        'YYYY-MM-DD HH24:MI:SS.FF6'
    ),
    'LAB_CDC_TEST',
    'manual/cdc_trigger_test',
    1,
    CURRENT_TIMESTAMP()::TIMESTAMP_LTZ(6)
FROM (
    SELECT
        CLIENT_ID, ACCOUNT_ID, INSTRUMENT_ID, SIDE,
        QUANTITY, PRICE, TRADE_CURRENCY, TRADE_DATE
    FROM IB_LAB.GOLD.CURRENT_TRADES
    WHERE IS_DELETED = FALSE
      AND CLIENT_ID IS NOT NULL
      AND QUANTITY > 0
      AND PRICE > 0
    QUALIFY ROW_NUMBER() OVER (ORDER BY TRADE_ID) = 1
) s
WHERE NOT EXISTS (
    SELECT 1
    FROM IB_LAB.BRONZE.RAW_TRADE_EVENTS_ICEBERG
    WHERE EVENT_ID = 'LAB_CDC_EVT_20261110_001'
);
-- First run expected INSERT: 1 row. Second run expected: 0 rows.

-- 3) Task execution history. TRIGGER / SUCCEEDED proves automatic execution.
-- TASK_NAME parameter accepts non-qualified task name, not a full object path.
SELECT
    NAME, STATE, SCHEDULED_FROM, ERROR_MESSAGE, COMPLETED_TIME
FROM TABLE(
    IB_LAB.INFORMATION_SCHEMA.TASK_HISTORY(
        TASK_NAME => 'APPLY_TRADE_CDC_TASK',
        RESULT_LIMIT => 10
    )
)
WHERE DATABASE_NAME = 'IB_LAB'
  AND SCHEMA_NAME = 'GOLD'
ORDER BY SCHEDULED_TIME DESC;

-- 4) Target business state. A new I event should be inserted as active trade.
SELECT
    TRADE_ID,
    LAST_EVENT_TYPE,
    LAST_SOURCE_SEQUENCE,
    IS_DELETED,
    UPDATED_AT
FROM IB_LAB.GOLD.CURRENT_TRADES
WHERE TRADE_ID = 'LAB_CDC_TRD_20261110_001';
-- Expected: one row, LAST_EVENT_TYPE=I, LAST_SOURCE_SEQUENCE=1,
-- IS_DELETED=FALSE.
--
-- Troubleshooting:
-- * If INSERT added 0 on first run, Gold bootstrap may be empty or event
--   was previously inserted: inspect source and event_id.
-- * If task not STARTED, ALTER TASK ... RESUME (above) and check task state.
-- * If TASK_HISTORY.state=FAILED, read ERROR_MESSAGE; do not replay event.
-- * SELECT FROM STREAM is NOT a safe acknowledgement; stream is consumed
--   through task's MERGE DML upon successful transaction commit.
-- * No production secrets or real trading data are used.
