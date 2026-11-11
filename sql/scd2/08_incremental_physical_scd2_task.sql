-- PRODUCTION-STYLE INCREMENTAL PHYSICAL SCD2 MAINTENANCE.
-- STATUS: PLANNED; not yet created, resumed, or executed in the user's Snowflake.
-- Run AFTER 07_physical_scd2_bootstrap.sql succeeded.
--
-- DESIGN:
-- - A DIFFERENT append-only stream than CURRENT_TRADES: independent offset.
-- - Stored procedure consumes *all* incoming stream rows via INSERT DML,
--   retains invalid-event diagnostics in a durable reject audit.
-- - For valid trade IDs in the batch, rebuild entire version chains from
--   authoritative event history. Handles out-of-order / late sequence events,
--   duplicate EVENT_ID replays, changed attributes, and deletion tombstones.
-- - DELETE old versions + INSERT new versions + audit + stream offset
--   advancement all happen inside one explicit Snowflake transaction.
-- - On SQL error, ROLLBACK preserves the old history and stream offset;
--   Snowflake TASK_HISTORY records failure.
--
-- NOT A FULL REGULATED-PRODUCTION DEPLOYMENT until access controls,
-- schema/contract tests, state hash collision policy, timestamp ordering,
-- monitoring, costs, validation of incomplete D events, and DR are addressed.
--
-- REQUIREMENT: no overlapping manual writes to SCD2_HISTORY while task runs.

USE WAREHOUSE COMPUTE_WH;

CREATE OR REPLACE PROCEDURE IB_LAB.GOLD.SP_APPLY_TRADE_SCD2()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
    v_run_id VARCHAR DEFAULT UUID_STRING();
    v_input_count NUMBER DEFAULT 0;
    v_changed_trade_count NUMBER DEFAULT 0;
BEGIN
    -- Procedure-scoped scratch table: not a durable queue.
    -- DDL occurs BEFORE BEGIN TRANSACTION, outside the atomic update.
    CREATE OR REPLACE PROCEDURE SCOPED TEMP TABLE scd2_delta_batch (
        EVENT_ID VARCHAR,
        TRADE_ID VARCHAR,
        SOURCE_FILE VARCHAR
    );

    BEGIN TRANSACTION;

    -- This is the DML that consumes this DEDICATED stream on COMMIT.
    -- Do NOT filter rows by dbt validation here; preserve invalid event keys
    -- so they can be written to the persistent REJECT_AUDIT.
    INSERT INTO scd2_delta_batch (EVENT_ID, TRADE_ID, SOURCE_FILE)
    SELECT EVENT_ID, TRADE_ID, SOURCE_FILE
    FROM IB_LAB.BRONZE.TRADE_EVENTS_SCD2_STREAM
    WHERE METADATA$ACTION = 'INSERT';

    SELECT COUNT(*) INTO :v_input_count
    FROM scd2_delta_batch;

    SELECT COUNT(DISTINCT d.TRADE_ID) INTO :v_changed_trade_count
    FROM scd2_delta_batch AS d
    WHERE EXISTS (
        SELECT 1
        FROM IB_LAB.DBT_DEV.INT_VALID_TRADES AS v
        WHERE v.EVENT_ID = d.EVENT_ID
          AND v.TRADE_ID = d.TRADE_ID
    );

    -- Preserve the failed validations; not silently dropped on stream advance.
    INSERT INTO IB_LAB.GOLD.SCD2_REJECT_AUDIT (
        RUN_ID, RECORDED_AT, EVENT_ID, TRADE_ID, SOURCE_FILE, REASON
    )
    SELECT
        :v_run_id,
        CURRENT_TIMESTAMP(),
        d.EVENT_ID,
        d.TRADE_ID,
        d.SOURCE_FILE,
        CASE
            WHEN NULLIF(TRIM(d.EVENT_ID), '') IS NULL THEN 'MISSING_EVENT_ID'
            WHEN NULLIF(TRIM(d.TRADE_ID), '') IS NULL THEN 'MISSING_TRADE_ID'
            ELSE 'NOT_IN_VALID_TRADES_VIEW'
        END
    FROM scd2_delta_batch AS d
    WHERE NOT EXISTS (
        SELECT 1
        FROM IB_LAB.DBT_DEV.INT_VALID_TRADES AS v
        WHERE v.EVENT_ID = d.EVENT_ID
          AND v.TRADE_ID = d.TRADE_ID
    );

    -- Rebuild ONLY affected valid TRADE_IDs; unrelated history is untouched.
    -- DELETE + INSERT share this transaction and are invisible until COMMIT.
    DELETE FROM IB_LAB.GOLD.TRADE_SCD2_HISTORY AS h
    WHERE EXISTS (
        SELECT 1
        FROM scd2_delta_batch AS d
        WHERE d.TRADE_ID = h.TRADE_ID
          AND EXISTS (
              SELECT 1
              FROM IB_LAB.DBT_DEV.INT_VALID_TRADES AS v
              WHERE v.EVENT_ID = d.EVENT_ID
                AND v.TRADE_ID = d.TRADE_ID
          )
    );

    INSERT INTO IB_LAB.GOLD.TRADE_SCD2_HISTORY (
        TRADE_ID, VERSION_NO, EVENT_ID, EVENT_TYPE,
        CLIENT_ID, ACCOUNT_ID, INSTRUMENT_ID, SIDE,
        QUANTITY, PRICE, TRADE_CURRENCY, TRADE_DATE,
        VALID_FROM_SEQUENCE, VALID_TO_SEQUENCE_EXCLUSIVE,
        VALID_FROM_TS, VALID_TO_TS_EXCLUSIVE,
        IS_CURRENT, IS_DELETED, ATTR_HASH, SOURCE_FILE, INGESTED_AT
    )
    SELECT
        h.TRADE_ID, h.VERSION_NO, h.EVENT_ID, h.EVENT_TYPE,
        h.CLIENT_ID, h.ACCOUNT_ID, h.INSTRUMENT_ID, h.SIDE,
        h.QUANTITY, h.PRICE, h.TRADE_CURRENCY, h.TRADE_DATE,
        h.VALID_FROM_SEQUENCE, h.VALID_TO_SEQUENCE_EXCLUSIVE,
        h.VALID_FROM_TS, h.VALID_TO_TS_EXCLUSIVE,
        h.IS_CURRENT, h.IS_DELETED, h.ATTR_HASH, h.SOURCE_FILE, h.INGESTED_AT
    FROM IB_LAB.GOLD.V_TRADE_SCD2_DESIRED AS h
    INNER JOIN (
        SELECT DISTINCT d.TRADE_ID
        FROM scd2_delta_batch AS d
        WHERE EXISTS (
            SELECT 1
            FROM IB_LAB.DBT_DEV.INT_VALID_TRADES AS v
            WHERE v.EVENT_ID = d.EVENT_ID
              AND v.TRADE_ID = d.TRADE_ID
        )
    ) AS changed
      ON changed.TRADE_ID = h.TRADE_ID;

    INSERT INTO IB_LAB.GOLD.SCD2_APPLY_AUDIT (
        RUN_ID, APPLIED_AT, INPUT_EVENT_COUNT, CHANGED_TRADE_COUNT, STATUS
    )
    VALUES (
        :v_run_id, CURRENT_TIMESTAMP(),
        :v_input_count, :v_changed_trade_count, 'SUCCEEDED'
    );

    COMMIT;

    RETURN 'SCD2 applied: input_events=' || v_input_count::VARCHAR
        || ', changed_trade_ids=' || v_changed_trade_count::VARCHAR
        || ', run_id=' || v_run_id;

EXCEPTION
    WHEN OTHER THEN
        ROLLBACK;
        RAISE;
END;
$$;

-- A triggered task invokes the transactional procedure.
-- Create and RESUME ONLY after testing a direct manual CALL in a safe lab.
CREATE TASK IF NOT EXISTS IB_LAB.GOLD.APPLY_TRADE_SCD2_TASK
    WAREHOUSE = COMPUTE_WH
    WHEN SYSTEM$STREAM_HAS_DATA(
        'IB_LAB.BRONZE.TRADE_EVENTS_SCD2_STREAM'
    )
AS
    CALL IB_LAB.GOLD.SP_APPLY_TRADE_SCD2();

-- When ready:
-- ALTER TASK IB_LAB.GOLD.APPLY_TRADE_SCD2_TASK RESUME;

-- Targeted task-state check:
SHOW TASKS LIKE 'APPLY_TRADE_SCD2_TASK' IN SCHEMA IB_LAB.GOLD;

-- One business integrity test (should return no rows):
SELECT TRADE_ID, COUNT_IF(IS_CURRENT) AS CURRENT_VERSIONS
FROM IB_LAB.GOLD.TRADE_SCD2_HISTORY
GROUP BY TRADE_ID
HAVING COUNT_IF(IS_CURRENT) <> 1;

-- One tiny audit inspection:
SELECT *
FROM IB_LAB.GOLD.SCD2_APPLY_AUDIT
ORDER BY APPLIED_AT DESC
LIMIT 5;
