-- Snowflake quality reconciliation — captured lab queries.
-- Input: existing native RAW and dbt views.
-- Expected confirmed counts for previous ingested batches: 1704 = 1696 + 8.
-- Do not include UNLOADED November 3 Parquet files in this count yet.

SELECT
    'STAGING' AS LAYER,
    COUNT(*) AS RECORDS
FROM IB_LAB.DBT_DEV.STG_TRADE_EVENTS
UNION ALL
SELECT
    'VALID',
    COUNT(*)
FROM IB_LAB.DBT_DEV.INT_VALID_TRADES
UNION ALL
SELECT
    'REJECTED',
    COUNT(*)
FROM IB_LAB.DBT_DEV.INT_REJECTED_TRADES;

-- Investigate the intentionally orphaned client:
-- Expected rejection reason after model fix: UNKNOWN_CLIENT_ID.
SELECT TRADE_ID, REJECTION_REASON
FROM IB_LAB.DBT_DEV.INT_REJECTED_TRADES
WHERE TRADE_ID = 'BAD0002082';

-- Reconciliation invariant:
SELECT
    (SELECT COUNT(*) FROM IB_LAB.DBT_DEV.STG_TRADE_EVENTS) AS STAGING_COUNT,
    (SELECT COUNT(*) FROM IB_LAB.DBT_DEV.INT_VALID_TRADES) AS VALID_COUNT,
    (SELECT COUNT(*) FROM IB_LAB.DBT_DEV.INT_REJECTED_TRADES) AS REJECTED_COUNT,
    (
        (SELECT COUNT(*) FROM IB_LAB.DBT_DEV.STG_TRADE_EVENTS)
      - (SELECT COUNT(*) FROM IB_LAB.DBT_DEV.INT_VALID_TRADES)
      - (SELECT COUNT(*) FROM IB_LAB.DBT_DEV.INT_REJECTED_TRADES)
    ) AS RECONCILIATION_DELTA;
-- Pass condition: RECONCILIATION_DELTA = 0.
