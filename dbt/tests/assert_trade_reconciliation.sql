-- dbt singular test: returns rows only when reconciliation fails.
WITH counts AS (
    SELECT
        (SELECT COUNT(*) FROM {{ ref('stg_trade_events') }}) AS staging_count,
        (SELECT COUNT(*) FROM {{ ref('int_valid_trades') }}) AS valid_count,
        (SELECT COUNT(*) FROM {{ ref('int_rejected_trades') }}) AS rejected_count
)
SELECT *
FROM counts
WHERE staging_count <> valid_count + rejected_count
