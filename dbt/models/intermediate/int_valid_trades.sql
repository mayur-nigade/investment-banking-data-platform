{{ config(materialized='view') }}

-- Reconstructed from observed lab model behavior. Cross-check your
-- Snowflake Workspaces original before treating this as the canonical source.
SELECT * EXCLUDE (DQ_STATUS)
FROM {{ ref('stg_trade_events') }}
WHERE DQ_STATUS = 'VALID'
