{{ config(materialized='view') }}

-- Reconstructed from observed lab behavior; check against actual Workspaces SQL.
SELECT * EXCLUDE (DQ_STATUS),
       DQ_STATUS AS REJECTION_REASON
FROM {{ ref('stg_trade_events') }}
WHERE DQ_STATUS <> 'VALID'
