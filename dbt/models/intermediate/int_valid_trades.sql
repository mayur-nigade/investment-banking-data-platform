{{ config(materialized='view') }}

-- Filter validated events using dbt ref(), which creates dependency lineage.
-- Reconstructed from the executed lab design; compare with Snowflake Workspaces
-- before representing it as the exact exported source.
SELECT
    EVENT_ID,
    TRADE_ID,
    CLIENT_ID,
    ACCOUNT_ID,
    INSTRUMENT_ID,
    EVENT_TYPE,
    SIDE,
    QUANTITY,
    PRICE,
    TRADE_DATE,
    EVENT_TS,
    SOURCE_SEQUENCE,
    TRADE_CURRENCY,
    SOURCE_FILE,
    FILE_ROW_NUMBER,
    INGESTED_AT
FROM {{ ref('stg_trade_events') }}
WHERE DQ_STATUS = 'VALID'
