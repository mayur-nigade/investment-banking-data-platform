{{ config(materialized='view') }}

-- Preserve the actual classified DQ_REASON, not generic DQ_STATUS='INVALID'.
-- Lab evidence: INVALID_PRICE=2, INVALID_EVENT_TYPE=1, INVALID_QUANTITY=1,
-- INVALID_TRADE_DATE=1, INVALID_EVENT_TS=1, MISSING_TRADE_ID=1;
-- after client integrity fix, UNKNOWN_CLIENT_ID was also classified.
-- Reconstructed from prior model intent; validate against live Workspace SQL.
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
    INGESTED_AT,
    DQ_REASON AS REJECTION_REASON
FROM {{ ref('stg_trade_events') }}
WHERE DQ_STATUS = 'INVALID'
