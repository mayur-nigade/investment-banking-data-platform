{{ config(materialized='view') }}

{#
  SCD Type 2-style READ MODEL, not a persisted dbt snapshot.
  Reconstructed from append-only Iceberg history via int_valid_trades.
  Every valid business event is a version, including D tombstones.
  Assumes increasing SOURCE_SEQUENCE per trade. Event timestamps must be
  monotonic for reliable timestamp-based "as of" joins; source sequence
  remains the authoritative version order.
#}

WITH source_events AS (
    SELECT
        EVENT_ID,
        TRADE_ID,
        EVENT_TYPE,
        SOURCE_SEQUENCE,
        EVENT_TS,
        CLIENT_ID,
        ACCOUNT_ID,
        INSTRUMENT_ID,
        SIDE,
        QUANTITY,
        PRICE,
        TRADE_CURRENCY,
        TRADE_DATE,
        SOURCE_FILE,
        FILE_ROW_NUMBER,
        INGESTED_AT,
        ROW_NUMBER() OVER (
            PARTITION BY EVENT_ID
            ORDER BY INGESTED_AT DESC,
                     SOURCE_FILE DESC,
                     FILE_ROW_NUMBER DESC
        ) AS EVENT_DEDUPE_RN
    FROM {{ ref('int_valid_trades') }}
    WHERE EVENT_ID IS NOT NULL
      AND TRADE_ID IS NOT NULL
      AND SOURCE_SEQUENCE IS NOT NULL
      AND EVENT_TS IS NOT NULL
),
ordered_versions AS (
    SELECT
        EVENT_ID,
        TRADE_ID,
        EVENT_TYPE,
        SOURCE_SEQUENCE,
        EVENT_TS,
        CLIENT_ID,
        ACCOUNT_ID,
        INSTRUMENT_ID,
        SIDE,
        QUANTITY,
        PRICE,
        TRADE_CURRENCY,
        TRADE_DATE,
        SOURCE_FILE,
        INGESTED_AT,
        ROW_NUMBER() OVER (
            PARTITION BY TRADE_ID
            ORDER BY SOURCE_SEQUENCE ASC, EVENT_TS ASC, EVENT_ID ASC
        ) AS VERSION_NO,
        LEAD(SOURCE_SEQUENCE) OVER (
            PARTITION BY TRADE_ID
            ORDER BY SOURCE_SEQUENCE ASC, EVENT_TS ASC, EVENT_ID ASC
        ) AS NEXT_SOURCE_SEQUENCE,
        LEAD(EVENT_TS) OVER (
            PARTITION BY TRADE_ID
            ORDER BY SOURCE_SEQUENCE ASC, EVENT_TS ASC, EVENT_ID ASC
        ) AS NEXT_EVENT_TS
    FROM source_events
    WHERE EVENT_DEDUPE_RN = 1
)
SELECT
    TRADE_ID,
    VERSION_NO,
    EVENT_ID,
    EVENT_TYPE,
    CLIENT_ID,
    ACCOUNT_ID,
    INSTRUMENT_ID,
    SIDE,
    QUANTITY,
    PRICE,
    TRADE_CURRENCY,
    TRADE_DATE,
    SOURCE_SEQUENCE AS VALID_FROM_SEQUENCE,
    NEXT_SOURCE_SEQUENCE AS VALID_TO_SEQUENCE_EXCLUSIVE,
    EVENT_TS AS VALID_FROM_TS,
    NEXT_EVENT_TS AS VALID_TO_TS_EXCLUSIVE,
    (NEXT_SOURCE_SEQUENCE IS NULL) AS IS_CURRENT,
    (EVENT_TYPE = 'D') AS IS_DELETED,
    SOURCE_FILE,
    INGESTED_AT
FROM ordered_versions
