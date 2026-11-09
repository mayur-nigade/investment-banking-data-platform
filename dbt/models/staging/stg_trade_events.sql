{{ config(materialized='view') }}

{#
  Trade-event staging and data-quality classification.
  Reconstructed from the completed Snowflake-native dbt lab and its observed
  counts/rejection categories. Confirm this file against the authoritative
  Snowflake Workspaces model before treating it as an exact export.

  Source: 14 raw trade string fields + SOURCE_FILE, FILE_ROW_NUMBER, INGESTED_AT.
  The source table remains immutable; TRY_TO_* conversions happen in this view.
#}

WITH typed_data AS (

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
        NULLIF(TRIM(SOURCE_SYSTEM), '') AS SOURCE_SYSTEM,
        SOURCE_FILE,
        FILE_ROW_NUMBER,
        INGESTED_AT
    -- Pending workspace cutover, GitHub snapshot now points at Iceberg Bronze.
    FROM {{ source('ib_iceberg', 'RAW_TRADE_EVENTS_ICEBERG') }}

),

validation AS (

    SELECT
        t.*,

        -- One prioritized reason per row; actual invalid input is preserved in RAW.
        CASE
            WHEN t.TRADE_ID IS NULL
                THEN 'MISSING_TRADE_ID'
            WHEN t.QUANTITY IS NULL OR t.QUANTITY <= 0
                THEN 'INVALID_QUANTITY'
            WHEN t.PRICE IS NULL OR t.PRICE <= 0
                THEN 'INVALID_PRICE'
            WHEN t.TRADE_DATE IS NULL
                THEN 'INVALID_TRADE_DATE'
            WHEN t.EVENT_TS IS NULL
                THEN 'INVALID_EVENT_TS'
            WHEN t.EVENT_TYPE IS NULL OR t.EVENT_TYPE NOT IN ('I', 'U', 'D')
                THEN 'INVALID_EVENT_TYPE'
            WHEN t.CLIENT_ID IS NULL
                THEN 'MISSING_CLIENT_ID'
            WHEN c.CLIENT_ID IS NULL
                THEN 'UNKNOWN_CLIENT_ID'
            ELSE NULL
        END AS DQ_REASON

    FROM typed_data AS t
    LEFT JOIN {{ ref('stg_clients') }} AS c
        ON t.CLIENT_ID = c.CLIENT_ID

)

SELECT
    *,
    IFF(DQ_REASON IS NULL, 'VALID', 'INVALID') AS DQ_STATUS
FROM validation
