-- Staged Parquet inspection (Snowflake SQL)
-- Executable against the existing IB_LAB RAW schema / external stage.
-- Batch 2026-11-03 is UNLOADED. These commands do NOT ingest records.
-- S3 base: s3://omera-ib-trade-lake/landingzone/

-- 1. Create a reusable reader for Parquet rather than the CSV reader on the stage.
CREATE FILE FORMAT IF NOT EXISTS IB_LAB.RAW.IB_PARQUET_FORMAT
    TYPE = PARQUET
    USE_LOGICAL_TYPE = TRUE
    USE_VECTORIZED_SCANNER = TRUE;

-- 2. List files visible through the IAM-backed external stage.
LIST @IB_LAB.RAW.IB_S3_STAGE/parquet/batches/batch_2026-11-03/;

-- 3. Inspect column names, types and nullability from Parquet metadata.
-- Verified output on 2026-11-08: 14 columns, all TYPE=TEXT/NULLABLE=TRUE.
SELECT COLUMN_NAME, TYPE, NULLABLE, FILENAMES
FROM TABLE(
    INFER_SCHEMA(
        LOCATION => '@IB_LAB.RAW.IB_S3_STAGE/parquet/batches/batch_2026-11-03/',
        FILE_FORMAT => 'IB_LAB.RAW.IB_PARQUET_FORMAT',
        MAX_FILE_COUNT => 5
    )
)
ORDER BY ORDER_ID;

-- 4. Count actual staged rows. Verified per file: 79 / 79 / 78 / 78 / 78 = 392.
SELECT METADATA$FILENAME AS SOURCE_FILE, COUNT(*) AS RECORD_COUNT
FROM @IB_LAB.RAW.IB_S3_STAGE/parquet/batches/batch_2026-11-03/
    (FILE_FORMAT => 'IB_LAB.RAW.IB_PARQUET_FORMAT') t
GROUP BY 1
ORDER BY 1;

-- 5. Read sample rows by Parquet FIELD NAME ($1 is a structured variant).
-- CSV would instead use positional $1/$2/$3.
SELECT
    t.$1:event_id::VARCHAR   AS EVENT_ID,
    t.$1:trade_id::VARCHAR   AS TRADE_ID,
    t.$1:quantity::VARCHAR   AS QUANTITY,
    t.$1:price::VARCHAR      AS PRICE,
    t.$1:event_type::VARCHAR AS EVENT_TYPE
FROM @IB_LAB.RAW.IB_S3_STAGE/parquet/batches/batch_2026-11-03/
    (FILE_FORMAT => 'IB_LAB.RAW.IB_PARQUET_FORMAT') t
LIMIT 10;
