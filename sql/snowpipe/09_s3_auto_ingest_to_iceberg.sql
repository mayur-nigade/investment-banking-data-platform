-- Snowpipe AUTO_INGEST -> existing Snowflake-managed Iceberg Bronze.
-- STATUS: PLANNED / NOT EXECUTED IN SNOWFLAKE (2026-11-11).
-- Prerequisites already completed in the lab:
--   - IB_LAB.RAW.IB_S3_STAGE points to s3://omera-ib-trade-lake/landingzone/
--   - IB_LAB.RAW.IB_PARQUET_FORMAT has TYPE=PARQUET
--   - IB_LAB.BRONZE.RAW_TRADE_EVENTS_ICEBERG is an existing managed Iceberg table
--   - Same 17-column mapping was already exercised by bulk COPY of five files.
--
-- NEW PREFIX ONLY:
--   Stage:  @IB_LAB.RAW.IB_S3_STAGE/parquet/auto/
--   S3:     s3://omera-ib-trade-lake/landingzone/parquet/auto/
-- The five 2026-11-03 files are under parquet/batches/ and are OUTSIDE this
-- pipe's watched prefix. DO NOT refresh/reload them through this pipe.
--
-- Snowpipe supports Iceberg FULL_INGEST and COPY SELECT transformations.
-- No WAREHOUSE option: standard Snowpipe file ingest uses serverless compute.
-- The S3 notification must still be configured in the AWS console.
-- NO PURGE (not supported for Snowpipe pipe COPY); source files remain in S3.
-- Snowpipe does NOT support ON_ERROR=ABORT_STATEMENT (bulk COPY does).
-- ON_ERROR=SKIP_FILE is the Snowpipe default; we set it explicitly so a
-- malformed file fails as a unit but does not block unrelated good files.
-- NO FORCE=TRUE / NO manual second COPY on files delivered to this pipe.

-- Step 1: Create the pipe once. USE same field mapping as proven manual COPY.
CREATE PIPE IF NOT EXISTS IB_LAB.BRONZE.TRADE_EVENTS_AUTO_PIPE
  AUTO_INGEST = TRUE
  COMMENT = 'Synthetic trade Parquet to Iceberg Bronze via S3 events; parquet/auto only'
AS
COPY INTO IB_LAB.BRONZE.RAW_TRADE_EVENTS_ICEBERG (
    EVENT_ID, TRADE_ID, EVENT_TYPE, SOURCE_SEQUENCE,
    CLIENT_ID, ACCOUNT_ID, INSTRUMENT_ID, SIDE,
    QUANTITY, PRICE, TRADE_CURRENCY, TRADE_DATE,
    EVENT_TS, SOURCE_SYSTEM, SOURCE_FILE,
    FILE_ROW_NUMBER, INGESTED_AT
)
FROM (
    SELECT
        t.$1:event_id::VARCHAR,
        t.$1:trade_id::VARCHAR,
        t.$1:event_type::VARCHAR,
        t.$1:source_sequence::VARCHAR,
        t.$1:client_id::VARCHAR,
        t.$1:account_id::VARCHAR,
        t.$1:instrument_id::VARCHAR,
        t.$1:side::VARCHAR,
        t.$1:quantity::VARCHAR,
        t.$1:price::VARCHAR,
        t.$1:trade_currency::VARCHAR,
        t.$1:trade_date::VARCHAR,
        t.$1:event_ts::VARCHAR,
        t.$1:source_system::VARCHAR,
        METADATA$FILENAME,
        METADATA$FILE_ROW_NUMBER,
        METADATA$START_SCAN_TIME::TIMESTAMP_LTZ(6)
    FROM @IB_LAB.RAW.IB_S3_STAGE/parquet/auto/ t
)
FILE_FORMAT = (FORMAT_NAME = 'IB_LAB.RAW.IB_PARQUET_FORMAT')
LOAD_MODE = FULL_INGEST
ON_ERROR = SKIP_FILE;

-- Step 2: Copy the actual Snowflake-managed SQS queue ARN from
-- the 'notification_channel' field in this output.
DESC PIPE IB_LAB.BRONZE.TRADE_EVENTS_AUTO_PIPE;

-- Step 3 (AWS console, NOT Snowflake SQL):
--   S3 bucket: omera-ib-trade-lake
--   Properties -> Event notifications -> Create
--   Name: omera-trade-snowpipe-auto
--   Event types: All object create (s3:ObjectCreated:*)
--   Prefix: landingzone/parquet/auto/
--   Suffix: .parquet
--   Destination: SQS queue -> Enter ARN from DESC PIPE notification_channel
--   Save. If notification overlaps an existing one, coordinate using SNS
--   instead of adding a conflicting overlapping S3 notification.
--   Never make this notification point at the Iceberg WRITE location!
--
-- Step 4: Upload a truly NEW synthetic Parquet file with the 14 known
-- LOWERCASE source field names to:
--   s3://omera-ib-trade-lake/landingzone/parquet/auto/trade_events_auto_001.parquet
-- For the Gold business tests, choose a valid CLIENT_ID and new EVENT_ID,
-- TRADE_ID and increasing SOURCE_SEQUENCE. Avoid copying previous file
-- contents; event-level idempotency is intentionally the NEXT hardening phase.
-- There is no need to call ALTER PIPE ... REFRESH for a new file uploaded
-- AFTER the S3 notification is configured.
--
-- Step 5: Status and ONE targeted proof of arrival.
SELECT SYSTEM$PIPE_STATUS('IB_LAB.BRONZE.TRADE_EVENTS_AUTO_PIPE') AS PIPE_STATUS;

SELECT
    SOURCE_FILE,
    COUNT(*) AS ROWS_INGESTED
FROM IB_LAB.BRONZE.RAW_TRADE_EVENTS_ICEBERG
WHERE SOURCE_FILE LIKE '%parquet/auto/%'
GROUP BY SOURCE_FILE
ORDER BY SOURCE_FILE;

-- OPTIONAL: recent Snowpipe load history and error messages
SELECT
    FILE_NAME,
    STATUS,
    ROW_COUNT,
    FIRST_ERROR_MESSAGE,
    LAST_LOAD_TIME
FROM TABLE(
    IB_LAB.INFORMATION_SCHEMA.COPY_HISTORY(
        TABLE_NAME => 'IB_LAB.BRONZE.RAW_TRADE_EVENTS_ICEBERG',
        START_TIME => DATEADD('hour', -3, CURRENT_TIMESTAMP())
    )
)
WHERE FILE_NAME LIKE '%parquet/auto/%'
ORDER BY LAST_LOAD_TIME DESC;

-- Once file is confirmed in Iceberg, the CURRENT_TRADES stream/task can run.
-- If physical SCD2 increment task has been created, it only runs automatically
-- once ALTER TASK IB_LAB.GOLD.APPLY_TRADE_SCD2_TASK RESUME succeeded.
-- Do not execute ALTER PIPE ... REFRESH here: it is for controlled backfill,
-- may enqueue files that were present before event notification setup.
