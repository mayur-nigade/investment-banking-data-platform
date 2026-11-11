# Snowpipe → Iceberg Bronze — auto ingest runbook (2026-11-11)

**Status:** SQL prepared in GitHub; **not yet executed** in the user's Snowflake account. The S3 bucket event notification must be set up using AWS Console. This is the final ingest automation component of the existing synthetic investment-banking learning pipeline.

## Existing resources — keep, don't recreate

- **Bucket**: `s3://omera-ib-trade-lake/`
- **External stage**: `IB_LAB.RAW.IB_S3_STAGE`, rooted at `s3://omera-ib-trade-lake/landingzone/`.
- **Parquet reader**: `IB_LAB.RAW.IB_PARQUET_FORMAT`, `TYPE=PARQUET USE_LOGICAL_TYPE=TRUE USE_VECTORIZED_SCANNER=TRUE`.
- **Bronze target**: `IB_LAB.BRONZE.RAW_TRADE_EVENTS_ICEBERG`, existing Snowflake-managed Iceberg table, 17 fields.
- **Already loaded files**: `landingzone/parquet/batches/batch_2026-11-03/` (five files/392 events), plus prior 1704 events.
- **NEW auto-load prefix**: `s3://omera-ib-trade-lake/landingzone/parquet/auto/`, which maps to `@IB_LAB.RAW.IB_S3_STAGE/parquet/auto/`.
- **Iceberg output storage**: `s3://omera-ib-trade-lake/iceberg/bronze/`. This is NOT an incoming stage or notification prefix.

## Deployment: 3 actions

### A — Create Snowpipe in Snowflake

Run the `CREATE PIPE ... AUTO_INGEST=TRUE AS COPY INTO ...` in [sql/snowpipe/09_s3_auto_ingest_to_iceberg.sql](../sql/snowpipe/09_s3_auto_ingest_to_iceberg.sql).

The COPY SELECT maps 14 lowercase Parquet source fields by name to 14 VARCHAR Iceberg columns, plus `METADATA$FILENAME`, `METADATA$FILE_ROW_NUMBER`, and `METADATA$START_SCAN_TIME::TIMESTAMP_LTZ(6)`. It uses `LOAD_MODE=FULL_INGEST`, since transformation is necessary; `ADD_FILES_COPY` cannot be used for transformations. No `PURGE` option (unsupported in pipe COPY); no `FORCE`. The pipe uses **Snowflake-managed serverless file-ingest compute**, not `COMPUTE_WH`.

```sql
DESC PIPE IB_LAB.BRONZE.TRADE_EVENTS_AUTO_PIPE;
```

Copy the actual **`notification_channel`** (Snowflake-managed SQS queue ARN) returned by Snowflake. Don't invent it.

### B — Wire AWS S3 object-created events to the pipe

AWS Console → S3 → bucket **`omera-ib-trade-lake`** → **Properties** → **Event notifications** → **Create event notification**:

- Name: `omera-trade-snowpipe-auto`
- Prefix: `landingzone/parquet/auto/`
- Suffix: `.parquet`
- Events: **All object create events** (`s3:ObjectCreated:*`, including multipart completion).
- Destination: **SQS queue** → **Enter SQS queue ARN**, paste `DESC PIPE.notification_channel`.
- Save.

AWS notifications operate on bucket object-key prefixes, **not** Snowflake stage-relative paths. If AWS reports overlapping notifications, **do not override an existing consumer**; use a compatible SNS fan-out configuration as described in the Snowflake guide. Ensure the storage integration's IAM access covers the new incoming prefix.

### C — Upload ONE new synthetic file, check once

Generate one new Parquet with same 14 source keys: `event_id, trade_id, event_type, source_sequence, client_id, account_id, instrument_id, side, quantity, price, trade_currency, trade_date, event_ts, source_system`.

Upload to **`s3://omera-ib-trade-lake/landingzone/parquet/auto/trade_events_auto_001.parquet`** (only after notification setup). To demonstrate Gold end-to-end without quality rejects, use new stable `EVENT_ID` and `TRADE_ID`, `EVENT_TYPE='I'`, sequence `1`, and a `CLIENT_ID` present in `DBT_DEV.STG_CLIENTS`; all numeric/date fields valid. Do not copy old events under a new filename yet; replay/idempotency lab is deferred.

```sql
SELECT SYSTEM$PIPE_STATUS('IB_LAB.BRONZE.TRADE_EVENTS_AUTO_PIPE') AS PIPE_STATUS;

SELECT SOURCE_FILE, COUNT(*) AS ROWS_INGESTED
FROM IB_LAB.BRONZE.RAW_TRADE_EVENTS_ICEBERG
WHERE SOURCE_FILE LIKE '%parquet/auto/%'
GROUP BY SOURCE_FILE ORDER BY SOURCE_FILE;
```

If the file appears in Iceberg, Snowpipe is working. The existing `TRADE_EVENTS_APPEND_STREAM` and `APPLY_TRADE_CDC_TASK` can process it into `GOLD.CURRENT_TRADES`. The independent `TRADE_EVENTS_SCD2_STREAM` can feed `GOLD.APPLY_TRADE_SCD2_TASK` only if that task is created and **RESUMED**.

For end-to-end proof, check the new TRADE_ID in `GOLD.CURRENT_TRADES` and in `GOLD.TRADE_SCD2_HISTORY` (if its task is enabled). No broad historical recount needed.

### Not automatically needed

`ALTER PIPE ... REFRESH` is **not required** for a file uploaded after S3 notifications are configured. It is for manual reconciliation of pre-existing files and supports a limited recent history window; run only intentionally, not against prior batches. Do not combine manual COPY and Snowpipe on the same auto-prefix unless you have coordinated file ingest history.

## Failure diagnosis

- **Pipe not created**: check `CREATE PIPE` privileges on `IB_LAB.BRONZE`, target INSERT, stage USAGE, storage integration access, valid Iceberg target and file format.
- **Pipe status healthy but no file**: inspect S3 prefix, S3 object-created notification ARN, suffix `.parquet`, IAM access, and whether file arrived **after** notifications were configured.
- **Pipe load error**: inspect `IB_LAB.INFORMATION_SCHEMA.COPY_HISTORY` for file error and field mapping / Parquet types.
- **Iceberg row exists but Gold missing**: inspect DQ classification / valid CLIENT_ID, task STARTED state and `TASK_HISTORY`.
- **S3 notification conflict**: don't add overlapping event destinations; use SNS fan-out.
- **Invalid events**: file ingestion can succeed while dbt quality checks subsequently reject business records; that's a separate concern.

## Current completion status

The user reports having created the physical SCD2 stored procedure and table, but no result has been shared for the SCD2 triggered task; do not label it active. Snowpipe CREATE PIPE, AWS notification and new-file load are **pending**.

## Official docs

- https://docs.snowflake.com/en/user-guide/data-load-snowpipe-auto-s3
- https://docs.snowflake.com/en/user-guide/data-load-snowpipe-intro
- https://docs.snowflake.com/en/sql-reference/sql/create-pipe
- https://docs.snowflake.com/en/user-guide/tables-iceberg-load

## 2026-11-11 correction and quick reference

**Snowpipe's pipe COPY does NOT support `ON_ERROR=ABORT_STATEMENT`.** The prepared SQL has been corrected to `ON_ERROR=SKIP_FILE`, its supported default (bad file skipped, other files continue). Do not use the original bulk-COPY option in a pipe. Also unlike older explanations, current Snowpipe billing is fixed credits per GB of loaded data, not a per-file charge; binary Parquet files are billed on observed size. Read the [Notion Snowpipe Interview Quick Revision](https://app.notion.com/p/4157b5a3502e812aa436c84b08fdbeae) for concise architecture, 15 Q&A, cost and operational limits.

Official: https://docs.snowflake.com/en/user-guide/data-load-snowpipe-intro ; https://docs.snowflake.com/en/user-guide/data-load-snowpipe-billing .
