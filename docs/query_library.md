# SQL and code reference index

**Primary curated query library in Notion:** [Snowflake & dbt — SQL Queries and Code Reference](https://app.notion.com/p/4127b5a3502e814eb844dd73e0868159).

**Learning explanations and milestone detail:** [Snowflake — Senior Engineer Fast Track](https://app.notion.com/p/3fc7b5a3502e816fa733f4682625df90).

## Canonical source files

| Topic | Code |
|---|---|
| S3 / Parquet inspection | [sql/ingestion/01_parquet_stage_inspection.sql](../sql/ingestion/01_parquet_stage_inspection.sql) |
| dbt row conservation / orphan checks | [sql/quality/01_trade_reconciliation.sql](../sql/quality/01_trade_reconciliation.sql) |
| dbt staging clients + property tests | [dbt/models/](../dbt/models/) |
| S3 Iceberg policy + trust | [aws/iam/](../aws/iam/) |
| Iceberg volume and CTAS | [sql/iceberg/01_external_volume_and_backfill.sql](../sql/iceberg/01_external_volume_and_backfill.sql) |
| Iceberg deployment steps | [docs/iceberg_aws_setup.md](iceberg_aws_setup.md) |
| Parquet 392-row batch → Iceberg COPY, full metadata | [sql/iceberg/02_load_new_trade_parquet_batch.sql](../sql/iceberg/02_load_new_trade_parquet_batch.sql) |
| Gold current trade state initial bootstrap (planned) | [sql/cdc/03_bootstrap_current_trades.sql](../sql/cdc/03_bootstrap_current_trades.sql) |
| Triggered append-only stream CDC MERGE task (planned) | [sql/cdc/04_triggered_trade_cdc_task.sql](../sql/cdc/04_triggered_trade_cdc_task.sql) |
| End-to-end triggered CDC smoke test (**verified**) | [sql/cdc/05_triggered_cdc_smoke_test.sql](../sql/cdc/05_triggered_cdc_smoke_test.sql) |
| SCD2 history VIEW over event log (planned) | [sql/scd2/06_trade_history_from_events.sql](../sql/scd2/06_trade_history_from_events.sql) |
| dbt version-history model and test (planned) | [dbt/models/marts/fct_trade_state_history.sql](../dbt/models/marts/fct_trade_state_history.sql) / [assert_single_current_trade_version.sql](../dbt/tests/assert_single_current_trade_version.sql) |
| Physical SCD2 Gold bootstrap + dedicated stream (planned) | [sql/scd2/07_physical_scd2_bootstrap.sql](../sql/scd2/07_physical_scd2_bootstrap.sql) |
| Transactional incremental SCD2 stored procedure + triggered task (user-reported SP created; task unverified) | [sql/scd2/08_incremental_physical_scd2_task.sql](../sql/scd2/08_incremental_physical_scd2_task.sql) |
| Snowpipe S3 auto-ingest → Iceberg Bronze (planned) | [sql/snowpipe/09_s3_auto_ingest_to_iceberg.sql](../sql/snowpipe/09_s3_auto_ingest_to_iceberg.sql) |
| Snowpipe AWS event notification setup (planned) | [docs/snowpipe_setup.md](snowpipe_setup.md) |

The end-to-end synthetic CDC test was executed: task history showed SUCCEEDED/TRIGGER and the user confirmed the trade appeared in Gold.
The repository is the source of truth for code and version history. Notion contains explanations and links. Status labels **VERIFIED / CAPTURED / PLANNED** should be preserved.

[Idempotency and replay safety review](idempotency_and_replay_design.md) — current COPY/stream/dbt/Gold/SCD2 gaps and planned canonical event deduplication.

## Task concepts reference

The [triggered CDC task SQL](../sql/cdc/04_triggered_trade_cdc_task.sql) demonstrates `CREATE TASK IF NOT EXISTS`, `WAREHOUSE` (user-managed compute), `WHEN SYSTEM$STREAM_HAS_DATA('<stream>')` (event trigger, not a SCHEDULE), `AS MERGE` (the transactional stream consumer), and `ALTER TASK ... RESUME`. User reported task creation on 2026-11-10; STARTED state and task execution are not yet verified. Full instructional notes: [Notion query reference](https://app.notion.com/p/4127b5a3502e814eb844dd73e0868159).

## Snowpipe interview revision notes

[Snowpipe — Senior Data Engineer Interview Quick Revision](https://app.notion.com/p/4157b5a3502e812aa436c84b08fdbeae) (standalone Notion page): definition, S3 → SQS → PIPE → Iceberg architecture, COPY vs Snowpipe vs Streaming, advantages, disadvantages, 14-day file metadata, REFRESH 7 days, error handling, 2026 credit-per-GB cost, troubleshooting SQL, 15 interview Q&A. Snowpipe pipeline SQL was corrected to `ON_ERROR=SKIP_FILE` (not `ABORT_STATEMENT`).
