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
| End-to-end triggered CDC smoke test (planned) | [sql/cdc/05_triggered_cdc_smoke_test.sql](../sql/cdc/05_triggered_cdc_smoke_test.sql) |

The end-to-end test is a synthetic event, not yet executed. Expected evidence: INSERT=1, task SUCCEEDED/TRIGGER, Gold active trade.\n
The repository is the source of truth for code and version history. Notion contains explanations and links. Status labels **VERIFIED / CAPTURED / PLANNED** should be preserved.

## Task concepts reference

The [triggered CDC task SQL](../sql/cdc/04_triggered_trade_cdc_task.sql) demonstrates `CREATE TASK IF NOT EXISTS`, `WAREHOUSE` (user-managed compute), `WHEN SYSTEM$STREAM_HAS_DATA('<stream>')` (event trigger, not a SCHEDULE), `AS MERGE` (the transactional stream consumer), and `ALTER TASK ... RESUME`. User reported task creation on 2026-11-10; STARTED state and task execution are not yet verified. Full instructional notes: [Notion query reference](https://app.notion.com/p/4127b5a3502e814eb844dd73e0868159).
