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

The repository is the source of truth for code and version history. Notion contains explanations and links. Status labels **VERIFIED / CAPTURED / PLANNED** should be preserved.
