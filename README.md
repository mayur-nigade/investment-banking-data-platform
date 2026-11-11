# Investment Banking Data Platform
### Snowflake · dbt · Apache Iceberg · AWS S3 · Parquet · CDC

**Independent, synthetic-data portfolio / learning project.** Inspired by common investment-banking trade ingestion and modernization patterns. It is **not** an official Jefferies repository, nor does it contain Jefferies data, proprietary source code, or a claim that this stack was deployed at that client.

## Objective

Build a traceable investment-banking trade-data pipeline, starting at AWS S3 landing and ending in reconciled trade analytics. Learn by implementing and verifying each layer: ingestion, data quality, relational integrity, incremental CDC, and an open Iceberg Bronze table.

```text
Synthetic trade / client / account / instrument sources
                   |
             AWS S3 landing
             CSV + Parquet
                   |
           Snowflake external stage
                   |
     Bronze: Snowflake-managed Iceberg on S3   [IMPLEMENTED]
                   |
         Silver: dbt staging / quality
                   |
       Silver: valid + rejected trades
                   |
    CDC current state -> Gold trade reporting    [PLANNED]
```

## Verified lab progress — 2026-11-08

| Milestone | State | Evidence |
|---|---|---|
| S3 stage + CSV trade ingestion | Completed | 1,704 rows in Snowflake RAW from prior batches |
| dbt staging, client referential integrity, valid/rejected split | Completed | Staging 1,704 = Valid 1,696 + Rejected 8 |
| dbt automated tests | Completed | 7 passed, 0 errors |
| Parquet conversion and S3 stage access | Completed | Five files from batch `2026-11-03` listed and schema inferred |
| Parquet file-level count | Completed | 79 + 79 + 78 + 78 + 78 = **392 rows** |
| Iceberg external volume / S3 IAM trust | Completed | External-volume verification passed read/write/list/delete |
| Iceberg Bronze table / schema | Completed | Table exists; all 17 columns and `INGESTED_AT TIMESTAMP_LTZ(6)` inspected |
| Iceberg vs RAW ID-set comparison | Completed | 0 missing distinct `EVENT_ID`s, 0 extra distinct `EVENT_ID`s |
| Iceberg historical row-count parity | Completed | 1,704 native RAW = 1,704 Iceberg Bronze; both-way distinct EVENT_ID delta = 0 |
| Five-file Parquet batch ingestion to Iceberg | Completed | Five files, 392 rows loaded, 0 reported errors |
| dbt source cutover to Iceberg | **Planned** | Source definitions updated in GitHub; workspace build not confirmed |
| Snowflake append-only stream | User reports created | Stream starts from creation offset; no historical replay |
| Current trade-state CDC bootstrap & Task | **End-to-end verified** | Triggered task SUCCEEDED; user confirms new trade reached GOLD.CURRENT_TRADES |
| SCD2 history VIEW + dbt equivalent | Planned | Learning path; history derived at query time |
| SCD2 physical table + incremental task | Planned | Separate native table; bootstrap + stored procedure + independent stream code committed |
| Snowpipe auto-ingestion | Planned | Not implemented yet |

**Important:** The five November 3 Parquet files were successfully loaded into Iceberg Bronze (392 rows, zero COPY errors); older Parquet copies of historical CSV batches are not reloaded. The expected total is 2,096, with post-load count checks reported complete by the user but no numeric result pasted.

## Repository contents

- [`docs/architecture.md`](docs/architecture.md) — proposed architecture, layers, and implementation constraints
- [`docs/idempotency_and_replay_design.md`](docs/idempotency_and_replay_design.md) — file-level vs event-level deduplication, identified gaps, replay-safety design and SQL diagnostics
- [`docs/iceberg_aws_setup.md`](docs/iceberg_aws_setup.md) — complete AWS IAM + Iceberg external-volume implementation runbook (planned)
- [`sql/iceberg/01_external_volume_and_backfill.sql`](sql/iceberg/01_external_volume_and_backfill.sql) — verified historical CTAS pattern and reconciliation SQL
- [`sql/iceberg/02_load_new_trade_parquet_batch.sql`](sql/iceberg/02_load_new_trade_parquet_batch.sql) — **next planned** load of 392 new Parquet events with source metadata
- [`docs/query_library.md`](docs/query_library.md) — cross-reference to the new Notion SQL query library
- [`docs/progress.md`](docs/progress.md) — dated milestone evidence and next actions
- [`sql/ingestion/01_parquet_stage_inspection.sql`](sql/ingestion/01_parquet_stage_inspection.sql) — file format, schema inference, row-count and sample queries
- [`sql/quality/01_trade_reconciliation.sql`](sql/quality/01_trade_reconciliation.sql) — RAW-to-dbt reconciliation and targeted reject investigation
- [`dbt/models/staging/stg_trade_events.sql`](dbt/models/staging/stg_trade_events.sql) — reconstructed trade staging view with client-aware quality validation
- [`dbt/models/intermediate/`](dbt/models/intermediate/) — validated and rejected trade views, generic schema tests
- [`dbt/tests/assert_trade_reconciliation.sql`](dbt/tests/assert_trade_reconciliation.sql) — dbt singular row-conservation test
- [`dbt/dbt_project.yml`](dbt/dbt_project.yml) — reproducible starter config, not a live workspace export
- [`sql/quality/02_trade_dq_diagnostics.sql`](sql/quality/02_trade_dq_diagnostics.sql) — reason-level DQ and orphan-client investigations
- [`dbt/README.md`](dbt/README.md) — model lineage, run instructions and code provenance
- [`sql/cdc/03_bootstrap_current_trades.sql`](sql/cdc/03_bootstrap_current_trades.sql) — **planned** one-time Gold current-state bootstrap using the latest valid event per trade
- [`sql/cdc/04_triggered_trade_cdc_task.sql`](sql/cdc/04_triggered_trade_cdc_task.sql) — **created (user-reported)** Iceberg stream-triggered MERGE task for future I/U/D events
- [`sql/cdc/05_triggered_cdc_smoke_test.sql`](sql/cdc/05_triggered_cdc_smoke_test.sql) — **verified** one-event CDC test with task history and Gold arrival
- [`sql/scd2/06_trade_history_from_events.sql`](sql/scd2/06_trade_history_from_events.sql) — **planned** SCD2-style historical timeline VIEW from preserved trade events
- [`dbt/models/marts/fct_trade_state_history.sql`](dbt/models/marts/fct_trade_state_history.sql) — **planned** dbt version-history model using ref()
- [`dbt/tests/assert_single_current_trade_version.sql`](dbt/tests/assert_single_current_trade_version.sql) — **planned** singular version integrity test
- [`sql/scd2/07_physical_scd2_bootstrap.sql`](sql/scd2/07_physical_scd2_bootstrap.sql) — **planned** separate native Gold SCD2 history table, state-change filtering and independent Iceberg stream
- [`sql/scd2/08_incremental_physical_scd2_task.sql`](sql/scd2/08_incremental_physical_scd2_task.sql) — **planned** transactional incremental history maintenance with durable reject/run audit

## Key design decisions

1. Keep **source landing files immutable** and raw event history append-only for CDC replay.
2. Preserve source values as strings in Bronze; use `TRY_TO_*` casts and data-quality rules downstream.
3. Record rejected rows with an explicit rejection reason rather than silently dropping them.
4. Treat **Parquet as a file format** and **Iceberg as a transactional table format**; simple Parquet files are not automatically Iceberg tables.
5. Introduce Iceberg in parallel with the known-good RAW pipeline; switch dbt sources only after identical reconciliation.
6. Never commit credentials, customer information, or real transaction data.

## Reproducing the verified checks

The SQL queries in this repository assume lab objects such as `IB_LAB.RAW.IB_S3_STAGE`, `IB_LAB.RAW.IB_PARQUET_FORMAT`, and `IB_LAB.DBT_DEV` already exist in your Snowflake account. Update object identifiers for your own environment.

Run `sql/ingestion/01_parquet_stage_inspection.sql` to inspect the five staged November 3 Parquet files, then run `sql/quality/01_trade_reconciliation.sql` for the previously ingested batches.

This repository is being built **incrementally**, with verified milestones labeled separately from planned work. **Trade dbt SQL files were reconstructed from the successful Snowflake Workspaces lab, not exported byte-for-byte; their execution from this checkout has not yet been independently reverified.**

---
**Portfolio disclosure:** all examples and source records are synthetic, for educational and demonstration use.

**Current execution priority (2026-11-11):** finish the SCD2 learning view, physical SCD2 bootstrap + incremental task, and Snowpipe on a new incoming-file prefix; validate one end-to-end new file. [Idempotency hardening](docs/idempotency_and_replay_design.md) is explicitly postponed and not claimed as implemented.

## Automated S3 Parquet ingestion (Snowpipe; planned)

- [Snowpipe S3 auto-ingest SQL](sql/snowpipe/09_s3_auto_ingest_to_iceberg.sql) — **PLANNED** pipe on fresh `parquet/auto/` prefix directly into managed Iceberg Bronze
- [Snowpipe AWS configuration runbook](docs/snowpipe_setup.md) — **PLANNED** S3 object-created notification → Snowflake-managed SQS queue, new-file test

This adds a new source-file prefix to the already verified Iceberg Bronze table, without rereading the five historical Parquet batch files. The user reports completing the physical SCD2 table and incremental stored procedure; the SCD2 task's active/run status has not yet been verified. Once Snowpipe is connected, a fresh synthetic Parquet file can exercise Snowpipe → Iceberg → the independent current-state and physical-SCD2 streams/tasks. Idempotency hardening remains deferred.
