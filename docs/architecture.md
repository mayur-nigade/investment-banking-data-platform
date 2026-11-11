# Architecture and implementation decisions

## Goal

An **independent synthetic-data lab** demonstrating how a trade ingestion / migration workflow can evolve into a Snowflake + dbt + Iceberg lakehouse. **The technology choices below are a portfolio modernization design, not assertions about an employer's production system.**

## Logical data flow

1. **Sources:** generated investment-banking trades and reference data (clients, accounts, instruments). The broader reference architecture includes SQL Server CDC via AWS DMS and Salesforce via AppFlow; these connectors are *not deployed in the current lab*.
2. **AWS S3 landing:** source CSV and Parquet batches are retained for replay. Actual staged path is `s3://omera-ib-trade-lake/landingzone/` (replace this with your own bucket).
3. **Snowflake external stage:** `IB_LAB.RAW.IB_S3_STAGE` uses a storage integration to **read** landed files.
4. **Bronze, current baseline:** existing native `IB_LAB.RAW.RAW_TRADE_EVENTS` (1,704 events from earlier CSV batches).
5. **Bronze, planned migration:** Snowflake-managed Apache Iceberg table with data and Iceberg metadata written to a **separate** S3 prefix. A Snowflake **external volume** supplies its writable S3 identity; it is *not* the existing read-only stage/storage integration.
6. **Silver, dbt:** `stg_trade_events` performs source-preserving casts and ordered DQ classification; `stg_clients` validates client references; `int_valid_trades` and `int_rejected_trades` separate accepted from quarantined events.
7. **Gold, planned:** apply event-sequenced CDC to current trade state, derive positions / P&L / risk aggregates.
8. **Operations, planned:** Snowpipe ingestion, streams/tasks (subject to Snowflake-managed Iceberg version capabilities), reconciliation, observability and recovery.

## Why Iceberg is in Bronze

Iceberg tracks columnar Parquet data files using snapshots, manifests and transactional table metadata. This enables auditable raw history and potential interoperability with compatible engines, **but Parquet files in a landing folder are not themselves an Iceberg table**. A catalog-committing writer must create the table.

The **Iceberg table is not yet provisioned**. We will first verify file types and counts, configure the S3 writable volume and Snowflake trust/IAM roles, create the table, and then reconcile a historical backfill. Earlier historical files that have already been loaded from CSV **must not be loaded again** as fresh events merely because Parquet copies exist.

## Data quality contract

- Maintain a stable `EVENT_ID` for uniqueness checks, and `TRADE_ID` for business-state CDC.
- Preserve `EVENT_TYPE` and `SOURCE_SEQUENCE`; do not assume file arrival order equals source event order.
- Keep all original fields in RAW as text to preserve deliberately malformed inputs; downstream models use `TRY_TO_*` casts.
- Explicit rejection reasons include `INVALID_QUANTITY`, `INVALID_PRICE`, `INVALID_TRADE_DATE`, `INVALID_EVENT_TS`, `MISSING_TRADE_ID`, `INVALID_EVENT_TYPE` and `UNKNOWN_CLIENT_ID`.
- Protect against duplicate CDC replays and out-of-order events before MERGE. Source sequence and event timestamps are inputs to this policy.
- Reconcile `staging_count = valid_count + rejected_count`.

## Security

The repository is public. Keep **real data, personal information, tokens, passwords, IAM credentials, private keys and proprietary client code** out of commits. Use synthetic data and sample identifiers only.

## Tooling / status

- **Implemented and user-verified:** S3 stage/native RAW ingestion, dbt views/tests, Snowflake-managed Iceberg external volume and 1,704-event backfill, 392 new Parquet events loaded, append-only stream, triggered CDC task and new trade reaching physical GOLD.CURRENT_TRADES.
- **Planned / code drafted:** learning SCD2 history view; separate physical SCD2 table and dedicated-stream triggered incremental procedure (see sql/scd2/07 and 08); automated Snowpipe, performance/production hardening.
- dbt execution in this lab is through **Snowflake Workspaces (native dbt project)**. The repository initially captures only known model/test files; do not assume a standalone local dbt project is ready to execute.

## SCD2 dual-path trade history (2026-11-11; not yet executed)

**Path A, learning:** `GOLD.V_TRADE_SCD2_HISTORY` is a read-time view over `DBT_DEV.INT_VALID_TRADES`, using each valid I/U/D event as one version; useful for comparing event-version history with full state changes.

**Path B, production-style:** `BRONZE.TRADE_EVENTS_SCD2_STREAM` is a **separate** Iceberg append-only stream, so current-state CDC task consumption does not affect SCD2 offsets. `GOLD.V_TRADE_SCD2_DESIRED` computes historical **attribute-change** versions from validated events (SHA-256 of tracked values including delete status). `GOLD.TRADE_SCD2_HISTORY` persists them in a native Snowflake table with validity intervals, current flag and soft-delete marker. After a one-time bootstrap, `GOLD.SP_APPLY_TRADE_SCD2` processes future appended keys and reconstructs only those trades' full chains (corrects late sequence events), wraps stream read + physical re-publication + reject/audit writes in one transaction; `GOLD.APPLY_TRADE_SCD2_TASK` invokes it when the dedicated stream has data.

This is a production-style lab **blueprint, not a completed production deployment**. Requirements needing contract validation: full-row D events, per-trade authoritative sequence, duplicate sequence conflicts, timestamps and bitemporal business meaning, security, task alerting, failover and workload cost.

## Snowpipe auto ingest — next deployment (planned, 2026-11-11)

New arrivals land only under `s3://omera-ib-trade-lake/landingzone/parquet/auto/`. S3 object-create notifications on that exact prefix flow to a Snowflake-managed SQS queue for `IB_LAB.BRONZE.TRADE_EVENTS_AUTO_PIPE` (ARN is supplied by `DESC PIPE`, not hardcoded). Its `COPY INTO` uses `FULL_INGEST` and preserves the previously successful 14-source + 3-metadata mapping to the same managed Iceberg Bronze table. Snowpipe uses serverless ingestion compute. This triggers two *independent* consumer offsets: Gold CURRENT_TRADES CDC and physical Gold SCD2, assuming both tasks are enabled. Existing batch folder `parquet/batches/` is outside the pipe prefix. This wiring has not yet been executed and S3 notification still needs to be configured in AWS Console.

Runbook: [Snowpipe S3 setup](snowpipe_setup.md), SQL: [09_s3_auto_ingest_to_iceberg.sql](../sql/snowpipe/09_s3_auto_ingest_to_iceberg.sql).
