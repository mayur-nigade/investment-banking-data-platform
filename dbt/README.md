# dbt models — Snowflake Workspaces project snapshot

These files are *captured/reconstructed examples* from the working Snowflake-native dbt lab, **not a byte-for-byte export**. The authoritative Snowflake Workspaces versions have not been supplied. `stg_trade_events.sql`, valid/rejected views and `dbt_project.yml` are now reconstructed from the prior observed model behavior. **Compare them with the live Workspace definitions and tests before claiming fully reproduced execution. Never commit credentials or `profiles.yml`.**

Verified Snowflake result: 1,704 trade events staged; 1,696 valid; 8 rejected; 7 selected dbt tests pass. An orphan client `CL99999`, trade `BAD0002082`, was removed from the valid subset by updated DQ classification.

`models/staging/stg_trade_events.sql`, `models/intermediate/int_valid_trades.sql` and `int_rejected_trades.sql` are **reconstructions** based on the confirmed behavior and will be compared with the real files when exported. The older repository placeholder incorrectly equated `DQ_STATUS='INVALID'` with the rejection reason; it has been corrected to `DQ_REASON AS REJECTION_REASON`. The `schema.yml` generic tests and `tests/assert_trade_reconciliation.sql` singular test reflect established lab logic.

The historical Iceberg backfill reconciled and a five-file Parquet COPY loaded 392 new events with no errors. **The GitHub reconstructed models are staged for dbt cutover**: `sources.yml` now defines `ib_iceberg` in `IB_LAB.BRONZE` and `stg_trade_events.sql` reads that Iceberg table. The user's authoritative Snowflake Workspaces project has not yet been reported rebuilt against Iceberg; do not label this version verified until the dbt build succeeds.

## Model dependency DAG

```text
source('ib_raw','RAW_CLIENTS') ---> stg_clients ---+
                                                |
source('ib_raw','RAW_TRADE_EVENTS') ----------> stg_trade_events
                                                |     |
                                int_valid_trades     int_rejected_trades
                                          \           /
                                    assert_trade_reconciliation
```

## Evidence and verification

1. **Observed baseline after source batch 2026-11-02:** 1,704 staging rows = 1,697 valid + 7 rejected. Rejected reasons: INVALID_PRICE 2; INVALID_QUANTITY 1; INVALID_EVENT_TYPE 1; INVALID_TRADE_DATE 1; INVALID_EVENT_TS 1; MISSING_TRADE_ID 1.
2. **After client-reference validation:** 1,704 staging = 1,696 valid + 8 rejected (user confirmed counts match), with invalid client CL99999, trade BAD0002082, excluded from valid. Seven dbt tests passed, 0 errors.
3. **Current code status:** Reconstructed models are not yet re-executed from this GitHub checkout. The evidence above refers to the user's earlier Snowflake Workspaces runs.
4. **Iceberg status:** Volume verified, Iceberg table schema includes TIMESTAMP_LTZ(6), and RAW/Iceberg distinct EVENT_ID set difference = 0 in both directions; table row counts remain to be confirmed.

## Running from this folder

- Snowflake-native: compare these files to the project in Snowflake Workspaces; run the models and tests there.
- dbt Core local use: `dbt_project.yml` is a starter configuration. Create a secure local Snowflake profile outside the repository or use environment variables; then `dbt run`, `dbt test`. Snowflake permissions/warehouse/database must be set for the target.
- Look at [trade DQ diagnostics](../sql/quality/02_trade_dq_diagnostics.sql) and [count reconciliation](../sql/quality/01_trade_reconciliation.sql) for verification SQL.

## SCD Type 2-style history (next hands-on)

The [event-sourced historical model](models/marts/fct_trade_state_history.sql) and [one-current-version test](tests/assert_single_current_trade_version.sql) are now versioned but **not executed** in the user's live Snowflake Workspaces project. The model is a **view**: it reconstructs earlier and latest versions from append-only trade events, rather than writing a conventional, physical SCD2 dimension table. Source sequence is authoritative; use event timestamp-based AS-OF analysis only if timestamps are monotonic per trade.

To build in Snowflake's dbt Workspaces project, copy the new model and test into the authoritative project and run `dbt build --select +fct_trade_state_history`. Alternatively execute the standalone [Gold history-view SQL](../sql/scd2/06_trade_history_from_events.sql) from a Snowflake worksheet. Use one of the two routes rather than conflating them. This fast-track approach illustrates `ROW_NUMBER`, `LEAD`, `IS_CURRENT`, and delete tombstones without a second stream.
