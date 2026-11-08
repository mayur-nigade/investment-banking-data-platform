# dbt models — Snowflake Workspaces project snapshot

These files are *captured/reconstructed examples* from the working Snowflake-native dbt lab, **not a byte-for-byte export**. The authoritative Snowflake Workspaces versions have not been supplied. `stg_trade_events.sql`, valid/rejected views and `dbt_project.yml` are now reconstructed from the prior observed model behavior. **Compare them with the live Workspace definitions and tests before claiming fully reproduced execution. Never commit credentials or `profiles.yml`.**

Verified Snowflake result: 1,704 trade events staged; 1,696 valid; 8 rejected; 7 selected dbt tests pass. An orphan client `CL99999`, trade `BAD0002082`, was removed from the valid subset by updated DQ classification.

`models/staging/stg_trade_events.sql`, `models/intermediate/int_valid_trades.sql` and `int_rejected_trades.sql` are **reconstructions** based on the confirmed behavior and will be compared with the real files when exported. The older repository placeholder incorrectly equated `DQ_STATUS='INVALID'` with the rejection reason; it has been corrected to `DQ_REASON AS REJECTION_REASON`. The `schema.yml` generic tests and `tests/assert_trade_reconciliation.sql` singular test reflect established lab logic.

The eventual Iceberg migration will update `sources.yml` to point to `IB_LAB.BRONZE.RAW_TRADE_EVENTS_ICEBERG`; **do not change source mappings until the Iceberg backfill and reconciliation succeed**.

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
