# dbt models — Snowflake Workspaces project snapshot

These files are *captured/reconstructed examples* from the working native Snowflake dbt lab. They are **not yet a complete runnable export**. The user's authoritative Workspaces models `stg_trade_events.sql`, `dbt_project.yml` and `profiles.yml` have not all been provided as files. **Do not fabricate them or commit credentials.**

Verified Snowflake result: 1,704 trade events staged; 1,696 valid; 8 rejected; 7 selected dbt tests pass. An orphan client `CL99999`, trade `BAD0002082`, was removed from the valid subset by updated DQ classification.

`models/intermediate/int_valid_trades.sql` and `int_rejected_trades.sql` are **reconstructions** based on the confirmed behavior and will be compared with the real files when exported. The `schema.yml` generic tests and `tests/assert_trade_reconciliation.sql` singular test reflect established lab logic.

The eventual Iceberg migration will update `sources.yml` to point to `IB_LAB.BRONZE.RAW_TRADE_EVENTS_ICEBERG`; **do not change source mappings until the Iceberg backfill and reconciliation succeed**.
