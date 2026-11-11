# Progress and evidence log

This file distinguishes observed outcomes from planned work. It is updated as new milestones are **actually verified**, not merely proposed.

## 2026-11-08 — Verified

| Check | Observed result | Scope |
|---|---|---|
| RAW trade-event count | 1,704 | CSV batches already ingested into native Snowflake RAW |
| dbt reconciliation | 1,704 staging = 1,696 valid + 8 rejected | After client-reference reclassification; user confirmed counts matched |
| dbt test execution | PASS=7, WARN=0, ERROR=0, SKIP=0 | Snowflake dbt test runner |
| Snowflake S3 stage `LIST` | 5 Parquet trade-event files | `batch_2026-11-03` |
| Snowflake `INFER_SCHEMA` | 14 trade-event columns, all inferred `TEXT`, nullable | Across the 5 files |
| Direct Parquet file scan | 79, 79, 78, 78, 78 = **392 rows** | File-level staged counts; no table load |

### File breakdown

| File | Staged records |
|---|---:|
| `trade_events_001.parquet` | 79 |
| `trade_events_002.parquet` | 79 |
| `trade_events_003.parquet` | 78 |
| `trade_events_004.parquet` | 78 |
| `trade_events_005.parquet` | 78 |
| **Total** | **392** |

S3 path: `s3://omera-ib-trade-lake/landingzone/parquet/batches/batch_2026-11-03/`.

The staged `2026-11-03` records have **not** yet been loaded into the Iceberg Bronze table. `INFER_SCHEMA` shows declared types and nullability, not actual null counts or per-row DQ validity.

## Next implementation milestones

1. Verify S3 Iceberg writable prefix and Snowflake external-volume IAM/trust configuration.
2. Create Snowflake-managed Iceberg Bronze table; backfill **historical RAW data once**, compare counts and sample keys.
3. Load only previously unprocessed Parquet trade events, then run dbt sources/models and reconciliation.
4. Complete client/account/instrument relationship checks, followed by Snowpipe, stream/task CDC MERGE and Gold analytics.

Do not label these future items `COMPLETE` until their outputs have been shared and verified.

## Evidence policy

- Actual Snowflake run outputs and user-confirmed metrics are documented as **verified**.
- SQL from prior guidance is labeled as **captured lab code** unless its execution is confirmed.
- Avoid inventing throughput, latency, cost savings, production volumes or employer implementation details.

## 2026-11-08 — Iceberg external volume IAM trust blocked

**Observed:** The first `SYSTEM$VERIFY_EXTERNAL_VOLUME('OMERA_IB_ICEBERG_VOL')` response returned `success=false`. Storage-location selection and AWS role ARN validation passed. Every data operation failed **before reaching S3** with AWS STS `AssumeRole` denied.

**Root-cause area:** The AWS IAM role trust relationship must permit the exact Snowflake-owned `STORAGE_AWS_IAM_USER_ARN` principal **and** `sts:ExternalId` from `DESC EXTERNAL VOLUME`. AWS accounts for Snowflake's principal and the bucket role differ legitimately.

**Next action:** Apply the corrected trust policy using the verified Snowflake IAM principal and external ID; rerun external volume verification. Current status **FAILED / fix pending**, not a completed Iceberg table.

See [Iceberg troubleshooting runbook](iceberg_aws_setup.md#troubleshooting-sts-assumerole-denied-observed-2026-11-08). Do not publish real AWS principal/account IDs in this public repository.

## 2026-11-08 — Iceberg external volume IAM fixed and verified

**Verified from user-provided Snowflake output:** `SYSTEM$VERIFY_EXTERNAL_VOLUME('OMERA_IB_ICEBERG_VOL')` now reports `success=true`. Storage location, AWS role ARN validation, and the **write, read, list and delete** checks all returned `PASSED`. Snowflake IAM role assumption now works after the trust-relationship correction.

**What this proves:** Cross-account Snowflake → AWS STS role assumption and data-file operations within `s3://omera-ib-trade-lake/iceberg/bronze/` succeeded. The earlier `sts:AssumeRole` blocker is resolved. It does **not** prove the Iceberg table exists or contains rows.

**Next:** Run `SHOW ICEBERG TABLES LIKE 'RAW_TRADE_EVENTS_ICEBERG' IN SCHEMA IB_LAB.BRONZE;`. If absent, create one Snowflake-managed Iceberg CTAS from `IB_LAB.RAW.RAW_TRADE_EVENTS`. Verify `NATIVE_RAW=1704` and `ICEBERG_BRONZE=1704` and no missing/extra EVENT_IDs. Do not replay the 392 November 3 events yet.

## 2026-11-08 — First Iceberg CTAS compilation failed (timestamp precision)

**Verified user error:** `SQL Compilation error: Invalid time type scale specified for column 'INGESTED_AT' with data type 'TIMESTAMP_LTZ(9)'`. Attempted `CREATE ICEBERG TABLE ... AS SELECT * FROM IB_LAB.RAW.RAW_TRADE_EVENTS` after external-volume verification passed.

**Diagnosis:** Iceberg v2 uses microsecond timestamps (`TIMESTAMP_LTZ(6)`), whereas native RAW `INGESTED_AT` is `TIMESTAMP_LTZ(9)`. The failure was in DDL compilation, not IAM or row validation.

**Code fix committed (not yet executed):** Use `SELECT * REPLACE (CAST(INGESTED_AT AS TIMESTAMP_LTZ(6)) AS INGESTED_AT)` within the one-time Iceberg CTAS. See [SQL runbook](../sql/iceberg/01_external_volume_and_backfill.sql). This does not modify RAW.

**Status:** Iceberg table/backfill not confirmed. Check `SHOW ICEBERG TABLES` before retry, never assume partial creation or use `OR REPLACE` without examination. After successful retry, compare native RAW and Iceberg row counts and event IDs.

## 2026-11-08 — Iceberg table existence confirmed; backfill not yet verified

**User reports** that `IB_LAB.BRONZE.RAW_TRADE_EVENTS_ICEBERG` exists after the previously documented `INGESTED_AT TIMESTAMP_LTZ(9)` CTAS failure and proposed precision correction. This confirms table existence **only**, not successful data backfill or the actual schema; the exact statement used to create it has not been provided.

**Next read-only checks:**

```sql
SELECT 'NATIVE_RAW' AS LAYER, COUNT(*) AS RECORDS
FROM IB_LAB.RAW.RAW_TRADE_EVENTS
UNION ALL
SELECT 'ICEBERG_BRONZE', COUNT(*)
FROM IB_LAB.BRONZE.RAW_TRADE_EVENTS_ICEBERG;

DESC ICEBERG TABLE IB_LAB.BRONZE.RAW_TRADE_EVENTS_ICEBERG;
```

**Expected:** 1704 in both if the one-time backfill succeeded; `INGESTED_AT` at timestamp scale 6 for Iceberg v2. **Do not rerun CTAS or drop existing table.** If 0 records, plan an explicit backfill with duplicate protection; if count diverges, investigate before writing.

## 2026-11-08 — Iceberg schema inspected: timestamp precision fixed

**User-verified `DESC ICEBERG TABLE IB_LAB.BRONZE.RAW_TRADE_EVENTS_ICEBERG` output:** 17 expected columns exist: the 14 source fields (`EVENT_ID`, `TRADE_ID`, `EVENT_TYPE`, `SOURCE_SEQUENCE`, `CLIENT_ID`, `ACCOUNT_ID`, `INSTRUMENT_ID`, `SIDE`, `QUANTITY`, `PRICE`, `TRADE_CURRENCY`, `TRADE_DATE`, `EVENT_TS`, `SOURCE_SYSTEM`) plus `SOURCE_FILE` (`VARCHAR`), `FILE_ROW_NUMBER` (`NUMBER(38,0)`), `INGESTED_AT` (`TIMESTAMP_LTZ(6)`). The prior `TIMESTAMP_LTZ(9)` incompatibility is therefore resolved in the **created table's schema**.

**Still NOT verified:** Whether historical data was copied at all, count parity (target 1704 on both native RAW/Iceberg), event-ID parity, and actual physical Iceberg files/snapshots. Do not mark the backfill or end-to-end Iceberg ingestion complete until these checks pass.

**Next read-only SQL:**
```sql
SELECT 'NATIVE_RAW' AS LAYER, COUNT(*) AS RECORDS
FROM IB_LAB.RAW.RAW_TRADE_EVENTS
UNION ALL
SELECT 'ICEBERG_BRONZE', COUNT(*)
FROM IB_LAB.BRONZE.RAW_TRADE_EVENTS_ICEBERG;

SELECT
  (SELECT COUNT(*) FROM (
    SELECT EVENT_ID FROM IB_LAB.RAW.RAW_TRADE_EVENTS
    MINUS
    SELECT EVENT_ID FROM IB_LAB.BRONZE.RAW_TRADE_EVENTS_ICEBERG
  )) AS MISSING_IN_ICEBERG,
  (SELECT COUNT(*) FROM (
    SELECT EVENT_ID FROM IB_LAB.BRONZE.RAW_TRADE_EVENTS_ICEBERG
    MINUS
    SELECT EVENT_ID FROM IB_LAB.RAW.RAW_TRADE_EVENTS
  )) AS EXTRA_IN_ICEBERG;
```

**Interpretation:** Expected 1704 vs 1704 and 0 missing/extra IDs. Set difference compares distinct IDs; it does not prove row-by-row content equality.

## 2026-11-08 — Distinct event-ID reconciliation PASSED

**User-provided output:** the two-direction `MINUS` checks returned `MISSING_IN_ICEBERG=0` and `EXTRA_IN_ICEBERG=0`.

This verifies **distinct event-ID sets only**. The source-versus-Iceberg **COUNT(*) output has not been provided**, so row-count parity and full row-content equivalence remain unverified.

## 2026-11-08 — Trade dbt code documentation gap addressed

The repository initially included client staging, dbt test YAML and a singular test, but did **not** contain the full `stg_trade_events.sql` model. Earlier intermediate valid/rejected views were placeholders, including an incorrect `DQ_STATUS AS REJECTION_REASON` expression. This was corrected to `DQ_REASON AS REJECTION_REASON` and the explicit models were restored.

**Committed:** `dbt/models/staging/stg_trade_events.sql`, corrected `dbt/models/intermediate/int_valid_trades.sql` and `int_rejected_trades.sql`, `dbt/dbt_project.yml`, `sql/quality/02_trade_dq_diagnostics.sql`, and updated docs. Generic tests are in `dbt/models/intermediate/schema.yml`; singular count test is `dbt/tests/assert_trade_reconciliation.sql`.

**Provenance:** Recovered/reconstructed from our previous Snowflake-native Workspaces learning steps and actual test outcomes; **not** byte-for-byte exported workspace source, and not rerun from this repository. Before marking the GitHub checkout reproducible, reconcile model files with the live Workspace project and rerun all tests.

**Previously observed (Snowflake Workspace):** after the second batch, staging 1704, valid 1697, rejected 7 (price 2; quantity, event type, trade date, event timestamp, missing trade ID 1 each). After client integrity validation the user confirmed stage 1704, valid 1696, rejected 8; selected dbt test run PASS=7/7. The orphan client `CL99999`, trade `BAD0002082`, was excluded from the valid view.

## 2026-11-09 — Historical Iceberg backfill reconciliation VERIFIED

**User-confirmed live Snowflake results:**
- `IB_LAB.RAW.RAW_TRADE_EVENTS` row count = **1,704**.
- `IB_LAB.BRONZE.RAW_TRADE_EVENTS_ICEBERG` row count = **1,704**.
- `MISSING_IN_ICEBERG = 0` and `EXTRA_IN_ICEBERG = 0` from both-direction EVENT_ID `MINUS` checks.
- The Iceberg table has the expected 17-column schema, including `INGESTED_AT TIMESTAMP_LTZ(6)`.

**This establishes:** Historical native-RAW-to-Iceberg count parity and matching distinct `EVENT_ID` membership. It does **not** prove every non-key field has identical values or check duplicate IDs directly. The 392 new events staged in the five 2026-11-03 Parquet files are **not yet loaded** into Iceberg.

**Next planned query:** [Parquet COPY INTO Iceberg Bronze](../sql/iceberg/02_load_new_trade_parquet_batch.sql). Uses `FULL_INGEST`, reads 14 lower-case Parquet fields by name, records source file/row/timestamp metadata, preserves original files (`PURGE=FALSE`), and never forces a replay. **Expected count after one successful load: 2,096; unverified.**

## 2026-11-09 — Five Parquet files loaded into Iceberg; fast-track dbt cutover

**Verified Snowflake COPY output supplied by user:** All five Parquet files for the 2026-11-03 batch have `STATUS=LOADED`, `ERRORS_SEEN=0`, and `ROWS_PARSED=ROWS_LOADED`:
- 001: 79
- 002: 79
- 003: 78
- 004: 78
- 005: 78
- **Total loaded 392, errors 0**.

Historical Iceberg count was **1704** and historical ID parity had been verified. The **expected** total after a successful once-only load is **2096**. The user reports having checked all final counts and asks to skip further repetitive validations; actual numeric post-load count output wasn't pasted in this exchange.

**Next implementation (PLANNED, not executed):** Change dbt `stg_trade_events` input from `source('ib_raw','RAW_TRADE_EVENTS')` to `source('ib_iceberg','RAW_TRADE_EVENTS_ICEBERG')`, while preserving `RAW_CLIENTS` in native RAW. Add `ib_iceberg` in `sources.yml`; run `dbt build --select +stg_trade_events+` in Snowflake Workspaces once to rebuild the lineage and run tests. Github's reconstructed `stg_trade_events.sql` now reflects **the intended cutover**, not a verified production/workspace edit.

**Next training in fast-track mode:** Snowpipe vs COPY, append-only Streams on Snowflake-managed Iceberg (note: creating a stream starts tracking *future* changes), Tasks and CDC MERGE / SCD2; no repeat of previous counts.

## 2026-11-09 — CDC current-state bootstrap and triggered task prepared (PLANNED)

**User confirmed:** Snowflake dbt source/event model updated and the append-only stream was created. Exact dbt build summary and stream definition output have not been pasted, so GitHub is still labeled accordingly.

Two new scripts are versioned:
- [03_bootstrap_current_trades.sql](../sql/cdc/03_bootstrap_current_trades.sql): create `IB_LAB.GOLD.CURRENT_TRADES` as one latest valid event per `TRADE_ID`, sorted by source sequence, event timestamp and event ID; keep `EVENT_TYPE='D'` as an `IS_DELETED=TRUE` tombstone.
- [04_triggered_trade_cdc_task.sql](../sql/cdc/04_triggered_trade_cdc_task.sql): create a **triggered**, warehouse-managed task on `IB_LAB.BRONZE.TRADE_EVENTS_APPEND_STREAM`; parse and validate stream inserts, deduplicate to latest event per trade and MERGE newer trade states into Gold. After verifying bootstrap, `ALTER TASK ... RESUME`.

**Important design assumptions:** incoming I/U/D events carry complete row images and `SOURCE_SEQUENCE` advances per `TRADE_ID`. Older sequence events cannot overwrite newer states. Invalid streamed events remain in raw Iceberg but are not separately persisted in an exception table by this learning slice; a production version must add a durable reject/audit flow and replay strategy. The task has NOT been created or resumed via connector in the user's Snowflake account.

**Sequence:** Execute Gold bootstrap SQL once, then task create/resume. No new post-stream events have yet been confirmed for CDC processing.

## 2026-11-10 — Triggered CDC task created (user-reported)

**User reports:** `IB_LAB.GOLD.APPLY_TRADE_CDC_TASK` was created. The task follows the versioned `CREATE TASK IF NOT EXISTS ... WAREHOUSE=COMPUTE_WH WHEN SYSTEM$STREAM_HAS_DATA('IB_LAB.BRONZE.TRADE_EVENTS_APPEND_STREAM') AS MERGE ...` pattern. This is a **task-creation milestone**, not evidence that `ALTER TASK ... RESUME` succeeded or that a new event has been processed.

**Core learning:** a Snowflake triggered task has no `SCHEDULE`; its `WHEN` stream condition detects potential new changes without consuming them; the MERGE reads the append-only stream in its DML transaction, and on success advances the stream offset. `IF NOT EXISTS` avoids recreation but won't modify existing definitions. `WAREHOUSE=COMPUTE_WH` means user-managed compute. The task logic validates event payloads, chooses latest by source sequence/timestamp, only MERGEs newer states, and uses `IS_DELETED` tombstones for D events.

**User preference:** fast-track learning, high-level MERGE only (advanced SQL already known); avoid repeat full counts. Next meaningful exercise: check task STARTED and one true post-stream event through task into `CURRENT_TRADES`.

**Caveats:** No actual TASK_HISTORY result, STARTED state, or CDC transaction success has been provided. The implementation also lacks persistent rejects for filtered invalid stream records.

## 2026-11-10 — Triggered CDC end-to-end test SQL published (NOT YET RUN)

One synthetic I event is inserted directly into Snowflake-managed Iceberg Bronze; the append-only stream should expose the appended row, triggered `APPLY_TRADE_CDC_TASK` should execute its MERGE, and `GOLD.CURRENT_TRADES` should show the new active trade.

**Canonical SQL:** [05_triggered_cdc_smoke_test.sql](../sql/cdc/05_triggered_cdc_smoke_test.sql). Uses fresh synthetic keys `LAB_CDC_EVT_20261110_001` / `LAB_CDC_TRD_20261110_001`, one existing active Gold record for known-good attributes, and a NOT EXISTS guard to prevent duplicate test inserts. The script includes task RESUME, one INSERT, `IB_LAB.INFORMATION_SCHEMA.TASK_HISTORY` (TASK_NAME takes the unqualified task name) and one Gold state query.

**Expected, not observed:** First insert = 1 row, task history contains `SCHEDULED_FROM='TRIGGER'` and `STATE='SUCCEEDED'`, Gold one row with `LAST_EVENT_TYPE='I'`, `LAST_SOURCE_SEQUENCE=1`, `IS_DELETED=FALSE`. Zero inserted on first run suggests missing Gold bootstrap or previous test event; a FAILED task requires inspection, not repeated INSERT.

**Next learning:** SCD Type 2 using historical event timeline and dbt snapshots vs purpose-built interval model. Keep verification minimal and advance once this single trigger path works.

## 2026-11-10 — Triggered CDC task succeeded; SKIPPED condition checks explained

**Actual user-provided TASK_HISTORY:** `IB_LAB.GOLD.APPLY_TRADE_CDC_TASK` has `STATE=SUCCEEDED`, `SCHEDULED_FROM=TRIGGER` at `2026-11-10 07:56:22.094 -0800`. Other `TRIGGER` records were `SKIPPED` at `07:37:24.289 -0800` and `07:56:49.972 -0800`, each with `Conditional expression for task evaluated to false.`.

**Interpretation (Snowflake docs):** A `SKIPPED` record means the task's `WHEN SYSTEM$STREAM_HAS_DATA('IB_LAB.BRONZE.TRADE_EVENTS_APPEND_STREAM')` evaluated FALSE, so Snowflake did not execute the MERGE or resume warehouse compute. `SCHEDULED_FROM=TRIGGER` denotes the task type/event mechanism, not proof that the condition evaluated TRUE. A `SUCCEEDED` run means the task SQL completed without errors; does not alone prove row-level Gold state changed.

**Stream semantics:** `SELECT * FROM stream` is non-consuming; a successful consuming DML (our MERGE) advances the stream offset. With no newer appended Iceberg changes, a read-only SELECT should now return zero pending rows. A new append would make stream data available again. `SYSTEM$STREAM_HAS_DATA` is designed to avoid false negatives and may yield a false positive (TRUE when net CDC rows are empty).

**Follow-up:** One targeted query of the synthetic trade `LAB_CDC_TRD_20261110_001` in `IB_LAB.GOLD.CURRENT_TRADES` is enough to confirm business-state success; do not repeat historical Bronze counts. No Gold result was supplied yet, so **do not mark CDC row-state assertion verified**.

Official references:
- https://docs.snowflake.com/en/sql-reference/functions/task_history
- https://docs.snowflake.com/en/sql-reference/functions/system_stream_has_data
- https://docs.snowflake.com/en/user-guide/streams-manage

## 2026-11-11 — End-to-end CDC Gold trade verified; SCD2 lesson prepared

**Verified from user:** The new synthetic CDC trade **is present in `IB_LAB.GOLD.CURRENT_TRADES`** after the previously observed triggered `APPLY_TRADE_CDC_TASK` `SUCCEEDED`. Together with the one-event Iceberg INSERT, this closes the **Iceberg → append-only stream → triggered Task → Gold current-state** smoke test. Exact Gold column values were not pasted; do not claim additional field-level checks.

**Next: SCD Type 2 from event-sourced history (PLANNED, not yet executed):**
- [06_trade_history_from_events.sql](../sql/scd2/06_trade_history_from_events.sql) creates `IB_LAB.GOLD.V_TRADE_SCD2_HISTORY` as a derived VIEW over `IB_LAB.DBT_DEV.INT_VALID_TRADES`. Uses `ROW_NUMBER` to dedupe EVENT_IDs and enumerate versions within TRADE_ID, `LEAD` to identify next sequence and timestamp, `IS_CURRENT` for latest event, `IS_DELETED` as tombstone. The event feed is already persisted in Iceberg; no need to replay the existing stream.
- [dbt model](../dbt/models/marts/fct_trade_state_history.sql) offers equivalent dbt `ref('int_valid_trades')` view, with [singular test](../dbt/tests/assert_single_current_trade_version.sql) enforcing one current version per trade.

**Caveats:** A derived view is **not a separately materialized SCD2 dimension table**. It recomputes from source events on query and reflects new source events automatically. Version order is based on per-TRADE_ID increasing `SOURCE_SEQUENCE`; timestamp-based validity intervals require EVENT_TS to be monotonic with the sequence, or use numeric sequence boundaries. A traditional mutable-source SCD2 can use dbt snapshots (not needed to reconstruct already-preserved events). SCD2 SQL and tests are committed but **not yet executed** in Snowflake.

**Next action:** Run just the SCD2 SQL view script or its dbt equivalent and view one multi-version trade. Then advance to Snowpipe/CDC architecture and performance tuning.

## 2026-11-11 — Dual-path SCD2 selected; physical Gold architecture committed (PLANNED)

**User decision:** Implement two distinct patterns for interview-ready learning: **(a)** a derived SCD2 history VIEW over the existing validated Iceberg event log, **(b)** a separate physically persisted Gold SCD2 history table with change-detection and incremental maintenance. Keep `GOLD.CURRENT_TRADES` untouched for latest-state queries.

**New code, not yet executed:**
- [07_physical_scd2_bootstrap.sql](../sql/scd2/07_physical_scd2_bootstrap.sql): creates a **separate** `TRADE_EVENTS_SCD2_STREAM` before bootstrap, derives `V_TRADE_SCD2_DESIRED` from `DBT_DEV.INT_VALID_TRADES`, uses SHA-256 fingerprints over tracked business attributes + deletion flag to suppress no-op event versions, creates `GOLD.TRADE_SCD2_HISTORY` and persistent reject/run audit tables, performs guarded one-time historical load.
- [08_incremental_physical_scd2_task.sql](../sql/scd2/08_incremental_physical_scd2_task.sql): creates Snowflake SQL stored procedure `SP_APPLY_TRADE_SCD2()` that consumes new events through a **dedicated** append-only Iceberg stream, captures invalid event identifiers in `SCD2_REJECT_AUDIT`, reconstitutes the **entire version chain only for touched valid TRADE_IDs** to handle late-arriving sequences, and atomically DELETEs+INSERTs these histories with one BEGIN TRANSACTION / COMMIT. Rollback preserves the stream offset if failure. Defines an event-triggered `APPLY_TRADE_SCD2_TASK` but leaves it **SUSPENDED** until manual acceptance.

**Key differences / safeguards:**
1. VIEW is computed upon each SELECT, version per valid event (including no-op updates); PHYSICAL table stores changed-business-state versions only, tracked by SHA-256 digest.
2. Multiple consumers need independent stream offsets. Never share `TRADE_EVENTS_APPEND_STREAM` with SCD2 task.
3. SCD2 uses `SOURCE_SEQUENCE` as authoritative version ordering; `EVENT_TS` validity windows are trustworthy only if timestamps increase along the sequence.
4. D events are stored as deletion tombstone states. They must be sufficiently complete to pass the current dbt DQ filters (current lab assumes full row images).
5. Transactional atomicity covers stream consume, reject records, physical history changes, and success audit. Failure/error history is observable through Snowflake TASK_HISTORY. No real account credentials are committed.
6. **Production-style reference design, not yet production-certified.** Before real deployment: schema contracts, sequence collision detection, bitemporal rules, permissions, idempotency concurrency tests, monitoring, task costs, operational recovery, and scale/performance must be verified.

**Next user action:** execute ONLY script 07 in Snowflake and verify one current history version per TRADE_ID; keep 08 untouched until bootstrap succeeds. No historical RAW/Iceberg counts need repeating.

## 2026-11-11 — Idempotency gap confirmed in existing pipeline

**User question:** If an identical file is submitted again, or an identical data set arrives with a different filename, does our pipeline remain idempotent?

**Answer from reviewed source:** **Partial only.** Snowflake `COPY INTO` load history generally stops reloading the same file path/identity within its supported history window (we do not use FORCE=TRUE), but a new filename containing the same events can write duplicate rows into Iceberg Bronze. Our dbt staging model has validation and client checks but no source event-ID deduplication. Gold CDC MERGE applies per-trade ordering and gives partial protection of current state, but it neither removes duplicated Bronze records nor guarantees conflict safety for different EVENT_IDs on the same source sequence. The SCD2 learning view and planned physical SCD2 desired-state view dedupe identical EVENT_IDs, but not differently identified events.

**Recommendation (NOT YET IMPLEMENTED):** file-ingestion ledger (file identity & status) + canonical deduplicated Silver event layer (producer-stable SOURCE_SYSTEM/EVENT_ID, preserve duplicate/conflict audit) + strictly ordered Gold state MERGE with source-sequence collision detection. Never rely on standard Snowflake UNIQUE/PK constraints to enforce this; they are informational. See [Idempotency and replay design](idempotency_and_replay_design.md) and the two read-only Bronze duplicate diagnostics therein.

**Next lab:** test a copied Parquet file under a distinct name, observe duplicate raw event keys, implement a canonical event model and audit, then confirm Gold current state stays unchanged under replay. This is a planned learning milestone, not verified.

## 2026-11-11 — Scope freeze: complete pipeline before idempotency hardening

**User decision:** Finish the full Snowflake+Iceberg+dbt+CDC+SCD2+Snowpipe teaching pipeline quickly. Explicitly defer same-file/new-filename deduplication, canonical-event layer, file ledger, full-scale replay controls and performance hardening until after the pipeline is working end-to-end. The documented gap remains acknowledged in [idempotency review](idempotency_and_replay_design.md), not fixed.

**Fast completion sequence:**
1. Execute learning SCD2 history VIEW from `sql/scd2/06_trade_history_from_events.sql` (or equivalent dbt model, don't need both executions); one look at version history.
2. Execute `sql/scd2/07_physical_scd2_bootstrap.sql` to create separate stream, physical SCD2 table and audit tables; one current-version check.
3. Execute `sql/scd2/08_incremental_physical_scd2_task.sql`: create stored procedure/task, test a new changed event, then enable the task. This is **planned code, not yet run**.
4. Add Snowpipe to a **new, dedicated incoming Parquet prefix** (not historical batch locations), retaining metadata columns. AWS S3 event integration and pipe configuration still need to be implemented and verified.
5. One end-to-end demonstration: new landing object → Snowpipe → Iceberg → Streams/Tasks → Gold current + persisted SCD2. The existing 1704/392 historical counts will NOT be repeatedly retested.

**Definition of done:** show one new, clean trade I/U event passing both Gold paths and an automatically triggered file ingestion; record execution evidence. Only then return to idempotency and operational hardening. Do not claim this is already done.

## 2026-11-11 — Snowpipe auto-ingest implementation prepared (NOT YET EXECUTED)

**User request / priority:** Finish the full pipeline quickly, defer file/record idempotency design. User reports completing **physical SCD2 table and incremental stored procedure**. This is user-reported; the SCD2 triggered task's STARTED state and actual run are not confirmed.

**Committed Snowpipe assets**:
- [09_s3_auto_ingest_to_iceberg.sql](../sql/snowpipe/09_s3_auto_ingest_to_iceberg.sql): `CREATE PIPE IF NOT EXISTS IB_LAB.BRONZE.TRADE_EVENTS_AUTO_PIPE AUTO_INGEST=TRUE AS COPY INTO IB_LAB.BRONZE.RAW_TRADE_EVENTS_ICEBERG`, preserving 14 incoming Parquet fields + 3 file metadata columns; Iceberg `FULL_INGEST`; no `PURGE`; no `FORCE`; serverless Snowpipe.
- [S3 → SQS → Snowpipe setup runbook](snowpipe_setup.md): `DESC PIPE` yields the real SQS queue ARN; configure AWS bucket `omera-ib-trade-lake` S3 notification with prefix `landingzone/parquet/auto/`, suffix `.parquet`, all object-create events, SQS destination. Use a new path `@IB_LAB.RAW.IB_S3_STAGE/parquet/auto/` to avoid reprocessing the previously loaded `parquet/batches/` files.

**Next external steps**: User runs SQL pipe definition, copies actual SQS ARN from `DESC PIPE`, creates S3 bucket event notification, uploads one **new** synthetically generated Parquet file with valid client and new event/trade IDs. Validate with `SYSTEM$PIPE_STATUS` + one target `SOURCE_FILE` query. The already-running current-state task should then react; physical SCD2 task must be RESUMED separately if created.

**Status:** Prepared in GitHub/Notion only. No Snowpipe pipe, AWS S3 event rule, or auto-loaded file confirmed. Existing raw and Bronze history must not be replayed merely to prove automation.
