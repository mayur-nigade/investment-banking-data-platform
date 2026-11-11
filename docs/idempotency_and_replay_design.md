# Idempotency and replay safety — design review

**Status (2026-11-11):** Reviewed against the versioned lab SQL. This is an analysis and proposed upgrade; the ingestion and dbt dedupe changes below are **NOT yet implemented or verified**. The user's CDC task and Gold single-event smoke test were previously verified separately. All data is synthetic.

## Question: same file or different filename with the same events?

There are **three different identities**:

1. **File identity:** staged S3 URI/path, object size and ETag/version. Snowflake bulk `COPY INTO` records file load history for the target table and generally suppresses the same staged file unless instructed to reload. File load metadata has a limited retention horizon (see Snowflake docs). It is NOT a permanent enterprise deduplication ledger. In Snowpipe, the per-pipe history is separate from bulk COPY history, so combining loaders on the same paths requires special care.
2. **Source event identity:** a stable business-event identifier such as `(SOURCE_SYSTEM, EVENT_ID)`. This is the authoritative dedupe key in the proposed architecture when the producer contract guarantees it.
3. **Business state identity and ordering:** `(SOURCE_SYSTEM, TRADE_ID)` plus `SOURCE_SEQUENCE` for source-order/CDC conflict handling. Repeating a state update is not equivalent to inserting a new distinct event.

### What our actual code protects

| Layer | Existing behavior | Gap |
| --- | --- | --- |
| S3 → Iceberg Bronze | `sql/iceberg/02_load_new_trade_parquet_batch.sql` uses `COPY INTO` with `LOAD_MODE=FULL_INGEST`, does not request `FORCE=TRUE`; records `SOURCE_FILE`, row number and ingest timestamp. | Another filename containing the same event payload will be treated as a new file and rows can be appended again. Duplicate events remain in Bronze. |
| dbt staging | `dbt/models/staging/stg_trade_events.sql` casts, validates and assigns DQ_STATUS. | **No source event dedupe** before valid/rejected views. The partition counts include duplicates; unique EVENT_ID tests on valid data may then fail. |
| CDC stream/task → CURRENT_TRADES | `sql/cdc/04_triggered_trade_cdc_task.sql` selects one latest incoming event per TRADE_ID and only updates target when event ordering compares newer; tests confirmed one event reaching Gold. | Replays still consume stream and incur processing. A *different* event ID on the same source sequence/timestamp can win the event-ID tie-break; this is not a guaranteed semantically idempotent conflict policy. |
| Learning SCD2 event-history view | `sql/scd2/06_trade_history_from_events.sql` deduplicates equal EVENT_IDs. | Re-identification with a new EVENT_ID is not recognized as the same event. |
| Planned physical SCD2 | `sql/scd2/07_physical_scd2_bootstrap.sql` deduplicates by EVENT_ID and fingerprints tracked attributes to skip no-op *business state* versions. Incremental script 08 not executed. | Replay and source-order contracts need explicit tests. Input Bronze still contains duplicate event rows. |

Snowflake standard-table PRIMARY KEY/UNIQUE constraints are not enforced, so simply adding `UNIQUE(EVENT_ID)` is **not** sufficient.

## Recommended production-style 3-stage protection

**A. Ingestion control / file ledger:** Maintain `CONTROL.FILE_INGEST_AUDIT` keyed by immutable object identity (S3 key + object version / ETag, loader, target, status). Preserve source files, run details, row counts, payload checksums and retry state. This supports operational replays, but does **not** substitute for event-level dedupe. If incoming file is replaced at the same path, agree a policy to reject/re-version it; do not silently trust filename. Only one ingestion writer should own a target/prefix or loaders must share a transactionally coordinated dedupe strategy.

**B. Canonical unique event layer:** Keep the Bronze/raw immutable append log for full traceability. Create a separate `SILVER.CANONICAL_TRADE_EVENTS` or dbt canonical model that retains only one row per stable `(SOURCE_SYSTEM,EVENT_ID)` (or producer-guaranteed `(SOURCE_SYSTEM,TRADE_ID,SOURCE_SEQUENCE)`). Deterministically select one occurrence using provenance; preserve a duplicate/replay audit with every rejected duplicate's filename and row number. Persist canonical events via `MERGE` with concurrency controls or use a validated view as a first teaching slice. Compare payloads for the same key; **if same key but different business fields, classify as a CONFLICT, do not silently choose one**. A content hash alone is not a safe key because two legitimate independent events can have identical values.

**C. Business-state idempotency:** MERGE Gold by business key with **strictly increasing, producer-defined sequence**, reject conflicting same-sequence events, and retain deletion tombstones. Late events should not revert newer state. For physical SCD2, rebuilding only affected trade version chains is the planned strategy; validate monotonic source sequencing and timestamp consistency.

## Quick read-only diagnostic — current duplicate keys in Bronze

```sql
-- Does the same source event ID appear more than once?
SELECT SOURCE_SYSTEM, EVENT_ID,
       COUNT(*) AS OCCURRENCES,
       COUNT(DISTINCT SOURCE_FILE) AS DISTINCT_SOURCE_FILES
FROM IB_LAB.BRONZE.RAW_TRADE_EVENTS_ICEBERG
WHERE EVENT_ID IS NOT NULL
GROUP BY SOURCE_SYSTEM, EVENT_ID
HAVING COUNT(*) > 1
ORDER BY OCCURRENCES DESC;

-- Same business version reported under different event IDs?
SELECT SOURCE_SYSTEM, TRADE_ID,
       TRY_TO_NUMBER(SOURCE_SEQUENCE) AS BUSINESS_SEQUENCE,
       COUNT(*) AS OCCURRENCES,
       COUNT(DISTINCT EVENT_ID) AS DISTINCT_EVENT_IDS
FROM IB_LAB.BRONZE.RAW_TRADE_EVENTS_ICEBERG
WHERE TRADE_ID IS NOT NULL
  AND TRY_TO_NUMBER(SOURCE_SEQUENCE) IS NOT NULL
GROUP BY SOURCE_SYSTEM, TRADE_ID, BUSINESS_SEQUENCE
HAVING COUNT(DISTINCT EVENT_ID) > 1
ORDER BY DISTINCT_EVENT_IDS DESC;
```

**Interpretation:** The first query finds exact source-event key replays. The second finds possible sequencing collisions/conflicts; they require inspecting payloads, not automatic deletion.

## Suggested hands-on acceptance test (NOT executed)

1. In synthetic test landing, copy an already-loaded Parquet event set to a **different filename** with the same EVENT_IDs.
2. Confirm it can produce duplicate Bronze rows (without `FORCE=TRUE` because file path differs).
3. Add canonical event dedupe and explicit replay/conflict audit.
4. Confirm exactly one canonical row per `(SOURCE_SYSTEM, EVENT_ID)`, duplicate provenance visible in audit, and unchanged Gold current trade state after a replay.
5. Confirm new legitimate event with a new event ID and higher source sequence DOES update Gold.

## Official Snowflake documentation

- File metadata / 64-day expiry: https://docs.snowflake.com/en/user-guide/data-load-considerations-load
- Separate Snowpipe and bulk COPY load histories: https://docs.snowflake.com/en/user-guide/data-load-snowpipe-intro
- Standard table keys not enforced: https://docs.snowflake.com/en/sql-reference/constraints
