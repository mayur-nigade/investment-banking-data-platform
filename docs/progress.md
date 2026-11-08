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
