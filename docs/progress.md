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
