# AWS S3 → Snowflake-managed Apache Iceberg: Implementation runbook

**Status: PROPOSED / not executed.** This is for the synthetic-data portfolio lab. Do not claim production use at Jefferies.

## Objective / separation of concerns

- Immutable source landing: `s3://omera-ib-trade-lake/landingzone/`
- Writable Iceberg root: `s3://omera-ib-trade-lake/iceberg/bronze/`
- First Iceberg table: `IB_LAB.BRONZE.RAW_TRADE_EVENTS_ICEBERG` with `BASE_LOCATION='trade_events'`
- External volume: `OMERA_IB_ICEBERG_VOL`. Existing external stage + storage integration (`OMERA_S3_INT`) are **not** substitutes for an Iceberg external volume.

## Execution order

1. In S3 create the `iceberg/bronze/` prefix (Snowflake will create Iceberg-managed subdirectories).
2. In IAM, create scoped [S3 policy](../aws/iam/iceberg_bronze_s3_policy.json) and attach it to `OMERA_SNOWFLAKE_ICEBERG_ROLE`. Trust your own AWS account temporarily with the intended external ID.
3. Run the external-volume DDL from [the SQL runbook](../sql/iceberg/01_external_volume_and_backfill.sql) after substituting your AWS account number.
4. Run `DESC EXTERNAL VOLUME` and copy the exact `STORAGE_AWS_IAM_USER_ARN` and `STORAGE_AWS_EXTERNAL_ID`.
5. Replace the role trust relationship using [trust template](../aws/iam/iceberg_role_trust.template.json), replacing the principal with the ARN returned by Snowflake. Preserve matching external ID.
6. Run `SELECT SYSTEM$VERIFY_EXTERNAL_VOLUME('OMERA_IB_ICEBERG_VOL');`. Verify `success=true` with successful write/read/list/delete.
7. Create the `BRONZE` schema; first run `SHOW ICEBERG TABLES LIKE 'RAW_TRADE_EVENTS_ICEBERG' IN SCHEMA IB_LAB.BRONZE;`. If no table exists, run the Iceberg CTAS once with `CREATE ICEBERG TABLE ... AS SELECT * FROM IB_LAB.RAW.RAW_TRADE_EVENTS`. Iceberg CTAS does NOT accept `IF NOT EXISTS`. This is a one-time backfill of the 1,704 events already loaded from CSV.
8. Verify row counts and check both directions of `EVENT_ID` set difference. Then inspect S3 `iceberg/bronze/trade_events/` for Iceberg-managed Parquet + metadata files.
9. **Stop:** Do not ingest November 3 Parquet files until the historical backfill is checked. That new batch contains 392 staged events.

## Critical mechanisms

| Item | Purpose |
|---|---|
| Snowflake external stage | Read-only access to external *source files* for ingestion |
| Snowflake external volume | Account-level storage identity allowing Iceberg table *read/write/maintenance* |
| IAM role and trust | Snowflake IAM principal assumes temporary-role credentials through AWS STS using `sts:ExternalId` |
| S3 policy | Restricts writes/deletes to Iceberg Bronze; keep landing outside the write scope |
| `CATALOG='SNOWFLAKE'` | Snowflake owns Iceberg metadata and atomic snapshots; no AWS Glue job or Glue Data Catalog required |
| `BASE_LOCATION` | Per-table location under the volume root |
| Iceberg table | Parquet data files plus Iceberg snapshot, manifest and metadata files; simply uploading Parquet isn't enough |

## IAM subtlety

`s3:GetBucketLocation` is in its own statement **without an `s3:prefix` condition**. The `s3:prefix` condition applies only to the `s3:ListBucket` operation; attaching the condition to `GetBucketLocation` could inadvertently deny that action.

If the S3 bucket uses an SSE-KMS customer-managed key, also grant the IAM role and KMS key policy necessary `kms:Encrypt`, `kms:Decrypt`, `kms:GenerateDataKey`, `kms:DescribeKey` (scoped to that key). Evaluate Snowflake external-volume `ENCRYPTION` options for your encryption requirements.

**Operational safety:** Never run `CREATE OR REPLACE EXTERNAL VOLUME` or `CREATE OR REPLACE ICEBERG TABLE` on established resources just to retry; first inspect the existing object and preserve files. Do not rerun Iceberg CTAS if a table already exists: inspect row counts and use a planned, idempotent backfill strategy rather than replacing or duplicating records. Keep table files exclusively managed through Iceberg-aware operations.

## References

- https://docs.snowflake.com/en/user-guide/tutorials/create-your-first-iceberg-table
- https://docs.snowflake.com/en/sql-reference/sql/create-external-volume
- https://docs.snowflake.com/en/sql-reference/sql/create-iceberg-table-snowflake
- https://docs.snowflake.com/en/sql-reference/functions/system_verify_external_volume

## Outcome to record after running

- S3 storage prefix created [ ]
- IAM policy/role attached [ ]
- `DESC EXTERNAL VOLUME` trust principal set [ ]
- Verify external volume = success true [ ]
- Iceberg table exists [ ]
- Native RAW 1704 = Iceberg Bronze 1704 [ ]
- No missing/extra `EVENT_ID` values [ ]
- Iceberg S3 location contains new data/metadata [ ]


## Troubleshooting: STS AssumeRole denied (observed 2026-11-08)

**Observed (before fix):** `SYSTEM$VERIFY_EXTERNAL_VOLUME('OMERA_IB_ICEBERG_VOL')` returned `success=false`; `storageLocationSelectionResult=PASSED`, `awsRoleArnValidationResult=PASSED`, while `writeResult`, `readResult`, `listResult`, `deleteResult` all failed with `Error assuming AWS_ROLE: ... is not authorized to perform sts:AssumeRole`.

**Diagnosis:** Snowflake cannot assume the AWS IAM role through STS. This is a *trust relationship / external ID* problem, not proof of missing S3 permissions, KMS permissions, or bad Iceberg table SQL; those checks have not yet run.

**Fix (AWS Console):**

1. Open **IAM → Roles → OMERA_SNOWFLAKE_ICEBERG_ROLE → Trust relationships → Edit trust policy** in the account that owns the role.
2. Run `DESC EXTERNAL VOLUME OMERA_IB_ICEBERG_VOL;` in Snowflake and copy the exact `STORAGE_AWS_IAM_USER_ARN` and `STORAGE_AWS_EXTERNAL_ID`.
3. Paste the principal and external ID into [iceberg_role_trust.template.json](../aws/iam/iceberg_role_trust.template.json). **Only the Snowflake IAM user ARN** belongs in `Principal.AWS`; the local IAM role ARN is the role you are editing, not the Principal. Both AWS account IDs can differ.
4. Save the trust policy, then retry `SELECT SYSTEM$VERIFY_EXTERNAL_VOLUME('OMERA_IB_ICEBERG_VOL');`.
5. If STS succeeds but one S3 operation then fails, investigate the separate S3/IAM/KMS permission layer. Do not create the Iceberg table until verification success is true.

**State:** IAM trust **fixed and verified**. External-volume verification later returned success=true and write/read/list/delete PASSED. Public GitHub templates intentionally omit tenant-specific AWS account IDs and identifiers.

Official: https://docs.snowflake.com/en/user-guide/tables-iceberg-configure-external-volume-s3

## Troubleshooting: `INGESTED_AT TIMESTAMP_LTZ(9)` unsupported by Iceberg v2 CTAS (observed 2026-11-08)

**Observed:** The first `CREATE ICEBERG TABLE ... AS SELECT * FROM IB_LAB.RAW.RAW_TRADE_EVENTS` failed at **SQL compilation** with `Invalid time type scale specified for column 'INGESTED_AT' with data type 'TIMESTAMP_LTZ(9)'`. External volume verification had already passed. **No successfully created Iceberg table has yet been confirmed.**

**Root cause:** Native Snowflake defaults many timestamp columns to nanosecond precision (`TIMESTAMP_LTZ(9)`), whereas Iceberg **v2** represents time/timestamps with microsecond precision (`TIMESTAMP_LTZ(6)` / `TIMESTAMP_NTZ(6)`). Iceberg **v3** can use nanosecond timestamp types but that's not our current interoperability-focused lab target.

**Remediation (NOT YET VERIFIED):** Check `SHOW ICEBERG TABLES ...` first. If absent, use the corrected SQL in [01_external_volume_and_backfill.sql](../sql/iceberg/01_external_volume_and_backfill.sql):

```sql
CREATE ICEBERG TABLE IB_LAB.BRONZE.RAW_TRADE_EVENTS_ICEBERG
    CATALOG = 'SNOWFLAKE'
    EXTERNAL_VOLUME = 'OMERA_IB_ICEBERG_VOL'
    BASE_LOCATION = 'trade_events'
AS
SELECT * REPLACE (
    CAST(INGESTED_AT AS TIMESTAMP_LTZ(6)) AS INGESTED_AT
)
FROM IB_LAB.RAW.RAW_TRADE_EVENTS;
```

**Mechanism:** `SELECT * REPLACE` keeps the 14+ source fields and position unchanged but casts the projected ingest timestamp from 9 to 6 fractional digits. The native RAW table is **not** updated. CTAS materializes a distinct physical Iceberg table, writing new Parquet data + Iceberg manifests/metadata to the dedicated S3 external volume. It is **not a view**, and future native RAW changes do not propagate automatically.

**Verify:** `SHOW ICEBERG TABLES`, `DESC ICEBERG TABLE`, comparison of native RAW vs Iceberg counts (expected 1704 vs 1704), and both-way `EVENT_ID` differences (expected no rows). `SELECT * REPLACE` has been documented but the corrected CTAS awaits user execution.

**Storage nuance:** Snowflake may generate `trade_events.<randomId>/data/` and `.../metadata/` under the volume root when `BASE_LOCATION='trade_events'`; the exact physical folder need not be exactly `trade_events/`.

Official sources: https://docs.snowflake.com/en/user-guide/tables-iceberg-data-types ; https://docs.snowflake.com/en/sql-reference/sql/create-iceberg-table-snowflake ; https://docs.snowflake.com/en/sql-reference/sql/select ; https://docs.snowflake.com/en/user-guide/tables-iceberg-managing-external-volumes
