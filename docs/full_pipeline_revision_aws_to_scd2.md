# Full Pipeline Revision — AWS IAM to Snowflake Iceberg, Snowpipe, dbt, CDC, and SCD2

**Revision date:** 12 November 2026. **Context:** our independent synthetic investment-banking trade-events lab, not any client's proprietary system. Study order follows what we configured and troubleshot. External details checked against current official Snowflake guides; live execution state is explicitly qualified.

## A. One-minute project explanation

"We built an event-driven trade-data pipeline. Synthetic trade and client files land in a private AWS S3 bucket. A Snowflake storage integration assumes an AWS IAM read role to access the external stage. Historical files were loaded with COPY INTO; subsequent Parquet arrivals are processed through S3 object-created events, SQS, and Snowpipe into a Snowflake-managed Iceberg Bronze table. A separate Snowflake external volume uses a write-enabled AWS role for Iceberg data and metadata. dbt models cast/validate the raw events and split valid/rejected records. Two independent append-only Iceberg streams feed triggered tasks: one MERGEs the latest trade state into Gold, and the other calls a transactional stored procedure to maintain physical SCD Type 2 history. We monitor file loads, DQ rejects, task history, and apply audits."

**Actual topology:**

~~~text
Synthetic CSV/Parquet + client master
  → s3://omera-ib-trade-lake/landingzone/    [READ integration + external stage]
  → historical COPY or NEW file S3 event → Snowflake-managed SQS → Snowpipe
  → IB_LAB.BRONZE.RAW_TRADE_EVENTS_ICEBERG   [separate WRITE external volume]
       ↘ dbt (STG_TRADE_EVENTS, STG_CLIENTS → INT_VALID / INT_REJECTED)
       ↘ TRADE_EVENTS_APPEND_STREAM → APPLY_TRADE_CDC_TASK → GOLD.CURRENT_TRADES
       ↘ TRADE_EVENTS_SCD2_STREAM → APPLY_TRADE_SCD2_TASK → SP_APPLY_TRADE_SCD2
                                                → GOLD.TRADE_SCD2_HISTORY
~~~

**Important nuance:** dbt valid/rejected views are used for business validation; the CDC task reads its own Iceberg stream and applies its own validity conditions. The SCD2 procedure reads its separate stream for touched trade/event IDs and uses dbt-valid views to rebuild chains. **A view doesn't enqueue anything.** A stream records change metadata/offset, and a task invokes SQL.

## B. Step-by-step setup — every small AWS and Snowflake trust detail

### 1. Create an AWS S3 landing bucket/prefix

- Bucket: **omera-ib-trade-lake**. Source input prefix for our final configuration: **landingzone/**. Earlier setup notes use landing/ as a generic shorthand; the actual stage is rooted at landingzone/.
- Keep Block Public Access ON. Store source CSV/Parquet under appropriate reference/batches/auto folders. Source files are for audit/replay, not public web access.
- **Bucket vs prefix:** the bucket is an AWS object container; a prefix is part of the object's key (looks like a folder). The S3 event prefix includes landingzone/; the stage-relative path does not.
- **S3 is an object store, not a SQL database or file system.** Iceberg managed output has a different prefix: **iceberg/bronze/**.

### 2. Create a least-privilege IAM *permission policy* for source READ

- Attach to role **OMERA_SNOWFLAKE_S3_ROLE**.
- Allow **s3:ListBucket** on bucket ARN and **s3:GetObject** on the needed source-object ARN prefix. Restrict s3:ListBucket using **s3:prefix** as appropriate.
- Do not grant unnecessary PutObject/DeleteObject to the landing-stage reader. If required, add SSE-KMS **kms:Decrypt/DescribeKey** access through IAM and key policy.
- **Policy vs role:** policy says which API actions/resources are allowed; IAM role is the AWS identity that receives those permissions.
- Two ARN forms: **arn:aws:s3:::bucket** for bucket operations; **arn:aws:s3:::bucket/prefix/* ** for object actions. Do not accidentally put GetObject on bucket-only ARN.

### 3. Create AWS role and integrate it into Snowflake

- AWS IAM → Roles → Create role → attach above S3 read policy. Initial trust can be a temporary controlled AWS identity; **replace that with the exact Snowflake principal after Snowflake integration exists.**
- In Snowflake create storage integration **OMERA_S3_INT**, with **STORAGE_PROVIDER='S3'**, **STORAGE_AWS_ROLE_ARN** (your actual AWS role ARN), **STORAGE_ALLOWED_LOCATIONS** restricted to the landing prefix. The precise original CREATE INTEGRATION text/ARN is not present in GitHub; the integration name and configuration mechanism are documented in our Notion lab.
- Example teaching pattern, not copied from the live DDL:

~~~sql
CREATE STORAGE INTEGRATION OMERA_S3_INT
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = '<your actual AWS role ARN>'
  STORAGE_ALLOWED_LOCATIONS = ('s3://omera-ib-trade-lake/landingzone/');
DESC INTEGRATION OMERA_S3_INT;
~~~

- **Role registration is a two-sided trust handshake**: Snowflake names the AWS role ARN; AWS role trust policy names Snowflake's AWS IAM principal. Merely creating an IAM role does NOT connect it.
- From **DESC INTEGRATION**, copy exact **STORAGE_AWS_IAM_USER_ARN** and **STORAGE_AWS_EXTERNAL_ID**. NEVER invent or paste an ARN from another Snowflake account. Snowflake account role such as ACCOUNTADMIN is NOT the same as the AWS IAM role.
- AWS Console → IAM → Roles → OMERA_SNOWFLAKE_S3_ROLE → **Trust relationships** → Edit → set **Principal.AWS = Snowflake's STORAGE_AWS_IAM_USER_ARN**, **Action = sts:AssumeRole**, **Condition.StringEquals.sts:ExternalId = STORAGE_AWS_EXTERNAL_ID**.
- **Trust policy vs permissions policy:** trust answers **WHO may assume** the role; permissions answer **WHAT that assumed role can do**. External ID helps prevent confused-deputy misuse.
- AWS **STS AssumeRole** issues temporary credentials. No AWS access key/password belongs in code, stage DDL or public repository.
- Verify source access with **LIST @IB_LAB.RAW.IB_S3_STAGE/reference/** or batch path. **LIST proves listing, not row/schema correctness.** If STS AssumeRole AccessDenied, verify trust principal/external ID BEFORE troubleshooting object policy.

### 4. File format and external stage

- Stage: **IB_LAB.RAW.IB_S3_STAGE**, rooted at S3 **landingzone/**, backed by **OMERA_S3_INT**.
- **Stage = reusable named external file locator**, not a data table. **File format = parser settings** (CSV delimiters/header/nulls/quotes, or Parquet).
- The exact historical CSV file-format options were not captured in the GitHub files, so do not invent a delimiter/header configuration. Existing RAW CSV ingestion was executed successfully.
- The actual Parquet format:

~~~sql
CREATE FILE FORMAT IF NOT EXISTS IB_LAB.RAW.IB_PARQUET_FORMAT
 TYPE = PARQUET
 USE_LOGICAL_TYPE = TRUE
 USE_VECTORIZED_SCANNER = TRUE;
LIST @IB_LAB.RAW.IB_S3_STAGE/parquet/batches/batch_2026-11-03/;
~~~

- Parquet is **columnar, binary**, not a human-readable CSV. We read **t.$1:event_id**, **t.$1:quantity** from the Parquet OBJECT; CSV uses positional **$1, $2 ...**.
- Use **INFER_SCHEMA** for structural inspection and **METADATA$FILENAME**, **METADATA$FILE_ROW_NUMBER** for provenance. **METADATA$START_SCAN_TIME** marks load-time scan start, not source business event time.

### 5. RAW and client reference ingestion

- Historical native Snowflake table **IB_LAB.RAW.RAW_TRADE_EVENTS** had 1,704 source events. **IB_LAB.RAW.RAW_CLIENTS** had 60 clients (user confirmed).
- Use **COPY INTO** with explicit column mapping and metadata capture. Preserve source quantity/price/date/sequence as VARCHAR in RAW so wrong values can be classified downstream rather than failing cast and disappearing.
- COPY INTO uses a user-managed warehouse such as **COMPUTE_WH**; warehouse **AUTO_SUSPEND/AUTO_RESUME** reduce idle compute costs.
- Why **ON_ERROR=ABORT_STATEMENT** for the manually performed bulk COPY? Fail an invalid bulk statement deliberately; this is **not supported inside a Snowpipe pipe definition**.
- Historical count was **1,704 staging = 1,696 valid + 8 rejected** after client-reference DQ; **7 dbt tests passed**. These are historical checkpoints, not expected totals after new files.

## C. Separate Iceberg WRITE identity — second IAM handshake

### 6. AWS IAM policy + role for Iceberg

- **Not the same as source read integration**. Role: **OMERA_SNOWFLAKE_ICEBERG_ROLE**; write destination: **s3://omera-ib-trade-lake/iceberg/bronze/**.
- Actual repository policy **aws/iam/iceberg_bronze_s3_policy.json** grants **s3:GetObject, GetObjectVersion, PutObject, DeleteObject, DeleteObjectVersion** on Iceberg objects; **s3:ListBucket** scoped to iceberg/bronze prefixes; **s3:GetBucketLocation** on bucket without s3:prefix filter.
- **s3:prefix only applies to ListBucket**, not GetBucketLocation. SSE-KMS may also require Encrypt/Decrypt/GenerateDataKey and KMS key-policy access.
- Role trust uses Snowflake principal + external ID exactly as the read role does, but **the external volume may have a different external ID** from the storage integration.

### 7. Create / verify external volume

~~~sql
-- Actual project object; replace account placeholder ONLY when rebuilding elsewhere
CREATE EXTERNAL VOLUME IF NOT EXISTS OMERA_IB_ICEBERG_VOL
 STORAGE_LOCATIONS = ((
    NAME = 'omera-iceberg-bronze',
    STORAGE_PROVIDER = 'S3',
    STORAGE_BASE_URL = 's3://omera-ib-trade-lake/iceberg/bronze/',
    STORAGE_AWS_ROLE_ARN =
      'arn:aws:iam::<AWS_ACCOUNT_ID>:role/OMERA_SNOWFLAKE_ICEBERG_ROLE',
    STORAGE_AWS_EXTERNAL_ID = 'OMERA_ICEBERG_BRONZE_2026'
 ))
 ALLOW_WRITES = TRUE;

DESC EXTERNAL VOLUME OMERA_IB_ICEBERG_VOL;
SELECT SYSTEM$VERIFY_EXTERNAL_VOLUME('OMERA_IB_ICEBERG_VOL');
~~~

- Copy **STORAGE_AWS_IAM_USER_ARN** + **STORAGE_AWS_EXTERNAL_ID** from **DESC EXTERNAL VOLUME**, edit **AWS IAM Role → Trust relationships** with these exact values. The IAM role ARN is the AWS resource being edited; it is NOT the Principal.AWS in trust policy.
- Our observed early failure: **STS AssumeRole unauthorized**. Role ARN validation passed but read/write/list/delete failed. We corrected **trust principal/external ID**; external-volume verification then passed read/write/list/delete.
- **Storage integration** grants Snowflake READ on source landing files for an external stage. **External volume** authorizes Iceberg-managed table data/metadata storage READ/WRITE in an S3 prefix. Do not confuse them.

### 8. Create Iceberg Bronze table, historical backfill

~~~sql
CREATE ICEBERG TABLE IB_LAB.BRONZE.RAW_TRADE_EVENTS_ICEBERG
  CATALOG = 'SNOWFLAKE'
  EXTERNAL_VOLUME = 'OMERA_IB_ICEBERG_VOL'
  BASE_LOCATION = 'trade_events'
AS SELECT * REPLACE (
   CAST(INGESTED_AT AS TIMESTAMP_LTZ(6)) AS INGESTED_AT
) FROM IB_LAB.RAW.RAW_TRADE_EVENTS;
~~~

- **CATALOG='SNOWFLAKE'**: Snowflake commits Iceberg snapshots and table metadata. No AWS Glue catalog was configured for this table.
- Iceberg table ≠ staged Parquet folder. Iceberg has **data files + metadata JSON + manifests/snapshots**, with atomic table snapshot changes.
- **External volume** is the physical storage location, **BASE_LOCATION** is the per-table path under it. Iceberg location may contain Snowflake-generated suffixes; don't hardcode physical directories.
- **Critical error encountered:** native **TIMESTAMP_LTZ(9)** could not create an Iceberg v2 column. Cast projected **INGESTED_AT** to **TIMESTAMP_LTZ(6)**. Does not modify original native RAW source.
- Historical CTAS loaded **1,704** events, verified against native RAW count and two-way EVENT_ID set difference. Do not rerun CTAS on an existing Iceberg table.
- **Iceberg SQL is not a view or automatic mirroring** of native RAW. Subsequent native RAW writes are not propagated unless ingested separately.

### 9. Load new Parquet (manual initial batch)

- Five files at **parquet/batches/batch_2026-11-03/** had **79+79+78+78+78=392** rows, loaded without reported errors. Iceberg expected checkpoint after this one batch = **2,096**, before later Snowpipe loads.
- Parquet source: 14 lowercase fields, mostly string values. Iceberg Bronze: **17 fields** = 14 sources + **SOURCE_FILE**, **FILE_ROW_NUMBER**, **INGESTED_AT**.
- Use **COPY INTO target column list FROM (SELECT t.$1:field::VARCHAR, ..., METADATA$...)** with **LOAD_MODE=FULL_INGEST**. Full ingest lets Snowflake rewrite input as Iceberg-managed Parquet and maintain metadata.
- **ADD_FILES_COPY** is not appropriate for our transformation; **MATCH_BY_COLUMN_NAME** cannot be combined with COPY SELECT transformations.
- Do not use **FORCE=TRUE** on retries or reload a prior batch under another name; file-level load history is NOT true business-event idempotency.

## D. dbt quality and lineage

### 10. Snowflake-native dbt Workspaces

- Key objects: **IB_LAB.DBT_DEV.STG_TRADE_EVENTS**, **STG_CLIENTS**, **INT_VALID_TRADES**, **INT_REJECTED_TRADES**.
- **sources.yml** defines external input tables and **source()**; **ref()** specifies other dbt models and builds the dependency graph (DAG).
- Iceberg migration changed the trade source to **source('ib_iceberg','RAW_TRADE_EVENTS_ICEBERG')**. **RAW_CLIENTS** stays native RAW. GitHub dbt models are **reconstructed snapshots**; do NOT claim the GitHub checkout itself has been successfully rerun.
- Staging **VIEW**: query-time cast + DQ classification. **TRY_TO_NUMBER**, **TRY_TO_DECIMAL**, **TRY_TO_DATE**, **TRY_TO_TIMESTAMP_NTZ** return NULL on malformed inputs rather than aborting the model.
- Typical DQ reasons: **MISSING_TRADE_ID**, **INVALID_QUANTITY** (NULL/nonpositive), **INVALID_PRICE**, **INVALID_TRADE_DATE**, **INVALID_EVENT_TS**, **INVALID_EVENT_TYPE**, **UNKNOWN_CLIENT_ID**. Join to client reference validates known CLIENT_ID.
- One prioritized **DQ_REASON** per row; **DQ_STATUS='VALID'/'INVALID'**. Valid and rejected are separate views. Invariant: **staging = valid + rejected**. Track reason and source-file provenance.
- **Business DQ is not guaranteed by file ingestion success.** Actual incident: **trade_settlements_001.parquet** landed **403** rows in Iceberg, but **all 403** failed **INVALID_QUANTITY**, because its eight settlement fields had no **quantity** column. Source inspection proved this was a **wrong-domain schema**; do not relax DQ.

## E. Continuous ingestion — Snowpipe

### 11. S3 → SQS → PIPE → Iceberg

- A Snowflake **PIPE** is a database object holding **COPY INTO** configuration; **Snowpipe** is the serverless ingestion service.
- Our pipe **IB_LAB.BRONZE.TRADE_EVENTS_AUTO_PIPE** uses **AUTO_INGEST=TRUE**; watches only **@IB_LAB.RAW.IB_S3_STAGE/parquet/auto/**.
- After **CREATE PIPE**, run **DESC PIPE**, copy **notification_channel** (Snowflake-managed SQS ARN); in AWS S3 **Properties → Event notifications** set **ObjectCreated:*; prefix landingzone/parquet/auto/; suffix .parquet; destination SQS ARN**.
- S3 sends the **object key**, not file data. Snowpipe reads the Parquet through the stage/integration and uses **LOAD_MODE=FULL_INGEST** with **14 fields + three metadata columns**.
- **No user warehouse for file Snowpipe**; the later triggered SQL Tasks use **WAREHOUSE=COMPUTE_WH**. Snowpipe is **near-real-time micro-batch, not guaranteed strict real-time**.
- Pipe COPY uses **ON_ERROR=SKIP_FILE**, not unsupported **ABORT_STATEMENT**. An invalid BUSINESS event can still load if the file is syntactically valid; dbt rejects it later.
- **SYSTEM$PIPE_STATUS('...')**, **DESC PIPE**, **COPY_HISTORY** for status/errors. **ALTER PIPE ... REFRESH** is for controlled recent backfills (7-day window), not routine new files.
- **Snowpipe vs COPY vs Streaming**: automatic staged files vs manually/orchestrated bulk staged files vs direct row/event stream without staging files.
- If overlapping S3 notifications conflict, consider SNS fan-out/EventBridge; don't overwrite another app's bucket notification.
- **Observed:** the 403-row settlement file did land under auto prefix but failed DQ. Subsequently a genuine **six-event** synthetic trade-events Parquet was generated. User confirms Snowpipe executed and later physical SCD2 history recorded; no pasted final complete six-event Gold task-history proof, so treat end-to-end record-level final status as user-reported, not independently audited.
- **Idempotency risk (deferred by user):** Snowpipe per-file history ≠ dedupe by EVENT_ID across different filenames. Separate canonical dedupe/ledger planned.

## F. CDC: two independent Gold consumers

### 12. Iceberg append-only Streams

- A **stream is change tracking + offset**, not a duplicate physical history table and not a file watcher. It exposes changes since its last successfully committed consumer DML.
- **APPEND_ONLY=TRUE** captures newly appended rows to Bronze; appropriate for our immutable event-ingestion design. Creating a stream *after* backfill does not deliver old rows.
- Two streams on **same Iceberg Bronze**: **BRONZE.TRADE_EVENTS_APPEND_STREAM** for current state and **BRONZE.TRADE_EVENTS_SCD2_STREAM** for physical SCD2. Each has a **separate consumption offset**. Using one stream for both would let one consumer move its offset and starve the other.
- A **SELECT** from a stream does not advance offset. Successful consuming **DML transaction COMMIT** advances it. **ROLLBACK** preserves pending data.
- The **METADATA$ACTION** indicates INSERT for our append-only source; it is not the business **EVENT_TYPE**. I/U/D are source business events embedded as rows.

### 13. Triggered Task and Gold latest state

- **GOLD.APPLY_TRADE_CDC_TASK** uses **WHEN SYSTEM$STREAM_HAS_DATA('...')** and **WAREHOUSE=COMPUTE_WH**, with a SQL **MERGE** to **GOLD.CURRENT_TRADES**. Triggered Task does not require a cron SCHEDULE.
- **Tasks start SUSPENDED**; run **ALTER TASK ... RESUME** explicitly after prerequisites. Use **SHOW TASKS**, **TASK_HISTORY** to inspect SUCCEEDED/FAILED/SKIPPED state. SKIPPED may mean no stream changes.
- CDC MERGE partitions incoming events by TRADE_ID, chooses latest by **SOURCE_SEQUENCE DESC, EVENT_TS DESC, EVENT_ID DESC**. For matches, only a newer event can update; inserts create new trade state.
- **TRADE_ID** is business key; **EVENT_ID** identifies source event; **SOURCE_SEQUENCE** orders versions. Arrival order and event timestamp alone are not authoritative if source sequence exists.
- For business **D** deletes, keep **IS_DELETED=TRUE tombstone** rather than physically removing Gold row. This helps protect against late events resurrecting deleted data.
- **Verified:** earlier manually appended synthetic trade caused triggered task SUCCEEDED and Gold CURRENT_TRADES arrival. The last six-event Snowpipe-generated test's exact Gold row output was not pasted.

### 14. Physical SCD Type 2 (different from a history view)

- Learning **GOLD.V_TRADE_SCD2_HISTORY**: a **VIEW** reconstructing event versions on query, not physically stored by its own task.
- Physical **GOLD.TRADE_SCD2_HISTORY**: separate native history table, using **VALID_FROM_SEQUENCE**, **VALID_TO_SEQUENCE_EXCLUSIVE**, **VALID_FROM_TS**, **VALID_TO_TS_EXCLUSIVE**, **VERSION_NO**, **IS_CURRENT**, **IS_DELETED**. Do not confuse **event count** with **actual changed business states**.
- **GOLD.V_TRADE_SCD2_DESIRED**: deduplicates by EVENT_ID and calculates **SHA-256 ATTR_HASH** over tracked trade attributes incl. deletion; skips no-op updates; uses **LAG / LEAD / ROW_NUMBER** to form valid intervals.
- **GOLD.SP_APPLY_TRADE_SCD2()**: consumer of independent SCD2 stream, records audit/rejects, identifies touched valid TRADE_IDs, rebuilds **only their entire version chains** from desired view in one explicit transaction; **DELETE old affected versions + INSERT recomputed chain + audit + COMMIT**.
- **Why recompute touched keys instead of expire/insert only?** Late-arriving source-sequence events must be inserted into the correct historical chain.
- **GOLD.APPLY_TRADE_SCD2_TASK** invokes procedure when SCD2 stream has data. The versioned SQL deliberately **commented out ALTER TASK ... RESUME** initially. The user later confirmed the target SCD2 record is now appearing, but exact task history was not provided.
- **Important nuance:** IS_CURRENT can be TRUE for a soft-deleted tombstone as latest *record state*, while IS_DELETED=TRUE means the business object is not active.
- Caveats: full-row events assumed; tied source sequences/conflicts and permanent event-level dedupe remain later hardening work. Never claim production compliance completed.

## G. Short troubleshooting playbook from real issues

| Symptom | First check | Root cause / treatment in our lab |
| --- | --- | --- |
| S3 LIST AccessDenied | IAM trust Principal.AWS + sts:ExternalId from DESC INTEGRATION, then S3 read policy | Source role not assumable / insufficient read scope |
| External-volume verify fails with sts:AssumeRole | DESC EXTERNAL VOLUME and AWS role trust policy | We fixed principal/external ID; subsequent read/write/list/delete passed |
| Iceberg CTAS rejects timestamp scale | RAW INGESTED_AT type | Iceberg v2 cannot take TIMESTAMP_LTZ(9) directly; cast to TIMESTAMP_LTZ(6) |
| File visible in Bronze but all DQ rejects | Raw Parquet OBJECT_KEYS and DQ_REASON | 403 settlements had no quantity, but trade pipe expected it |
| Good Parquet not loaded after upload | S3 event-prefix/suffix, SQS ARN, PIPE_STATUS, COPY_HISTORY | Notification or file-name mapping failure; don't repeatedly upload new names |
| Stream pending but Gold unchanged | Task STARTED, TASK_HISTORY, SQL DML/audit | SUSPENDED/FAILED task or invalid business event |
| Gold current works but SCD2 missing | Separate SCD2 Task, stream offset, audit, desired-state view | Script 08 left SCD2 task resume commented |
| Source event arrives with lower sequence | Business SOURCE_SEQUENCE | Gold MERGE must not replace a newer state |
| File re-uploaded under new filename | File ledger/event identity | No guaranteed event-level idempotency yet; planned hardening |

## H. Operational monitoring and small points

- **SHOW TASKS** = configured task and state; **INFORMATION_SCHEMA.TASK_HISTORY** = executions and errors; **SCD2_APPLY_AUDIT / SCD2_REJECT_AUDIT** = domain-level outcome.
- **COPY_HISTORY** = loaded/skipped/error files, not business validity. **SYSTEM$PIPE_STATUS** = Snowpipe queue/execution health. **PIPE_USAGE_HISTORY** = serverless Snowpipe credits/bytes.
- **SHOW ICEBERG TABLES / DESC ICEBERG TABLE** = confirm Iceberg object. **SYSTEM$VERIFY_EXTERNAL_VOLUME** tests IAM-backed Iceberg storage operations, not whether business data is valid.
- **METADATA$FILENAME**, **METADATA$FILE_ROW_NUMBER**, **INGESTED_AT** = lineage. **EVENT_TS**, **TRADE_DATE** = business time, not ingestion time.
- **Snowflake DB/schema/role vs AWS account/role** are different authorization domains. Creating a Snowflake object also requires Snowflake privileges even if AWS IAM is correct.
- Never put **access keys, passwords, account-specific IDs, private/proprietary source code or real client data** into our public GitHub repository.
- Cost: warehouse auto suspend/resume for manual COPY/Tasks; serverless Snowpipe charged separately. Monitor workload latency, small files, storage growth, failures and bytes/credits.
- **Not implemented in this lab:** AWS DMS from SQL Server; AppFlow from Salesforce; Glue catalog synchronization; a complete production ledger for file/event idempotency; contractual schema routing for settlement domain.

## I. 25 fast interview Q&A (one line each)

1. **Why IAM role?** STS temporary credentials; no long-lived S3 access key in Snowflake SQL.
2. **Trust policy?** Specifies who may call sts:AssumeRole, conditioned by external ID.
3. **Permission policy?** Specifies what the assumed AWS role can do to bucket/object/KMS resources.
4. **External ID?** Third-party assume-role guard against confused-deputy access.
5. **Where do Snowflake principal and external ID come from?** DESC INTEGRATION for stage; DESC EXTERNAL VOLUME for Iceberg.
6. **Why both storage integration and external volume?** Landing S3 file reader versus Snowflake-managed Iceberg storage writer.
7. **Stage?** Named external S3 location secured by storage integration.
8. **File format?** Parser behavior; Parquet binary columnar, CSV positional.
9. **LIST vs INFER_SCHEMA?** Detect S3 object visibility versus source schema.
10. **COPY INTO vs Snowpipe?** User-managed batch versus event-triggered serverless file microbatch.
11. **Snowpipe SQS message content?** New object key/path, not row payload.
12. **How identify pipe queue ARN?** DESC PIPE notification_channel.
13. **Why unique S3 prefix?** Avoid previously loaded batches and overlapping notifications.
14. **Why Iceberg?** Parquet + snapshots/manifests + cross-engine-oriented open table semantics.
15. **Who maintains catalog?** Snowflake, in CATALOG='SNOWFLAKE' mode.
16. **Why Iceberg time scale 6?** v2 microsecond temporal precision.
17. **FULL_INGEST vs ADD_FILES_COPY?** Rewrite/transform to managed files vs register compatible files without our transforms.
18. **Why store raw numbers as text?** Preserve malformed source for TRY_TO_* DQ and audit.
19. **dbt source vs ref?** External table dependency versus dbt model dependency.
20. **Why staging valid/rejected views?** Explicit quality gate and transparent reject reasons.
21. **Stream vs Task?** Change offset/access versus compute execution trigger.
22. **Why two streams?** Independent consumption offsets for current state and history.
23. **CDC business key vs event key?** TRADE_ID for state; EVENT_ID for event identity.
24. **Why SCD2 hash?** Detect tracked attribute changes, not identify business keys.
25. **If 403 rows ingest but 403 reject?** File service healthy; wrong settlement schema violates trade-event DQ contract.

## J. Five practice questions — answer aloud without notes

1. Walk me through the exact AWS IAM read-role registration handshake. Which ARN goes into AWS trust policy?
2. Why did we create a second IAM role for the Iceberg external volume? How did we diagnose sts:AssumeRole denied?
3. Why were 14 string Parquet fields mapped to a 17-column Iceberg table with FULL_INGEST?
4. A new file appears in Iceberg Bronze but not CURRENT_TRADES: identify the first three checks, in order.
5. A price update comes with SOURCE_SEQUENCE=2 after sequence=3 has already been applied. What do current Gold and physical SCD2 do?

## Source of truth and verification levels

- [GitHub AWS / Iceberg IAM runbook](https://github.com/mayur-nigade/investment-banking-data-platform/blob/main/docs/iceberg_aws_setup.md)
- [Actual scoped Iceberg AWS IAM policy](https://github.com/mayur-nigade/investment-banking-data-platform/blob/main/aws/iam/iceberg_bronze_s3_policy.json)
- [Actual Iceberg role trust template](https://github.com/mayur-nigade/investment-banking-data-platform/blob/main/aws/iam/iceberg_role_trust.template.json)
- [Iceberg creation SQL](https://github.com/mayur-nigade/investment-banking-data-platform/blob/main/sql/iceberg/01_external_volume_and_backfill.sql)
- [Snowpipe SQL](https://github.com/mayur-nigade/investment-banking-data-platform/blob/main/sql/snowpipe/09_s3_auto_ingest_to_iceberg.sql)
- [Snowpipe S3 AWS event runbook](https://github.com/mayur-nigade/investment-banking-data-platform/blob/main/docs/snowpipe_setup.md)
- [Gold CDC Task](https://github.com/mayur-nigade/investment-banking-data-platform/blob/main/sql/cdc/04_triggered_trade_cdc_task.sql)
- [SCD2 bootstrap](https://github.com/mayur-nigade/investment-banking-data-platform/blob/main/sql/scd2/07_physical_scd2_bootstrap.sql)
- [SCD2 incremental Task/SP](https://github.com/mayur-nigade/investment-banking-data-platform/blob/main/sql/scd2/08_incremental_physical_scd2_task.sql)
- [Official Snowflake S3 storage integration](https://docs.snowflake.com/en/user-guide/data-load-s3-config-storage-integration)
- [Official Snowflake Iceberg S3 external volume](https://docs.snowflake.com/en/user-guide/tables-iceberg-configure-external-volume-s3)
- [Official Snowpipe S3 auto ingest](https://docs.snowflake.com/en/user-guide/data-load-snowpipe-auto-s3)

**Evidence distinction:** 1704 historical backfill, 392 five-file load, 403 settlement DQ failure, original stream/current-Gold smoke test are confirmed from prior user outputs. Snowpipe execution and later physical SCD2 record appearance were **reported by the user**; exact final six-event target reconciliation and task-history screenshot have not been independently supplied. dbt files in GitHub are reconstructions, not authoritative byte-identical exports from the Snowflake Workspace.
