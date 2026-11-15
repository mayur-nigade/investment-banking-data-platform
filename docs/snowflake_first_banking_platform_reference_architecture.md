# Snowflake-first Investment Banking Data Platform — Enterprise Reference Architecture

**Date:** 2026-11-15  
**Design classification:** PROPOSED modernization blueprint, NOT deployed as a whole.  
**Data/privacy:** All examples are synthetic. Based on generic enterprise migration patterns and our public synthetic Snowflake/Iceberg lab. Never interpret this as an internal or verified production deployment at any named bank.

## 0. Executive design choice

Retain SQL Server -> AWS DMS full-load/CDC -> S3; add a lightweight source-aware S3 event router (EventBridge/SQS/Lambda) to identify and register every source object; keep **Snowflake CONTROL tables** as durable file manifest and source-watermark control plane; publish only validated-source objects to domain-specific ready prefixes; use **Snowpipe** to load Snowflake-managed **Iceberg Bronze**; apply **dbt** typed/DQ/canonical Silver models; use two independent Snowflake Streams + Tasks to publish **Gold current-state** and **Gold physical SCD2 history**, with Snowflake Gold serving investment-bank analytics. Exclude Glue entirely. No mandatory DynamoDB. Optional Salesforce -> AppFlow -> S3 reference-data branch.

Historical output in the reference-style migration design was Redshift. In this modernized design, Snowflake replaces Redshift as the authoritative Gold warehouse; Redshift is no longer required. Existing analytical consumption can be migrated or given a separate explicit interface if necessary.

### Flow

~~~text
SQL Server Trades / Accounts / Settlements
       -> AWS DMS full load + ongoing transaction-log CDC
       -> S3 /landingzone/raw/dms/{domain}/   [immutable, versioned]
Salesforce Account / reference data
       -> AWS AppFlow
       -> S3 /landingzone/raw/appflow/{domain}/
       -> [S3 object-created -> EventBridge -> SQS -> Lambda FILE ROUTER]
       -> Lambda: decode object key, HEAD/validate metadata, schema contract,
                  extract source/domain, register file in Snowflake CONTROL
       -> S3 /landingzone/ready/{domain}/  [deterministic key, only approved files]
       -> S3 object-created -> Snowflake-managed SQS -> domain Snowpipe
       -> BRONZE.{domain}_EVENTS_ICEBERG (one per domain, immutable append log)
                    |                      |
             Snowflake CONTROL          dbt Silver
             FILE_MANIFEST             stg_trade_events, stg_clients,
             PIPE_RUN_AUDIT            canonical_event, valid, rejected
             SOURCE_WATERMARK                  |
                    |               Silver published tables / quality gates
                    |                      |
                    +----------------------+-----------------+
                    |                                        |
               Current state                            Physical history
              independent stream                       independent stream
                    |                                        |
             Task + CDC MERGE                       Task + stored procedure
                    |                                        |
               GOLD.FCT_TRADE_CURRENT                 GOLD.TRADE_SCD2_HISTORY
                      \                                   /
                       -> positions / P&L / settlement / risk marts
                                   |
                       reconciliation + SLA + file status SUCCEEDED
                                   |
                         monitoring / notifications / audit
~~~

Important distinction: one raw S3 object-created notification is consumed by a **router**, and a *second* notification on the distinct READY prefix is consumed by **Snowpipe**. This prevents Lambda and Snowpipe from independently loading the same source object and avoids sending unknown schemas into a trade-events pipe.

## 1. Comparison: reference migration guide vs verified lab vs proposed design

| Responsibility | Reference migration-guide approach | Hands-on lab actually built / observed | Proposed conversion |
| --- | --- | --- | --- |
| Source extraction | SQL Server via DMS full load + CDC, Salesforce via AppFlow | Synthetic files uploaded to S3 (NOT live DMS/AppFlow) | Keep DMS/AppFlow, implement/test source endpoints |
| S3 landing | Files from DMS and AppFlow | Private bucket omera-ib-trade-lake, landingzone | Split immutable raw by source + business domain |
| File arrival/orchestration | EventBridge, Step Functions, Lambda | S3 event notification -> Snowflake-managed queue/Snowpipe, Lambda not deployed | EventBridge -> SQS -> minimal Lambda file router; Snowflake Tasks downstream |
| Transformation | AWS Glue | dbt Snowflake Workspaces models, TRY_TO_* DQ | dbt Silver models, no Glue |
| Analytical target | Amazon Redshift | Snowflake Gold CURRENT_TRADES + physical SCD2 user-reported | Snowflake as Gold fact/dim/positions/P&L |
| Raw storage | S3 Parquet | Native RAW 1704; managed Iceberg 1704 + new 392 batch | Snowflake-managed Iceberg Bronze, separate domain tables |
| CDC apply | Redshift staged DML/upserts | Append-only Iceberg stream + Task Gold MERGE smoke tested | Producer-order-safe canonical Silver events and Gold MERGE |
| History | Source-specific warehouse CDC logic | Physical SCD2 procedure/table later confirmed by user; exact task history not posted | Independent stream + persisted SCD2, late events handled |
| File ledger / watermark | DynamoDB operational/reconciliation run log | No durable file ledger yet | Snowflake CONTROL.FILE_MANIFEST + SOURCE_CDC_WATERMARK |
| Observability | CloudWatch, SNS, CloudTrail | Snowflake pipe/task and DQ diagnostics | Hybrid CloudWatch/CloudTrail + Snowflake ingestion/DQ/recon audits |
| Domain contract | Source-specific staging | One settlement Parquet file misrouted, all 403 rejected INVALID_QUANTITY | Domain prefix allowlist, schema contract before READY |

**Proof standard:** The source documents are implementation guides/design descriptions, NOT independent evidence of a deployed bank production pipeline. The synthetic lab verifies selected functionality, not the full migration reference architecture.

## 2. Layer 0: source systems and SQL Server DMS

**Source SQL Server:** OLTP stores authoritative Trade, Account, Client reference, Settlement and Position changes; source primary keys and commit order must be specified per table. CDC is the ordered record of Inserts, Updates and Deletes. It is not the same thing as periodic SELECT of rows where ModifiedDate changed.

**AWS DMS** runs migration tasks. Components: DMS replication instance/serverless replication (deployment choice), networking/VPC/subnet/security group, SQL Server source endpoint, S3 target endpoint, and task mode **full-load-and-CDC**. The service reads SQL Server transaction-log changes using MS-Replication/MS-CDC mechanisms; operation code I/U/D is carried in S3 output according to endpoint settings. The source needs supported recovery/backup configuration, sufficient log retention, a correctly privileged replication login, network connectivity and monitoring. Avoid heavy CDC SQL reads from the source application's production transaction path.

**Target S3 settings:** Parquet, compression/partition layout, source/table-specific path and optional commit-timestamp field (TimestampColumnName) selected and contract-tested. On S3, DMS Parquet does not always preserve original cross-table transaction order. Never assume an S3 filename or object-created timestamp equals an SQL Server LSN. If source LSN/transaction ordering is required, explicitly capture supported metadata/transform rules and test it with I/U/D scenarios.

**Full load vs CDC:** Full load establishes a baseline; CDC applies subsequent changes. Define a consistent handover boundary and reconciliation checkpoint to avoid gaps or double-application. D events may provide only key columns, so our lab's requirement for all fields on DELETE must be modified before a real DMS pipeline is accepted.

**Synthetic example:** Source trade T100: I sequence=1 price=101.20, U sequence=2 price=103.10, D sequence=3. The current business state becomes deleted tombstone, while the event log and SCD2 history retain preceding states. For real DMS, source sequence is an engineered normalization based on producer ordering (not automatically the synthetic SOURCE_SEQUENCE field).

**Optional Salesforce branch:** AppFlow extracts Account/Client reference data into a separate source-specific S3 prefix, with its own schema, load schedule, DQ SLA and natural keys. Salesforce is optional for the first trade CDC vertical slice; it must not share a generic trade-event parser.

**Not yet implemented in our lab:** real SQL Server endpoint, DMS replication task, AppFlow connector, source LSN capture or CDC handover.

## 3. Layer 1: AWS S3 landing and security

**Raw zone:** S3 /landingzone/raw/dms/trades/, /landingzone/raw/dms/settlements/, /landingzone/raw/appflow/clients/. Objects are immutable from the pipeline's perspective; bucket versioning ON and object-key/version metadata retained. Restrict bucket public access; use SSE-KMS if required with matching IAM + key policy. Apply lifecycle/retention according to compliance; do not allow file-upload Lambda to overwrite a source object with the same key silently.

**Ready zone:** /landingzone/ready/trades/ and /landingzone/ready/settlements/ are separate approved-file prefixes. Every ready object must correspond to an auditable source manifest entry. Write a deterministic ready path including domain/source and stable file identity so retries do not generate new filenames; avoid self-trigger loops by filtering raw vs ready.

**Separate Iceberg storage:** /iceberg/bronze/ is not source landing or a Snowpipe notification target. Only the Snowflake-managed Iceberg external-volume role writes its managed data and metadata.

**IAM roles:**
- DMS writer role: scoped S3 PutObject/KMS access to raw DMS output; DMS service trust.
- AppFlow role/connector permissions: scoped S3 output + relevant source access.
- Lambda file-router execution role: S3 HeadObject/GetObject/Copy to ready, KMS, CloudWatch logs, secret retrieval for restricted Snowflake API service identity; no blanket S3 admin privileges.
- Snowflake external-stage reader role: **OMERA_SNOWFLAKE_S3_ROLE** in lab, and integration **OMERA_S3_INT**; Snowflake reads ready source files.
- Iceberg external-volume writer role: **OMERA_SNOWFLAKE_ICEBERG_ROLE**, external volume **OMERA_IB_ICEBERG_VOL**, S3 read/write/list/delete on Iceberg-managed prefix.
- For Snowflake-owned AWS role trust: **DESC INTEGRATION** or **DESC EXTERNAL VOLUME** -> copy Snowflake-generated IAM principal and External ID to AWS IAM Trust Relationship; STS AssumeRole requires both trust and permissions. No permanent AWS access keys in Snowflake SQL.
- Data platform Snowflake roles: separate MONITOR/OPERATE, ingestion INSERT, dbt SELECT/CREATE/WRITE, Gold DML and security-policy administration. Scope data access with RBAC and where appropriate masking/row-access policies.

**Why two Snowflake S3 identities?** A storage integration lets an external stage read source files; an Iceberg external volume enables Snowflake to own and mutate Iceberg data/manifests on S3. Different trust values and permissions. Our original lab encountered and corrected sts:AssumeRole trust misconfiguration before successfully verifying read/write/list/delete on the external volume.

## 4. Layer 2: S3 event -> EventBridge/SQS -> Lambda router

**Trigger:** S3 ObjectCreated event for only /landingzone/raw/ -> EventBridge rule filtered by bucket and prefix -> SQS queue -> Lambda event-source mapping. SQS decouples bursty DMS file output and allows retry/DLQ; Lambda receives batches, and failures must not acknowledge unprocessed messages. For simple low-rate sources, direct EventBridge -> Lambda is possible but loses the SQS buffering advantage.

**Object metadata from event:** S3 bucket, URL-decoded object key (event shape-dependent), versionId if available, eTag, object size, event time, source domain derived from strict prefix allowlist. Recheck S3 HEAD rather than trusting an event. Source-file identity: (bucket, object_key, version_id) if versioned; otherwise a carefully defined etag/size or content checksum fingerprint. ETag is not reliably a payload MD5 for multipart/SSE-KMS objects, so treat it as object-version metadata, not a universal content hash. S3 ObjectCreated notifications are at-least-once and may be out of order.

**Lambda responsibilities, in this order:**
1. Decode event, reject unknown source prefix/extension/domain and invalid/zero-size object.
2. Register identity, source, received timestamp, size/ETag/version and run/correlation ID in Snowflake CONTROL.FILE_MANIFEST.
3. Optionally inspect Parquet footer/schema cheaply or rely on source-specific schema-contract validation at Snowflake stage. HEAD alone cannot detect a settlement file masquerading as trade event (we observed 403 such rows rejected).
4. Mark RECEIVED/VALIDATED only after evidence; copy/route to deterministic, domain-specific READY key. If copy fails, mark FAILED or leave retryable and preserve original object.
5. Record READY key and PUBLISHED status. **Do not mark SUCCEEDED yet.** Successful Lambda invocation only means accepted for ingestion, not loaded/validated/published into Gold.
6. On retry, look up immutable file identity; return already-accepted / reconcile stalled states rather than publish a new filename.

**Concurrency:** Standard Snowflake table PK/UNIQUE constraints are informational. For the POC, serialize FILE_MANIFEST registration by one SQS-consumer concurrency lane and reconcile duplicate ObjectCreated messages, plus canonical event dedupe after Bronze. For high-throughput multiple Lambda workers that require atomic conditional ownership, **DynamoDB conditional PutItem** or a properly designed enforced-key store can add strong file-claim semantics; optional, not the default. SQS FIFO dedup alone is not a permanent file-idempotency ledger.

**Don't double-ingest:** Lambda does NOT manually execute COPY INTO a file that will also be loaded by Snowpipe; it only routes approved objects and writes manifest metadata.

## 5. Layer 3: Snowflake CONTROL schema — file ledger, audit and watermarks

**Snowflake chosen over DynamoDB for this lab extension** because file status, COPY_HISTORY, dbt DQ, task audits and reconciliation are all queryable/joinable in one data platform. This is a **proposed extension**; it has not been implemented in the current lab.

### 5.1 File manifest (one state machine per immutable object)

Recommended columns:
- FILE_ID deterministic digest of source_system + bucket + raw_object_key + object_version_id; SOURCE_SYSTEM, DOMAIN, S3_BUCKET, RAW_OBJECT_KEY, OBJECT_VERSION_ID, ETAG, OBJECT_BYTES
- READY_OBJECT_KEY, FILE_FORMAT, SOURCE_SCHEMA_VERSION, CORRELATION_ID, FIRST_SEEN_AT, LAST_UPDATED_AT
- STATUS, ATTEMPT_NO, ERROR_CODE, ERROR_MESSAGE, LAST_RETRY_AT
- SNOWPIPE_PIPE, BRONZE_TARGET, BRONZE_ROW_COUNT, SILVER_VALID_ROW_COUNT, SILVER_REJECT_ROW_COUNT
- GOLD_APPLIED_ROW_COUNT, RECON_STATUS, CHECKSUM_OR_SOURCE_HIGH_LSN, CREATED_AT, COMPLETED_AT

State transitions:
**RECEIVED -> VALIDATED -> PUBLISHED_TO_READY -> BRONZE_LOADED -> SILVER_VALIDATED -> GOLD_APPLIED -> RECONCILED -> SUCCEEDED**;
failure branches: **QUARANTINED**, **FAILED_RETRYABLE**, **FAILED_PERMANENT**. A file with DQ rejects can have status **PARTIAL/QUARANTINED** per business contract; don't automatically treat parser-load success as data success.

**Example Snowflake schema (design sketch, NOT production-ready exactly-once implementation):**

~~~sql
CREATE SCHEMA IF NOT EXISTS IB_LAB.CONTROL;

CREATE TABLE IF NOT EXISTS IB_LAB.CONTROL.FILE_MANIFEST (
    FILE_ID VARCHAR,
    SOURCE_SYSTEM VARCHAR,
    DOMAIN VARCHAR,
    S3_BUCKET VARCHAR,
    RAW_OBJECT_KEY VARCHAR,
    OBJECT_VERSION_ID VARCHAR,
    ETAG VARCHAR,
    OBJECT_BYTES NUMBER,
    READY_OBJECT_KEY VARCHAR,
    PIPE_NAME VARCHAR,
    STATUS VARCHAR,
    ATTEMPT_NO NUMBER DEFAULT 0,
    BRONZE_ROW_COUNT NUMBER,
    SILVER_VALID_ROW_COUNT NUMBER,
    SILVER_REJECT_ROW_COUNT NUMBER,
    LAST_ERROR VARCHAR,
    FIRST_SEEN_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    LAST_UPDATED_AT TIMESTAMP_LTZ,
    COMPLETED_AT TIMESTAMP_LTZ
);

CREATE TABLE IF NOT EXISTS IB_LAB.CONTROL.SOURCE_CDC_WATERMARK (
    SOURCE_SYSTEM VARCHAR,
    SOURCE_ENTITY VARCHAR,
    LAST_CONTIGUOUS_COMMIT_POSITION VARCHAR,
    LAST_COMMIT_TS TIMESTAMP_NTZ,
    LAST_SUCCEEDED_RUN_ID VARCHAR,
    UPDATED_AT TIMESTAMP_LTZ
);

CREATE TABLE IF NOT EXISTS IB_LAB.CONTROL.PIPELINE_RUN_AUDIT (
    RUN_ID VARCHAR,
    FILE_ID VARCHAR,
    STEP_NAME VARCHAR,
    STEP_STATUS VARCHAR,
    STARTED_AT TIMESTAMP_LTZ,
    ENDED_AT TIMESTAMP_LTZ,
    ROWS_READ NUMBER,
    ROWS_WRITTEN NUMBER,
    ERROR_TEXT VARCHAR
);
~~~

**Do not interpret a declared FILE_ID PK/UNIQUE as enforced on standard Snowflake tables.** This example intentionally leaves that out. Implement serialized idempotent registration or explicit concurrency control, atomic status transitions and monitoring before production.

### 5.2 Three distinct watermarks

1. **File checkpoint:** which object key + version has been seen/loaded/validated. Used for reprocessing exact files and partial failures; file-arrival order is not CDC business order.
2. **Source CDC checkpoint:** SQL Server LSN / an appropriate per-source ordered commit position. Advance only through **contiguously and successfully committed** changes, after required Gold and reconciliation steps; gaps/late files must not be skipped. An event timestamp alone may be insufficient to uniquely order transactions.
3. **Gold business version:** SOURCE_SEQUENCE or derived source commit order per TRADE_ID; prevents stale CDC updates overwriting newer current state. Physical SCD2 uses this for historical intervals.

**Definition of success:** file SUCCEEDED only when Bronze load, business validation, the required Gold publications and reconciliation meet the declared SLA. For a failed record, preserve source file identity and DQ detail; never advance a global source watermark past an unresolved gap.

### 5.3 Reconciling Snowpipe -> manifest

- Query **IB_LAB.INFORMATION_SCHEMA.COPY_HISTORY** for recent file statuses (14-day window), and **SNOWFLAKE.ACCOUNT_USAGE.COPY_HISTORY** for longer historical investigations (subject to metadata latency).
- Map **FILE_NAME/SOURCE_FILE** to READY_OBJECT_KEY. Snowpipe cannot atomically commit its copy and an arbitrary Lambda-created Snowflake control-row update in one transaction. Build a **reconciler task** to reflect actual load outcomes and detect stuck PUBLISHED/LOADING states.
- Keep a durable manifest for audits beyond Snowpipe's own retention. Replaying based only on a pipe's history is not enterprise idempotency.
- Use Snowflake task history/query history, SCD2 audit, DQ reasons and event identity for later statuses.

## 6. Layer 4: Snowpipe ingestion

**Pipe** contains the file COPY statement; **Snowpipe** is the managed serverless ingestion service. On **/landingzone/ready/trades/** S3 ObjectCreated, S3 notifies Snowflake-managed SQS (use direct notification for nonoverlapping path or SNS for existing overlapping consumers), and the corresponding pipe loads that exact source-domain file into the domain Bronze table.

**Use separate pipes per physical schema/domain**: trades, settlements and client reference have different fields and validation rules. Our actual lab mistake is the best proof: **trade_settlements_001.parquet** contained eight settlement keys; the trade pipe expected 14 trade fields, and 403 Bronze rows were all rejected for INVALID_QUANTITY. Production design must fail fast on contract mismatch rather than simply inserting NULLs.

**COPY mapping:** Our existing trade lab **TRADE_EVENTS_AUTO_PIPE** maps 14 lowercase Parquet fields plus METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, METADATA$START_SCAN_TIME::TIMESTAMP_LTZ(6) to 17 Bronze columns, **LOAD_MODE=FULL_INGEST**, **ON_ERROR=SKIP_FILE**. SQL Server DMS output will have different source columns/operation names; do not point its raw Parquet directly at this lab pipe without normalization or a separate source-specific COPY.

**Correctness:**
- Snowpipe file load metadata is *file-level*, not stable EVENT_ID deduplication across filenames.
- **Snowpipe doesn't perform the dbt model build or Gold MERGE**; downstream triggers/schedules are separate.
- Snowpipe supports Snowflake-managed Iceberg tables; no separate Glue ETL needed.
- **SYSTEM$PIPE_STATUS**, **DESC PIPE**, **COPY_HISTORY** give queue, queue ARN and loader errors.
- File upload into READY is not a reason to run manual COPY. That risks double loaders.

## 7. Layer 5: Bronze — Snowflake-managed Apache Iceberg

**What physically exists:** S3 Parquet data plus Iceberg table metadata/manifests/snapshots, read by Snowflake SQL through its catalog. A pile of Parquet objects under /landingzone/ is not an Iceberg table until an Iceberg-aware engine commits metadata/snapshot.

**Our lab:** **IB_LAB.BRONZE.RAW_TRADE_EVENTS_ICEBERG**, Snowflake catalog, external volume **OMERA_IB_ICEBERG_VOL**, base location under /iceberg/bronze/; 17 source-and-audit columns. Historical native RAW 1704 -> Iceberg 1704 reconciled, then five new Parquet files with 392 rows loaded. **INGESTED_AT TIMESTAMP_LTZ(9)** originally failed Iceberg v2 CTAS; projected cast to **TIMESTAMP_LTZ(6)** resolved it.

**Proposed expanded domains:** BRONZE.DMS_TRADE_EVENTS_ICEBERG, BRONZE.DMS_SETTLEMENTS_ICEBERG, BRONZE.APPFLOW_CLIENTS_ICEBERG, or reuse the lab's trade table only after explicit schema-adapter normalization. Keep all source operations as append-only records, source schema version, raw and ready file IDs/paths, source commit metadata and ingest timestamps.

**Iceberg vs native Snowflake table:** Iceberg provides open Parquet/snapshot metadata in external S3, compatible with selected engines/catalog patterns; native Snowflake tables provide simpler Snowflake-native governance/maintenance. Use Iceberg where open storage is an intentional requirement, not because every stage must be Iceberg. Proposed Gold can remain native Snowflake.

**FULL_INGEST** transforms data into Iceberg-managed storage and metadata; **ADD_FILES_COPY** can register suitable files but does not support our metadata+column transformations. No casual modification of Iceberg-managed S3 files outside a catalog-aware writer.

## 8. Layer 6: dbt Silver — typed validation, canonical CDC

**dbt's job** is to express and test transformation DAGs in Snowflake SQL. It does not move source S3 files by itself.

**Existing lab:**
- **STG_TRADE_EVENTS** (Iceberg Bronze source) normalizes text fields with TRY_TO_NUMBER/DECIMAL/DATE/TIMESTAMP and produces DQ_STATUS + DQ_REASON.
- **STG_CLIENTS** reads native RAW_CLIENTS and parses onboarding dates.
- **INT_VALID_TRADES** retains records passing all configured rules.
- **INT_REJECTED_TRADES** surfaces invalid records with SOURCE_FILE and REJECTION_REASON.
- In historical lab baseline, 1704 staging = 1696 valid + 8 rejected, with 7 tests passing. GitHub dbt files are reconstructed from the original Snowflake Workspace and not a byte-identical verified live export.

**Proposed true Silver:**
- SILVER.STG_DMS_TRADES (DMS op mapping + source commit metadata, still source-preserving).
- SILVER.STG_APPFLOW_CLIENTS (reference sources).
- SILVER.INT_TRADE_EVENT_VALID / INT_TRADE_EVENT_REJECTED (field contract, positive quantity, known client; D events get operation-aware validation because deleted rows may carry keys only).
- SILVER.CANONICAL_TRADE_EVENTS (one accepted source business event per source-system/event-id, rejects duplicate/conflicting source identities; ordering from producer commit position). Incremental physical models with MERGE to persist canonical event identity, not merely views.
- SILVER.CANONICAL_CLIENT, SILVER.CANONICAL_SETTLEMENTS, with source-specific SCD and reconciliation contracts.

**Quality gates:** bronze loaded rows = valid + rejected + documented ignored/replayed rows. Source client validity and event time must be evaluated under explicit source snapshots. Duplicate event-id with differing payload is a conflict and requires audit. The 403-settlement incident is a business/domain DQ failure, not a Snowpipe parser failure.

**dbt orchestration:** For continuous low-latency access, a dbt VIEW gives query-time logic but **does not persist a clean Silver table**. In production, deploy dbt incremental TABLE models and run them on a schedule/task as SLA requires. Snowflake supports **EXECUTE DBT PROJECT** from a user-managed-warehouse Task when the dbt project is deployed as a dbt project object; this is a new part of the proposed design, not something our old lab CDC Task automatically performs. Or, if low-latency incremental refresh is needed, choose Snowflake Dynamic Tables or task-driven SQL with dbt maintaining SQL models. Do not claim simply pointing a stream at Bronze executes a dbt job.

## 9. Layer 7: Gold — Snowflake native current state, SCD2 and banking marts

**Gold.current_trade** — one latest source-valid state per TRADE_ID. A Stream on the persisted canonical Silver event table and triggered Task use ordered MERGE to apply I/U/D; monotonic source commit sequence prevents old files overwriting newer state. Deletion is represented by a tombstone **IS_DELETED**. In the existing lab, **GOLD.CURRENT_TRADES** reads a separate stream directly from Iceberg and validates again; the proposed canonical-Silver handoff is an upgrade, not already wired.

**Gold.trade_scd2_history** — all *changed business-state* versions for each trade; VALID_FROM/TO sequence/time intervals, VERSION_NO, IS_CURRENT and IS_DELETED. Distinguish *event history* from physical SCD2 change history. Detect attribute change with SHA-256/column compare; source ordering remains authoritative. Late-arriving sequence 2 after already receiving 1 and 3 requires recomputing affected TRADE_ID chain or an equivalent interval-splitting algorithm. Independent stream consumer offset prevents CDC current and history tasks from interfering.

**Existing lab:** **GOLD.SP_APPLY_TRADE_SCD2** reads its independent Iceberg stream and for touched valid trade IDs atomically DELETEs/INSERTs the desired chain with audits. User later confirmed SCD2 history records appeared after an initial zero-result query. Specific latest six-event row/version counts and full Task history not pasted; mark user-reported not separately audited.

**Gold banking outputs (proposed):**
- FCT_TRADE_CURRENT: one business key per trade, flags/status and last sequence.
- FCT_TRADE_EVENT_AUDIT: immutable normalized event history for lineage.
- FCT_TRADE_SETTLEMENT: settlement_id, trade_id, amount/currency/date/custodian/status, joined to valid trade reference.
- FCT_POSITION_DAILY: account/instrument/date net quantity; derive from business state and correct BUY/SELL and reversals.
- FCT_PNL_DAILY: realized/unrealized P&L; require trusted marks/pricing, FX and valuation contracts before publishing.
- DIM_CLIENT_SCD2 / ACCOUNT_MASTER: reference data historization and regulatory reporting joins.
- MART_RECONCILIATION: run/file/source/check totals and break categories.

**Important:** P&L, full settlement branch, external consumer entitlement/governance are **not implemented** in our current lab. Gold must be purpose-built, not just a copy of Silver.

## 10. Layer 8: Orchestration — operational flows

**AWS side:** DMS/AppFlow -> immutable S3 raw -> EventBridge/SQS -> Lambda, which registers/routes **files**. AWS CloudWatch monitors Lambda/DMS/SQS retries and DLQ; CloudTrail audits AWS control-plane activity. Secrets Manager holds external credentials or tokens; Snowflake key-pair/OAuth service authentication should be restricted, rotated and never committed to GitHub.

**Snowflake side:** S3 READY -> Snowpipe -> Bronze -> scheduled/triggered Silver refresh -> canonical stream -> Gold current MERGE and SCD2 procedure -> reconciliation -> manifest state SUCCEEDED. Configure task dependencies so Gold doesn't consume dirty data while Silver incremental run is in-flight. A triggered stream task can consume a persisted Silver table's committed delta; scheduling of deployed dbt project execution is separate and uses a warehouse.

**Avoid two independent writers to Gold on the same source:** production must have one known ordering policy and target writer per business key to avoid concurrency/duplicate processing. Keep stream offsets separate.

**Stop/Start and recovery:** raw files never delete during transient failures; a failed Lambda can retry from SQS; a failed Snowpipe load remains observable with COPY_HISTORY; failed dbt build is retryable with deterministic canonical keys; a failed Gold procedure transaction rolls back target DML + stream offset; a file cannot become SUCCEEDED until all required downstream checks complete.

## 11. Layer 9: reconciliation, financial correctness, observability

**Per-file:** raw/ready identity, expected schema version, file bytes/row counts, Snowpipe loaded/parsed/rejected rows, Silver valid/reject/conflict, Gold applied keys. For CDC do not compare raw event count with *current trade row count*: several I/U/D records may map to one latest trade.

**Source-to-Gold financial reconciliation:** compare source SQL Server trade snapshot at a **known commit checkpoint** to target latest active trades at the same checkpoint. Check record hash/keys, total quantity, sum(quantity*price) by currency (no cross-currency sum without FX), active/deleted counts, settlement amount vs trade notional by settlement lifecycle, client/portfolio ownership and late-arrival lag. Keep tolerances, break reason and escalation owner.

**Control reconciliation:** FILE_MANIFEST statuses vs Snowpipe COPY_HISTORY vs Bronze SOURCE_FILE vs DQ_REASON vs Gold run/task audits. Alert if PUBLISHED but no loaded confirmation, or loaded but no Silver/Gold progress after SLA. Only advance source committed watermark after contiguous, reconciled success for that entity. Maintain a separate load/run audit so investigation does not require re-running the entire dataset.

**Security:** segregate operational/raw KMS keys and least-privilege roles; mask client sensitive attributes; access/audit logging, network policies, object versioning, backup/DR, retention and schema contract versioning.

## 12. Suggested phased implementation from our current lab

**Already exercised:** S3/reader integration, Iceberg writer external volume, 1704 historical copy, five-file 392 Parquet batch, dbt valid/reject views and tests, Snowpipe auto-prefix file arrival, independent Bronze Streams, triggered current-state MERGE, user-confirmed physical SCD2 record.

**Not yet built (do NOT label complete):**
1. Source SQL Server->DMS CDC and real producer commit/LSN ordering, Salesforce AppFlow connector.
2. Separate per-domain raw/ready S3 prefixes and 3 corresponding Snowpipes/contracts. Fix original settlement misrouting by design.
3. EventBridge/SQS/Lambda file router with idempotent file-registration/retry and S3 ready publish.
4. Snowflake CONTROL.FILE_MANIFEST, PIPELINE_RUN_AUDIT, SOURCE_CDC_WATERMARK and periodic load-status reconciliation.
5. Persisted Silver canonical/deduplicated CDC tables; modernize existing direct Iceberg-to-Gold task to read canonical persisted Silver stream.
6. dbt scheduling/materialization freshness SLA and production reconciliation.
7. Gold settlement/positions/P&L, KMS/RBAC/data masking, alerts, late-arrival/backup tests.

**Minimum next demonstrable slice:** A *new* synthetic DMS-like trade file lands in raw/trades, Lambda extracts object_key and registers Snowflake file identity, routes to ready/trades, Snowpipe loads Iceberg, dbt canonicalizes with source op + ordering, Gold current & physical SCD2 update and manifest transitions to RECONCILED. A second delivery of identical raw key/version must not generate a new ready path or second canonical event. No real bank source data needed.

## 13. Interview challenges and precise answers

1. **Why not DynamoDB?** Snowflake CONTROL joins easily to native COPY_HISTORY/DQ/Gold audits; if concurrent atomic conditional file claims become critical, DynamoDB or enforced-key control is a valid optional addition. Standard Snowflake PK isn't enforced.
2. **Why Lambda if Snowpipe already auto-loads?** Lambda is needed only for file *domain classification, metadata registration, preflight contract and routing*; Snowpipe is the bulk file reader/loader. Lambda must not duplicate ingestion.
3. **Why not simply call the S3 file received a success?** File arrival is not ingestion, DQ, Gold or source reconciliation. Status transitions reflect verified downstream evidence.
4. **Can we use DMS files unchanged in our 14-field lab pipe?** Not reliably; DMS op, source metadata and field image differ. Add source-specific Bronze mapping + adapter.
5. **What's the difference between filename and watermark?** File key/version tracks an S3 object; source CDC LSN orders database changes; Gold trade sequence orders state versions. These are not interchangeable.
6. **How do you preserve changes arriving out of S3 order?** Source commit sequence/LSN and canonical event ordering, with explicit conflict and gap handling.
7. **Why two Streams?** Each consumer has its own transactional offset for latest Gold vs SCD2; Silver refresh introduces its own committed table and stream.
8. **What does Glue add here?** Nothing essential for our targeted SQL-centric design; dbt, Snowpipe, Streams/Tasks, Snowflake SQL replace its transformation orchestration.
9. **Why replace Redshift Gold?** Single Snowflake governance/compute/query/control plane with simpler lineage and SQL task integration; benchmark pricing/features and preserve external feeds if contractual consumers remain.
10. **Why did 403 records load but all fail?** Settlement file had no quantity and was routed to trade pipe; technically valid Parquet does not satisfy business schema contract.

## 14. References and provenance

Our public lab sources:
- [GitHub synthetic repo](https://github.com/mayur-nigade/investment-banking-data-platform)
- [Snowpipe pipe SQL](https://github.com/mayur-nigade/investment-banking-data-platform/blob/main/sql/snowpipe/09_s3_auto_ingest_to_iceberg.sql)
- [SCD2 implementation SQL](https://github.com/mayur-nigade/investment-banking-data-platform/blob/main/sql/scd2/08_incremental_physical_scd2_task.sql)
- [Iceberg AWS/IAM integration](https://github.com/mayur-nigade/investment-banking-data-platform/blob/main/docs/iceberg_aws_setup.md)
- [Full lab study revision](https://github.com/mayur-nigade/investment-banking-data-platform/blob/main/docs/full_pipeline_revision_aws_to_scd2.md)

Public vendor references:
- [AWS DMS SQL Server source CDC](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Source.SQLServer.html)
- [AWS DMS S3 target](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Target.S3.html)
- [AWS S3 event delivery](https://docs.aws.amazon.com/AmazonS3/latest/userguide/EventNotifications.html)
- [Snowpipe S3 auto ingest](https://docs.snowflake.com/en/user-guide/data-load-snowpipe-auto-s3)
- [Snowflake Iceberg load](https://docs.snowflake.com/en/user-guide/tables-iceberg-load)
- [Snowflake constraints](https://docs.snowflake.com/en/sql-reference/constraints)
- [Snowflake dbt tasks](https://docs.snowflake.com/en/user-guide/data-engineering/dbt-projects-on-snowflake-schedule-project-execution)
- [Snowflake COPY_HISTORY](https://docs.snowflake.com/en/sql-reference/functions/copy_history)

**Source separation:** This is a new independent architecture proposal informed by generic requirements in user-supplied migration/implementation documents and by our own synthetic lab. It does NOT reproduce internal client documents or claim actual deployment at a bank. The design and lab-evidence status are kept separate throughout.
