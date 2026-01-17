# The Compactor 🔄

**Bronze → Silver Merge Operations**

> *"From many fragments, one truth emerges."*

## Overview

The Sluice produces many small Parquet files in staging (Bronze). The Compactor merges these into the unified Silver layer—an Iceberg table optimized for analytical queries.

## Architecture

```
Bronze (Staging)                              Silver (The Deep)
─────────────────                             ─────────────────
s3://confluence/staging/                      S3 Tables Bucket
  └── 2026/                      MERGE        venture-os-the-deep
       ├── batch_001.parquet    ═══════►        └── silver.events
       ├── batch_002.parquet                         (Iceberg)
       └── ...
```

## Components

| File | Purpose |
|------|---------|
| `setup-bronze.sql` | Creates external table over staging Parquets |
| `create-silver-table.sql` | DDL for the Iceberg events table |
| `merge-staging.sql` | The MERGE query |
| `setup-all.js` | Automated setup orchestrator |

## The Merge

The MERGE is idempotent—running it twice with the same data produces no duplicates:

```sql
MERGE INTO silver.events AS target
USING bronze.staging_events AS source
ON target.event_id = source.event_id
WHEN MATCHED THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT ...
```

**Key:** The deterministic `event_id` ensures same-row updates rather than duplicates.

## Athena Setup

The Silver table lives in an **S3 Tables** bucket, which provides managed Iceberg:

1. **Data Source:** `AwsDataCatalog`
2. **Catalog:** `s3tablescatalog/venture-os-the-deep`
3. **Database:** `silver`
4. **Table:** `events`

Bronze is a standard Glue external table:
- **Database:** `bronze`
- **Table:** `staging_events`
- **Location:** `s3://venture-os-confluence/staging/`

## Running the Merge

**Manual (Athena Console):**
```sql
-- Switch to silver database in S3 Tables catalog
MERGE INTO events ...
```

**Scheduled (Future):**
- Athena Scheduled Query: `cron(0 6 * * ? *)`
- Or EventBridge → Lambda trigger

## Post-Merge Cleanup

After a successful merge, staging files can be:
1. Deleted immediately
2. Moved to archive
3. Left for S3 Lifecycle rules (recommended)

---

*"The Compactor forges order from chaos."*
