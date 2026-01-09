# Architecture Clarification: Bronze → Silver → Archive Flow

## Your Understanding (Mostly Correct!)

### Bronze Layer (Regular S3 Bucket)
- **Purpose:** Raw, immutable audit trail
- **Structure:** `bronze/daily/{agency}/{type}/{date}/violation.json`
- **Content:** Individual violation files (one per violation)
- **Why Regular S3:** 
  - Mutable (can delete old test files)
  - Cheap storage
  - No auto-compaction needed (just archive old files)
- **Lifecycle:** Files accumulate daily, then get archived/deleted after processing

### Silver Layer (S3 Tables/Iceberg)
- **Purpose:** Processed, queryable data
- **Current Implementation:** Writes directly to S3 Tables (Iceberg Parquet)
- **Problem:** No buffer files created!

### The Gap

**What Should Happen:**
1. **Bronze → Silver (Parquet Writer):**
   - Reads individual JSON files from Bronze
   - Normalizes and writes to **BOTH**:
     - S3 Tables (for immediate querying via DuckDB/Athena)
     - Regular S3 buffer files (for consolidation later)

2. **Buffer Files (Regular S3):**
   - **Path:** `silver/violations/buffer/ingest_date={date}/{type}-{uuid}.parquet`
   - **Purpose:** Hot data, accumulates daily
   - **Why Regular S3:** Consolidator needs to read/manage these files

3. **Monthly Consolidation:**
   - Consolidator runs on 1st of month
   - Reads all buffer files from previous month
   - Consolidates into: `silver/violations/archive/violation_year={year}/month={month}/data.parquet`
   - Deletes individual buffer files

4. **Yearly Consolidation (After 6-Month Grace):**
   - Runs in July (6 months after year-end)
   - Consolidates all monthly files into: `silver/violations/archive/violation_year={year}/data.parquet`
   - Files become immutable (historical data)

## Current Implementation Issue

**Problem:** Parquet writer only writes to S3 Tables, not buffer files!

**Code Location:** `VentureOS/utils/parquetWriter.js`
- Line 127: Checks if `outputPath` starts with `arn:aws:s3tables:`
- Line 154: Writes to S3 Tables
- Line 260: Has `else` branch for regular S3 files, but **never reached** because Lambda always passes S3 Table ARN

## Proposed Solution

### Option A: Dual Write (Recommended)
Write to **both** S3 Tables AND buffer files:

```javascript
// In parquetWriter.js, after writing to S3 Tables:
if (outputPath.startsWith('arn:aws:s3tables:')) {
  // Write to S3 Tables (for querying)
  await con.run(`INSERT INTO ${targetTable} SELECT * FROM violations`);
  
  // ALSO write buffer file (for consolidation)
  const bufferPath = `s3://${bucket}/silver/violations/buffer/ingest_date=${ingestDate}/${type}-${uuid}.parquet`;
  await con.run(`COPY violations TO '${bufferPath}' (FORMAT PARQUET, COMPRESSION SNAPPY)`);
}
```

**Pros:**
- S3 Tables for immediate querying (DuckDB/Athena)
- Buffer files for consolidation
- Consolidator works as designed

**Cons:**
- 2x storage cost (but buffer files get deleted after consolidation)
- Slightly slower writes

### Option B: Consolidator Works with S3 Tables
Modify consolidator to read from S3 Tables instead of regular S3 files.

**Pros:**
- Single source of truth
- No duplicate storage

**Cons:**
- More complex (need to query S3 Tables, not just list files)
- Can't easily partition by ingest_date (S3 Tables partitions by violation_year)

## Your Questions Answered

### Q: When would there be regular S3 files?
**A:** Buffer files in `silver/violations/buffer/` - these are regular S3 Parquet files that get consolidated monthly.

### Q: API feeds - batched or individual?
**A:** Current: Individual files. **Recommendation:** Use SQS for batching:
- API → SQS Queue → Normalizer Lambda (batches of 10-100)
- Reduces Lambda invocations
- Better error handling/retries

### Q: Monthly consolidation timing?
**A:** Yes, but with grace period:
- **Monthly:** Consolidate previous month on 1st (e.g., Feb 1 consolidates January)
- **Yearly:** Consolidate previous year in July (6-month grace for late filings)
- **Late violations:** If a January violation arrives in March, it goes to buffer, then gets included in yearly consolidation

### Q: 6-month grace period for monthly?
**A:** No - monthly consolidation happens immediately. The 6-month grace is only for **yearly** consolidation:
- January violations can arrive through June
- All get consolidated into `violation_year=2024` in July
- After July, 2024 data is immutable

## Recommended Architecture

```
Bronze (Regular S3):
├── daily/
│   ├── osha/severe_injury/2026-01-09/violation-001.json
│   └── osha/severe_injury/2026-01-09/violation-002.json
└── (files deleted after processing)

Silver - S3 Tables (Iceberg):
└── silver.violations (queryable immediately)

Silver - Buffer Files (Regular S3):
├── buffer/ingest_date=2026-01-09/osha-severe-abc123.parquet
└── buffer/ingest_date=2026-01-09/osha-severe-def456.parquet
    (consolidated monthly → archive)

Silver - Archive (Regular S3):
├── archive/violation_year=2024/month=01/data.parquet
├── archive/violation_year=2024/month=02/data.parquet
└── archive/violation_year=2024/data.parquet (yearly consolidation)
```

## Next Steps

1. **Fix parquetWriter:** Add dual-write (S3 Tables + buffer files)
2. **Test consolidator:** Should now find buffer files
3. **Add yearly consolidation:** Separate Lambda or extend consolidator
4. **Consider SQS:** For API feed batching

