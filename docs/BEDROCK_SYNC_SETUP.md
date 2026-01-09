# Bedrock Sync Lambda Setup

## Overview

Real-time sync of Bedrock content from DynamoDB overlays to S3 Tables using DynamoDB Streams.

## Architecture

```
DynamoDB Stream (VentureOS-Violations)
  ↓ INSERT/MODIFY events
Lambda (ventureos-bedrock-sync)
  ↓ Processes batch
Temp Table (in-memory)
  ↓ MERGE
S3 Tables (ocean.silver.violations)
```

## Configuration Checklist

### ✅ Lambda Configuration

**Batching Settings (AWS Console):**
- **Batch Size:** 100 records
- **Batching Window:** 300 seconds (5 minutes)
- **Maximum Batching Window:** 300 seconds

**Why:** Processes records in batches, reducing Lambda invocations and costs.

**Environment Variables:**
- `S3_TABLE_BUCKET_ARN`: `arn:aws:s3tables:us-east-1:611538926352:bucket/sirsluginston-ventureos-data-ocean`
  - ⚠️ Must be FULL ARN, not just bucket name!

**Lambda Settings:**
- **Runtime:** `nodejs24.x`
- **Memory:** 1024 MB
- **Timeout:** 60 seconds
- **Ephemeral Storage:** 1024 MB
- **Layer:** `arn:aws:lambda:us-east-1:041475135427:layer:duckdb-nodejs-x86:21`

### ✅ DynamoDB Stream

**Table:** `VentureOS-Violations`
**Stream View Type:** `NEW_AND_OLD_IMAGES`
**Status:** Enabled ✅

**Stream ARN:** `arn:aws:dynamodb:us-east-1:611538926352:table/VentureOS-Violations/stream/2026-01-09T03:39:10.474`

### ✅ IAM Permissions

**Role:** `ventureos-bedrock-sync-role`

**Policies:**
- `bedrock-sync-policy` (custom)
  - DynamoDB Streams read
  - S3 Tables write (`s3tables:WriteData`, `s3tables:PutData`)
  - S3 PutObject (for S3 Tables bucket)
- `AWSLambdaBasicExecutionRole` (AWS managed)

### ✅ Event Source Mapping

**Trigger:** DynamoDB Stream
**Stream ARN:** (from table)
**Starting Position:** `LATEST` (or `TRIM_HORIZON` for backfill)
**Batch Size:** 100
**Maximum Batching Window:** 300 seconds

## How It Works

1. **Bedrock generates content** → Stored in DynamoDB `VentureOS-Violations` table
   - Key: `PK: VIOLATION#{violation_id}`, `SK: BEDROCK_CONTENT`

2. **DynamoDB Stream fires** → Sends INSERT/MODIFY event to Lambda

3. **Lambda processes batch:**
   - Creates temp table in DuckDB
   - Inserts all updates using prepared statements (`?` placeholders)
   - MERGEs temp table into `ocean.silver.violations` S3 Table
   - Works with partitioned tables (MERGE supports partitions)

4. **S3 Tables updated** → Bedrock content now queryable via DuckDB/Athena

## Testing

### Manual Test (Put Item to DynamoDB)

```bash
aws dynamodb put-item \
  --table-name VentureOS-Violations \
  --item '{
    "PK": {"S": "VIOLATION#TEST-001"},
    "SK": {"S": "BEDROCK_CONTENT"},
    "title_bedrock": {"S": "Test Title"},
    "description_bedrock": {"S": "Test Description"},
    "tags": {"L": [{"S": "test-tag"}]},
    "generated_at": {"S": "2026-01-09T10:00:00Z"}
  }'
```

### Verify Update

```sql
-- Query S3 Tables via DuckDB
SELECT violation_id, bedrock_title, bedrock_description 
FROM ocean.silver.violations 
WHERE violation_id = 'TEST-001';
```

## Error Handling

- **Array Type Safety:** Tags are always converted to array: `Array.isArray(tags) ? tags : [tags]`
- **Null Handling:** All fields accept `null` (DuckDB handles gracefully)
- **Partitioned Tables:** Uses MERGE instead of UPDATE (works with partitions)
- **Retry Logic:** Lambda retries on failure (DynamoDB Streams built-in)

## Cost Optimization

- **Batching:** 5-minute window reduces Lambda invocations
- **Batch Size:** 100 records per invocation (maximize throughput)
- **Estimated Cost:** ~$0.20 per million records processed

## Troubleshooting

### Issue: "UPDATE not supported on partitioned tables"
**Fix:** Already using MERGE (handles partitions)

### Issue: "Array type mismatch"
**Fix:** Tags are normalized to array: `Array.isArray(tags) ? tags : [tags]`

### Issue: "S3_TABLE_BUCKET_ARN not found"
**Fix:** Check environment variable is set correctly (full ARN, not bucket name)

### Issue: "Permission denied"
**Fix:** Verify IAM role has `s3tables:WriteData` permission

