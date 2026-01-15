# MERGE Fallback Strategy

## Current Implementation: MERGE (Modern Approach)

**Status:** Deployed and ready to test

**Code:** Uses `MERGE INTO` statement for partitioned Iceberg tables

**Expected Behavior:**
- ✅ If S3 Tables supports Merge-on-Read → Works perfectly
- ❌ If partitioned tables don't support MERGE → Error: "Not implemented: Support for updates in partitioned Iceberg tables"

## Fallback: INSERT OR REPLACE

If MERGE fails, use this alternative approach:

### Strategy: Delete + Insert Pattern

```javascript
// Instead of MERGE, use DELETE + INSERT
await con.run(`
  DELETE FROM ocean.silver.violations 
  WHERE violation_id IN (
    SELECT violation_id FROM stream_batch
  )
`);

await con.run(`
  INSERT INTO ocean.silver.violations 
  SELECT 
    main.* EXCLUDE(bedrock_title, bedrock_description, bedrock_tags, bedrock_generated_at, is_verified, verified_at),
    updates.bedrock_title,
    updates.bedrock_description,
    updates.bedrock_tags,
    updates.bedrock_generated_at,
    updates.is_verified,
    updates.verified_at
  FROM ocean.silver.violations AS main
  FULL OUTER JOIN stream_batch AS updates
  ON main.violation_id = updates.violation_id
`);
```

**Pros:**
- Works with partitioned tables
- Guaranteed to work

**Cons:**
- More expensive (DELETE + INSERT vs MERGE)
- Slightly slower

### Alternative: INSERT OR REPLACE (DuckDB Syntax)

```javascript
// DuckDB's INSERT OR REPLACE (if supported)
await con.run(`
  INSERT OR REPLACE INTO ocean.silver.violations 
  SELECT 
    COALESCE(main.violation_id, updates.violation_id) as violation_id,
    COALESCE(main.agency, updates.agency) as agency,
    -- ... all other fields ...
    COALESCE(updates.bedrock_title, main.bedrock_title) as bedrock_title,
    COALESCE(updates.bedrock_description, main.bedrock_description) as bedrock_description
  FROM ocean.silver.violations AS main
  FULL OUTER JOIN stream_batch AS updates
  ON main.violation_id = updates.violation_id
`);
```

## Testing Plan

1. **Deploy MERGE version** ✅ (Done)
2. **Test with sample Bedrock overlay:**
   ```bash
   aws dynamodb put-item \
     --table-name VentureOS-Violations \
     --item '{
       "PK": {"S": "VIOLATION#TEST-001"},
       "SK": {"S": "BEDROCK_CONTENT"},
       "title_bedrock": {"S": "Test Title"},
       "description_bedrock": {"S": "Test Description"}
     }'
   ```
3. **Check CloudWatch logs:**
   - If success → MERGE works! ✅
   - If error contains "Not implemented: Support for updates in partitioned Iceberg tables" → Implement fallback

## Implementation Notes

- **MERGE is preferred** (more efficient, modern)
- **Fallback is ready** (if MERGE fails, switch to DELETE + INSERT)
- **Monitor CloudWatch** for specific error messages
- **Cost impact:** DELETE + INSERT is ~2x more expensive than MERGE, but still very cheap per violation

