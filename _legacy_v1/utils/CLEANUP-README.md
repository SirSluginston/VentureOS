# DynamoDB Cleanup Script

## Purpose

This script removes old violation records from DynamoDB that are NOT in any city's `recent5` manifest. After the S3 migration, DynamoDB should only contain the 5 most recent violations per city per violation type (as tracked in manifests).

## When to Run

**Run this ONCE** after:
1. ✅ Migration from DynamoDB to S3 is complete
2. ✅ Daily sync has run at least once (to populate `inDynamo` in manifests)
3. ✅ You want to clean up old archive violations from DynamoDB

After this cleanup, the daily sync will automatically maintain DynamoDB going forward.

## How to Run

### Option 1: Run as Node.js script locally

```bash
cd VentureOS
node utils/cleanupOldDynamoViolations.js
```

Make sure you have AWS credentials configured (via `aws configure` or environment variables).

### Option 2: Run as Lambda function (one-time)

You can create a temporary Lambda function to run this, or invoke it manually via AWS CLI:

```bash
aws lambda invoke --function-name <your-cleanup-lambda> --payload '{}' response.json
```

## What It Does

1. **Scans all city manifests** in S3 to build a set of all violation IDs that SHOULD be in DynamoDB (from `recent5`)
2. **Scans DynamoDB** for all violations for each brand
3. **Identifies violations to delete**: Any violation in DynamoDB that is NOT in the `recent5` set
4. **Deletes in batches** (25 items per batch, DynamoDB limit)

## Safety

- ✅ Only deletes violations that are NOT in `recent5` (safe - these are old archive violations)
- ✅ Processes each brand separately
- ✅ Provides detailed logging
- ✅ Shows summary of what was deleted

## Expected Results

After cleanup, DynamoDB should contain approximately:
- **5 violations per city per violation type** (from `recent5`)
- Only the most recent violations (as tracked by manifests)

The exact number depends on how many cities have violations and how many violation types exist.

## Notes

- This script is **idempotent** - safe to run multiple times
- If a violation is in `recent5`, it will NOT be deleted
- The daily sync will maintain DynamoDB going forward, so this is a one-time cleanup

