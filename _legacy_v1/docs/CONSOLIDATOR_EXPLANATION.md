# Consolidator Lambda Explanation

## What It Does

The `lambda-consolidator` moves Parquet files from the **Buffer** layer to the **Archive** layer:

- **Source:** `s3://bucket/silver/violations/buffer/ingest_date=YYYY-MM-DD/*.parquet`
- **Target:** `s3://bucket/silver/violations/archive/violation_year=YYYY/*.parquet`

## Why It's Needed

**S3 Tables auto-compaction** handles:
- Merging small files within the same table/partition
- Optimizing file sizes for query performance

**S3 Tables does NOT handle:**
- Moving files between different folder structures (buffer → archive)
- Changing partition keys (ingest_date → violation_year)
- Consolidating across time boundaries (daily files → yearly files)

## The Consolidator's Job

1. **Identifies old buffer files** (default: 30+ days old)
2. **Reads all matching files** into DuckDB
3. **Re-partitions by violation_year** (extracted from `event_date`)
4. **Writes to archive** with proper year-based partitioning
5. **Deletes source buffer files** (cleanup)

## When It Runs

- **Manual trigger:** Via admin panel or CLI
- **Scheduled:** Monthly (recommended) or weekly (if high volume)
- **NOT needed:** For every ingestion (that would be wasteful)

## Is It Legacy?

**NO** - This is still needed even with S3 Tables. The consolidator handles the architectural pattern of:
- **Hot data** (buffer, recent, frequently updated)
- **Cold data** (archive, historical, immutable)

S3 Tables handles compaction WITHIN each layer, but doesn't move data BETWEEN layers.

## Cost Impact

- Runs monthly: ~$0.10-0.50 per run (depends on data volume)
- Saves money by moving old data to cheaper storage tiers
- Enables Intelligent-Tiering to archive old years automatically

