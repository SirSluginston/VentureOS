# S3 Intelligent Tiering Setup

## Status
⚠️ **Not yet configured** - Needs to be set up via AWS Console

## Why It's Needed
Intelligent Tiering automatically moves old Bronze layer files to cheaper storage tiers:
- **0-30 days:** Standard storage ($0.023/GB)
- **30-90 days:** Infrequent Access ($0.0125/GB) - 46% savings
- **90+ days:** Archive Instant Access ($0.004/GB) - 83% savings

This is especially important for Bronze layer files which accumulate over time.

## Setup Instructions (AWS Console)

1. **Navigate to S3 Console:**
   - Go to https://console.aws.amazon.com/s3/
   - Select bucket: `sirsluginston-ventureos-data`

2. **Enable Intelligent Tiering:**
   - Click **Management** tab
   - Scroll to **Intelligent-Tiering** section
   - Click **Create Intelligent-Tiering configuration**
   - **Configuration name:** `EntireBucket`
   - **Scope:** Select **Apply to all objects in the bucket**
   - **Optional filters:** Leave empty (applies to entire bucket)
   - Click **Create configuration**

3. **Verify:**
   ```bash
   aws s3api get-bucket-intelligent-tiering-configuration \
     --bucket sirsluginston-ventureos-data \
     --id "EntireBucket"
   ```

## Expected Behavior

- Files automatically transition after 30 days of no access
- No manual intervention needed
- Query performance unchanged (Archive Instant Access is <10ms)
- Cost savings accumulate over time

## Cost Impact

**Example (100GB Bronze files):**
- Month 1-3: 100GB × $0.023 = $2.30/month
- Month 4+: 100GB × $0.004 = $0.40/month
- **Savings:** $1.90/month per 100GB archived

