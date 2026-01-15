# VentureOS Scripts

Scripts for managing VentureOS infrastructure and data.

## Directory Structure

- **`utilities/`** - Utility scripts for data management and setup
  - `create-pages.js` - Populate DynamoDB with page entries for brands
  - `create-test-violations.js` - Create test violations (development only)
  - `check-bedrock-sync.js` - Verify Bedrock content sync to S3 Tables
  - `verify-bedrock-sync.js` - Check sync status

- **`deploy.bat`** - Core infrastructure deployment (parquet writer Lambda)
  - Only needed for infrastructure updates
  - Not needed for adding new brands

## Infrastructure Configuration

Infrastructure configuration files are in `../infrastructure/`:
- DynamoDB table definitions (`create-*-table.json`)
- API Gateway configurations (`api-gateway.yaml`)
- S3 event triggers (`s3-batch-result-trigger.json`)
- Lambda environment variables (`batch-*-env.json`)
- Gateway response configs (`gateway-response-*.json`)

## Adding New Brands

New brands (e.g., TransportTrail, EPA Trail) plug into existing infrastructure:

1. **Create Brand Entry** - Add `BRAND#` entry to `VentureOS-Projects` DynamoDB table
2. **Create Pages** - Run `utilities/create-pages.js` to add page entries
3. **Configure Agency Filters** - Update batch processing configs if needed (in `utils/bedrockBatchConfig.js`)
4. **Deploy Frontend** - Deploy brand-specific frontend pointing to same API Gateway

**No Lambda redeployment needed** - All brands share the same backend infrastructure.

The only differences between brands:
- **Agency filters** (OSHA vs NHTSA/FAA/USCG/FRA vs EPA)
- **UI focus** (violations vs stats/scores)
- **Branding** (colors, logos, names)

## Usage

```bash
# Create pages for all brands
node scripts/utilities/create-pages.js

# Check Bedrock sync status
node scripts/utilities/check-bedrock-sync.js <violation_id>

# Deploy infrastructure updates (rarely needed)
scripts\deploy.bat
```
