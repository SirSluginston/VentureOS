# VentureOS

Unified backend infrastructure for SirSluginston Co. brands.

## Architecture

- **Medallion Architecture**: Bronze (raw JSON) → Silver (Parquet) → Gold (aggregated stats)
- **Data Lake**: AWS S3 Tables (Iceberg format) with DuckDB processing
- **API**: Lambda functions behind API Gateway

## Directory Structure

```
VentureOS/
├── api/              # API Lambda handlers (config, admin, data)
├── docs/             # Documentation and examples
├── iam/              # IAM policy definitions
├── infrastructure/   # Infrastructure as code (API Gateway, etc.)
├── lambdas/          # Lambda function handlers
├── scripts/          # Deployment and utility scripts
├── tests/            # Test scripts and sample data
└── utils/            # Shared utility functions
```

## Lambda Functions

- **`ventureos-parquet-writer`** - Converts Bronze JSON to Silver Parquet
- **`ventureos-consolidator`** - Moves data from Buffer to Archive
- **`ventureos-query`** - Serves data from Parquet data lake
- **`ventureos-api-config`** - Configuration API
- **`ventureos-api-admin`** - Admin operations API
- **`ventureos-api-data`** - Data query API
- **`ventureos-daily-sync`** - Syncs recent data to DynamoDB
- **`ventureos-stats-rebuild`** - Rebuilds Gold layer stats

## Deployment

Deployment scripts are in `scripts/`. Each Lambda has its own script (`deploy-*.bat` or `.sh`):

```bash
# Windows
scripts\deploy-query-handler.bat

# Linux/Mac
bash scripts/deploy-stats-rebuild.sh
```

Scripts:
1. Package the Lambda code
2. Install dependencies (excluding native binaries provided by Layers)
3. Create deployment zip
4. Update Lambda function code

## IAM Policies

See `iam/` directory for policy definitions. Policies follow least-privilege principles.

## Data Flow

1. Raw data lands in S3 Bronze layer (`bronze/raw/{agency}/{timestamp}/`)
2. `ventureos-parquet-writer` converts to Parquet in Silver Buffer (`silver/violations/buffer/ingest_date=YYYY-MM-DD/`)
3. `ventureos-consolidator` moves to Archive (`silver/violations/archive/violation_year=YYYY/`)
4. `ventureos-query` serves data from both Buffer and Archive
5. Recent violations synced to DynamoDB for fast access

## Testing

Test scripts are in `tests/`. Use AWS CLI to invoke:

```bash
scripts\test-api-city.bat
```

See `tests/README.md` for more details.

