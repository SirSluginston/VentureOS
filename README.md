# VentureOS

VentureOS is the backend backbone for SirSluginston Co.'s pSEO projects. It handles data ingestion, AI enhancement (Bedrock), and API delivery.

## Structure

*   `api/` - API definitions and configurations.
*   `iam/` - AWS IAM policies and role definitions.
*   `infrastructure/` - AWS resource definitions (DynamoDB, S3, etc.).
*   `lambdas/` - Source code for AWS Lambda functions.
*   `scripts/` - Deployment and utility scripts.
*   `utils/` - Shared utility functions (Parquet writers, Bedrock processors).

## Key Workflows

### 1. Data Ingestion (Bronze Layer)
Raw data is ingested from government APIs into S3 (Bronze).

### 2. AI Processing
Bedrock batch jobs enrich the data with titles, descriptions, and tags.

### 3. Normalization (Silver Layer)
Data is normalized and written to S3 Tables (Iceberg) for querying.
*   *Note: See `SYSTEM_OVERVIEW.md` for details on the dual-write buffer strategy.*

### 4. API Serving (Gold Layer)
Processed data is synced to DynamoDB for high-speed access by the frontend applications.

## Deployment
Use the scripts in `scripts/` for deployment.
*   `deploy-gold-sync.bat` - Deploys the Gold Sync Lambda.
*   (Ensure AWS credentials are configured).

## Secrets
Bedrock configurations (`bedrock-production-config.js`) are git-ignored.
