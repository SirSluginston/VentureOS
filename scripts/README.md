# Deployment Scripts

Scripts for deploying VentureOS Lambda functions and infrastructure.

## Lambda Deployment Scripts

- **`deploy.bat`** - Deploy parquet writer Lambda
- **`deploy-admin-api.bat`** - Deploy admin API Lambda
- **`deploy-config-api.bat`** - Deploy config API Lambda
- **`deploy-consolidator.bat`** - Deploy consolidator Lambda
- **`deploy-data-api.bat`** - Deploy data API Lambda
- **`deploy-query-handler.bat`** - Deploy query handler Lambda
- **`deploy-stats-rebuild.sh`** - Deploy stats rebuild Lambda (Linux/Mac)

## Utility Scripts

- **`create-pages.js`** - Populate DynamoDB with initial page entries for brands
- **`swap-api.bat`** - One-time script to swap API Gateway routes (legacy)
- **`swap-options.bat`** - One-time script to configure OPTIONS routes (legacy)
- **`deploy_apis.ps1`** - PowerShell script for API deployment (legacy)

## Usage

Run from the `VentureOS` directory:

```bash
# Windows
scripts\deploy-query-handler.bat

# Linux/Mac
bash scripts/deploy-stats-rebuild.sh
```


