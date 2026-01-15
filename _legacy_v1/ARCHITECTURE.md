# VentureOS Architecture

**VentureOS** is the shared backend infrastructure for all Trail brands. It provides data ingestion, AI enhancement, storage, and API serving.

## Core Architecture: Medallion Pattern

```
External APIs (OSHA, FDA, NHTSA, etc.)
    ↓
Bronze Layer (Temporary Landing Zone)
    - Standard S3 bucket
    - Raw JSON files
    - S3 event triggers
    ↓
Normalizer Lambdas (per agency)
    - Parse agency-specific formats
    - Standardize to common schema
    ↓
Silver Layer (Permanent Source of Truth)
    - S3 Tables with Iceberg
    - Parquet format (columnar, compressed)
    - Auto-compaction (AWS managed)
    - Intelligent-Tiering (87% savings after 90 days)
    ↓
Gold Layer (Fast Access)
    - DynamoDB (recent5, stats)
    - Pre-aggregated data
    - Sub-second lookups
    ↓
Frontend APIs (per Trail brand)
    - API Gateway + Lambda
    - CloudFront CDN
    - React frontends
```

## Key Components

### 1. Parquet Writer (`utils/parquetWriter.js`)
- Uses DuckDB for local processing
- Writes to S3 Tables (Iceberg format)
- Generates Gold layer aggregates (company/city/state stats)
- Optionally syncs to DynamoDB (recent5)

**Trigger**: S3 event when JSON file lands in Bronze

**Process**:
1. Read JSON from Bronze
2. Normalize schema (ensure DATE types, DOUBLE for fines)
3. Write to Silver (S3 Tables)
4. Aggregate stats and write to Gold (S3 Tables)
5. If live data (not historical), update DynamoDB recent5

### 2. Normalizers (`utils/normalizers/*.js`)
**Purpose**: Convert agency-specific data to common schema

**How It Works**:
1. Admin panel upload sets path: `bronze/historical/{agency}/{normalizer}/{date}/file.csv`
2. Parquet writer extracts normalizer name from path
3. Looks up normalizer function in `NORMALIZERS` map
4. Applies normalizer to each row → Unified Schema

**Naming Convention**: `{agency}-{data_type}` (e.g., `osha-severe_injury`, `fda-recall`)

**Implemented**:
- `utils/normalizers/osha.js`:
  - `normalizeSevereInjuryReport` → `osha-severe_injury`
  - `normalizeEnforcementData` → `osha-enforcement`

**To Create**:
- `utils/normalizers/fda.js`: FDA recalls (`fda-recall`), warning letters (`fda-warning_letter`)
- `utils/normalizers/nhtsa.js`: Vehicle recalls (`nhtsa-recall`)
- `utils/normalizers/faa.js`: Aviation incidents (`faa-drone_incident`)
- `utils/normalizers/epa.js`: Environmental violations (`epa-violation`)

**Common Schema**: See [DATA_SOURCES.md](../DATA_SOURCES.md)

**Documentation**: See [NORMALIZER_ARCHITECTURE.md](docs/NORMALIZER_ARCHITECTURE.md) for detailed guide on creating new normalizers

### 3. Bedrock Integration
**Purpose**: AI-generated titles/descriptions for violations

**Current Status**: Needs debugging (showing "pending" instead of content)

**Architecture**:
```
Violation (raw text)
    ↓
Bedrock API (Mistral Large 24.02)
    ↓
AI-generated content
    ↓
DynamoDB overlay (lazy load or recent5)
    ↓
Frontend displays enhanced content
```

**Lazy Load Strategy**:
- Generate on first view
- Store in DynamoDB
- Yearly consolidation merges into Parquet

**Batch Strategy** (future):
- Use Bedrock batch inference (50% discount)
- For historical imports (not time-sensitive)

### 4. DuckDB Lambda Layer
**Why**: 10x faster than parquetjs-lite, 90% less RAM

**Features**:
- Parquet write/read
- Local SQL queries (no Athena costs)
- S3 streaming via httpfs extension
- Iceberg table support

**Configuration**:
- Layer ARN: `arn:aws:lambda:us-east-1:041475135427:layer:duckdb-nodejs-x86:21`
- Memory: 2GB minimum
- Ephemeral storage: 10GB for large batches

### 5. S3 Tables (Iceberg)
**Why**: Auto-compaction, Intelligent-Tiering, ACID transactions

**Bucket**: `sirsluginston-ventureos-data-ocean`

**Tables**:
- `silver.osha`: Workplace safety violations
- `silver.fda`: (future) Food/drug/device recalls
- `silver.nhtsa`: (future) Vehicle recalls
- `gold.company_stats`: Company aggregates (not created yet)
- `gold.city_stats`: City aggregates
- `gold.state_stats`: State aggregates

**Benefits**:
- No manual consolidation needed (AWS merges files)
- Data auto-archives after 90 days (87% cost savings)
- Query speed unchanged (<10ms even on archived data)

### 6. DynamoDB Tables
**`VentureOS-Violations`**: Recent5 per entity/agency
- PK: `CITY#{slug}` / `STATE#{code}` / `COMPANY#{slug}`
- SK: `AGENCY#{agency}#DATE#{date}#{id}`

**`VentureOS-Entities`**: Company metadata (CompaniesTrail)
- PK: `COMPANY#{slug}`/ `STATE#{code}` / `CITY#{slug}`
- SK: `STATS#all` / `COMPANY#{slug}`

**`VentureOS-Users`**: Notification subscriptions (FDATrail)
- PK: `USER#{AWS Cognito}`
- SK: `SETTINGS` / `NOTIFICATIONS` / `SUBSCRIPTIONS`

## Data Flow Examples

### Live Data (OSHA Daily Sync)
1. Cron triggers Lambda (06:00 UTC daily)
2. Query DoL Open Data Portal API (last 24 hours)
3. Write JSON to Bronze: `bronze/daily/osha/enforcement/2026-01-10/`
4. S3 event triggers parquet writer
5. Normalize → Write Silver → Aggregate Gold → Update DynamoDB
6. Frontend queries DynamoDB (recent5) or Athena (view more)

### Historical Import
1. Upload CSV to Bronze: `bronze/historical/{agency}/{normalizer}/{YYYY-MM-DD}/data.csv`
   - Example: `bronze/historical/osha/osha-severe_injury/2026-01-10/dumped_data.csv`
   - Normalizer is selected based on path (e.g., `osha-severe_injury` → `normalizeSevereInjuryReport`)
2. S3 event triggers parquet writer Lambda automatically
3. Parquet writer:
   - Parses CSV from Bronze
   - Selects normalizer based on path segment (e.g., `osha-severe_injury`)
   - Normalizes data to common schema
   - Writes to Silver (S3 Tables/Iceberg)
   - Aggregates to Gold layer (company/city/state stats)
   - **Skips DynamoDB** (detects `/historical/` in path)
4. Frontend queries Athena for historical data (not DynamoDB recent5)

## Cost Optimization

### Caching Strategy
- **CloudFront**: 1-hour cache for API responses
- **DynamoDB**: 24-hour cache for aggregates
- **Browser**: 5 minutes (SWR)

### Query Optimization
- **Partition pruning**: Athena only scans relevant folders
- **DuckDB local**: Simple stats calculated in Lambda (free)
- **Batch requests**: Hourly aggregation vs per-violation

### Estimated Monthly Costs (at scale)
- S3 Tables (50GB, with archiving): $0.50
- DynamoDB (10k entities, 10 reads/day): $15
- Lambda (processing + APIs): $15
- Athena (10k queries/day, 10MB avg): $3
- Bedrock (lazy load only): $3
- **Total: ~$37/month** (one API subscriber covers it!)

## Multi-Tenancy

**Current**: Each Trail brand shares infrastructure but has separate frontends

**Future**: Config-driven brand creation
```javascript
{
  "brand": "FDATrail",
  "agencies": ["FDA"],
  "theme": { "primary": "#e91e63" },
  "features": ["barcode_scanner", "notifications"],
  "domain": "fdatrail.com"
}
```

## Security

### IAM Policies
- Least privilege (separate policies per Lambda)
- Stored in `iam/` folder (gitignored to protect account IDs)

### API Keys
- Bedrock config gitignored
- API keys in environment variables (Lambda config)
- Future: AWS Secrets Manager

### Data Privacy
- Public data only (government records)
- No PII stored
- Subscription emails encrypted at rest (KMS)

## Monitoring

### CloudWatch
- Lambda execution times
- API response times
- Error rates
- Cost tracking

### Alerts (To Set Up)
- Lambda failures
- API Gateway 5xx errors
- Cost exceeds $100/month
- DynamoDB throttling

---

**For detailed technical docs**, see `docs/` folder:
- [BEDROCK_BATCH_PROCESSING.md](docs/BEDROCK_BATCH_PROCESSING.md)
- [ARCHITECTURE_CLARIFICATION.md](docs/ARCHITECTURE_CLARIFICATION.md)
- [CONSOLIDATOR_EXPLANATION.md](docs/CONSOLIDATOR_EXPLANATION.md)

**Last Updated**: January 10, 2026

