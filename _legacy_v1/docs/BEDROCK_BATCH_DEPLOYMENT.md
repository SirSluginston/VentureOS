# Bedrock Batch Processing Deployment Guide

## Overview
This guide covers deploying the Bedrock batch processing system for cost-effective bulk AI content generation.

## Components

### 1. Lambda Functions
- **`ventureos-bedrock-batch-processor`**: Queries violations, applies filters/limits, creates Bedrock batch jobs
- **`ventureos-bedrock-batch-result`**: Processes completed batch results, stores in DynamoDB (triggers sync Lambda)

### 2. IAM Roles & Policies
- **`ventureos-bedrock-batch-processor-role`**: Lambda execution role for batch processor
- **`ventureos-bedrock-batch-result-role`**: Lambda execution role for batch result handler
- **`ventureos-bedrock-batch-job-role`**: Service role for Bedrock to access S3 (used by Bedrock service, not Lambda)

### 3. S3 Event Trigger
- Automatically invokes `ventureos-bedrock-batch-result` when batch output files are written

## Deployment Steps

### Step 1: Create Bedrock Batch Job Role
```bash
cd VentureOS/scripts
.\create-bedrock-batch-job-role.bat
```

This creates the IAM role that Bedrock service uses to read input files and write output files to S3.

### Step 2: Deploy Batch Processor Lambda
```bash
cd VentureOS/scripts
.\deploy-bedrock-batch-processor.bat
```

This Lambda:
- Queries violations from S3 Tables using DuckDB
- Applies filters (fine amount, dates, agencies, violation types)
- Applies limits (max violations, max spend, priority tiers)
- Creates Bedrock batch input files in S3
- Creates Bedrock batch jobs
- Stores job metadata in DynamoDB

**Configuration:**
- Memory: 2048MB (for DuckDB queries)
- Timeout: 300s (5 minutes)
- Ephemeral Storage: 2048MB
- Layer: `ventureos-duckdb-neo:2`

### Step 3: Deploy Batch Result Handler Lambda
```bash
cd VentureOS/scripts
.\deploy-bedrock-batch-result.bat
```

This Lambda:
- Reads batch output JSONL files from S3
- Parses Bedrock responses
- Stores content in DynamoDB (triggers sync Lambda automatically)
- Updates job status in DynamoDB

**Configuration:**
- Memory: 1024MB
- Timeout: 300s (5 minutes)
- Ephemeral Storage: 1024MB

### Step 4: Set Up S3 Event Trigger
```bash
cd VentureOS/scripts
.\setup-bedrock-batch-s3-trigger.bat
```

This configures S3 to automatically invoke the batch result handler when:
- Files are created in `bedrock-batches/output/`
- File extension is `.jsonl`

## Environment Variables

### Batch Processor
- `S3_TABLE_BUCKET_ARN`: ARN of S3 Tables bucket (for querying violations)
- `DATA_BUCKET`: S3 bucket for batch input/output files
- `BATCH_JOBS_TABLE`: DynamoDB table for job metadata

### Batch Result Handler
- `VIOLATIONS_TABLE`: DynamoDB table for storing Bedrock content
- `BATCH_JOBS_TABLE`: DynamoDB table for job metadata

## API Endpoints

### Admin API (`/admin/bedrock/batch`)

#### POST `/admin/bedrock/batch`
Create a new batch job.

**Request Body:**
```json
{
  "config": {
    "filters": {
      "minFineAmount": 50000,
      "maxFineAmount": null,
      "agencies": ["osha"],
      "dateRange": {
        "start": "2024-01-01",
        "end": "2024-12-31"
      },
      "violationTypes": ["Serious"]
    },
    "limits": {
      "maxViolations": 10000,
      "maxSpendUSD": 50,
      "priority": "medium"
    }
  }
}
```

**Response:**
```json
{
  "message": "Batch job created successfully",
  "jobId": "batch-1234567890-abc123",
  "jobArn": "arn:aws:bedrock:us-east-1:...",
  "violationCount": 5000,
  "estimate": {
    "violationCount": 5000,
    "totalTokens": 1500000,
    "costUSD": 1.50,
    "costPerViolation": 0.0003
  },
  "status": "InProgress",
  "expectedCompletion": "2025-01-10T12:00:00Z"
}
```

#### POST `/admin/bedrock/batch/estimate`
Estimate cost without creating a job (dry-run).

**Request Body:** Same as `/admin/bedrock/batch`

**Response:**
```json
{
  "message": "Cost estimation complete",
  "violationCount": 5000,
  "estimate": {
    "violationCount": 5000,
    "totalTokens": 1500000,
    "costUSD": 1.50,
    "costPerViolation": 0.0003
  },
  "dryRun": true
}
```

#### GET `/admin/bedrock/batch/jobs`
List all batch jobs.

**Response:**
```json
{
  "jobs": [
    {
      "PK": "JOB#batch-1234567890-abc123",
      "SK": "METADATA",
      "jobArn": "arn:aws:bedrock:...",
      "status": "Completed",
      "violationCount": 5000,
      "createdAt": "2025-01-09T12:00:00Z",
      "processedAt": "2025-01-09T14:30:00Z",
      "stats": {
        "processed": 4950,
        "errors": 50,
        "total": 5000
      }
    }
  ]
}
```

#### GET `/admin/bedrock/batch/{jobId}`
Get status of a specific batch job.

**Response:**
```json
{
  "PK": "JOB#batch-1234567890-abc123",
  "SK": "METADATA",
  "jobArn": "arn:aws:bedrock:...",
  "status": "InProgress",
  "violationCount": 5000,
  "config": { ... },
  "estimate": { ... },
  "createdAt": "2025-01-09T12:00:00Z"
}
```

## DynamoDB Tables

### `VentureOS-BedrockBatchJobs`
Stores batch job metadata.

**Schema:**
- `PK`: `JOB#{jobId}`
- `SK`: `METADATA`
- `jobArn`: Bedrock job ARN
- `status`: `InProgress` | `Completed` | `Failed`
- `violationCount`: Number of violations in batch
- `config`: Batch configuration
- `estimate`: Cost estimate
- `createdAt`: ISO timestamp
- `processedAt`: ISO timestamp (when completed)
- `stats`: Processing statistics
- `ttl`: 30 days expiration

### `VentureOS-Violations`
Stores Bedrock content (same table used by sync Lambda).

**Schema:**
- `PK`: `VIOLATION#{violation_id}`
- `SK`: `BEDROCK_CONTENT`
- `title_bedrock`: AI-generated title
- `description_bedrock`: AI-generated description
- `tags`: Array of tags
- `generated_at`: ISO timestamp
- `attribution`: `"Generated by AWS Bedrock (Batch)"`

## S3 Structure

```
sirsluginston-ventureos-data/
└── bedrock-batches/
    ├── input/
    │   └── {jobId}/
    │       └── input.jsonl
    └── output/
        └── {jobId}/
            └── output.jsonl
```

## Workflow

1. **Admin creates batch job** via `/admin/bedrock/batch`
2. **Batch processor Lambda**:
   - Queries violations from S3 Tables
   - Applies filters and limits
   - Creates input JSONL file in S3
   - Creates Bedrock batch job
   - Stores job metadata in DynamoDB
3. **Bedrock processes batch** (asynchronous, can take hours)
4. **Bedrock writes output** to S3 (`bedrock-batches/output/{jobId}/output.jsonl`)
5. **S3 event triggers** batch result handler Lambda
6. **Batch result handler**:
   - Reads output JSONL file
   - Parses Bedrock responses
   - Stores content in DynamoDB (triggers sync Lambda)
   - Updates job status
7. **Sync Lambda** (via DynamoDB Stream):
   - Merges Bedrock content into S3 Tables

## Cost Estimation

Batch processing uses Bedrock's batch pricing (50% discount):
- **On-demand**: ~$0.002 per 1K tokens
- **Batch**: ~$0.001 per 1K tokens

Average violation: ~300 tokens
- **On-demand**: ~$0.0006 per violation
- **Batch**: ~$0.0003 per violation

**Example:**
- 10,000 violations = ~3M tokens = ~$3.00 (batch) vs ~$6.00 (on-demand)

## Troubleshooting

### Batch Processor Errors

**"No violations to process"**
- Check filters are not too restrictive
- Verify violations exist in S3 Tables
- Check that violations don't already have Bedrock content

**"Failed to create batch job"**
- Verify Bedrock batch job role exists and has correct permissions
- Check S3 bucket permissions
- Verify model ID is correct

### Batch Result Handler Errors

**"Failed to parse Bedrock response"**
- Check CloudWatch logs for specific parsing errors
- Verify Bedrock output format matches expected structure
- May need to adjust parsing logic for different model responses

**"S3 event not triggering"**
- Verify S3 notification configuration
- Check Lambda permissions for S3 invoke
- Verify file path matches trigger filter (`bedrock-batches/output/*.jsonl`)

## Next Steps

1. Deploy all components using the scripts above
2. Test with a small batch (10-100 violations)
3. Monitor CloudWatch logs for errors
4. Verify DynamoDB entries are created correctly
5. Verify sync Lambda merges content into S3 Tables
6. Build Admin UI for batch job management

