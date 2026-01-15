# Test Files

Test scripts and sample data for VentureOS Lambda functions.

## Test Scripts

- **`test-api-city.bat`** - Test API Gateway city endpoint
- **`test-consolidate.bat`** - Test consolidator Lambda
- **`test-lambda-s3.bat`** - Test parquet writer with S3 event
- **`test-lambda.bat`** - Test Lambda function directly
- **`test-query.bat`** - Test query handler Lambda

## Sample Data

- **`bronze-batch.json`** - Sample violation data for testing Bronze → Silver conversion

## Usage

Test scripts invoke Lambda functions with sample events. Ensure AWS CLI is configured:

```bash
scripts\test-api-city.bat
```


