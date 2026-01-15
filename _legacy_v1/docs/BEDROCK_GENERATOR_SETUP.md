# Bedrock Generator Lambda Setup

## ✅ **What's Been Created**

1. **Lambda Function:** `VentureOS/lambdas/lambda-bedrock-generator.js`
   - Fetches violation data from S3 Tables
   - Calls Bedrock API with violation data
   - Stores result in DynamoDB (triggers sync Lambda automatically)

2. **API Endpoint:** Added to `lambda-query-handler.js`
   - Route: `POST /api/bedrock/generate`
   - Accepts: `{ violation_id: "..." }`
   - Returns: `202 Accepted` (async invocation)

3. **IAM Policy:** `iam/bedrock-generator-policy.json`
   - Bedrock invoke permissions
   - DynamoDB read/write
   - S3 Tables read

4. **Deployment Script:** `scripts/deploy-bedrock-generator.bat`

---

## 🚀 **Deployment Steps**

### 1. Deploy Bedrock Generator Lambda

```bash
cd VentureOS/scripts
.\deploy-bedrock-generator.bat
```

### 2. Add Lambda Invoke Permission to Query Handler

The query handler Lambda needs permission to invoke the Bedrock Generator:

```bash
aws lambda add-permission \
  --function-name ventureos-bedrock-generator \
  --statement-id allow-query-handler-invoke \
  --action lambda:InvokeFunction \
  --principal lambda.amazonaws.com \
  --source-arn arn:aws:lambda:us-east-1:611538926352:function:ventureos-query
```

Or add to query handler's IAM role policy:
```json
{
  "Effect": "Allow",
  "Action": "lambda:InvokeFunction",
  "Resource": "arn:aws:lambda:us-east-1:611538926352:function:ventureos-bedrock-generator"
}
```

### 3. Update Query Handler Environment Variable

Add to `ventureos-query` Lambda environment variables:
```
BEDROCK_GENERATOR_FUNCTION=ventureos-bedrock-generator
```

### 4. Redeploy Query Handler

Redeploy the query handler to pick up the new route:
```bash
cd VentureOS/scripts
.\deploy-query-handler.bat
```

---

## 🧪 **Testing**

### Test Bedrock Generation

```bash
# Via API Gateway
curl -X POST https://ez3tbm2djc.execute-api.us-east-1.amazonaws.com/v2/api/bedrock/generate \
  -H "Content-Type: application/json" \
  -d '{"violation_id": "TEST-001"}'
```

### Expected Flow

1. Frontend calls `/api/bedrock/generate` with `violation_id`
2. Query handler invokes Bedrock Generator Lambda (async)
3. Bedrock Generator:
   - Queries S3 Tables for violation data
   - Calls Bedrock API
   - Stores result in DynamoDB
4. DynamoDB Stream triggers Sync Lambda
5. Sync Lambda updates S3 Tables
6. Next API call returns Bedrock content

---

## 📋 **Frontend Integration**

When displaying a violation:

```javascript
// Check if Bedrock content exists
if (!violation.bedrock_title && !violation.bedrock_description) {
  // Trigger generation (fire-and-forget)
  fetch('/api/bedrock/generate', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ violation_id: violation.violation_id })
  }).catch(err => console.warn('Bedrock generation failed:', err));
  
  // Display raw data immediately
  displayViolation(violation.raw_description);
} else {
  // Display Bedrock content
  displayViolation(violation.bedrock_title, violation.bedrock_description);
}
```

---

## ✅ **Verification Checklist**

- [ ] Bedrock Generator Lambda deployed
- [ ] IAM permissions configured
- [ ] Query handler has invoke permission
- [ ] Query handler environment variable set
- [ ] Query handler redeployed with new route
- [ ] Test endpoint works
- [ ] Frontend integration added

---

## 🐛 **Troubleshooting**

**Error: "Lambda function not found"**
- Check function name matches `BEDROCK_GENERATOR_FUNCTION` env var

**Error: "AccessDeniedException"**
- Verify query handler has `lambda:InvokeFunction` permission

**Error: "Bedrock model not found"**
- Verify IAM policy includes correct model ARN

**Error: "Violation not found"**
- Check S3 Tables exist and contain the violation
- Verify violation_id format matches

