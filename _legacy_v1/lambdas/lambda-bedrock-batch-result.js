/**
 * Bedrock Batch Result Handler Lambda
 * 
 * Purpose: Process completed Bedrock batch job results
 * Trigger: S3 Event (when batch output is written)
 * 
 * Process:
 * 1. Read batch output JSONL file from S3
 * 2. Parse Bedrock responses
 * 3. Store results in DynamoDB (triggers sync Lambda automatically)
 * 4. Update job status
 */

import { S3Client, GetObjectCommand } from '@aws-sdk/client-s3';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, PutCommand, UpdateCommand, BatchWriteCommand } from '@aws-sdk/lib-dynamodb';
import { BedrockClient, GetModelInvocationJobCommand } from '@aws-sdk/client-bedrock';

const s3Client = new S3Client({ region: process.env.AWS_REGION || 'us-east-1' });
const dynamoClient = new DynamoDBClient({ region: process.env.AWS_REGION || 'us-east-1' });
const docClient = DynamoDBDocumentClient.from(dynamoClient);
const bedrockClient = new BedrockClient({ region: process.env.AWS_REGION || 'us-east-1' });

const VIOLATIONS_TABLE = process.env.VIOLATIONS_TABLE || 'VentureOS-Violations';
const BATCH_JOBS_TABLE = process.env.BATCH_JOBS_TABLE || 'VentureOS-BedrockBatchJobs';

/**
 * Extract job ID from S3 key
 */
function extractJobId(s3Key) {
  // Format: bedrock-batches/output/{jobId}/output.jsonl
  const match = s3Key.match(/bedrock-batches\/output\/([^/]+)\//);
  return match ? match[1] : null;
}

/**
 * Read and parse JSONL file from S3
 */
async function readJSONLFromS3(bucket, key) {
  const response = await s3Client.send(new GetObjectCommand({
    Bucket: bucket,
    Key: key
  }));
  
  const content = await response.Body.transformToString();
  const lines = content.split('\n').filter(line => line.trim());
  
  return lines.map(line => {
    try {
      return JSON.parse(line);
    } catch (e) {
      console.warn('⚠️ Failed to parse JSONL line:', line);
      return null;
    }
  }).filter(Boolean);
}

/**
 * Parse Bedrock response
 */
function parseBedrockResponse(response) {
  try {
    // Bedrock batch output format
    const body = JSON.parse(response.body || '{}');
    
    // Mistral returns content in 'outputs' array
    const content = body.outputs?.[0]?.text || body.text || body.content || '';
    
    // Extract JSON from response
    const jsonMatch = content.match(/```json\s*([\s\S]*?)\s*```/) || content.match(/\{[\s\S]*\}/);
    const jsonStr = jsonMatch ? jsonMatch[1] || jsonMatch[0] : content;
    
    return JSON.parse(jsonStr);
  } catch (e) {
    console.warn('⚠️ Failed to parse Bedrock response:', e.message);
    return null;
  }
}

/**
 * Store Bedrock content in DynamoDB
 */
async function storeBedrockContent(violationId, bedrockContent) {
  const item = {
    PK: `VIOLATION#${violationId}`,
    SK: 'BEDROCK_CONTENT',
    title_bedrock: bedrockContent.title,
    description_bedrock: bedrockContent.description,
    tags: bedrockContent.tags || [],
    generated_at: new Date().toISOString(),
    attribution: 'Data synthesized by SirSluginston VentureOS (Batch)'
  };
  
  await docClient.send(new PutCommand({
    TableName: VIOLATIONS_TABLE,
    Item: item
  }));
  
  return item;
}

/**
 * Update job status
 */
async function updateJobStatus(jobId, status, stats) {
  await docClient.send(new UpdateCommand({
    TableName: BATCH_JOBS_TABLE,
    Key: {
      PK: `JOB#${jobId}`,
      SK: 'METADATA'
    },
    UpdateExpression: 'SET #status = :status, processedAt = :processedAt, stats = :stats',
    ExpressionAttributeNames: {
      '#status': 'status'
    },
    ExpressionAttributeValues: {
      ':status': status,
      ':processedAt': new Date().toISOString(),
      ':stats': stats
    }
  }));
}

export const handler = async (event) => {
  console.log('📥 Bedrock Batch Result Handler Started');
  
  try {
    // Process S3 events
    for (const record of event.Records || []) {
      if (record.eventSource !== 'aws:s3') continue;
      
      const bucket = record.s3.bucket.name;
      const key = decodeURIComponent(record.s3.object.key.replace(/\+/g, ' '));
      
      // Only process output files
      if (!key.includes('bedrock-batches/output/') || !key.endsWith('.jsonl')) {
        console.log(`⏭️ Skipping non-batch output file: ${key}`);
        continue;
      }
      
      console.log(`📂 Processing batch output: ${key}`);
      
      // Extract job ID
      const jobId = extractJobId(key);
      if (!jobId) {
        console.warn(`⚠️ Could not extract job ID from key: ${key}`);
        continue;
      }
      
      console.log(`🔍 Job ID: ${jobId}`);
      
      // Read batch results
      const results = await readJSONLFromS3(bucket, key);
      console.log(`📊 Found ${results.length} results`);
      
      let processed = 0;
      let errors = 0;
      const items = [];
      
      // Process each result
      for (const result of results) {
        try {
          const violationId = result.customAttributes?.violation_id;
          if (!violationId) {
            console.warn('⚠️ Missing violation_id in result');
            errors++;
            continue;
          }
          
          // Parse Bedrock response
          const bedrockContent = parseBedrockResponse(result);
          if (!bedrockContent) {
            console.warn(`⚠️ Failed to parse Bedrock response for ${violationId}`);
            errors++;
            continue;
          }
          
          // Store in DynamoDB (will trigger sync Lambda)
          await storeBedrockContent(violationId, bedrockContent);
          processed++;
          
        } catch (error) {
          console.error(`❌ Error processing result:`, error);
          errors++;
        }
      }
      
      // Update job status
      await updateJobStatus(jobId, 'Completed', {
        processed,
        errors,
        total: results.length
      });
      
      console.log(`✅ Batch processing complete. Processed: ${processed}, Errors: ${errors}`);
    }
    
    return {
      statusCode: 200,
      body: JSON.stringify({ message: 'Batch results processed' })
    };
    
  } catch (error) {
    console.error('❌ Batch Result Handler Error:', error);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: error.message })
    };
  }
};

