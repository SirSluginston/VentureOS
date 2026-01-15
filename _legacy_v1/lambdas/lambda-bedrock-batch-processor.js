/**
 * Bedrock Batch Processor Lambda
 * 
 * Purpose: Create Bedrock batch jobs for cost-effective bulk processing
 * Trigger: Manual (API) or EventBridge (scheduled)
 * 
 * Process:
 * 1. Query violations from S3 Tables based on filters
 * 2. Apply priority tiers and limits
 * 3. Format violations for Bedrock batch input
 * 4. Create Bedrock batch job
 * 5. Store job metadata in DynamoDB
 */

import { BedrockClient, CreateModelInvocationJobCommand } from '@aws-sdk/client-bedrock';
import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, PutCommand } from '@aws-sdk/lib-dynamodb';
import { DuckDBInstance } from '@duckdb/node-api';
import { BEDROCK_CONFIG } from './bedrock-production-config.js';
import { applyFilters, applyLimits, estimateBatchCost, DEFAULT_BATCH_CONFIG } from './bedrockBatchConfig.js';

const bedrockClient = new BedrockClient({ region: process.env.AWS_REGION || 'us-east-1' });
const s3Client = new S3Client({ region: process.env.AWS_REGION || 'us-east-1' });
const dynamoClient = new DynamoDBClient({ region: process.env.AWS_REGION || 'us-east-1' });
const docClient = DynamoDBDocumentClient.from(dynamoClient);

const S3_TABLE_BUCKET_ARN = process.env.S3_TABLE_BUCKET_ARN || 
  'arn:aws:s3tables:us-east-1:611538926352:bucket/sirsluginston-ventureos-data-ocean';
const DATA_BUCKET = process.env.DATA_BUCKET || 'sirsluginston-ventureos-data';
const BATCH_JOBS_TABLE = process.env.BATCH_JOBS_TABLE || 'VentureOS-BedrockBatchJobs';

const AGENCIES = ['osha', 'msha', 'nhtsa', 'faa', 'uscg', 'fra', 'epa'];

/**
 * Format violation for Bedrock prompt
 */
function formatViolationForBedrock(violation) {
  const details = violation.violation_details || {};
  
  return {
    company: violation.company_name || 'Unknown Company',
    fine_amount: violation.fine_amount || 0,
    agency: violation.agency || 'Unknown Agency',
    violation_type: violation.violation_type || 'Unknown',
    description: violation.raw_description || violation.raw_title || 'No description available',
    city: violation.city,
    state: violation.state,
    event_date: violation.event_date,
    ...details
  };
}

/**
 * Query violations from S3 Tables
 */
async function queryViolations(config, con) {
  const { filters } = config;
  const agencies = filters.agencies && filters.agencies.length > 0 
    ? filters.agencies.map(a => a.toLowerCase())
    : AGENCIES;
  
  const allViolations = [];
  
  for (const agency of agencies) {
    try {
      let query = `
        SELECT 
          t.violation_id,
          t.agency,
          t.company_name,
          t.fine_amount,
          t.violation_type,
          t.raw_description,
          t.raw_title,
          t.violation_details,
          t.city,
          t.state,
          t.event_date
        FROM ocean.silver.${agency} t
        LEFT JOIN ocean.silver.bedrock_overlays b ON t.violation_id = b.violation_id
        WHERE 1=1
      `;
      
      const params = [];
      
      // Fine amount filters
      if (filters.minFineAmount) {
        query += ` AND t.fine_amount >= ${filters.minFineAmount}`;
      }
      if (filters.maxFineAmount) {
        query += ` AND t.fine_amount <= ${filters.maxFineAmount}`;
      }
      
      // Date range filters
      if (filters.dateRange?.start) {
        query += ` AND t.event_date >= '${filters.dateRange.start}'`;
      }
      if (filters.dateRange?.end) {
        query += ` AND t.event_date <= '${filters.dateRange.end}'`;
      }
      
      // Violation type filters
      if (filters.violationTypes && filters.violationTypes.length > 0) {
        const types = filters.violationTypes.map(t => `'${t.replace(/'/g, "''")}'`).join(',');
        query += ` AND t.violation_type IN (${types})`;
      }
      if (filters.excludeTypes && filters.excludeTypes.length > 0) {
        const types = filters.excludeTypes.map(t => `'${t.replace(/'/g, "''")}'`).join(',');
        query += ` AND t.violation_type NOT IN (${types})`;
      }
      
      // Only get violations without Bedrock content (from Overlays)
      // Check if bedrock_title is NULL in the joined table
      query += ` AND b.bedrock_title IS NULL`;
      
      query += ` LIMIT 50000`; // Reasonable limit per agency
      
      const reader = await con.run(query);
      const rows = await reader.getRows();
      
      for (const row of rows) {
        allViolations.push({
          violation_id: row[0],
          agency: row[1],
          company_name: row[2],
          fine_amount: row[3],
          violation_type: row[4],
          raw_description: row[5],
          raw_title: row[6],
          violation_details: row[7] ? JSON.parse(row[7]) : {},
          city: row[8],
          state: row[9],
          event_date: row[10]
        });
      }
    } catch (e) {
      console.warn(`⚠️ Error querying ${agency}:`, e.message);
      continue;
    }
  }
  
  return allViolations;
}

/**
 * Create Bedrock batch input file
 */
async function createBatchInputFile(violations, jobId) {
  const { systemPrompt } = BEDROCK_CONFIG;
  
  // Format violations for Bedrock
  const batchInput = violations.map(v => {
    const violationData = formatViolationForBedrock(v);
    const prompt = `${systemPrompt}\n\nINPUT DATA:\n${JSON.stringify(violationData, null, 2)}`;
    
    return {
      customAttributes: {
        violation_id: v.violation_id,
        source_table: `ocean.silver.${v.agency}` // Pass source table for efficient lookup later
      },
      body: JSON.stringify({
        prompt,
        ...BEDROCK_CONFIG.inferenceConfig
      })
    };
  });
  
  // Convert to JSONL format
  const jsonlContent = batchInput.map(item => JSON.stringify(item)).join('\n');
  
  // Upload to S3
  const inputKey = `bedrock-batches/input/${jobId}/input.jsonl`;
  await s3Client.send(new PutObjectCommand({
    Bucket: DATA_BUCKET,
    Key: inputKey,
    Body: jsonlContent,
    ContentType: 'application/jsonl'
  }));
  
  return `s3://${DATA_BUCKET}/${inputKey}`;
}

/**
 * Create Bedrock batch job
 */
async function createBatchJob(inputS3Uri, outputS3Uri, jobId) {
  const command = new CreateModelInvocationJobCommand({
    jobName: `bedrock-batch-${jobId}`,
    roleArn: process.env.BEDROCK_BATCH_ROLE_ARN || 
      'arn:aws:iam::611538926352:role/ventureos-bedrock-batch-job-role',
    modelId: BEDROCK_CONFIG.modelId,
    inferenceConfig: {
      maxTokens: BEDROCK_CONFIG.inferenceConfig.maxTokens,
      temperature: BEDROCK_CONFIG.inferenceConfig.temperature,
      topP: BEDROCK_CONFIG.inferenceConfig.topP
    },
    inputDataConfig: {
      s3Uri: inputS3Uri,
      contentType: 'application/jsonl'
    },
    outputDataConfig: {
      s3Uri: outputS3Uri
    }
  });
  
  const response = await bedrockClient.send(command);
  return response.jobArn;
}

/**
 * Store job metadata in DynamoDB
 */
async function storeJobMetadata(jobId, jobArn, violations, config, estimate) {
  // Determine PK based on agencies filter
  // If single agency: AGENCY#osha#JOB#jobId
  // If multiple/no agencies: ALL_AGENCIES#JOB#jobId
  const agencies = config.filters?.agencies || [];
  let pkPrefix;
  if (agencies.length === 1) {
    pkPrefix = `AGENCY#${agencies[0].toLowerCase()}`;
  } else {
    pkPrefix = 'ALL_AGENCIES';
  }
  
  await docClient.send(new PutCommand({
    TableName: BATCH_JOBS_TABLE,
    Item: {
      PK: `${pkPrefix}#JOB#${jobId}`,
      SK: 'METADATA',
      jobArn,
      status: 'InProgress',
      violationCount: violations.length,
      config,
      estimate,
      agencies: agencies.length > 0 ? agencies : ['all'], // Store for filtering
      createdAt: new Date().toISOString(),
      ttl: Math.floor(Date.now() / 1000) + (30 * 24 * 60 * 60) // 30 days TTL
    }
  }));
}

export const handler = async (event) => {
  console.log('📦 Bedrock Batch Processor Started');
  
  try {
    // Check for dry-run mode (estimation only)
    const isDryRun = event.dryRun === true || event.estimateOnly === true;
    
    // Parse configuration from event
    const config = {
      ...DEFAULT_BATCH_CONFIG,
      ...(event.config || {})
    };
    
    console.log('⚙️ Configuration:', JSON.stringify(config, null, 2));
    if (isDryRun) {
      console.log('💰 DRY RUN MODE: Estimation only, no job will be created');
    }
    
    // Initialize DuckDB
    const instance = await DuckDBInstance.create(':memory:');
    const con = await instance.connect();
    
    try {
      process.env.HOME = '/tmp';
      await con.run("SET temp_directory='/tmp/duckdb_temp'");
      await con.run("SET home_directory='/tmp'");
      await con.run("INSTALL aws; LOAD aws; INSTALL iceberg; LOAD iceberg;");
      await con.run("CREATE SECRET (TYPE S3, PROVIDER credential_chain);");
      await con.run(`ATTACH '${S3_TABLE_BUCKET_ARN}' AS ocean (TYPE iceberg, ENDPOINT_TYPE s3_tables)`);
      
      // Query violations
      console.log('🔍 Querying violations from S3 Tables...');
      let violations = await queryViolations(config, con);
      console.log(`📊 Found ${violations.length} violations`);
      
      // Apply filters
      violations = applyFilters(violations, config);
      console.log(`✅ After filters: ${violations.length} violations`);
      
      // Apply limits
      violations = applyLimits(violations, config);
      console.log(`✅ After limits: ${violations.length} violations`);
      
      if (violations.length === 0) {
        return {
          statusCode: 200,
          body: JSON.stringify({
            message: 'No violations to process',
            violationCount: 0
          })
        };
      }
      
      // Estimate cost
      const estimate = estimateBatchCost(violations.length);
      console.log(`💰 Estimated cost: $${estimate.costUSD.toFixed(2)}`);
      
      // If dry-run, return estimate only
      if (isDryRun) {
        return {
          statusCode: 200,
          body: JSON.stringify({
            message: 'Cost estimation complete',
            violationCount: violations.length,
            estimate,
            dryRun: true
          })
        };
      }
      
      // Generate job ID
      const jobId = `batch-${Date.now()}-${Math.random().toString(36).substring(7)}`;
      
      // Create batch input file
      console.log('📝 Creating batch input file...');
      const inputS3Uri = await createBatchInputFile(violations, jobId);
      
      // Create output S3 URI
      const outputS3Uri = `s3://${DATA_BUCKET}/bedrock-batches/output/${jobId}/`;
      
      // Create Bedrock batch job
      console.log('🚀 Creating Bedrock batch job...');
      const jobArn = await createBatchJob(inputS3Uri, outputS3Uri, jobId);
      console.log(`✅ Batch job created: ${jobArn}`);
      
      // Store job metadata
      await storeJobMetadata(jobId, jobArn, violations, config, estimate);
      
      return {
        statusCode: 200,
        body: JSON.stringify({
          message: 'Batch job created successfully',
          jobId,
          jobArn,
          violationCount: violations.length,
          estimate,
          status: 'InProgress',
          expectedCompletion: new Date(Date.now() + 24 * 60 * 60 * 1000).toISOString()
        })
      };
      
    } finally {
      try {
        if (instance) await instance.terminate();
      } catch (e) {
        console.warn('⚠️ Cleanup note:', e.message);
      }
    }
    
  } catch (error) {
    console.error('❌ Batch Processor Error:', error);
    return {
      statusCode: 500,
      body: JSON.stringify({
        error: error.message
      })
    };
  }
};

