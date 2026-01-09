/**
 * Bedrock Content Generator Lambda
 * 
 * Purpose: Generate AI summaries for violations on-demand (lazy overlay)
 * Trigger: API endpoint POST /api/bedrock/generate
 * 
 * Process:
 * 1. Receive violation_id from API request
 * 2. Query S3 Tables to fetch violation data
 * 3. Call Bedrock API with violation data
 * 4. Store result in DynamoDB (triggers sync Lambda automatically)
 */

import { BedrockRuntimeClient, InvokeModelCommand } from '@aws-sdk/client-bedrock-runtime';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, PutCommand, GetCommand } from '@aws-sdk/lib-dynamodb';
import { DuckDBInstance } from '@duckdb/node-api';
import { BEDROCK_CONFIG } from './bedrock-production-config.js';

const bedrockClient = new BedrockRuntimeClient({ region: process.env.AWS_REGION || 'us-east-1' });
const dynamoClient = new DynamoDBClient({ region: process.env.AWS_REGION || 'us-east-1' });
const docClient = DynamoDBDocumentClient.from(dynamoClient);

const S3_TABLE_BUCKET_ARN = process.env.S3_TABLE_BUCKET_ARN || 
  'arn:aws:s3tables:us-east-1:611538926352:bucket/sirsluginston-ventureos-data-ocean';
const VIOLATIONS_TABLE = process.env.VIOLATIONS_TABLE || 'VentureOS-Violations';

// Common agencies to check (in order of likelihood)
const AGENCIES = ['osha', 'msha', 'nhtsa', 'faa', 'uscg', 'fra', 'epa'];

/**
 * Fetch violation data from S3 Tables by violation_id
 */
async function fetchViolationData(violationId, con) {
  // Try each agency table until we find the violation
  for (const agency of AGENCIES) {
    try {
      const query = `
        SELECT 
          violation_id,
          agency,
          company_name,
          fine_amount,
          violation_type,
          raw_description,
          raw_title,
          violation_details,
          city,
          state,
          event_date
        FROM ocean.silver.${agency}
        WHERE violation_id = '${violationId.replace(/'/g, "''")}'
        LIMIT 1
      `;
      
      const reader = await con.run(query);
      const rows = await reader.getRows();
      
      if (rows && rows.length > 0) {
        const row = rows[0];
        return {
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
        };
      }
    } catch (e) {
      // Table might not exist or violation not found, try next agency
      continue;
    }
  }
  
  return null;
}

/**
 * Format violation data for Bedrock prompt
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
    ...details // Include any additional details from violation_details JSON
  };
}

/**
 * Call Bedrock API to generate content
 */
async function generateBedrockContent(violationData) {
  const { modelId, inferenceConfig, systemPrompt } = BEDROCK_CONFIG;
  
  // Format violation data as JSON string for prompt
  const violationJson = JSON.stringify(formatViolationForBedrock(violationData), null, 2);
  const prompt = `${systemPrompt}\n\nINPUT DATA:\n${violationJson}`;
  
  console.log(`🤖 Calling Bedrock (${modelId})...`);
  
  const response = await bedrockClient.send(new InvokeModelCommand({
    modelId,
    body: JSON.stringify({
      prompt,
      ...inferenceConfig
    }),
    contentType: 'application/json',
    accept: 'application/json'
  }));
  
  const responseBody = JSON.parse(new TextDecoder().decode(response.body));
  
  // Mistral returns content in 'outputs' array
  const content = responseBody.outputs?.[0]?.text || responseBody.text || '';
  
  // Parse JSON from response (Bedrock should return JSON)
  try {
    // Extract JSON from markdown code blocks if present
    const jsonMatch = content.match(/```json\s*([\s\S]*?)\s*```/) || content.match(/\{[\s\S]*\}/);
    const jsonStr = jsonMatch ? jsonMatch[1] || jsonMatch[0] : content;
    return JSON.parse(jsonStr);
  } catch (e) {
    console.error('⚠️ Failed to parse Bedrock response as JSON:', content);
    // Fallback: try to extract title and description manually
    return {
      title: violationData.raw_title || 'Violation Summary',
      description: content.substring(0, 500),
      tags: []
    };
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
    attribution: 'Generated by AWS Bedrock'
  };
  
  console.log(`💾 Storing Bedrock content in DynamoDB...`);
  
  await docClient.send(new PutCommand({
    TableName: VIOLATIONS_TABLE,
    Item: item
  }));
  
  // This will automatically trigger the sync Lambda via DynamoDB Streams!
  console.log(`✅ Bedrock content stored. Sync Lambda will process automatically.`);
  
  return item;
}

export const handler = async (event) => {
  console.log('🤖 Bedrock Generator Lambda Started');
  
  try {
    // Parse request body
    let body;
    if (typeof event.body === 'string') {
      body = JSON.parse(event.body);
    } else {
      body = event.body || {};
    }
    
    const violationId = body.violation_id || event.pathParameters?.violation_id;
    
    if (!violationId) {
      return {
        statusCode: 400,
        headers: {
          'Access-Control-Allow-Origin': '*',
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({ error: 'violation_id is required' })
      };
    }
    
    console.log(`📋 Processing violation: ${violationId}`);
    
    // Check if Bedrock content already exists
    try {
      const existing = await docClient.send(new GetCommand({
        TableName: VIOLATIONS_TABLE,
        Key: {
          PK: `VIOLATION#${violationId}`,
          SK: 'BEDROCK_CONTENT'
        }
      }));
      
      if (existing.Item) {
        console.log(`✅ Bedrock content already exists for ${violationId}`);
        return {
          statusCode: 200,
          headers: {
            'Access-Control-Allow-Origin': '*',
            'Content-Type': 'application/json'
          },
          body: JSON.stringify({
            message: 'Content already exists',
            violation_id: violationId,
            ...existing.Item
          })
        };
      }
    } catch (e) {
      // Continue if check fails
    }
    
    // Initialize DuckDB to query S3 Tables
    const instance = await DuckDBInstance.create(':memory:');
    const con = await instance.connect();
    
    try {
      // Set up Lambda environment
      process.env.HOME = '/tmp';
      await con.run("SET temp_directory='/tmp/duckdb_temp'");
      await con.run("SET home_directory='/tmp'");
      await con.run("INSTALL aws; LOAD aws; INSTALL iceberg; LOAD iceberg;");
      await con.run("CREATE SECRET (TYPE S3, PROVIDER credential_chain);");
      await con.run(`ATTACH '${S3_TABLE_BUCKET_ARN}' AS ocean (TYPE iceberg, ENDPOINT_TYPE s3_tables)`);
      
      // Fetch violation data
      console.log(`🔍 Fetching violation data from S3 Tables...`);
      const violationData = await fetchViolationData(violationId, con);
      
      if (!violationData) {
        return {
          statusCode: 404,
          headers: {
            'Access-Control-Allow-Origin': '*',
            'Content-Type': 'application/json'
          },
          body: JSON.stringify({ error: `Violation ${violationId} not found` })
        };
      }
      
      console.log(`✅ Found violation: ${violationData.agency} - ${violationData.company_name}`);
      
      // Generate Bedrock content
      const bedrockContent = await generateBedrockContent(violationData);
      
      // Store in DynamoDB (triggers sync Lambda automatically)
      const stored = await storeBedrockContent(violationId, bedrockContent);
      
      return {
        statusCode: 200,
        headers: {
          'Access-Control-Allow-Origin': '*',
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          message: 'Bedrock content generated successfully',
          violation_id: violationId,
          ...stored
        })
      };
      
    } finally {
      await con.close();
      instance.closeSync();
    }
    
  } catch (error) {
    console.error('❌ Bedrock Generator Error:', error);
    return {
      statusCode: 500,
      headers: {
        'Access-Control-Allow-Origin': '*',
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({ 
        error: error.message,
        violation_id: body?.violation_id || event.pathParameters?.violation_id
      })
    };
  }
};

