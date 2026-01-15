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
import { DynamoDBDocumentClient, PutCommand, GetCommand, UpdateCommand, QueryCommand } from '@aws-sdk/lib-dynamodb';
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
  
  // Mistral Large uses different parameter names for on-demand invocation
  // Map inferenceConfig to Mistral's expected format
  const mistralBody = {
    prompt,
    temperature: inferenceConfig.temperature,
    max_tokens: inferenceConfig.maxTokens, // Mistral uses max_tokens, not maxTokens
    top_p: inferenceConfig.topP // Mistral uses top_p, not topP
  };
  
  const response = await bedrockClient.send(new InvokeModelCommand({
    modelId,
    body: JSON.stringify(mistralBody),
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
 * Store Bedrock content directly in both Silver and DynamoDB
 * 1. MERGE into Silver S3 Tables (based on violation_id)
 * 2. Update all violation items in DynamoDB that reference this violation (entity PKs)
 * 
 * This eliminates the need for a separate sync Lambda - content exists in both places immediately.
 */
async function storeBedrockContent(violationId, bedrockContent, violationData, con) {
  console.log(`💾 Storing Bedrock content in Silver and DynamoDB...`);
  
  const agency = violationData.agency?.toLowerCase() || 'osha'; // Default to osha if not found
  
  // 1. MERGE directly into Silver S3 Tables
  try {
    console.log(`🔄 Merging bedrock content into ocean.silver.${agency}...`);
    
    // Check if columns exist in Silver table, add if missing
    try {
      const schemaReader = await con.run(`DESCRIBE ocean.silver.${agency}`);
      const schemaRows = await schemaReader.getRows();
      const existingCols = schemaRows.map(row => row[0]);
      
      const requiredCols = [
        { name: 'bedrock_title', type: 'VARCHAR' },
        { name: 'bedrock_description', type: 'VARCHAR' },
        { name: 'bedrock_tags', type: 'VARCHAR[]' },
        { name: 'bedrock_generated_at', type: 'TIMESTAMP' }
      ];
      
      for (const col of requiredCols) {
        if (!existingCols.includes(col.name)) {
          console.log(`➕ Adding missing column ${col.name} to ocean.silver.${agency}...`);
          await con.run(`ALTER TABLE ocean.silver.${agency} ADD COLUMN ${col.name} ${col.type}`);
        }
      }
    } catch (e) {
      console.warn(`⚠️ Failed to check/update schema for ocean.silver.${agency}: ${e.message}`);
    }

    // Create temp table with bedrock content
    await con.run(`DROP TABLE IF EXISTS bedrock_update`);
    await con.run(`
      CREATE TEMP TABLE bedrock_update (
        violation_id VARCHAR,
        bedrock_title VARCHAR,
        bedrock_description VARCHAR,
        bedrock_tags VARCHAR[],
        bedrock_generated_at TIMESTAMP
      )
    `);
    
    // Insert bedrock content
    const esc = (val) => val ? `'${String(val).replace(/'/g, "''")}'` : 'NULL';
    const tags = bedrockContent.tags || [];
    const tagsSql = tags.length > 0 
      ? `[${tags.map(t => `'${String(t).replace(/'/g, "''")}'`).join(',')}]` 
      : 'NULL';
    
    await con.run(`
      INSERT INTO bedrock_update VALUES (
        ${esc(violationId)}::VARCHAR,
        ${esc(bedrockContent.title)}::VARCHAR,
        ${esc(bedrockContent.description)}::VARCHAR,
        ${tagsSql}::VARCHAR[],
        CURRENT_TIMESTAMP::TIMESTAMP
      )
    `);
    
    // MERGE into Silver table
    await con.run(`
      MERGE INTO ocean.silver.${agency} AS main
      USING bedrock_update AS updates
      ON main.violation_id = updates.violation_id
      WHEN MATCHED THEN 
        UPDATE SET 
          bedrock_title = updates.bedrock_title,
          bedrock_description = updates.bedrock_description,
          bedrock_tags = updates.bedrock_tags,
          bedrock_generated_at = updates.bedrock_generated_at
    `);
    
    console.log(`✅ Bedrock content merged into Silver`);
  } catch (e) {
    console.error(`❌ Failed to merge into Silver: ${e.message}`);
    // Continue - still update DynamoDB for immediate frontend availability
  }
  
  // 2. Update all violation items in DynamoDB that reference this violation
  // Query by entity PKs (COMPANY#, CITY#, STATE#) instead of scanning
  // We can construct PKs from violation data we already have
  try {
    const { company_name, company_slug, city, state } = violationData;
    
    // Construct entity PKs (same format as gold-sync/parquet-writer)
    const slugify = (str) => {
      if (!str) return '';
      return str.toLowerCase()
        .replace(/[^a-z0-9]+/g, '-')
        .replace(/^-+|-+$/g, '');
    };
    
    const companySlug = company_slug || slugify(company_name);
    const citySlug = slugify(city);
    const stateUpper = state?.toUpperCase();
    
    const entityPKs = [
      `COMPANY#${companySlug}`,
      `CITY#${citySlug}-${stateUpper}`,
      `STATE#${stateUpper}`
    ].filter(Boolean);
    
    console.log(`🔍 Querying DynamoDB by entity PKs: ${entityPKs.join(', ')}`);
    
    // Query each entity PK to find violation items
    const itemsToUpdate = [];
    for (const pk of entityPKs) {
      try {
        const queryRes = await docClient.send(new QueryCommand({
          TableName: VIOLATIONS_TABLE,
          KeyConditionExpression: 'PK = :pk',
          ExpressionAttributeValues: {
            ':pk': pk
          },
          FilterExpression: 'violation_id = :vid',
          ExpressionAttributeValues: {
            ':pk': pk,
            ':vid': violationId
          }
        }));
        
        if (queryRes.Items && queryRes.Items.length > 0) {
          itemsToUpdate.push(...queryRes.Items.filter(item => item.SK !== 'BEDROCK_CONTENT'));
        }
      } catch (e) {
        console.warn(`⚠️ Failed to query ${pk}: ${e.message}`);
      }
    }
    
    console.log(`📋 Found ${itemsToUpdate.length} violation items to update`);
    
    // Update each violation item to include bedrock content
    for (const item of itemsToUpdate) {
      await docClient.send(new UpdateCommand({
        TableName: VIOLATIONS_TABLE,
        Key: {
          PK: item.PK,
          SK: item.SK
        },
        UpdateExpression: 'SET bedrock_title = :title, bedrock_description = :desc, attribution = :attr',
        ExpressionAttributeValues: {
          ':title': bedrockContent.title,
          ':desc': bedrockContent.description,
          ':attr': 'Data synthesized by SirSluginston VentureOS'
        }
      }));
    }
    
    if (itemsToUpdate.length > 0) {
      console.log(`✅ Updated ${itemsToUpdate.length} violation items in DynamoDB with bedrock content`);
    } else {
      console.log(`ℹ️ No violation items found to update in DynamoDB (may not be in recent5 yet)`);
    }
  } catch (e) {
    console.warn(`⚠️ Failed to update DynamoDB items: ${e.message}`);
  }
  
  console.log(`✅ Bedrock content stored in both Silver and DynamoDB`);
  
  return {
    violation_id: violationId,
    bedrock_title: bedrockContent.title,
    bedrock_description: bedrockContent.description,
    attribution: 'Data synthesized by SirSluginston VentureOS'
  };
}

export const handler = async (event) => {
  console.log('🤖 Bedrock Generator Lambda Started');
  
  // Parse request body (outside try so it's accessible in catch)
  let body;
  let violationId;
  
  try {
    // Parse request body
    if (typeof event.body === 'string') {
      body = JSON.parse(event.body);
    } else {
      body = event.body || {};
    }
    
    violationId = body.violation_id || event.pathParameters?.violation_id;
    
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
    
    // Check if Bedrock content already exists in overlay
    let existingOverlay = null;
    let syncedToItems = false;
    try {
      const existing = await docClient.send(new GetCommand({
        TableName: VIOLATIONS_TABLE,
        Key: {
          PK: `VIOLATION#${violationId}`,
          SK: 'BEDROCK_CONTENT'
        }
      }));
      
      if (existing.Item) {
        existingOverlay = existing.Item;
        console.log(`✅ Bedrock content exists in overlay for ${violationId}`);
        
        // Check if it's also in violation items (entity PKs)
        // If not, sync it to violation items (legacy content from before dual-write)
        try {
          console.log(`🔍 Checking for violation items with violation_id: ${violationId}`);
          
          // Try scanning with both string and number formats (violation_id might be stored as either)
          // Also try without ProjectionExpression to see all fields
          const scanRes = await docClient.send(new ScanCommand({
            TableName: VIOLATIONS_TABLE,
            FilterExpression: 'violation_id = :vid',
            ExpressionAttributeValues: {
              ':vid': violationId
            }
            // Don't use ProjectionExpression - get all fields to debug
          }));
          
          console.log(`🔍 Scan raw result:`, JSON.stringify(scanRes, null, 2).substring(0, 500));
          
          console.log(`📊 Scan found ${scanRes.Items?.length || 0} items total`);
          const violationItems = (scanRes.Items || []).filter(item => item.SK !== 'BEDROCK_CONTENT');
          console.log(`📋 Found ${violationItems.length} violation items (excluding overlay)`);
          
          const hasBedrockInItems = violationItems.some(item => item.bedrock_title);
          console.log(`🔍 Has bedrock in items: ${hasBedrockInItems}`);
          
          if (violationItems.length > 0 && !hasBedrockInItems) {
            console.log(`🔄 Syncing existing bedrock content to ${violationItems.length} violation items...`);
            console.log(`📝 Bedrock title: ${existingOverlay.bedrock_title}`);
            console.log(`📝 Bedrock description length: ${(existingOverlay.bedrock_description || '').length}`);
            
            // Sync to violation items
            let syncedCount = 0;
            for (const item of violationItems) {
              try {
                const bTitle = existingOverlay.bedrock_title;
                const bDesc = existingOverlay.bedrock_description;
                
                await docClient.send(new UpdateCommand({
                  TableName: VIOLATIONS_TABLE,
                  Key: {
                    PK: item.PK,
                    SK: item.SK
                  },
                  UpdateExpression: 'SET bedrock_title = :title, bedrock_description = :desc, attribution = :attr',
                  ExpressionAttributeValues: {
          ':title': bTitle,
          ':desc': bDesc,
          ':attr': existingOverlay.attribution || 'Data synthesized by SirSluginston VentureOS'
        }
                }));
                syncedCount++;
                console.log(`✅ Updated ${item.PK}/${item.SK}`);
              } catch (e) {
                console.error(`❌ Failed to update ${item.PK}/${item.SK}: ${e.message}`);
              }
            }
            
            console.log(`✅ Synced bedrock content to ${syncedCount}/${violationItems.length} violation items`);
            syncedToItems = syncedCount > 0;
          } else if (hasBedrockInItems) {
            console.log(`✅ Bedrock content already in violation items`);
            syncedToItems = true;
          } else if (violationItems.length === 0) {
            console.log(`ℹ️ No violation items found in DynamoDB (may not be in recent5 yet)`);
          }
        } catch (e) {
          console.error(`❌ Failed to sync to violation items: ${e.message}`);
          console.error(`Stack: ${e.stack}`);
        }
        
        return {
          statusCode: 200,
          headers: {
            'Access-Control-Allow-Origin': '*',
            'Content-Type': 'application/json'
          },
          body: JSON.stringify({
            message: 'Content already exists',
            violation_id: violationId,
            synced_to_items: syncedToItems,
            ...existingOverlay
          })
        };
      }
    } catch (e) {
      // Continue if check fails
    }
    
    // Initialize DuckDB to query S3 Tables
    let instance;
    let con;
    
    try {
      instance = await DuckDBInstance.create(':memory:');
      con = await instance.connect();
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
      
      // Store directly in both Silver and DynamoDB (no sync Lambda needed)
      const stored = await storeBedrockContent(violationId, bedrockContent, violationData, con);
      
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
      // Close DuckDB instance (connection is managed by instance)
      try {
        if (instance) {
          instance.closeSync();
        }
      } catch (e) {
        console.warn('Warning: Error closing DuckDB instance:', e);
      }
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
        violation_id: violationId || event.pathParameters?.violation_id || 'unknown'
      })
    };
  }
};

