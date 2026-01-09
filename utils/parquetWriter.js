/**
 * Parquet Writer Utility (VentureOS)
 * 
 * Converts JSON violations to Parquet format using DuckDB
 * Used by: Batch Processor Lambda (Bronze → Silver layer)
 * 
 * CRITICAL REQUIREMENTS:
 * - event_date stored as DATE type (not string) for fast Athena range scans
 * - fine_amount stored as DOUBLE (not string) for accurate math
 * - Uses /tmp for DuckDB temp files (Lambda requirement)
 * - Uses IAM role credentials (no hardcoded keys)
 */

import { DuckDBInstance } from '@duckdb/node-api';
import { writeFile, unlink } from 'fs/promises';
import { getDuckDBSchema } from './schema.js';
import { tmpdir } from 'os';
import { join } from 'path';
import { DynamoDBClient, PutItemCommand, QueryCommand, DeleteItemCommand } from '@aws-sdk/client-dynamodb';

const ddbClient = new DynamoDBClient({ region: process.env.AWS_REGION || 'us-east-1' });

/**
 * Initialize DuckDB with Lambda-safe configuration
 * @returns {Promise<{db: DuckDBInstance, con: any}>} Configured DuckDB instance and connection
 */
async function initDuckDB() {
  console.log('🦆 Initializing DuckDB (Neo Client)...');
  
  // Detect environment (Lambda vs Local)
  const isLambda = process.env.AWS_LAMBDA_FUNCTION_NAME !== undefined;
  const tempDir = isLambda ? '/tmp' : tmpdir();
  const duckDBTemp = isLambda ? '/tmp/duckdb_temp' : join(tempDir, 'duckdb_temp');
  const safeHome = tempDir.replace(/\\/g, '/');

  // Set process.env.HOME for extensions that might check it
  if (isLambda) {
    process.env.HOME = tempDir;
  }

  console.log(`📂 Using temp directory: ${tempDir}`);

  // FIX: Force set HOME env var for extensions that rely on it (like avro/iceberg)
  if (isLambda) {
    process.env.HOME = tempDir;
  }
  
  // Create database instance
  const db = await DuckDBInstance.create(':memory:');
  const con = await db.connect();
  
  // Set temp directory (redundant but safe)
  await con.run(`SET temp_directory='${duckDBTemp}'`);
  
  // Set home directory (redundant but safe)
  await con.run(`SET home_directory='${safeHome}'`);
  
  // Install and load AWS extensions (httpfs + aws for S3 Tables/Iceberg)
  await con.run("INSTALL aws; LOAD aws;");
  
  // Install and load Iceberg extension
  await con.run("INSTALL iceberg; LOAD iceberg;");

  // Create Secret (Critical for S3 Tables IAM Auth)
  if (isLambda) {
    // In Lambda, 'credential_chain' uses the Execution Role automatically
    await con.run("CREATE SECRET (TYPE S3, PROVIDER credential_chain);");
    console.log('✅ Created S3 Secret (Credential Chain)');
  } else {
    // Local: Also use credential_chain if AWS CLI is configured
    await con.run("CREATE SECRET (TYPE S3, PROVIDER credential_chain);");
    console.log('⚠️  Running locally (using credential_chain)');
  }
  
  return { db, con };
}

/**
 * Convert violations array to Parquet file in S3 Table Bucket (Iceberg format)
 * and optionally sync recent items to DynamoDB (Top-N strategy)
 */
export async function convertToParquet(violations, outputPath, options = {}) {
  console.log(`📝 Converting ${violations.length} violations...`);
  console.log(`📂 Output: ${outputPath}`);
  
  const { db, con } = await initDuckDB();
  
  // --- DYNAMODB SYNC (TOP-N) ---
  if (options.writeToDynamo) {
      console.log('⚡ Starting DynamoDB Sync (Top-N)...');
      await syncToDynamo(violations, options.type);
  } else {
      console.log('zz DynamoDB Sync Skipped (Configured)');
  }
  
  // Use OS temp dir for the intermediate JSON file
  const isLambda = process.env.AWS_LAMBDA_FUNCTION_NAME !== undefined;
  const tempDir = isLambda ? '/tmp' : tmpdir();
  const tempJsonPath = join(tempDir, 'temp_violations.json');
  
  // Normalize path for SQL
  const sqlJsonPath = tempJsonPath.replace(/\\/g, '/');
  
  try {
    // Write violations to a temp JSON file
    await writeFile(tempJsonPath, JSON.stringify(violations));
    
    // Create temporary table with explicit schema enforcement
    const schema = getDuckDBSchema();
    
    // Helper to format schema for SQL interpolation
    const schemaEntries = Object.entries(schema)
      .map(([col, type]) => `${col}: '${type}'`)
      .join(',\n        ');

    await con.run(`
      CREATE TEMPORARY TABLE violations AS
      SELECT *
      FROM read_json('${sqlJsonPath}', columns={
        ${schemaEntries}
      })
    `);
    
    console.log('✅ Temporary table created');
    
    // Check if output is S3 Table ARN or File Path
    if (outputPath.startsWith('arn:aws:s3tables:')) {
       // S3 Tables Logic
       console.log(`➡️ Detected S3 Table ARN: ${outputPath}`);
       
       // Extract Bucket ARN and Table details
       // Format: arn:aws:s3tables:region:account:bucket/bucket-name/table/namespace/table-name
       const bucketArnMatch = outputPath.match(/(arn:aws:s3tables:[^:]+:[^:]+:bucket\/[^/]+)/);
       if (!bucketArnMatch) {
           throw new Error(`Invalid S3 Table ARN format. Could not extract Bucket ARN from: ${outputPath}`);
       }
       const bucketArn = bucketArnMatch[1];
       
       const tableMatch = outputPath.match(/\/table\/([^/]+)\/([^/]+)$/);
       if (!tableMatch) {
           throw new Error(`Invalid S3 Table ARN format. Could not extract Namespace/Table from: ${outputPath}`);
       }
       const namespace = tableMatch[1];
       const tableName = tableMatch[2];
       
       console.log(`➡️ Attaching Bucket: ${bucketArn}`);
       // Use s3_tables endpoint type for Iceberg on S3 Tables
       await con.run(`ATTACH '${bucketArn}' AS ventureos_lake (TYPE iceberg, ENDPOINT_TYPE s3_tables)`);
       
       const targetTable = `ventureos_lake.${namespace}.${tableName}`;
       console.log(`➡️ Target table: ${targetTable}`);
       
       // Check if table exists and get its schema
       let tableExists = false;
       let existingColumns = [];
       
       try {
         // Query table structure - con.run() returns a reader for SELECT queries
         const schemaReader = await con.run(`DESCRIBE ${targetTable}`);
         const schemaRows = await schemaReader.getRows();
         // DESCRIBE returns rows as arrays: [column_name, type, null, key, default, extra]
         existingColumns = schemaRows.map(row => row[0]); // First column is column_name
         tableExists = true;
         console.log(`✅ Table ${targetTable} exists with ${existingColumns.length} columns: ${existingColumns.join(', ')}`);
       } catch (err) {
         if (err.message.includes('does not exist') || err.message.includes('Catalog Error')) {
           tableExists = false;
           console.log(`ℹ️ Table ${targetTable} does not exist, will create`);
         } else {
           // Some other error, rethrow
           throw err;
         }
       }
       
       if (!tableExists) {
         // Create table if it doesn't exist
         console.log(`📝 Creating table: ${targetTable}`);
         await con.run(`CREATE TABLE ${targetTable} AS SELECT * FROM violations`);
         console.log(`✅ Created and populated new Iceberg table`);
       } else {
         // Table exists, insert only matching columns
         console.log(`➡️ Inserting into existing table: ${targetTable}`);
         
         // Get columns from violations temp table
         const violationsReader = await con.run(`DESCRIBE violations`);
         const violationsRows = await violationsReader.getRows();
         const violationsColumns = violationsRows.map(row => row[0]); // First column is column_name
         
         // Find columns that exist in both tables
         const matchingColumns = violationsColumns.filter(col => existingColumns.includes(col));
         
         if (matchingColumns.length === 0) {
           throw new Error(`No matching columns between violations table and ${targetTable}`);
         }
         
         console.log(`📋 Inserting ${matchingColumns.length} matching columns: ${matchingColumns.join(', ')}`);
         
         const columnList = matchingColumns.join(', ');
         await con.run(`INSERT INTO ${targetTable} (${columnList}) SELECT ${columnList} FROM violations`);
         console.log(`✅ Appended to S3 Table`);
       }
       
       // --- GOLD LAYER AGGREGATION ---
       // Aggregates statistics immediately after Silver ingestion.
       // This uses the "Delta/Incremental" pattern: we only insert the *new* stats from this batch.
       // The Gold tables will grow, but compaction will handle optimization.
       
       const goldNamespace = 'gold'; // Default Gold namespace
       
       try {
         console.log('🥇 Processing Gold Layer (Aggregates)...');
         
         // 1. Company Stats
         // Incremental insert: calculates count/sum for THIS batch only
         const goldCompanyTable = `ventureos_lake.${goldNamespace}.company_stats`;
         
         console.log(`🥇 Aggregating for: ${goldCompanyTable}`);
         
         // Create the aggregation from the current batch (temp table 'violations')
         const companyAggQuery = `
           SELECT 
             company_name,
             company_slug,
             COUNT(*) as violation_count,
             SUM(fine_amount) as total_fines,
             MAX(event_date) as last_violation_date,
             LIST(distinct agency) as agency
           FROM violations
           GROUP BY company_name, company_slug
         `;
         
         // Check if table exists (lazy check via try/catch insert)
         try {
            await con.run(`INSERT INTO ${goldCompanyTable} ${companyAggQuery}`);
            console.log(`✅ Appended to Gold: company_stats`);
         } catch (e) {
            if (e.message.includes('not exist')) {
                // First time creation
                await con.run(`CREATE TABLE ${goldCompanyTable} AS ${companyAggQuery}`);
                console.log(`✅ Created Gold Table: company_stats`);
            } else {
                console.warn(`⚠️ Gold Aggregation Failed (Company): ${e.message}`);
            }
         }
         
         // 2. City Stats
         const goldCityTable = `ventureos_lake.${goldNamespace}.city_stats`;
         const cityAggQuery = `
           SELECT 
             city,
             state,
             COUNT(*) as violation_count,
             SUM(fine_amount) as total_fines
           FROM violations
           WHERE city IS NOT NULL AND city != ''
           GROUP BY city, state
         `;
         
         try {
            await con.run(`INSERT INTO ${goldCityTable} ${cityAggQuery}`);
            console.log(`✅ Appended to Gold: city_stats`);
         } catch (e) {
             if (e.message.includes('not exist')) {
                await con.run(`CREATE TABLE ${goldCityTable} AS ${cityAggQuery}`);
                console.log(`✅ Created Gold Table: city_stats`);
             } else {
                console.warn(`⚠️ Gold Aggregation Failed (City): ${e.message}`);
             }
         }

         // 3. State Stats
         const goldStateTable = `ventureos_lake.${goldNamespace}.state_stats`;
         const stateAggQuery = `
           SELECT 
             state,
             COUNT(*) as violation_count,
             SUM(fine_amount) as total_fines
           FROM violations
           WHERE state IS NOT NULL AND state != ''
           GROUP BY state
         `;

         try {
            await con.run(`INSERT INTO ${goldStateTable} ${stateAggQuery}`);
            console.log(`✅ Appended to Gold: state_stats`);
         } catch (e) {
             if (e.message.includes('not exist')) {
                await con.run(`CREATE TABLE ${goldStateTable} AS ${stateAggQuery}`);
                console.log(`✅ Created Gold Table: state_stats`);
             } else {
                console.warn(`⚠️ Gold Aggregation Failed (State): ${e.message}`);
             }
         }
         
       } catch (goldError) {
           // Don't fail the whole Lambda if Gold fails (Silver is safe)
           console.error(`❌ Gold Layer Error: ${goldError.message}`);
       }

    } else {
       // Standard Parquet File Write
       console.log(`➡️ Writing to Parquet file: ${outputPath}`);
       await con.run(`COPY violations TO '${outputPath}' (FORMAT PARQUET, COMPRESSION SNAPPY)`);
       console.log(`✅ Wrote Parquet file`);
    }
    
    return {
      rowCount: violations.length,
      fileSize: 0
    };
    
  } finally {
    try { await unlink(tempJsonPath); } catch (e) {}
    // Neo client cleanup if needed, usually db/con just go out of scope or have close()
    // await con.close();
    // await db.close();
  }
}

/**
 * Sync violations to DynamoDB with Top-N Logic
 */
async function syncToDynamo(violations, type) {
    const TABLE_NAME = 'VentureOS-Violations';
    const MAX_ITEMS = 5; // Top-5
    
    // Extract agency from type (e.g. 'osha-severe' -> 'OSHA')
    const agency = type ? type.split('-')[0].toUpperCase() : 'UNKNOWN';

    for (const v of violations) {
        // Construct Keys
        // PK: ENTITY#<slug> (Company, City, State)
        // We write 3 items per violation so it appears in Company, City, and State feeds
        
        const entities = [
            { pk: `COMPANY#${v.company_slug}`, type: 'Company' },
            { pk: `CITY#${slugify(v.city)}-${v.state}`, type: 'City' }, // e.g. CITY#austin-tx
            { pk: `STATE#${v.state}`, type: 'State' }
        ];

        const itemDate = v.event_date || new Date().toISOString().split('T')[0];
        const sk = `AGENCY#${agency.toLowerCase()}#DATE#${itemDate}#${v.violation_id}`;
        
        // Prepare Item Payload (Sparse)
        const item = {
            PK: { S: '' }, // Placeholder
            SK: { S: sk },
            violation_id: { S: v.violation_id },
            company_name: { S: v.company_name },
            city: { S: v.city },
            state: { S: v.state },
            fine: { N: (v.fine_amount || 0).toString() },
            type: { S: v.violation_type },
            raw_desc: { S: v.raw_description ? v.raw_description.substring(0, 200) : '' } // Brief snippet
        };

        for (const entity of entities) {
            item.PK = { S: entity.pk };
            
            try {
                // 1. Put Item
                await ddbClient.send(new PutItemCommand({
                    TableName: TABLE_NAME,
                    Item: item
                }));
                
                // 2. Trim (Check Count)
                // Query items for this Entity + Agency (SK begins_with AGENCY#osha)
                // Limit to MAX_ITEMS + 1 to check if we need to delete
                const queryCmd = new QueryCommand({
                    TableName: TABLE_NAME,
                    KeyConditionExpression: 'PK = :pk AND begins_with(SK, :prefix)',
                    ExpressionAttributeValues: {
                        ':pk': { S: entity.pk },
                        ':prefix': { S: `AGENCY#${agency.toLowerCase()}` }
                    },
                    ScanIndexForward: false, // Descending (Newest first)
                    // We just need the keys to delete
                    ProjectionExpression: 'PK, SK' 
                });
                
                const qRes = await ddbClient.send(queryCmd);
                const items = qRes.Items || [];
                
                if (items.length > MAX_ITEMS) {
                    // Delete everything after index 4 (5th item)
                    const toDelete = items.slice(MAX_ITEMS);
                    for (const delItem of toDelete) {
                        console.log(`✂️ Trimming old item: ${delItem.PK.S} / ${delItem.SK.S}`);
                        await ddbClient.send(new DeleteItemCommand({
                            TableName: TABLE_NAME,
                            Key: { PK: delItem.PK, SK: delItem.SK }
                        }));
                    }
                }
                
            } catch (e) {
                console.warn(`⚠️ DynamoDB Sync Failed for ${entity.pk}: ${e.message}`);
            }
        }
    }
}

// Helper slugify (duplicate of osha.js logic, but safe to have here)
function slugify(str) {
  if (!str) return '';
  return str.toLowerCase().trim()
    .replace(/[^\w\s-]/g, '')
    .replace(/[\s_-]+/g, '-')
    .replace(/^-+|-+$/g, '');
}

/**
 * Test function (local development only)
 * Usage: node VentureOS/utils/parquetWriter.js
 */
async function testLocal() {
  console.log('🧪 Running local test...');
  
  // Sample violations for testing
  const violations = [
    {
      agency: 'OSHA',
      state: 'TX',
      city: 'Austin',
      company_name: 'Test Company Inc',
      company_slug: 'test-company-inc',
      event_date: '2024-01-15',
      fine_amount: 50000,
      violation_type: 'Serious',
      violation_id: 'OSHA-2024-001',
      source_url: 'https://osha.gov/...',
      raw_title: 'Fall hazard violation',
      raw_description: 'Worker fell from height due to lack of fall protection equipment.',
      bedrock_title: '$50K Fine: Austin Company Cited for Fatal Fall Hazard',
      bedrock_description: 'OSHA issued a $50,000 fine after a worker fell from height. The company failed to provide fall protection equipment.',
      tags: ['fall-protection', 'construction', 'serious']
    },
    {
      agency: 'OSHA',
      state: 'CA',
      city: 'Los Angeles',
      company_name: 'Another Company LLC',
      company_slug: 'another-company-llc',
      event_date: '2024-02-20',
      fine_amount: 125000,
      violation_type: 'Willful',
      violation_id: 'OSHA-2024-002',
      source_url: 'https://osha.gov/...',
      raw_title: 'Chemical exposure violation',
      raw_description: 'Workers exposed to hazardous chemicals without proper PPE.',
      bedrock_title: '$125K Fine: LA Company Exposed Workers to Toxic Chemicals',
      bedrock_description: 'OSHA issued a $125,000 fine after workers were exposed to hazardous chemicals without PPE.',
      tags: ['chemical-exposure', 'ppe', 'willful']
    }
  ];
  
  // For local testing, we'll use a local path (DuckDB can write to local files too)
  // In production, this would be an S3 Tables table name
  const outputPath = './test-output.parquet';
  
  try {
    // Note: Local test writes to file, not S3 Table
    // In production Lambda, outputPath would be S3 Tables table name
    await convertToParquet(violations, outputPath);
    console.log('✅ Local test completed successfully!');
    console.log(`📂 Check file: ${outputPath}`);
    console.log('⚠️  Note: Production will use S3 Tables ARN, not local file path');
  } catch (error) {
    console.error('❌ Local test failed:', error);
    process.exit(1);
  }
}

// Run test if executed directly
if (import.meta.url.startsWith('file:')) {
  const modulePath = new URL(import.meta.url).pathname;
  if (process.argv[1] === modulePath || process.argv[1].endsWith('parquetWriter.js')) {
    testLocal().catch(console.error);
  }
}

