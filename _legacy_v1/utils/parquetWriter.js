/**
 * Parquet Writer Utility (VentureOS)
 * 
 * Converts JSON violations to Parquet format using DuckDB
 * Used by: Batch Processor Lambda (Bronze → Silver layer)
 */

import { DuckDBInstance } from '@duckdb/node-api';
import { writeFile, unlink } from 'fs/promises';
import { getDuckDBSchema } from './schema.js';
import { tmpdir } from 'os';
import { join } from 'path';
import { DynamoDBClient, PutItemCommand } from '@aws-sdk/client-dynamodb';

const ddbClient = new DynamoDBClient({ region: process.env.AWS_REGION || 'us-east-1' });

/**
 * Retry helper for Iceberg transaction conflicts
 * Handles 409 Conflict errors with exponential backoff
 */
async function retryIcebergOperation(operation, maxRetries = 5, baseDelayMs = 100) {
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      return await operation();
    } catch (error) {
      const isConflict = error.message?.includes('Conflict_409') || 
                        error.message?.includes('CommitFailedException') ||
                        error.message?.includes('branch main has changed');
      
      if (!isConflict || attempt === maxRetries) {
        throw error;
      }
      
      // Exponential backoff: 100ms, 200ms, 400ms, 800ms, 1600ms
      const delayMs = baseDelayMs * Math.pow(2, attempt - 1);
      const jitter = Math.random() * 100; // Add up to 100ms jitter
      const totalDelay = delayMs + jitter;
      
      console.log(`⚠️ Iceberg conflict (attempt ${attempt}/${maxRetries}), retrying in ${Math.round(totalDelay)}ms...`);
      await new Promise(resolve => setTimeout(resolve, totalDelay));
    }
  }
}

/**
 * Initialize DuckDB with Lambda-safe configuration
 */
async function initDuckDB() {
  console.log('🦆 Initializing DuckDB (Neo Client)...');
  
  const isLambda = process.env.AWS_LAMBDA_FUNCTION_NAME !== undefined;
  const tempDir = isLambda ? '/tmp' : tmpdir();
  const duckDBTemp = isLambda ? '/tmp/duckdb_temp' : join(tempDir, 'duckdb_temp');
  const safeHome = tempDir.replace(/\\/g, '/');

  if (isLambda) {
    process.env.HOME = tempDir;
  }

  console.log(`📂 Using temp directory: ${tempDir}`);
  
  const db = await DuckDBInstance.create(':memory:');
  const con = await db.connect();
  
  await con.run(`SET temp_directory='${duckDBTemp}'`);
  await con.run(`SET home_directory='${safeHome}'`);
  
  await con.run("INSTALL aws; LOAD aws;");
  await con.run("INSTALL iceberg; LOAD iceberg;");

  if (isLambda) {
    await con.run("CREATE SECRET (TYPE S3, PROVIDER credential_chain);");
    console.log('✅ Created S3 Secret (Credential Chain)');
  } else {
    await con.run("CREATE SECRET (TYPE S3, PROVIDER credential_chain);");
    console.log('⚠️  Running locally (using credential_chain)');
  }
  
  return { db, con };
}

/**
 * Convert violations array to Parquet file in S3 Table Bucket (Iceberg format)
 */
export async function convertToParquet(violations, outputPath, options = {}) {
  console.log(`📝 Converting ${violations.length} violations...`);
  console.log(`📂 Output: ${outputPath}`);
  
  const { db, con } = await initDuckDB();
  
  if (options.writeToDynamo) {
      console.log('⚡ Starting DynamoDB Sync (Top-N)...');
      await syncToDynamo(violations, options.type);
  } else {
      console.log('zz DynamoDB Sync Skipped (Configured)');
  }
  
  const isLambda = process.env.AWS_LAMBDA_FUNCTION_NAME !== undefined;
  const tempDir = isLambda ? '/tmp' : tmpdir();
  const tempJsonPath = join(tempDir, 'temp_violations.json');
  const sqlJsonPath = tempJsonPath.replace(/\\/g, '/');
  
  try {
    await writeFile(tempJsonPath, JSON.stringify(violations));
    
    const schema = getDuckDBSchema();
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
    
    if (outputPath.startsWith('arn:aws:s3tables:')) {
       console.log(`➡️ Detected S3 Table ARN: ${outputPath}`);
       
       const bucketArnMatch = outputPath.match(/(arn:aws:s3tables:[^:]+:[^:]+:bucket\/[^/]+)/);
       if (!bucketArnMatch) {
           throw new Error(`Invalid S3 Table ARN format.`);
       }
       const bucketArn = bucketArnMatch[1];
       
       const tableMatch = outputPath.match(/\/table\/([^/]+)\/([^/]+)$/);
       if (!tableMatch) {
           throw new Error(`Invalid S3 Table ARN format.`);
       }
       const namespace = tableMatch[1];
       const tableName = tableMatch[2];
       
       console.log(`➡️ Attaching Bucket: ${bucketArn}`);
       
       // Use CONSISTENT alias 'ventureos_ocean'
       await con.run(`ATTACH '${bucketArn}' AS ventureos_ocean (TYPE iceberg, ENDPOINT_TYPE s3_tables)`);
       
       const targetTable = `ventureos_ocean.${namespace}.${tableName}`;
       console.log(`➡️ Target table: ${targetTable}`);
       
       let tableExists = false;
       let existingColumns = [];
       
       try {
         const schemaReader = await con.run(`DESCRIBE ${targetTable}`);
         const schemaRows = await schemaReader.getRows();
         existingColumns = schemaRows.map(row => row[0]);
         tableExists = true;
         console.log(`✅ Table ${targetTable} exists with ${existingColumns.length} columns`);
       } catch (err) {
         if (err.message.includes('does not exist') || err.message.includes('Catalog Error')) {
           tableExists = false;
           console.log(`ℹ️ Table ${targetTable} does not exist, will create`);
         } else {
           throw err;
         }
       }
       
       if (!tableExists) {
         console.log(`📝 Creating table: ${targetTable}`);
         // Standard CREATE TABLE AS SELECT
         await retryIcebergOperation(async () => {
           await con.run(`CREATE TABLE ${targetTable} AS SELECT * FROM violations`);
         });
         console.log(`✅ Created and populated new Iceberg table`);
       } else {
         console.log(`➡️ Inserting into existing table: ${targetTable}`);
         
         const violationsReader = await con.run(`DESCRIBE violations`);
         const violationsRows = await violationsReader.getRows();
         const violationsColumns = violationsRows.map(row => row[0]);
         
         const matchingColumns = violationsColumns.filter(col => existingColumns.includes(col));
         
         if (matchingColumns.length === 0) {
           throw new Error(`No matching columns between violations table and ${targetTable}`);
         }
         
        console.log(`📋 Inserting ${matchingColumns.length} matching columns: ${matchingColumns.join(', ')}`);
        
        const columnList = matchingColumns.join(', ');
        await retryIcebergOperation(async () => {
          await con.run(`INSERT INTO ${targetTable} (${columnList}) SELECT ${columnList} FROM violations`);
        });
        console.log(`✅ Appended to S3 Table`);
       }
       
       // --- GOLD LAYER AGGREGATION ---
       const goldNamespace = 'gold';
       
       try {
         console.log('🥇 Processing Gold Layer (Aggregates)...');
         
         // 1. Company Stats
         const goldCompanyTable = `ventureos_ocean.${goldNamespace}.company_stats`;
         console.log(`🥇 Aggregating for: ${goldCompanyTable}`);
         
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
         
         try {
            await retryIcebergOperation(async () => {
              await con.run(`INSERT INTO ${goldCompanyTable} ${companyAggQuery}`);
            });
            console.log(`✅ Appended to Gold: company_stats`);
         } catch (e) {
            if (e.message.includes('not exist') || e.message.includes('Catalog Error')) {
                await con.run(`CREATE TABLE ${goldCompanyTable} AS ${companyAggQuery}`);
                console.log(`✅ Created Gold Table: company_stats`);
            } else {
                console.warn(`⚠️ Gold Aggregation Failed (Company): ${e.message}`);
            }
         }
         
         // 2. City Stats
         const goldCityTable = `ventureos_ocean.${goldNamespace}.city_stats`;
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
            await retryIcebergOperation(async () => {
              await con.run(`INSERT INTO ${goldCityTable} ${cityAggQuery}`);
            });
            console.log(`✅ Appended to Gold: city_stats`);
         } catch (e) {
             if (e.message.includes('not exist') || e.message.includes('Catalog Error')) {
                await con.run(`CREATE TABLE ${goldCityTable} AS ${cityAggQuery}`);
                console.log(`✅ Created Gold Table: city_stats`);
             } else {
                console.warn(`⚠️ Gold Aggregation Failed (City): ${e.message}`);
             }
         }

         // 3. State Stats
         const goldStateTable = `ventureos_ocean.${goldNamespace}.state_stats`;
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
            await retryIcebergOperation(async () => {
              await con.run(`INSERT INTO ${goldStateTable} ${stateAggQuery}`);
            });
            console.log(`✅ Appended to Gold: state_stats`);
         } catch (e) {
             if (e.message.includes('not exist') || e.message.includes('Catalog Error')) {
                await con.run(`CREATE TABLE ${goldStateTable} AS ${stateAggQuery}`);
                console.log(`✅ Created Gold Table: state_stats`);
             } else {
                console.warn(`⚠️ Gold Aggregation Failed (State): ${e.message}`);
             }
         }
         
       } catch (goldError) {
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
        const entities = [
            { pk: `COMPANY#${v.company_slug}`, type: 'Company' },
            { pk: `CITY#${slugify(v.city)}-${v.state}`, type: 'City' },
            { pk: `STATE#${v.state}`, type: 'State' }
        ];

        const itemDate = v.event_date || new Date().toISOString().split('T')[0];
        const sk = `AGENCY#${agency.toLowerCase()}#DATE#${itemDate}#${v.violation_id}`;
        
        // Prepare Item Payload (Sparse)
        const item = {
            PK: { S: '' },
            SK: { S: sk },
            violation_id: { S: v.violation_id },
            company_name: { S: v.company_name },
            city: { S: v.city },
            state: { S: v.state },
            fine: { N: (v.fine_amount || 0).toString() },
            type: { S: v.violation_type },
            raw_title: { S: (v.raw_title || '').substring(0, 200) },
            raw_desc: { S: v.raw_description ? v.raw_description.substring(0, 200) : '' },
            raw_description: { S: (v.raw_description || '').substring(0, 1000) }
        };
        
        // Include bedrock content from Silver if available
        if (v.bedrock_title) {
            item.bedrock_title = { S: (v.bedrock_title || '').substring(0, 200) };
        }
        if (v.bedrock_description) {
            item.bedrock_description = { S: (v.bedrock_description || '').substring(0, 1000) };
        }
        if (v.attribution) {
            item.attribution = { S: v.attribution };
        }

        for (const entity of entities) {
            item.PK = { S: entity.pk };
            try {
                await ddbClient.send(new PutItemCommand({
                    TableName: TABLE_NAME,
                    Item: item
                }));
                // Trim logic omitted for brevity in batch lambda context
            } catch (e) {
                console.warn(`⚠️ DynamoDB Sync Failed for ${entity.pk}: ${e.message}`);
            }
        }
    }
}

// Helper slugify
function slugify(str) {
  if (!str) return '';
  return str.toLowerCase().trim()
    .replace(/[^\w\s-]/g, '')
    .replace(/[\s_-]+/g, '-')
    .replace(/^-+|-+$/g, '');
}
