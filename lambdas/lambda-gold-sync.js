/**
 * VentureOS Sync Lambda
 * 
 * Purpose: Periodically sync Gold Layer aggregates (S3 Tables) to DynamoDB (VentureOS-Entities).
 * Trigger: EventBridge Schedule (e.g. Daily or Hourly) OR Triggered by Ingestion Lambda.
 * 
 * Logic:
 * 1. Connect to DuckDB (S3 Tables).
 * 2. Query 'company_stats' (latest totals).
 * 3. BatchWriteItem/UpdateItem to DynamoDB 'VentureOS-Entities'.
 */

import { DuckDBInstance } from '@duckdb/node-api';
import { DynamoDBClient, UpdateItemCommand } from '@aws-sdk/client-dynamodb';
import { tmpdir } from 'os';
import { join } from 'path';

const ddbClient = new DynamoDBClient({ region: process.env.AWS_REGION || 'us-east-1' });
const ENTITY_TABLE = 'VentureOS-Entities';

export async function handler(event) {
  console.log('🔄 Sync Lambda Started');
  
  // Initialize DuckDB
  const db = await DuckDBInstance.create(':memory:');
  const con = await db.connect();
  
  // Set up S3 Tables Auth
  const isLambda = process.env.AWS_LAMBDA_FUNCTION_NAME !== undefined;
  if (isLambda) {
      process.env.HOME = '/tmp'; // Fix for extensions
      await con.run(`SET temp_directory='/tmp/duckdb_temp'`);
  }
  
  await con.run("INSTALL aws; LOAD aws;");
  await con.run("INSTALL iceberg; LOAD iceberg;");
  await con.run("CREATE SECRET (TYPE S3, PROVIDER credential_chain);");
  
  const oceanArn = process.env.S3_OCEAN_ARN; 
  // e.g. arn:aws:s3tables:us-east-1:123:bucket/ocean
  
  if (!oceanArn) throw new Error("Missing S3_OCEAN_ARN env var");
  
  console.log(`🔗 Attaching Ocean: ${oceanArn}`);
  await con.run(`ATTACH '${oceanArn}' AS ocean (TYPE iceberg, ENDPOINT_TYPE s3_tables)`);
  
  try {
      // 1. Sync Company Stats
      await syncCompanyStats(con);
      
      // 2. Sync City Stats
      await syncCityStats(con);
      
      // 3. Sync State Stats
      await syncStateStats(con);
      
      return { statusCode: 200, body: 'Sync Complete' };
      
  } catch (e) {
      console.error('❌ Sync Failed:', e);
      throw e;
  }
}

async function syncCompanyStats(con) {
    console.log('🏢 Syncing Company Stats...');
    const reader = await con.run(`
        SELECT company_slug, company_name, violation_count, total_fines 
        FROM ocean.gold.company_stats 
        WHERE violation_count > 0
    `);
    const rows = await reader.getRows();
    
    console.log(`Found ${rows.length} companies to sync.`);
    
    // In production, use BatchWriteItem (25 at a time). 
    // For simplicity/safety with UpdateItem (Upsert), we loop.
    // Since this runs daily/hourly, sequential is acceptable for MVP scale.
    
    for (const row of rows) {
        const [slug, name, count, fines] = row;
        const pk = `COMPANY#${slug}`;
        
        await updateEntityStat(pk, 'STATS#all', {
            name: name,
            total_violations: Number(count),
            total_fines: Number(fines)
        });
    }
}

async function syncCityStats(con) {
    console.log('city Syncing City Stats...');
    // DuckDB query... needs logic to form PK (CITY#slug-state)
    // We might need a helper function in SQL or JS to slugify correctly match Ingestion logic.
    // For now, assume city/state are clean.
    
    const reader = await con.run(`
        SELECT city, state, violation_count, total_fines 
        FROM ocean.gold.city_stats
    `);
    const rows = await reader.getRows();
    
    for (const row of rows) {
        const [city, state, count, fines] = row;
        const pk = `CITY#${slugify(city)}-${state}`;
        
        await updateEntityStat(pk, 'STATS#all', {
            name: `${city}, ${state}`,
            total_violations: Number(count),
            total_fines: Number(fines)
        });
    }
}

async function syncStateStats(con) {
    console.log('🗺️ Syncing State Stats...');
    const reader = await con.run(`
        SELECT state, violation_count, total_fines 
        FROM ocean.gold.state_stats
    `);
    const rows = await reader.getRows();
    
    for (const row of rows) {
        const [state, count, fines] = row;
        const pk = `STATE#${state}`;
        
        await updateEntityStat(pk, 'STATS#all', {
            name: state,
            total_violations: Number(count),
            total_fines: Number(fines)
        });
    }
}

// Helper to update DynamoDB Entity
async function updateEntityStat(pk, sk, data) {
    // We store stats as attributes on the entity item
    const command = new UpdateItemCommand({
        TableName: ENTITY_TABLE,
        Key: {
            PK: { S: pk },
            SK: { S: sk }
        },
        UpdateExpression: 'SET #n = :n, total_violations = :tv, total_fines = :tf, last_updated = :lu',
        ExpressionAttributeNames: {
            '#n': 'name' // 'name' is reserved
        },
        ExpressionAttributeValues: {
            ':n': { S: data.name },
            ':tv': { N: data.total_violations.toString() },
            ':tf': { N: data.total_fines.toString() },
            ':lu': { S: new Date().toISOString() }
        }
    });
    
    try {
        await ddbClient.send(command);
    } catch (e) {
        console.warn(`Failed to update ${pk}: ${e.message}`);
    }
}

function slugify(str) {
  if (!str) return '';
  return str.toLowerCase().trim()
    .replace(/[^\w\s-]/g, '')
    .replace(/[\s_-]+/g, '-')
    .replace(/^-+|-+$/g, '');
}


