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
import { DynamoDBClient, UpdateItemCommand, PutItemCommand, QueryCommand, DeleteItemCommand, BatchWriteItemCommand } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { tmpdir } from 'os';
import { join } from 'path';

const ddbClient = new DynamoDBClient({ region: process.env.AWS_REGION || 'us-east-1' });
const docClient = DynamoDBDocumentClient.from(ddbClient);
const ENTITY_TABLE = 'VentureOS-Entities';
const VIOLATIONS_TABLE = 'VentureOS-Violations';
const SNAPSHOT_BUCKET = process.env.S3_BUCKET || 'sirsluginston-ventureos-data';
const SNAPSHOT_PREFIX = 'gold/snapshots';

export async function handler(event, context) {
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
      await syncCompanyStats(con, context);
      
      // 2. Sync City Stats
      await syncCityStats(con, context);

      // 2b. Sync City-Company Directory (Adjacency List)
      await syncCityCompanies(con, context);
      
      // 3. Sync State Stats
      await syncStateStats(con, context);
      
      // 4. Sync National Stats (aggregated from state stats)
      await syncNationalStats(con, context);
      
      // 5. Sync Recent5 Violations (from Silver layer)
      await syncRecent5Violations(con, context);
      
      return { statusCode: 200, body: 'Sync Complete' };
      
  } catch (e) {
      console.error('❌ Sync Failed:', e);
      throw e;
  }
}

function checkTimeout(context) {
    if (context && context.getRemainingTimeInMillis() < 60000) { // 60 seconds buffer
        console.warn('⚠️ Time limit approaching. Stopping gracefully.');
        return true;
    }
    return false;
}

async function syncCompanyStats(con, context) {
    console.log('🏢 Syncing Company Stats (Delta Sync)...');
    if (checkTimeout(context)) return;

    // 1. Calculate Current State
    await con.run(`
        CREATE OR REPLACE TABLE current_companies AS 
        SELECT 
            company_slug, 
            MAX(company_name) as company_name,
            SUM(violation_count) as violation_count, 
            SUM(total_fines) as total_fines 
        FROM ocean.gold.company_stats 
        WHERE violation_count > 0
        GROUP BY company_slug
    `);
    
    // 2. Load Previous State (Snapshot)
    const snapshotPath = `s3://${SNAPSHOT_BUCKET}/${SNAPSHOT_PREFIX}/companies_latest.parquet`;
    let hasSnapshot = false;
    try {
        // Check if snapshot exists by trying to read it
        await con.run(`CREATE OR REPLACE TABLE prev_companies AS SELECT * FROM read_parquet('${snapshotPath}')`);
        hasSnapshot = true;
        console.log('  Loaded previous snapshot from S3.');
    } catch (e) {
        console.log('  No previous snapshot found (First run). Syncing all.');
        await con.run(`CREATE OR REPLACE TABLE prev_companies AS SELECT * FROM current_companies WHERE 1=0`);
    }
    
    // 3. Calculate Diff (Rows in Current that are different/new compared to Prev)
    // Note: This detects NEW companies and CHANGED stats (count/fines)
    // It does NOT detect DELETED companies (which we ignore for now as history persists)
    const diffReader = await con.run(`
        SELECT * FROM current_companies 
        EXCEPT 
        SELECT * FROM prev_companies
    `);
    const rows = await diffReader.getRowObjectsJson();
    
    console.log(`Found ${rows.length} changed/new companies to sync.`);
    
    if (rows.length === 0) {
        console.log('✅ No changes detected. Skipping DynamoDB writes.');
        return;
    }
    
    // 4. Use BatchWriteItem for performance (25 items per batch)
    const batchSize = 25;
    for (let i = 0; i < rows.length; i += batchSize) {
        if (checkTimeout(context)) return;

        const batch = rows.slice(i, i + batchSize);
        const writeRequests = batch.map(row => {
            const slug = row.company_slug;
            const name = row.company_name;
            const count = row.violation_count;
            const fines = row.total_fines;
            return {
                PutRequest: {
                    Item: {
                        PK: { S: `COMPANY#${slug}` },
                        SK: { S: 'STATS#all' },
                        name: { S: name },
                        total_violations: { N: Number(count).toString() },
                        total_fines: { N: Number(fines).toString() },
                        last_updated: { S: new Date().toISOString() }
                    }
                }
            };
        });
        
        await ddbClient.send(new BatchWriteItemCommand({
            RequestItems: {
                [ENTITY_TABLE]: writeRequests
            }
        }));
        
        if ((i + batchSize) % 1000 < batchSize) {
            console.log(`  Progress: Synced ${Math.min(i + batchSize, rows.length)}/${rows.length} companies...`);
        }
    }
    console.log(`✅ Synced ${rows.length} companies`);
    
    // 5. Update Snapshot
    // Only update if we successfully wrote (or if we want to advance state regardless)
    // We update snapshot so next run compares against this new state
    console.log('  Updating snapshot...');
    await con.run(`COPY current_companies TO '${snapshotPath}' (FORMAT PARQUET)`);
}

async function syncCityStats(con, context) {
    console.log('🏙️ Syncing City Stats...');
    if (checkTimeout(context)) return;

    // Aggregate by city/state to handle duplicates from incremental inserts
    const reader = await con.run(`
        SELECT 
            city, 
            state, 
            SUM(violation_count) as violation_count, 
            SUM(total_fines) as total_fines 
        FROM ocean.gold.city_stats
        WHERE city IS NOT NULL AND city != ''
        GROUP BY city, state
    `);
    const rows = await reader.getRowObjectsJson();
    
    console.log(`Found ${rows.length} unique cities to sync.`);
    
    // Use BatchWriteItem for performance (25 items per batch max)
    // We are writing 2 items per city, so batch size must be <= 12
    const batchSize = 12;
    for (let i = 0; i < rows.length; i += batchSize) {
        if (checkTimeout(context)) return;

        const batch = rows.slice(i, i + batchSize);
        const writeRequests = [];
        
        batch.forEach(row => {
            const city = row.city;
            const state = row.state;
            const count = row.violation_count || 0;
            const fines = row.total_fines || 0;
            
            if (!city || !state) {
                // console.warn(`Skipping invalid city row:`, row);
                return;
            }
            
            const citySlug = slugify(city);
            const countStr = Number(count).toString();
            const finesStr = Number(fines).toString();
            
            // 1. Main City Entity (for City Page)
            writeRequests.push({
                PutRequest: {
                    Item: {
                        PK: { S: `CITY#${citySlug}-${state}` },
                        SK: { S: 'STATS#all' },
                        name: { S: `${city}, ${state}` },
                        total_violations: { N: countStr },
                        total_fines: { N: finesStr },
                        last_updated: { S: new Date().toISOString() }
                    }
                }
            });
            
            // 2. State Directory Entry (for efficient "Cities in State" query)
            writeRequests.push({
                PutRequest: {
                    Item: {
                        PK: { S: `STATE#${state}` },
                        SK: { S: `CITY#${citySlug}` },
                        name: { S: city },
                        total_violations: { N: countStr },
                        last_updated: { S: new Date().toISOString() }
                    }
                }
            });
        });
        
        if (writeRequests.length > 0) {
            await ddbClient.send(new BatchWriteItemCommand({
                RequestItems: {
                    [ENTITY_TABLE]: writeRequests
                }
            }));
        }
        
        if ((i + batchSize) % 1000 < batchSize) { // Approx every 1000 items
            console.log(`  Progress: Synced ${Math.min(i + batchSize, rows.length)}/${rows.length} cities...`);
        }
    }
    console.log(`✅ Synced ${rows.length} cities`);
}

async function syncCityCompanies(con, context) {
    console.log('🏭 Syncing City-Company Directories...');
    if (checkTimeout(context)) return;

    // Get list of agency tables
    const tablesReader = await con.run("SHOW TABLES FROM ocean.silver;");
    const tables = await tablesReader.getRows();
    const agencyTables = tables.map(row => row[0]).filter(name => name && name !== 'violations');
    
    for (const agencyTable of agencyTables) {
        console.log(`  Processing ${agencyTable} for directories...`);
        if (checkTimeout(context)) return;

        // Group companies by city
        const reader = await con.run(`
            SELECT 
                city, 
                state, 
                company_slug, 
                MAX(company_name) as name, 
                COUNT(*) as count 
            FROM ocean.silver.${agencyTable}
            WHERE city IS NOT NULL AND city != '' 
              AND company_slug IS NOT NULL
            GROUP BY city, state, company_slug
        `);
        
        const rows = await reader.getRowObjectsJson();
        console.log(`  Found ${rows.length} company-city relationships.`);
        
        // Batch Write
        const batchSize = 25;
        for (let i = 0; i < rows.length; i += batchSize) {
            if (checkTimeout(context)) return;
            
            const batch = rows.slice(i, i + batchSize);
            const writeRequests = [];
            
            batch.forEach(row => {
                const city = row.city;
                const state = row.state;
                const slug = row.company_slug;
                const name = row.name;
                const count = row.count || 0;
                
                if (!city || !state || !slug) return;
                
                const citySlug = slugify(city);
                
                writeRequests.push({
                    PutRequest: {
                        Item: {
                            PK: { S: `CITY#${citySlug}-${state}` },
                            SK: { S: `COMPANY#${slug}` },
                            name: { S: name },
                            violation_count: { N: Number(count).toString() },
                            last_updated: { S: new Date().toISOString() }
                        }
                    }
                });
            });
            
            if (writeRequests.length > 0) {
                try {
                    await ddbClient.send(new BatchWriteItemCommand({
                        RequestItems: {
                            [ENTITY_TABLE]: writeRequests
                        }
                    }));
                } catch (e) {
                    console.error(`  ❌ Failed to batch write companies: ${e.message}`);
                }
            }
            
            if ((i + batchSize) % 5000 < batchSize) {
                console.log(`  Progress: Synced ${Math.min(i + batchSize, rows.length)}/${rows.length} relationships...`);
            }
        }
    }
    console.log('✅ Synced City-Company directories');
}

async function syncStateStats(con, context) {
    console.log('🗺️ Syncing State Stats...');
    if (checkTimeout(context)) return;

    // Aggregate by state to handle duplicates from incremental inserts
    const reader = await con.run(`
        SELECT 
            state, 
            SUM(violation_count) as violation_count, 
            SUM(total_fines) as total_fines 
        FROM ocean.gold.state_stats
        WHERE state IS NOT NULL AND state != ''
        GROUP BY state
    `);
    const rows = await reader.getRowObjectsJson();
    
    console.log(`Found ${rows.length} unique states to sync.`);
    
    // Use BatchWriteItem for performance (25 items per batch)
    const batchSize = 25;
    for (let i = 0; i < rows.length; i += batchSize) {
        if (checkTimeout(context)) return;

        const batch = rows.slice(i, i + batchSize);
        const writeRequests = batch.map(row => {
            const state = row.state;
            const count = row.violation_count;
            const fines = row.total_fines;
            return {
                PutRequest: {
                    Item: {
                        PK: { S: `STATE#${state}` },
                        SK: { S: 'STATS#all' },
                        name: { S: state },
                        total_violations: { N: Number(count).toString() },
                        total_fines: { N: Number(fines).toString() },
                        last_updated: { S: new Date().toISOString() }
                    }
                }
            };
        });
        
        await ddbClient.send(new BatchWriteItemCommand({
            RequestItems: {
                [ENTITY_TABLE]: writeRequests
            }
        }));
        
        if ((i + batchSize) % 1000 === 0) {
            console.log(`Synced ${Math.min(i + batchSize, rows.length)}/${rows.length} states...`);
        }
    }
    console.log(`✅ Synced ${rows.length} states`);
}

async function syncNationalStats(con, context) {
    console.log('🇺🇸 Syncing National Stats...');
    if (checkTimeout(context)) return;
    
    // Aggregate national totals from state stats
    const nationalReader = await con.run(`
        SELECT 
            SUM(violation_count) as total_violations,
            SUM(total_fines) as total_fines,
            COUNT(*) as total_states
        FROM ocean.gold.state_stats
        WHERE state IS NOT NULL AND state != ''
    `);
    const nationalRows = await nationalReader.getRows();
    
    // Get total cities
    const cityReader = await con.run(`
        SELECT COUNT(*) as total_cities
        FROM ocean.gold.city_stats
        WHERE city IS NOT NULL AND city != ''
    `);
    const cityRows = await cityReader.getRows();
    
    if (nationalRows.length > 0 && cityRows.length > 0) {
        const [totalViolations, totalFines, totalStates] = nationalRows[0];
        const totalCities = cityRows[0][0];
        
        // Use BatchWriteItem (single item, but consistent with other syncs)
        await ddbClient.send(new BatchWriteItemCommand({
            RequestItems: {
                [ENTITY_TABLE]: [{
                    PutRequest: {
                        Item: {
                            PK: { S: 'NATIONAL#USA' },
                            SK: { S: 'STATS#all' },
                            name: { S: 'United States' },
                            total_violations: { N: Number(totalViolations).toString() },
                            total_fines: { N: Number(totalFines).toString() },
                            total_states: { N: Number(totalStates).toString() },
                            total_cities: { N: Number(totalCities).toString() },
                            last_updated: { S: new Date().toISOString() }
                        }
                    }
                }]
            }
        }));
        
        console.log(`✅ Synced national stats: ${totalStates} states, ${totalCities} cities, ${totalViolations} violations`);
    }
}

// Helper to update DynamoDB Entity
async function updateEntityStat(pk, sk, data) {
    // Build update expression dynamically to handle optional fields
    const updates = ['#n = :n', 'total_violations = :tv', 'total_fines = :tf', 'last_updated = :lu'];
    const values = {
        ':n': { S: data.name },
        ':tv': { N: data.total_violations.toString() },
        ':tf': { N: data.total_fines.toString() },
        ':lu': { S: new Date().toISOString() }
    };
    
    // Add optional fields if present
    if (data.total_states !== undefined) {
        updates.push('total_states = :ts');
        values[':ts'] = { N: data.total_states.toString() };
    }
    if (data.total_cities !== undefined) {
        updates.push('total_cities = :tc');
        values[':tc'] = { N: data.total_cities.toString() };
    }
    
    const command = new UpdateItemCommand({
        TableName: ENTITY_TABLE,
        Key: {
            PK: { S: pk },
            SK: { S: sk }
        },
        UpdateExpression: `SET ${updates.join(', ')}`,
        ExpressionAttributeNames: {
            '#n': 'name' // 'name' is reserved
        },
        ExpressionAttributeValues: values
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

/**
 * Sync Recent5 Violations from Silver layer to DynamoDB
 * Queries Silver for top 5 most recent violations per entity (Company, City, State)
 */
async function syncRecent5Violations(con, context) {
    console.log('🕒 Syncing Recent5 Violations from Silver...');
    if (checkTimeout(context)) return;
    
    // Query recent violations from all agency tables in Silver
    // We'll get top 5 per entity (company, city, state) grouped by agency
    
    try {
        // Get list of agency tables
        const tablesReader = await con.run("SHOW TABLES FROM ocean.silver;");
        const tables = await tablesReader.getRows();
        const agencyTables = tables.map(row => row[0]).filter(name => name && name !== 'violations');
        
        console.log(`📊 Found ${agencyTables.length} agency tables: ${agencyTables.join(', ')}`);
        
        if (agencyTables.length === 0) {
            console.warn('⚠️ No agency tables found in Silver layer');
            return;
        }
        
        // For each agency table, get recent5 violations per entity
        for (const agencyTable of agencyTables) {
            if (checkTimeout(context)) return;

            const agency = agencyTable.toUpperCase(); // 'osha' -> 'OSHA'
            console.log(`🔄 Processing ${agency} violations...`);
            
            // 1. Get Recent5 per Company
            await syncEntityRecent5(con, `ocean.silver.${agencyTable}`, 'company', agency, context);
            
            // 2. Get Recent5 per City
            await syncEntityRecent5(con, `ocean.silver.${agencyTable}`, 'city', agency, context);
            
            // 3. Get Recent5 per State
            await syncEntityRecent5(con, `ocean.silver.${agencyTable}`, 'state', agency, context);
        }
        
        console.log('✅ Recent5 violations sync complete');
    } catch (e) {
        console.error(`❌ Recent5 sync failed: ${e.message}`);
        // Don't throw - stats sync is more important
    }
}

async function syncEntityRecent5(con, tableName, entityType, agency, context) {
    if (checkTimeout(context)) return;

    const MAX_ITEMS = 5;
    
    let groupBy, pkPrefix, nameField, slugField;
    
    if (entityType === 'company') {
        groupBy = 'company_slug, company_name';
        pkPrefix = 'COMPANY#';
        nameField = 'company_name';
        slugField = 'company_slug';
    } else if (entityType === 'city') {
        groupBy = 'city, state';
        pkPrefix = 'CITY#';
        nameField = 'city';
        slugField = 'city'; // Will combine with state
    } else { // state
        groupBy = 'state';
        pkPrefix = 'STATE#';
        nameField = 'state';
        slugField = 'state';
    }
    
    // Query: Get most recent 5 violations per entity
    // Using window function to rank violations by date per entity
    const query = `
        WITH ranked AS (
            SELECT 
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY ${groupBy}
                    ORDER BY event_date DESC, violation_id DESC
                ) as rn
            FROM ${tableName}
            WHERE ${nameField} IS NOT NULL AND ${nameField} != ''
        )
        SELECT *
        FROM ranked
        WHERE rn <= ${MAX_ITEMS}
        ORDER BY ${groupBy}, event_date DESC
    `;
    
    const reader = await con.run(query);
    // Use getRowObjectsJson to get objects with keys (avoids index mapping)
    const rows = await reader.getRowObjectsJson();
    
    console.log(`  Found ${rows.length} recent violations for ${entityType}s from ${tableName}`);
    
    if (rows.length === 0) {
        console.log(`  ⚠️ No violations found in ${tableName} for ${entityType}s`);
        return;
    }
    
    // Group by entity and sync to DynamoDB
    const entityMap = new Map();
    
    for (const row of rows) {
        let pk;
        if (entityType === 'company') {
            const slug = row.company_slug;
            if (!slug) continue;
            pk = `${pkPrefix}${slug}`;
        } else if (entityType === 'city') {
            const city = row.city;
            const state = row.state;
            if (!city || !state) continue;
            pk = `${pkPrefix}${slugify(city)}-${state}`;
        } else { // state
            const state = row.state;
            if (!state) continue;
            pk = `${pkPrefix}${state}`;
        }
        
        if (!entityMap.has(pk)) {
            entityMap.set(pk, []);
        }
        entityMap.get(pk).push(row);
    }
    
    console.log(`  Grouped into ${entityMap.size} unique ${entityType} entities`);
    
    // Sync each entity's recent5 to DynamoDB
    let syncedCount = 0;
    for (const [pk, violations] of entityMap.entries()) {
        if (checkTimeout(context)) return;

        await syncEntityViolationsToDynamo(pk, violations, agency.toLowerCase());
        syncedCount++;
        if (syncedCount % 1000 === 0) {
            console.log(`  Progress: Synced ${syncedCount}/${entityMap.size} ${entityType}s...`);
        }
    }
    
    console.log(`  ✅ Completed syncing ${syncedCount} ${entityType}s for ${agency}`);
}

async function syncEntityViolationsToDynamo(pk, violations, agency) {
    const MAX_ITEMS = 5;
    
    // 1. Prepare new items to write
    const toWrite = violations.slice(0, MAX_ITEMS);
    const writeRequests = [];
    const newKeys = new Set(); // To track keys we are adding
    const processedSKs = new Set(); // Deduplication within this execution
    
    for (const v of toWrite) {
        if (!v.violation_id) continue;
        
        const itemDate = v.event_date || new Date().toISOString().split('T')[0];
        const sk = `AGENCY#${agency}#DATE#${itemDate}#${v.violation_id}`;
        
        // Prevent duplicates in the same batch (DynamoDB limitation)
        if (processedSKs.has(sk)) {
            console.warn(`  ⚠️ Duplicate violation SK detected for ${pk}: ${sk}. Skipping duplicate.`);
            continue;
        }
        processedSKs.add(sk);
        
        // Track SK so we don't delete it later if it already exists
        newKeys.add(sk);
        
        writeRequests.push({
            PutRequest: {
                Item: {
                    PK: { S: pk },
                    SK: { S: sk },
                    violation_id: { S: v.violation_id },
                    company_name: { S: v.company_name || '' },
                    city: { S: v.city || '' },
                    state: { S: v.state || '' },
                    fine: { N: Number(v.fine_amount || 0).toString() },
                    type: { S: v.violation_type || '' },
                    raw_desc: { S: (v.raw_description || '').substring(0, 200) },
                    event_date: { S: itemDate }
                }
            }
        });
    }
    
    // 2. Query existing to identify deletes
    const deleteRequests = [];
    try {
        const queryCmd = new QueryCommand({
            TableName: VIOLATIONS_TABLE,
            KeyConditionExpression: 'PK = :pk AND begins_with(SK, :prefix)',
            ExpressionAttributeValues: {
                ':pk': { S: pk },
                ':prefix': { S: `AGENCY#${agency}` }
            },
            ProjectionExpression: 'PK, SK'
        });
        
        const existing = await ddbClient.send(queryCmd);
        
        // Delete anything that isn't in the new set (or update if needed, but Put overwrites anyway)
        // Optimization: Only delete if NOT in newKeys. 
        // If it IS in newKeys, the PutRequest will overwrite it, so explicit delete is redundant but harmless.
        // However, DynamoDB doesn't allow two ops on same key in one batch. 
        // So we MUST NOT delete keys we are about to Put.
        
        for (const item of existing.Items || []) {
            if (!newKeys.has(item.SK.S)) {
                deleteRequests.push({
                    DeleteRequest: {
                        Key: { PK: item.PK, SK: item.SK }
                    }
                });
            }
        }
    } catch (e) {
        // Table might not exist or query failed
        console.warn(`  Warning querying existing for ${pk}: ${e.message}`);
    }
    
    // 3. Execute Batch Writes (Combine Puts and Deletes)
    const allRequests = [...deleteRequests, ...writeRequests];
    
    if (allRequests.length === 0) return;
    
    // Split into chunks of 25 (DynamoDB limit)
    const batchSize = 25;
    for (let i = 0; i < allRequests.length; i += batchSize) {
        const batch = allRequests.slice(i, i + batchSize);
        try {
            await ddbClient.send(new BatchWriteItemCommand({
                RequestItems: {
                    [VIOLATIONS_TABLE]: batch
                }
            }));
        } catch (e) {
            console.error(`  ❌ Failed to batch write for ${pk}: ${e.message}`);
        }
    }
    
    // console.log(`  ✅ Synced ${toWrite.length} items, deleted ${deleteRequests.length} for ${pk}`);
}


