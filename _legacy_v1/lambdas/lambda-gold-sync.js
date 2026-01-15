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
  console.log(`📋 Event: ${JSON.stringify(event)}`);
  
  // Determine sync mode from event
  // Default: 'stats-only' (fast, stops after national stats)
  // Options: 'stats-only', 'recent5-only', 'all'
  const mode = event?.mode || event?.syncMode || 'stats-only';
  const syncRecent5 = mode === 'all' || mode === 'recent5-only';
  const syncStats = mode === 'all' || mode === 'stats-only';
  
  console.log(`🎯 Sync Mode: ${mode} (stats: ${syncStats}, recent5: ${syncRecent5})`);
  
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
      if (syncStats) {
          // 1. Sync Company Stats
          await syncCompanyStats(con, context, event);
          
          // 2. Sync City Stats
          await syncCityStats(con, context, event);

          // 2b. Sync City-Company Directory (Adjacency List)
          await syncCityCompanies(con, context);
          
          // 3. Sync State Stats
          await syncStateStats(con, context);
          
          // 4. Sync National Stats (aggregated from state stats)
          await syncNationalStats(con, context);
          
          console.log('✅ Stats sync complete!');
      }
      
      if (syncRecent5) {
          // 5. Sync Recent5 Violations (from Silver layer)
          // This can be memory-intensive, so it's optional and can be run separately
          await syncRecent5Violations(con, context, event);
          console.log('✅ Recent5 violations sync complete!');
      }
      
      const completedModes = [];
      if (syncStats) completedModes.push('stats');
      if (syncRecent5) completedModes.push('recent5');
      
      return { 
          statusCode: 200, 
          body: `Sync Complete: ${completedModes.join(', ')}`,
          mode: mode,
          completed: completedModes
      };
      
  } catch (e) {
      console.error('❌ Sync Failed:', e);
      throw e;
  }
}

function checkTimeout(context, operation = '', progress = '') {
    if (context && context.getRemainingTimeInMillis() < 60000) { // 60 seconds buffer
        const remaining = Math.round(context.getRemainingTimeInMillis() / 1000);
        console.warn(`⚠️ Time limit approaching (${remaining}s remaining). Stopping gracefully.`);
        if (operation) {
            console.warn(`   Operation: ${operation}`);
        }
        if (progress) {
            console.warn(`   Progress: ${progress}`);
        }
        return true;
    }
    return false;
}

async function syncCompanyStats(con, context, event = {}) {
    console.log('🏢 Syncing Company Stats (Delta Sync)...');
    if (checkTimeout(context)) return;

    // Check for pagination parameters from event
    const startOffset = event?.startOffset || 0;
    const chunkSize = event?.chunkSize || 50000; // Process 50k companies per invocation
    const isChunked = startOffset > 0 || event?.chunkSize;

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
    // Create temp table for efficient pagination
    await con.run(`
        CREATE OR REPLACE TABLE diff_companies AS 
        SELECT * FROM current_companies 
        EXCEPT 
        SELECT * FROM prev_companies
        ORDER BY company_slug
    `);
    
    // Get total count for progress tracking
    const totalCountReader = await con.run(`SELECT COUNT(*) as total FROM diff_companies`);
    const totalCount = (await totalCountReader.getRows())[0][0];
    
    // Paginate from temp table
    const diffReader = await con.run(`
        SELECT * FROM diff_companies
        LIMIT ${chunkSize} OFFSET ${startOffset}
    `);
    const rows = await diffReader.getRowObjectsJson();
    
    console.log(`Found ${rows.length} companies to sync in this chunk (${startOffset + rows.length}/${totalCount} total).`);
    
    if (rows.length === 0) {
        console.log('✅ No more companies to sync.');
        // If this was the last chunk, update snapshot
        if (startOffset === 0 || startOffset >= totalCount) {
            console.log('  Updating snapshot...');
            await con.run(`COPY current_companies TO '${snapshotPath}' (FORMAT PARQUET)`);
        }
        return;
    }
    
    // 4. Use BatchWriteItem for performance (25 items per batch)
    const batchSize = 25;
    let syncedCount = 0;
    for (let i = 0; i < rows.length; i += batchSize) {
        if (checkTimeout(context)) {
            console.warn(`⚠️ Timeout approaching. Processed ${syncedCount}/${rows.length} companies in this chunk.`);
            // Save progress - next invocation should continue from startOffset + syncedCount
            console.log(`💡 Next invocation should use: { "startOffset": ${startOffset + syncedCount}, "chunkSize": ${chunkSize} }`);
            return;
        }

        const batch = rows.slice(i, i + batchSize);
        const writeRequests = [];
        
        batch.forEach(row => {
            const slug = row.company_slug;
            const name = row.company_name;
            const count = row.violation_count;
            const fines = row.total_fines;
            
            // Skip rows with missing required fields
            if (!slug || slug === '') {
                console.warn(`⚠️ Skipping company with missing slug:`, row);
                return;
            }
            
            // Validate and provide defaults
            const companyName = (name && name !== '') ? name : 'Unknown Company';
            const violationCount = (count != null && !isNaN(count)) ? Number(count) : 0;
            const totalFines = (fines != null && !isNaN(fines)) ? Number(fines) : 0;
            
            writeRequests.push({
                PutRequest: {
                    Item: {
                        PK: { S: `COMPANY#${slug}` },
                        SK: { S: 'STATS#all' },
                        name: { S: companyName },
                        total_violations: { N: violationCount.toString() },
                        total_fines: { N: totalFines.toString() },
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
            syncedCount += writeRequests.length;
        }
        
        if ((i + batchSize) % 1000 < batchSize) {
            console.log(`  Progress: Synced ${syncedCount}/${rows.length} companies in chunk (${startOffset + syncedCount}/${totalCount} total)...`);
        }
    }
    console.log(`✅ Synced ${syncedCount} companies in this chunk (${startOffset + syncedCount}/${totalCount} total)`);
    
    // 5. If there are more companies, trigger next chunk (or return info for manual trigger)
    const nextOffset = startOffset + syncedCount;
    if (nextOffset < totalCount) {
        console.log(`📋 More companies remaining. Next chunk should start at offset ${nextOffset}`);
        console.log(`💡 To continue, invoke with: { "startOffset": ${nextOffset}, "chunkSize": ${chunkSize} }`);
    } else {
        // Last chunk - update snapshot
        console.log('  Updating snapshot (final chunk)...');
        await con.run(`COPY current_companies TO '${snapshotPath}' (FORMAT PARQUET)`);
        console.log('✅ Company stats sync complete!');
    }
}

async function syncCityStats(con, context, event = {}) {
    console.log('🏙️ Syncing City Stats (Delta Sync)...');
    if (checkTimeout(context, 'syncCityStats')) return;

    // Check for pagination parameters from event
    const cityStartOffset = event?.cityStartOffset || 0;
    const cityChunkSize = event?.cityChunkSize || 50000; // Process 50k cities per invocation

    // 1. Calculate Current State
    await con.run(`
        CREATE OR REPLACE TABLE current_cities AS 
        SELECT 
            city, 
            state, 
            SUM(violation_count) as violation_count, 
            SUM(total_fines) as total_fines 
        FROM ocean.gold.city_stats
        WHERE city IS NOT NULL AND city != ''
        GROUP BY city, state
        ORDER BY city, state
    `);
    
    // 2. Load Previous State (Snapshot)
    const snapshotPath = `s3://${SNAPSHOT_BUCKET}/${SNAPSHOT_PREFIX}/cities_latest.parquet`;
    let hasSnapshot = false;
    try {
        await con.run(`CREATE OR REPLACE TABLE prev_cities AS SELECT * FROM read_parquet('${snapshotPath}')`);
        hasSnapshot = true;
        console.log('  Loaded previous snapshot from S3.');
    } catch (e) {
        console.log('  No previous snapshot found (First run). Syncing all.');
        await con.run(`CREATE OR REPLACE TABLE prev_cities AS SELECT * FROM current_cities WHERE 1=0`);
    }
    
    // 3. Calculate Diff (Rows in Current that are different/new compared to Prev)
    await con.run(`
        CREATE OR REPLACE TABLE diff_cities AS 
        SELECT * FROM current_cities 
        EXCEPT 
        SELECT * FROM prev_cities
        ORDER BY city, state
    `);
    
    // Get total count
    const totalCountReader = await con.run(`SELECT COUNT(*) as total FROM diff_cities`);
    const totalCount = (await totalCountReader.getRows())[0][0];
    
    // Paginate from diff table
    const reader = await con.run(`
        SELECT * FROM diff_cities
        LIMIT ${cityChunkSize} OFFSET ${cityStartOffset}
    `);
    const rows = await reader.getRowObjectsJson();
    
    console.log(`Found ${rows.length} cities to sync in this chunk (${cityStartOffset + rows.length}/${totalCount} total changed/new).`);
    
    if (rows.length === 0) {
        console.log('✅ No more cities to sync.');
        // If this was the last chunk, update snapshot
        if (cityStartOffset === 0 || cityStartOffset >= totalCount) {
            console.log('  Updating snapshot...');
            await con.run(`COPY current_cities TO '${snapshotPath}' (FORMAT PARQUET)`);
        }
        return;
    }
    
    // Use BatchWriteItem for performance (25 items per batch max)
    // We are writing 2 items per city, so batch size must be <= 12
    const batchSize = 12;
    let syncedCount = 0;
    for (let i = 0; i < rows.length; i += batchSize) {
        if (checkTimeout(context, 'syncCityStats', `Synced ${syncedCount}/${rows.length} cities in chunk (${cityStartOffset + syncedCount}/${totalCount} total)`)) {
            console.warn(`💡 Next invocation should use: { "cityStartOffset": ${cityStartOffset + syncedCount}, "cityChunkSize": ${cityChunkSize} }`);
            return;
        }

        const batch = rows.slice(i, i + batchSize);
        const writeRequests = [];
        
        // Use a Map to deduplicate by PK+SK to prevent duplicate key errors
        const requestMap = new Map();
        
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
            const cityKey = `CITY#${citySlug}-${state}|STATS#all`;
            if (!requestMap.has(cityKey)) {
                requestMap.set(cityKey, {
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
            }
            
            // 2. State Directory Entry (for efficient "Cities in State" query)
            const stateKey = `STATE#${state}|CITY#${citySlug}`;
            if (!requestMap.has(stateKey)) {
                requestMap.set(stateKey, {
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
            }
        });
        
        // Convert Map values to array
        const deduplicatedRequests = Array.from(requestMap.values());
        
        if (deduplicatedRequests.length > 0) {
            await ddbClient.send(new BatchWriteItemCommand({
                RequestItems: {
                    [ENTITY_TABLE]: deduplicatedRequests
                }
            }));
        }
        
        syncedCount += Math.min(batchSize, rows.length - i);
        
        if ((i + batchSize) % 1000 < batchSize) { // Approx every 1000 items
            console.log(`  Progress: Synced ${syncedCount}/${rows.length} cities in chunk (${cityStartOffset + syncedCount}/${totalCount} total)...`);
        }
    }
    
    const nextOffset = cityStartOffset + syncedCount;
    if (nextOffset < totalCount) {
        console.log(`📋 More cities remaining. Next chunk should start at offset ${nextOffset}`);
        console.log(`💡 To continue, invoke with: { "cityStartOffset": ${nextOffset}, "cityChunkSize": ${cityChunkSize} }`);
    } else {
        // Last chunk - update snapshot
        console.log('  Updating snapshot (final chunk)...');
        await con.run(`COPY current_cities TO '${snapshotPath}' (FORMAT PARQUET)`);
        console.log(`✅ Synced all ${totalCount} changed/new cities`);
    }
}

async function syncCityCompanies(con, context) {
    console.log('🏭 Syncing City-Company Directories (Delta Sync)...');
    if (checkTimeout(context)) return;

    // Get list of agency tables
    const tablesReader = await con.run("SHOW TABLES FROM ocean.silver;");
    const tables = await tablesReader.getRows();
    // Filter out known non-violation tables
    const excludedTables = ['violations', 'bedrock_overlays'];
    const agencyTables = tables.map(row => row[0]).filter(name => name && !excludedTables.includes(name));
    
    for (const agencyTable of agencyTables) {
        console.log(`  Processing ${agencyTable} for directories...`);
        if (checkTimeout(context, `syncCityCompanies - ${agencyTable}`)) {
            console.warn(`💡 Processing stopped at table: ${agencyTable}. Re-run to continue from this table.`);
            return;
        }

        try {
            // 1. Calculate Current State
            await con.run(`
                CREATE OR REPLACE TABLE current_city_companies_${agencyTable} AS 
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
            
            // 2. Load Previous State (Snapshot)
            const snapshotPath = `s3://${SNAPSHOT_BUCKET}/${SNAPSHOT_PREFIX}/city_companies_${agencyTable}_latest.parquet`;
            let hasSnapshot = false;
            try {
                await con.run(`CREATE OR REPLACE TABLE prev_city_companies_${agencyTable} AS SELECT * FROM read_parquet('${snapshotPath}')`);
                hasSnapshot = true;
                console.log(`    Loaded previous snapshot for ${agencyTable}.`);
            } catch (e) {
                console.log(`    No previous snapshot found for ${agencyTable} (First run). Syncing all.`);
                await con.run(`CREATE OR REPLACE TABLE prev_city_companies_${agencyTable} AS SELECT * FROM current_city_companies_${agencyTable} WHERE 1=0`);
            }
            
            // 3. Calculate Diff (Rows in Current that are different/new compared to Prev)
            await con.run(`
                CREATE OR REPLACE TABLE diff_city_companies_${agencyTable} AS 
                SELECT * FROM current_city_companies_${agencyTable} 
                EXCEPT 
                SELECT * FROM prev_city_companies_${agencyTable}
                ORDER BY city, state, company_slug
            `);
            
            const diffReader = await con.run(`SELECT * FROM diff_city_companies_${agencyTable}`);
            const rows = await diffReader.getRowObjectsJson();
            
            const totalCountReader = await con.run(`SELECT COUNT(*) as total FROM diff_city_companies_${agencyTable}`);
            const totalCount = (await totalCountReader.getRows())[0][0];
            
            console.log(`  Found ${rows.length} changed/new company-city relationships (${totalCount} total).`);
            
            if (rows.length === 0) {
                console.log(`  ✅ No changes for ${agencyTable}. Skipping.`);
                continue;
            }
            
            // Batch Write
            const batchSize = 25;
            for (let i = 0; i < rows.length; i += batchSize) {
                if (checkTimeout(context, `syncCityCompanies - ${agencyTable}`, `Synced ${i}/${rows.length} relationships`)) {
                    console.warn(`💡 Processing stopped at table ${agencyTable}, offset ${i}. Re-run to continue.`);
                    return;
                }
                
                const batch = rows.slice(i, i + batchSize);
                // Use a Map to deduplicate by PK+SK to prevent duplicate key errors
                const writeRequestsMap = new Map();
                
                batch.forEach(row => {
                    const city = row.city;
                    const state = row.state;
                    const slug = row.company_slug;
                    const name = row.name;
                    const count = row.count || 0;
                    
                    if (!city || !state || !slug) return;
                    
                    const citySlug = slugify(city);
                    const companyName = (name && name !== '') ? name : 'Unknown Company';
                    const violationCount = (count != null && !isNaN(count)) ? Number(count) : 0;
                    
                    // Create unique key for deduplication
                    const pk = `CITY#${citySlug}-${state}`;
                    const sk = `COMPANY#${slug}`;
                    const key = `${pk}#${sk}`;
                    
                    // Only add if not already in map (deduplication)
                    if (!writeRequestsMap.has(key)) {
                        writeRequestsMap.set(key, {
                            PutRequest: {
                                Item: {
                                    PK: { S: pk },
                                    SK: { S: sk },
                                    name: { S: companyName },
                                    violation_count: { N: violationCount.toString() },
                                    last_updated: { S: new Date().toISOString() }
                                }
                            }
                        });
                    } else {
                        // If duplicate found, log a warning but use the latest data
                        console.warn(`  ⚠️ Duplicate city-company relationship detected: ${city}, ${state} - ${companyName}. Using latest.`);
                        writeRequestsMap.set(key, {
                            PutRequest: {
                                Item: {
                                    PK: { S: pk },
                                    SK: { S: sk },
                                    name: { S: companyName },
                                    violation_count: { N: violationCount.toString() },
                                    last_updated: { S: new Date().toISOString() }
                                }
                            }
                        });
                    }
                });
                
                // Convert Map values to array
                const deduplicatedRequests = Array.from(writeRequestsMap.values());
                
                if (deduplicatedRequests.length > 0) {
                    try {
                        await ddbClient.send(new BatchWriteItemCommand({
                            RequestItems: {
                                [ENTITY_TABLE]: deduplicatedRequests
                            }
                        }));
                    } catch (e) {
                        console.error(`  ❌ Failed to batch write companies: ${e.message}`);
                        // Log the problematic batch for debugging
                        console.error(`  Debug: Batch had ${deduplicatedRequests.length} items, starting at row ${i}`);
                    }
                }
                
                if ((i + batchSize) % 5000 < batchSize) {
                    console.log(`  Progress: Synced ${Math.min(i + batchSize, rows.length)}/${rows.length} relationships...`);
                }
            }
            
            // Update snapshot after successful sync
            console.log(`  Updating snapshot for ${agencyTable}...`);
            await con.run(`COPY current_city_companies_${agencyTable} TO '${snapshotPath}' (FORMAT PARQUET)`);
            console.log(`  ✅ Completed sync for ${agencyTable}`);
        } catch (e) {
            // Skip tables that don't have violation columns (e.g., bedrock_overlays)
            if (e.message?.includes('not found in FROM clause') || e.message?.includes('column')) {
                console.log(`  ⚠️ Skipping ${agencyTable} - table doesn't have violation columns`);
            } else {
                throw e; // Re-throw unexpected errors
            }
        }
    }
    console.log('✅ Synced City-Company directories');
}

async function syncStateStats(con, context) {
    console.log('🗺️ Syncing State Stats...');
    if (checkTimeout(context, 'syncStateStats')) return;

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
        if (checkTimeout(context, 'syncStateStats', `Synced ${i}/${rows.length} states`)) return;

        const batch = rows.slice(i, i + batchSize);
        const writeRequests = [];
        
        batch.forEach(row => {
            const state = row.state;
            const count = row.violation_count;
            const fines = row.total_fines;
            
            // Skip rows with missing required fields
            if (!state || state === '') {
                console.warn(`⚠️ Skipping state with missing name:`, row);
                return;
            }
            
            // Validate and provide defaults
            const violationCount = (count != null && !isNaN(count)) ? Number(count) : 0;
            const totalFines = (fines != null && !isNaN(fines)) ? Number(fines) : 0;
            
            writeRequests.push({
                PutRequest: {
                    Item: {
                        PK: { S: `STATE#${state}` },
                        SK: { S: 'STATS#all' },
                        name: { S: state },
                        total_violations: { N: violationCount.toString() },
                        total_fines: { N: totalFines.toString() },
                        last_updated: { S: new Date().toISOString() }
                    }
                }
            });
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
    if (checkTimeout(context, 'syncNationalStats')) return;
    
    // Aggregate national totals from state stats
    // First aggregate by state to handle duplicates from incremental inserts, then sum
    const nationalReader = await con.run(`
        WITH state_aggregates AS (
            SELECT 
                state,
                SUM(violation_count) as violation_count,
                SUM(total_fines) as total_fines
            FROM ocean.gold.state_stats
            WHERE state IS NOT NULL AND state != ''
            GROUP BY state
        )
        SELECT 
            SUM(violation_count) as total_violations,
            SUM(total_fines) as total_fines,
            COUNT(*) as total_states
        FROM state_aggregates
    `);
    const nationalRows = await nationalReader.getRows();
    
    // Get total cities (distinct city+state combinations)
    // First aggregate by city+state to handle duplicates, then count
    const cityReader = await con.run(`
        WITH city_aggregates AS (
            SELECT 
                city,
                state
            FROM ocean.gold.city_stats
            WHERE city IS NOT NULL AND city != ''
              AND state IS NOT NULL AND state != ''
            GROUP BY city, state
        )
        SELECT COUNT(*) as total_cities
        FROM city_aggregates
    `);
    const cityRows = await cityReader.getRows();
    
    if (nationalRows.length > 0 && cityRows.length > 0) {
        const [totalViolations, totalFines, totalStates] = nationalRows[0];
        const totalCities = cityRows[0][0];
        
        // Helper to safely convert BigInt or number to number
        const safeNumber = (val) => {
            if (val == null) return 0;
            if (typeof val === 'bigint') {
                // BigInt - convert to number (may lose precision for very large numbers)
                return Number(val);
            }
            const num = Number(val);
            return isNaN(num) ? 0 : num;
        };
        
        // Validate and provide defaults (handle BigInt from DuckDB)
        const violations = safeNumber(totalViolations);
        const fines = safeNumber(totalFines);
        const states = safeNumber(totalStates);
        const cities = safeNumber(totalCities);
        
        // Use BatchWriteItem (single item, but consistent with other syncs)
        await ddbClient.send(new BatchWriteItemCommand({
            RequestItems: {
                [ENTITY_TABLE]: [{
                    PutRequest: {
                        Item: {
                            PK: { S: 'NATIONAL#USA' },
                            SK: { S: 'STATS#all' },
                            name: { S: 'United States' },
                            total_violations: { N: violations.toString() },
                            total_fines: { N: fines.toString() },
                            total_states: { N: states.toString() },
                            total_cities: { N: cities.toString() },
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
async function syncRecent5Violations(con, context, event = {}) {
    console.log('🕒 Syncing Recent5 Violations from Silver...');
    if (checkTimeout(context, 'syncRecent5Violations')) return;
    
    // Query recent violations from all agency tables in Silver
    // We'll get top 5 per entity (company, city, state) grouped by agency
    
    // Check if we should process a specific table or entity type
    const targetTable = event?.table; // e.g., 'osha_ita'
    const targetEntityType = event?.entityType; // e.g., 'company', 'city', 'state'
    
    try {
        // Get list of agency tables
        const tablesReader = await con.run("SHOW TABLES FROM ocean.silver;");
        const tables = await tablesReader.getRows();
        // Filter out known non-violation tables
        const excludedTables = ['violations', 'bedrock_overlays'];
        let agencyTables = tables.map(row => row[0]).filter(name => name && !excludedTables.includes(name));
        
        // Filter to specific table if requested
        if (targetTable) {
            agencyTables = agencyTables.filter(name => name === targetTable);
            if (agencyTables.length === 0) {
                console.warn(`⚠️ Table ${targetTable} not found in Silver layer`);
                return;
            }
        }
        
        console.log(`📊 Processing ${agencyTables.length} agency table(s): ${agencyTables.join(', ')}`);
        
        if (agencyTables.length === 0) {
            console.warn('⚠️ No agency tables found in Silver layer');
            return;
        }
        
        // Determine which entity types to process
        const entityTypes = targetEntityType 
            ? [targetEntityType] 
            : ['company', 'city', 'state'];
        
        // For each agency table, get recent5 violations per entity
        for (const agencyTable of agencyTables) {
            if (checkTimeout(context, `syncRecent5Violations - ${agencyTable}`)) {
                console.warn(`💡 Processing stopped at table: ${agencyTable}. Re-run to continue.`);
                return;
            }

            const agency = agencyTable.toUpperCase(); // 'osha' -> 'OSHA'
            console.log(`🔄 Processing ${agency} violations...`);
            
            try {
                // Process each entity type
                for (const entityType of entityTypes) {
                    await syncEntityRecent5(con, `ocean.silver.${agencyTable}`, entityType, agency, context, event);
                }
            } catch (e) {
                // Skip tables that don't have violation columns
                if (e.message?.includes('not found in FROM clause') || e.message?.includes('column')) {
                    console.log(`  ⚠️ Skipping ${agencyTable} - table doesn't have violation columns`);
                } else {
                    console.error(`  ❌ Error processing ${agencyTable}: ${e.message}`);
                    // Continue with other tables
                }
            }
        }
        
        console.log('✅ Recent5 violations sync complete');
    } catch (e) {
        console.error(`❌ Recent5 sync failed: ${e.message}`);
        // Don't throw - stats sync is more important
    }
}

async function syncEntityRecent5(con, tableName, entityType, agency, context, event = {}) {
    if (checkTimeout(context, `syncEntityRecent5 - ${tableName} - ${entityType}`)) return;

    const MAX_ITEMS = 5;
    const ENTITY_BATCH_SIZE = 1000; // Process 1000 entities at a time to avoid memory issues
    const entityStartOffset = event?.entityStartOffset || 0;
    
    let groupBy, pkPrefix, nameField, slugField, entitySelect;
    
    if (entityType === 'company') {
        groupBy = 'company_slug, company_name';
        entitySelect = 'DISTINCT company_slug, company_name';
        pkPrefix = 'COMPANY#';
        nameField = 'company_name';
        slugField = 'company_slug';
    } else if (entityType === 'city') {
        groupBy = 'city, state';
        entitySelect = 'DISTINCT city, state';
        pkPrefix = 'CITY#';
        nameField = 'city';
        slugField = 'city'; // Will combine with state
    } else { // state
        groupBy = 'state';
        entitySelect = 'DISTINCT state';
        pkPrefix = 'STATE#';
        nameField = 'state';
        slugField = 'state';
    }
    
    // Step 1: Get list of unique entities (paginated to avoid memory issues)
    const entitiesQuery = `
        SELECT ${entitySelect}
        FROM ${tableName}
        WHERE ${nameField} IS NOT NULL AND ${nameField} != ''
        ORDER BY ${groupBy}
        LIMIT ${ENTITY_BATCH_SIZE} OFFSET ${entityStartOffset}
    `;
    
    const entitiesReader = await con.run(entitiesQuery);
    const entities = await entitiesReader.getRowObjectsJson();
    
    if (entities.length === 0) {
        if (entityStartOffset === 0) {
            console.log(`  ⚠️ No ${entityType}s found in ${tableName}`);
        } else {
            console.log(`  ✅ Completed syncing all ${entityType}s for ${agency}`);
        }
        return;
    }
    
    console.log(`  Processing ${entities.length} ${entityType}s (offset ${entityStartOffset})...`);
    
    // Step 2: For each entity, get top 5 violations (process in smaller batches)
    const PROCESS_BATCH_SIZE = 100; // Process 100 entities at a time
    let syncedCount = 0;
    
    for (let i = 0; i < entities.length; i += PROCESS_BATCH_SIZE) {
        if (checkTimeout(context, `syncEntityRecent5 - ${tableName} - ${entityType}`, `Synced ${syncedCount}/${entities.length} entities in batch`)) {
            console.warn(`💡 Processing stopped. Next invocation should use: { "entityStartOffset": ${entityStartOffset + syncedCount} }`);
            return;
        }
        
        const entityBatch = entities.slice(i, i + PROCESS_BATCH_SIZE);
        
        // Build WHERE clause for this batch of entities
        const whereConditions = [];
        for (const entity of entityBatch) {
            if (entityType === 'company') {
                whereConditions.push(`(company_slug = '${entity.company_slug?.replace(/'/g, "''")}' AND company_name = '${entity.company_name?.replace(/'/g, "''")}')`);
            } else if (entityType === 'city') {
                whereConditions.push(`(city = '${entity.city?.replace(/'/g, "''")}' AND state = '${entity.state?.replace(/'/g, "''")}')`);
            } else { // state
                whereConditions.push(`state = '${entity.state?.replace(/'/g, "''")}'`);
            }
        }
        
        // Query top 5 violations for this batch of entities
        const violationsQuery = `
            WITH ranked AS (
                SELECT 
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY ${groupBy}
                        ORDER BY event_date DESC, violation_id DESC
                    ) as rn
                FROM ${tableName}
                WHERE (${whereConditions.join(' OR ')})
                  AND ${nameField} IS NOT NULL AND ${nameField} != ''
            )
            SELECT *
            FROM ranked
            WHERE rn <= ${MAX_ITEMS}
            ORDER BY ${groupBy}, event_date DESC
        `;
        
        const violationsReader = await con.run(violationsQuery);
        const violations = await violationsReader.getRowObjectsJson();
        
        // Group violations by entity PK
        const entityMap = new Map();
        for (const row of violations) {
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
        
        // Sync each entity's recent5 to DynamoDB
        for (const [pk, violationList] of entityMap.entries()) {
            await syncEntityViolationsToDynamo(pk, violationList, agency.toLowerCase());
            syncedCount++;
        }
        
        if ((i + PROCESS_BATCH_SIZE) % 500 === 0 || (i + PROCESS_BATCH_SIZE) >= entities.length) {
            console.log(`  Progress: Synced ${syncedCount}/${entities.length} ${entityType}s in this batch (${entityStartOffset + syncedCount} total)...`);
        }
    }
    
    // Step 3: If there are more entities, we need to continue
    if (entities.length === ENTITY_BATCH_SIZE) {
        console.log(`  📋 More ${entityType}s remaining. Processed ${entityStartOffset + syncedCount} so far.`);
        console.log(`  💡 To continue, invoke with: { "entityStartOffset": ${entityStartOffset + syncedCount} }`);
    } else {
        console.log(`  ✅ Completed syncing ${syncedCount} ${entityType}s in this batch for ${agency}`);
    }
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
        
        // Build item with bedrock content from Silver/Gold if available
        const item = {
            PK: { S: pk },
            SK: { S: sk },
            violation_id: { S: v.violation_id },
            company_name: { S: v.company_name || '' },
            city: { S: v.city || '' },
            state: { S: v.state || '' },
            fine: { N: Number(v.fine_amount || 0).toString() },
            type: { S: v.violation_type || '' },
            raw_title: { S: (v.raw_title || '').substring(0, 200) },
            raw_desc: { S: (v.raw_description || '').substring(0, 200) },
            raw_description: { S: (v.raw_description || '').substring(0, 1000) }, // Longer version for API
            event_date: { S: itemDate }
        };
        
        // Include bedrock content from Silver/Gold if available (synced by bedrock-sync Lambda)
        // Check for new standard field (bedrock_title) first, then fallback to legacy (title_bedrock)
        const bedrockTitle = v.bedrock_title || v.title_bedrock;
        const bedrockDesc = v.bedrock_description || v.description_bedrock;
        
        if (bedrockTitle) {
            item.bedrock_title = { S: (bedrockTitle || '').substring(0, 200) }; 
        }
        if (bedrockDesc) {
            item.bedrock_description = { S: (bedrockDesc || '').substring(0, 1000) }; 
        }
        if (v.attribution) {
            item.attribution = { S: v.attribution };
        }
        
        writeRequests.push({
            PutRequest: {
                Item: item
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
        
        // Before deleting old items, sync bedrock content back to Silver
        for (const item of existing.Items || []) {
            if (!newKeys.has(item.SK.S)) {
                // Get full item to check for bedrock content
                try {
                    const fullItem = await ddbClient.send(new GetItemCommand({
                        TableName: VIOLATIONS_TABLE,
                        Key: { PK: item.PK, SK: item.SK }
                    }));
                    
                        // Bedrock content is already in Silver (written directly by bedrock-generator)
                        // No need to sync back - it's already there!
                        if (fullItem.Item && (fullItem.Item.bedrock_title)) {
                            console.log(`ℹ️ Bedrock content already in Silver for ${fullItem.Item.violation_id?.S} (no sync needed)`);
                        }
                } catch (e) {
                    console.warn(`⚠️ Failed to sync bedrock content before deletion: ${e.message}`);
                }
                
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



