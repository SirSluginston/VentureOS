/**
 * Cleanup Gold Layer Tables
 * 
 * Removes duplicate rows by recreating tables with aggregated data.
 * This fixes the issue where incremental INSERTs created duplicates.
 * Renames 'agencies' to 'agency'.
 */

import { DuckDBInstance } from '@duckdb/node-api';

async function cleanupGold() {
  console.log('🧹 Cleaning up Gold Layer Tables...');
  
  const db = await DuckDBInstance.create(':memory:');
  const con = await db.connect();
  
  // Setup AWS Auth
  await con.run("INSTALL aws; LOAD aws;");
  await con.run("INSTALL iceberg; LOAD iceberg;");
  await con.run("CREATE SECRET (TYPE S3, PROVIDER credential_chain);");
  
  const tableBucketArn = 'arn:aws:s3tables:us-east-1:611538926352:bucket/sirsluginston-ventureos-data-ocean';
  
  // Attach Catalog
  console.log(`🔗 Attaching Ocean...`);
  await con.run(`ATTACH '${tableBucketArn}' AS ocean (TYPE iceberg, ENDPOINT_TYPE s3_tables)`);
  
  try {
    // 1. Company Stats
    console.log('\n📊 Processing company_stats...');
    
    // Create temp table in memory with cleaned data
    console.log('   Creating temp table...');
    await con.run(`
      CREATE TABLE temp_company_stats AS
      SELECT 
        company_slug,
        MAX(company_name) as company_name,
        CAST(SUM(violation_count) AS BIGINT) as violation_count,
        CAST(SUM(total_fines) AS DOUBLE) as total_fines,
        MAX(last_violation_date) as last_violation_date,
        list(distinct agency_item) as agency
      FROM (
        SELECT 
          company_slug,
          company_name,
          violation_count,
          total_fines,
          last_violation_date,
          unnest(agencies) as agency_item
        FROM ocean.gold.company_stats
      ) sub
      GROUP BY company_slug
    `);
    
    // Drop old table
    console.log('   Dropping old table...');
    await con.run(`DROP TABLE IF EXISTS ocean.gold.company_stats`);
    
    // Create new table from temp
    console.log('   Creating new table...');
    await con.run(`CREATE TABLE ocean.gold.company_stats AS SELECT * FROM temp_company_stats`);
    console.log('✅ Company stats cleaned and renamed');
    
    
    // 2. City Stats
    console.log('\n📊 Processing city_stats...');
    await con.run(`
      CREATE TABLE temp_city_stats AS
      SELECT 
        city,
        state,
        CAST(SUM(violation_count) AS BIGINT) as violation_count,
        CAST(SUM(total_fines) AS DOUBLE) as total_fines
      FROM ocean.gold.city_stats
      WHERE city IS NOT NULL AND city != ''
      GROUP BY city, state
    `);
    
    await con.run(`DROP TABLE IF EXISTS ocean.gold.city_stats`);
    await con.run(`CREATE TABLE ocean.gold.city_stats AS SELECT * FROM temp_city_stats`);
    console.log('✅ City stats cleaned');
    
    
    // 3. State Stats
    console.log('\n📊 Processing state_stats...');
    await con.run(`
      CREATE TABLE temp_state_stats AS
      SELECT 
        state,
        CAST(SUM(violation_count) AS BIGINT) as violation_count,
        CAST(SUM(total_fines) AS DOUBLE) as total_fines
      FROM ocean.gold.state_stats
      WHERE state IS NOT NULL AND state != ''
      GROUP BY state
    `);
    
    await con.run(`DROP TABLE IF EXISTS ocean.gold.state_stats`);
    await con.run(`CREATE TABLE ocean.gold.state_stats AS SELECT * FROM temp_state_stats`);
    console.log('✅ State stats cleaned');
    
    // Verify counts
    console.log('\n📊 Verifying cleanup...');
    const companyCount = await con.run("SELECT COUNT(*) FROM ocean.gold.company_stats");
    const companyRows = await companyCount.getRows();
    console.log(`   Company stats: ${companyRows[0][0]} unique companies`);
    
    const cityCount = await con.run("SELECT COUNT(*) FROM ocean.gold.city_stats");
    const cityRows = await cityCount.getRows();
    console.log(`   City stats: ${cityRows[0][0]} unique cities`);
    
    const stateCount = await con.run("SELECT COUNT(*) FROM ocean.gold.state_stats");
    const stateRows = await stateCount.getRows();
    console.log(`   State stats: ${stateRows[0][0]} unique states`);
    
    console.log('\n✅ Gold layer cleanup complete!');
    
  } catch (e) {
    console.error('❌ Cleanup Failed:', e.message);
    throw e;
  }

  process.exit(0);
}

cleanupGold().catch(console.error);
