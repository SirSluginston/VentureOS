/**
 * Rebuild Gold Layer Tables
 * 
 * Rebuilds Gold tables from Silver layer (ocean.silver.osha).
 * This fixes duplicates, renames columns, and ensures data consistency.
 */

import { DuckDBInstance } from '@duckdb/node-api';

async function rebuildGold() {
  console.log('🏗️ Rebuilding Gold Layer Tables from Silver...');
  
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
    // Check if Silver table exists
    const tables = await con.run("SHOW TABLES FROM ocean.silver");
    const tableRows = await tables.getRows();
    const hasOsha = tableRows.some(row => row[0] === 'osha');
    
    if (!hasOsha) {
        throw new Error("ocean.silver.osha table not found!");
    }

    // 1. Company Stats
    console.log('\n📊 Rebuilding company_stats...');
    await con.run(`DROP TABLE IF EXISTS ocean.gold.company_stats`);
    
    await con.run(`
      CREATE TABLE ocean.gold.company_stats AS
      SELECT 
        company_slug,
        MAX(company_name) as company_name,
        CAST(COUNT(*) AS BIGINT) as violation_count,
        CAST(SUM(fine_amount) AS DOUBLE) as total_fines,
        MAX(event_date) as last_violation_date,
        LIST(DISTINCT agency) as agency
      FROM ocean.silver.osha
      GROUP BY company_slug
    `);
    console.log('✅ Company stats rebuilt');
    
    
    // 2. City Stats
    console.log('\n📊 Rebuilding city_stats...');
    await con.run(`DROP TABLE IF EXISTS ocean.gold.city_stats`);
    
    await con.run(`
      CREATE TABLE ocean.gold.city_stats AS
      SELECT 
        city,
        CASE 
          WHEN state = 'PU' THEN 'PR' 
          WHEN state = 'AM' THEN 'AS'
          ELSE state 
        END as state,
        CAST(COUNT(*) AS BIGINT) as violation_count,
        CAST(SUM(fine_amount) AS DOUBLE) as total_fines
      FROM ocean.silver.osha
      WHERE city IS NOT NULL AND city != ''
        AND state IN ('AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA','HI','ID','IL','IN','IA','KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VT','VA','WA','WV','WI','WY','DC','PR','VI','GU','AS','MP', 'PU', 'AM')
      GROUP BY city, 2
    `);
    console.log('✅ City stats rebuilt');
    
    
    // 3. State Stats
    console.log('\n📊 Rebuilding state_stats...');
    await con.run(`DROP TABLE IF EXISTS ocean.gold.state_stats`);
    
    await con.run(`
      CREATE TABLE ocean.gold.state_stats AS
      SELECT 
        CASE 
          WHEN state = 'PU' THEN 'PR' 
          WHEN state = 'AM' THEN 'AS'
          ELSE state 
        END as state,
        CAST(COUNT(*) AS BIGINT) as violation_count,
        CAST(SUM(fine_amount) AS DOUBLE) as total_fines
      FROM ocean.silver.osha
      WHERE state IS NOT NULL AND state != ''
        AND state IN ('AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA','HI','ID','IL','IN','IA','KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VT','VA','WA','WV','WI','WY','DC','PR','VI','GU','AS','MP', 'PU', 'AM')
      GROUP BY 1
    `);
    console.log('✅ State stats rebuilt');
    
    // Verify counts
    console.log('\n📊 Verifying rebuild...');
    const companyCount = await con.run("SELECT COUNT(*) FROM ocean.gold.company_stats");
    const companyRows = await companyCount.getRows();
    console.log(`   Company stats: ${companyRows[0][0]} unique companies`);
    
    const cityCount = await con.run("SELECT COUNT(*) FROM ocean.gold.city_stats");
    const cityRows = await cityCount.getRows();
    console.log(`   City stats: ${cityRows[0][0]} unique cities`);
    
    const stateCount = await con.run("SELECT COUNT(*) FROM ocean.gold.state_stats");
    const stateRows = await stateCount.getRows();
    console.log(`   State stats: ${stateRows[0][0]} unique states`);
    
    console.log('\n✅ Gold layer rebuild complete!');
    
  } catch (e) {
    console.error('❌ Rebuild Failed:', e.message);
    throw e;
  }

  process.exit(0);
}

rebuildGold().catch(console.error);

