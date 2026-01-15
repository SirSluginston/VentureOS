import { DuckDBInstance } from '@duckdb/node-api';

/**
 * Clear all Gold stats tables (company_stats, city_stats, state_stats)
 * 
 * Usage: node clearGoldStats.js
 * 
 * This will delete all gold layer aggregate tables.
 * Run rebuildGold.js afterwards to rebuild them from Silver layer data.
 */
async function clearGoldStats() {
  console.log('🗑️ Clearing all Gold stats tables...');
  
  const db = await DuckDBInstance.create(':memory:');
  const con = await db.connect();
  
  // Setup AWS Auth
  await con.run("INSTALL aws; LOAD aws;");
  await con.run("INSTALL iceberg; LOAD iceberg;");
  await con.run("CREATE SECRET (TYPE S3, PROVIDER credential_chain);");
  
  const tableBucketArn = process.env.S3_TABLE_BUCKET_ARN || 'arn:aws:s3tables:us-east-1:611538926352:bucket/sirsluginston-ventureos-data-ocean';
  
  console.log(`🔗 Attaching Ocean: ${tableBucketArn}`);
  await con.run(`ATTACH '${tableBucketArn}' AS ocean (TYPE iceberg, ENDPOINT_TYPE s3_tables)`);
  
  try {
    // Check which tables exist
    const tables = await con.run("SHOW TABLES FROM ocean.gold");
    const tableRows = await tables.getRows();
    const existingTables = tableRows.map(row => row[0]);
    
    console.log(`\n📊 Found ${existingTables.length} tables in gold namespace:`, existingTables.join(', '));
    
    // 1. Delete company_stats
    if (existingTables.includes('company_stats')) {
      const countRes = await con.run("SELECT COUNT(*) FROM ocean.gold.company_stats");
      const countRows = await countRes.getRows();
      const rowCount = countRows[0][0];
      console.log(`\n🗑️ Deleting company_stats (${rowCount} rows)...`);
      await con.run(`DROP TABLE IF EXISTS ocean.gold.company_stats`);
      console.log('✅ Deleted company_stats');
    } else {
      console.log('\nℹ️ company_stats does not exist, skipping...');
    }
    
    // 2. Delete city_stats
    if (existingTables.includes('city_stats')) {
      const countRes = await con.run("SELECT COUNT(*) FROM ocean.gold.city_stats");
      const countRows = await countRes.getRows();
      const rowCount = countRows[0][0];
      console.log(`\n🗑️ Deleting city_stats (${rowCount} rows)...`);
      await con.run(`DROP TABLE IF EXISTS ocean.gold.city_stats`);
      console.log('✅ Deleted city_stats');
    } else {
      console.log('\nℹ️ city_stats does not exist, skipping...');
    }
    
    // 3. Delete state_stats
    if (existingTables.includes('state_stats')) {
      const countRes = await con.run("SELECT COUNT(*) FROM ocean.gold.state_stats");
      const countRows = await countRes.getRows();
      const rowCount = countRows[0][0];
      console.log(`\n🗑️ Deleting state_stats (${rowCount} rows)...`);
      await con.run(`DROP TABLE IF EXISTS ocean.gold.state_stats`);
      console.log('✅ Deleted state_stats');
    } else {
      console.log('\nℹ️ state_stats does not exist, skipping...');
    }
    
    console.log('\n✨ Gold stats cleanup complete!');
    console.log('📝 Run rebuildGold.js to rebuild stats from Silver layer data.');
    
  } catch (e) {
    console.error('❌ Cleanup Failed:', e.message);
    throw e;
  }
  
  process.exit(0);
}

clearGoldStats().catch(console.error);


