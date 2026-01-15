import { DuckDBInstance } from '@duckdb/node-api';

/**
 * Delete osha_ita table from Silver layer
 * 
 * Usage: node deleteOshaIta.js
 */
async function deleteOshaIta() {
  console.log('🗑️ Deleting OSHA ITA table from Silver layer...');
  
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
    // Check if table exists first
    const tables = await con.run("SHOW TABLES FROM ocean.silver");
    const tableRows = await tables.getRows();
    const hasTable = tableRows.some(row => row[0] === 'osha_ita');
    
    if (!hasTable) {
      console.log('ℹ️ Table ocean.silver.osha_ita does not exist. Nothing to delete.');
      process.exit(0);
    }
    
    // Get row count before deletion
    const countRes = await con.run("SELECT COUNT(*) FROM ocean.silver.osha_ita");
    const countRows = await countRes.getRows();
    const rowCount = countRows[0][0];
    
    console.log(`📊 Found ${rowCount} rows in osha_ita table`);
    
    // Delete table
    console.log('\n🗑️ Deleting ocean.silver.osha_ita...');
    await con.run(`DROP TABLE IF EXISTS ocean.silver.osha_ita`);
    console.log('✅ Successfully deleted osha_ita table');
    
    console.log('\n✨ Cleanup Complete!');
    console.log('📝 You can now reupload your ITA files from scratch.');
    
  } catch (e) {
    console.error('❌ Deletion Failed:', e.message);
    throw e;
  }
  
  process.exit(0);
}

deleteOshaIta().catch(console.error);


