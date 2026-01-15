import { DuckDBInstance } from '@duckdb/node-api';

/**
 * Delete osha_odi table from Silver layer
 * 
 * Usage: node deleteOshaOdi.js
 */
async function deleteOshaOdi() {
  console.log('🗑️ Deleting OSHA ODI table from Silver layer...');
  
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
    const hasTable = tableRows.some(row => row[0] === 'osha_odi');
    
    if (!hasTable) {
      console.log('ℹ️ Table ocean.silver.osha_odi does not exist. Nothing to delete.');
      process.exit(0);
    }
    
    // Get row count before deletion
    const countRes = await con.run("SELECT COUNT(*) FROM ocean.silver.osha_odi");
    const countRows = await countRes.getRows();
    const rowCount = countRows[0][0];
    
    console.log(`📊 Found ${rowCount} rows in osha_odi table`);
    
    // Delete table
    console.log('\n🗑️ Deleting ocean.silver.osha_odi...');
    await con.run(`DROP TABLE IF EXISTS ocean.silver.osha_odi`);
    console.log('✅ Successfully deleted osha_odi table');
    
    console.log('\n✨ Cleanup Complete!');
    console.log('📝 You can now reupload your ODI files from scratch.');
    
  } catch (e) {
    console.error('❌ Deletion Failed:', e.message);
    throw e;
  }
  
  process.exit(0);
}

deleteOshaOdi().catch(console.error);


