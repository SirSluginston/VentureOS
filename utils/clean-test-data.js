import { DuckDBInstance } from '@duckdb/node-api';

async function cleanTestData() {
  console.log('🧹 Cleaning Test Data from Silver Layer...');
  
  const db = await DuckDBInstance.create(':memory:');
  const con = await db.connect();
  
  // Setup AWS Auth
  await con.run("INSTALL aws; LOAD aws;");
  await con.run("INSTALL iceberg; LOAD iceberg;");
  await con.run("CREATE SECRET (TYPE S3, PROVIDER credential_chain);");
  
  const tableBucketArn = process.env.S3_TABLE_BUCKET_ARN || 'arn:aws:s3tables:us-east-1:611538926352:bucket/sirsluginston-ventureos-data-ocean';
  
  console.log(`🔗 Attaching Ocean: ${tableBucketArn}`);
  await con.run(`ATTACH '${tableBucketArn}' AS ocean (TYPE iceberg, ENDPOINT_TYPE s3_tables)`);
  
  const agencies = ['osha']; // Add others as needed
  
  for (const agency of agencies) {
      const tableName = `ocean.silver.${agency}`;
      console.log(`\n🔍 Checking ${tableName}...`);
      
      // Count test rows
      const countRes = await con.run(`
          SELECT COUNT(*) 
          FROM ${tableName} 
          WHERE company_name ILIKE '%test%' 
             OR violation_id LIKE 'TEST-%'
             OR city ILIKE '%test%'
      `);
      const count = (await countRes.getRows())[0][0];
      
      if (Number(count) > 0) {
          console.log(`⚠️ Found ${count} test rows in ${tableName}. Deleting...`);
          
          await con.run(`
              DELETE FROM ${tableName} 
              WHERE company_name ILIKE '%test%' 
                 OR violation_id LIKE 'TEST-%'
                 OR city ILIKE '%test%'
          `);
          
          console.log(`✅ Deleted ${count} rows.`);
      } else {
          console.log(`✅ No test data found in ${tableName}.`);
      }
  }
  
  console.log('\n✨ Cleanup Complete');
  process.exit(0);
}

cleanTestData().catch(e => {
    console.error('❌ Cleanup Failed:', e);
    process.exit(1);
});

