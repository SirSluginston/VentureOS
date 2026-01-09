import { DuckDBInstance } from '@duckdb/node-api';

async function cleanBadCities() {
  console.log('🧹 Cleaning Bad Cities from Silver Layer...');
  
  const db = await DuckDBInstance.create(':memory:');
  const con = await db.connect();
  
  // Setup AWS Auth
  await con.run("INSTALL aws; LOAD aws;");
  await con.run("INSTALL iceberg; LOAD iceberg;");
  await con.run("CREATE SECRET (TYPE S3, PROVIDER credential_chain);");
  
  const tableBucketArn = process.env.S3_TABLE_BUCKET_ARN || 'arn:aws:s3tables:us-east-1:611538926352:bucket/sirsluginston-ventureos-data-ocean';
  
  console.log(`🔗 Attaching Ocean: ${tableBucketArn}`);
  await con.run(`ATTACH '${tableBucketArn}' AS ocean (TYPE iceberg, ENDPOINT_TYPE s3_tables)`);
  
  const tableName = `ocean.silver.osha`;
  console.log(`\n🔍 Checking ${tableName}...`);
  
  // Count bad rows
  // Looking for cities with commas (likely parsing errors) or "Unspecified"
  const countRes = await con.run(`
      SELECT COUNT(*) 
      FROM ${tableName} 
      WHERE city LIKE '%,%' 
         OR city ILIKE 'Unspecified%'
  `);
  const count = (await countRes.getRows())[0][0];
  
  if (Number(count) > 0) {
      console.log(`⚠️ Found ${count} rows with bad city names. Deleting...`);
      
      // Preview a few
      const preview = await con.run(`
          SELECT city, state, company_name 
          FROM ${tableName} 
          WHERE city LIKE '%,%' OR city ILIKE 'Unspecified%'
          LIMIT 3
      `);
      const previewRows = await preview.getRows();
      console.log('Sample bad rows:', previewRows);
      
      await con.run(`
          DELETE FROM ${tableName} 
          WHERE city LIKE '%,%' 
             OR city ILIKE 'Unspecified%'
      `);
      
      console.log(`✅ Deleted ${count} rows.`);
  } else {
      console.log(`✅ No bad city names found.`);
  }
  
  console.log('\n✨ Cleanup Complete');
  process.exit(0);
}

cleanBadCities().catch(e => {
    console.error('❌ Cleanup Failed:', e);
    process.exit(1);
});

