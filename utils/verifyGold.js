
import { DuckDBInstance } from '@duckdb/node-api';

async function verifyGold() {
  console.log('🔍 Verifying Gold Layer Data...');
  
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
  
  // 1. Verify Company Stats
  console.log('\n📊 Querying ocean.gold.company_stats...');
  try {
    const reader = await con.run("SELECT company_name, violation_count, total_fines FROM ocean.gold.company_stats ORDER BY company_name");
    const rows = await reader.getRows();
    
    console.log(`✅ Found ${rows.length} rows in company_stats:`);
    for (const row of rows) {
      console.log(`   - ${row[0]}: ${row[1]} violations ($${row[2]})`);
    }
  } catch (e) {
    console.error('❌ Failed to query company_stats:', e.message);
  }

  // 2. Verify State Stats
  console.log('\n📊 Querying ocean.gold.state_stats...');
  try {
    const reader = await con.run("SELECT state, violation_count, total_fines FROM ocean.gold.state_stats ORDER BY state");
    const rows = await reader.getRows();
    
    console.log(`✅ Found ${rows.length} rows in state_stats:`);
    for (const row of rows) {
      console.log(`   - ${row[0]}: ${row[1]} violations ($${row[2]})`);
    }
  } catch (e) {
    console.error('❌ Failed to query state_stats:', e.message);
  }

  process.exit(0);
}

verifyGold().catch(console.error);



