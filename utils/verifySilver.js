
import { DuckDBInstance } from '@duckdb/node-api';

async function verifySilver() {
  console.log('🔍 Verifying Silver Layer Data...');
  
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
  
  // Query
  console.log('📊 Querying ocean.silver.osha...');
  try {
    const reader = await con.run("SELECT violation_id, agency, company_name, city, state, fine_amount, violation_type, violation_details FROM ocean.silver.osha");
    const rows = await reader.getRows();
    
    console.log(`\n✅ Found ${rows.length} rows in silver.osha:\n`);
    
    // Print formatted rows
    for (const row of rows) {
      console.log('------------------------------------------------');
      console.log(`🆔 ID:      ${row[0]}`);
      console.log(`🏢 Agency:  ${row[1]}`);
      console.log(`🏭 Company: ${row[2]}`);
      console.log(`📍 Loc:     ${row[3]}, ${row[4]}`);
      console.log(`💰 Fine:    $${row[5]}`);
      console.log(`⚠️ Type:    ${row[6]}`);
      console.log(`📋 Details: ${row[7]}`); // JSON string
    }
    console.log('------------------------------------------------');
    
  } catch (e) {
    console.error('❌ Query Failed:', e.message);
  }

  process.exit(0);
}

verifySilver().catch(console.error);



