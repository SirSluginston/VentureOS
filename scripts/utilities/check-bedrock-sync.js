/**
 * Check if Bedrock content synced to S3 Tables
 * 
 * Usage: node check-bedrock-sync.js <violation_id>
 * Example: node check-bedrock-sync.js TEST-001
 */

import { DuckDBInstance } from '@duckdb/node-api';

const S3_TABLE_BUCKET_ARN = process.env.S3_TABLE_BUCKET_ARN || 
  'arn:aws:s3tables:us-east-1:611538926352:bucket/sirsluginston-ventureos-data-ocean';

const violationId = process.argv[2];

if (!violationId) {
  console.error('Usage: node check-bedrock-sync.js <violation_id>');
  console.error('Example: node check-bedrock-sync.js TEST-001');
  process.exit(1);
}

async function checkBedrockSync() {
  console.log(`🔍 Checking Bedrock sync for violation: ${violationId}`);
  
  const instance = await DuckDBInstance.create(':memory:');
  const con = await instance.connect();
  
  try {
    process.env.HOME = '/tmp';
    await con.run("SET temp_directory='/tmp/duckdb_temp'");
    await con.run("SET home_directory='/tmp'");
    await con.run("INSTALL aws; LOAD aws; INSTALL iceberg; LOAD iceberg;");
    await con.run("CREATE SECRET (TYPE S3, PROVIDER credential_chain);");
    await con.run(`ATTACH '${S3_TABLE_BUCKET_ARN}' AS ocean (TYPE iceberg, ENDPOINT_TYPE s3_tables)`);
    
    // List available tables
    console.log('\n📊 Available S3 Tables:');
    const tables = await con.run("SHOW TABLES FROM ocean.silver;");
    console.log(tables);
    
    // Try to find the violation in each agency table
    const agencies = ['osha', 'epa', 'nhtsa', 'faa', 'uscg', 'fra', 'msha'];
    
    for (const agency of agencies) {
      try {
        console.log(`\n🔎 Checking ocean.silver.${agency}...`);
        const result = await con.run(`
          SELECT 
            violation_id,
            bedrock_title,
            bedrock_description,
            bedrock_tags,
            bedrock_generated_at,
            is_verified,
            verified_at
          FROM ocean.silver.${agency}
          WHERE violation_id = ?
          LIMIT 1
        `, [violationId]);
        
        if (result && result.length > 0) {
          console.log(`\n✅ Found in ${agency} table!`);
          console.log(JSON.stringify(result[0], null, 2));
          
          if (result[0].bedrock_title || result[0].bedrock_description) {
            console.log('\n✅ Bedrock content synced successfully!');
          } else {
            console.log('\n⚠️ Violation found but no Bedrock content yet.');
          }
          return;
        } else {
          console.log(`   No violation found in ${agency} table.`);
        }
      } catch (e) {
        if (e.message.includes('does not exist')) {
          console.log(`   Table ocean.silver.${agency} does not exist yet.`);
        } else {
          console.log(`   Error querying ${agency}: ${e.message}`);
        }
      }
    }
    
    console.log('\n❌ Violation not found in any S3 Table.');
    console.log('   This could mean:');
    console.log('   1. The violation hasn\'t been ingested yet');
    console.log('   2. The violation_id doesn\'t match');
    console.log('   3. The S3 Tables haven\'t been created yet');
    
  } catch (error) {
    console.error('\n❌ Error:', error.message);
    console.error(error.stack);
  } finally {
    await instance.terminate();
  }
}

checkBedrockSync().catch(console.error);

