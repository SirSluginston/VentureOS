/**
 * Rename Gold Layer Column
 * 
 * Renames 'agencies' column to 'agency' in company_stats table.
 */

import { DuckDBInstance } from '@duckdb/node-api';

async function renameColumn() {
  console.log('🔄 Renaming column in Gold Layer...');
  
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
    // Check current schema
    console.log('\n📊 Current schema:');
    const desc = await con.run("DESCRIBE ocean.gold.company_stats");
    const descRows = await desc.getRows();
    descRows.forEach(row => {
      console.log(`   ${row[0]}: ${row[1]}`);
    });
    
    // Rename column
    console.log('\n🔄 Renaming agencies -> agency...');
    await con.run("ALTER TABLE ocean.gold.company_stats RENAME COLUMN agencies TO agency");
    console.log('✅ Column renamed successfully');
    
    // Verify new schema
    console.log('\n📊 New schema:');
    const desc2 = await con.run("DESCRIBE ocean.gold.company_stats");
    const descRows2 = await desc2.getRows();
    descRows2.forEach(row => {
      console.log(`   ${row[0]}: ${row[1]}`);
    });
    
    console.log('\n✅ Rename complete!');
    
  } catch (e) {
    console.error('❌ Rename Failed:', e.message);
    throw e;
  }

  process.exit(0);
}

renameColumn().catch(console.error);


