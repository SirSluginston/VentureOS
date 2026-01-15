import { DuckDBInstance } from '@duckdb/node-api';

/**
 * Delete osha_ita and osha_odi tables from Silver layer
 * Also drops and rebuilds Gold stats tables to remove duplicates
 */
async function deleteOshaTables() {
  console.log('🗑️ Deleting OSHA ITA and ODI tables from Silver layer...');
  
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
    // 1. Delete osha_ita table
    console.log('\n🗑️ Deleting ocean.silver.osha_ita...');
    try {
      await con.run(`DROP TABLE IF EXISTS ocean.silver.osha_ita`);
      console.log('✅ Deleted osha_ita table');
    } catch (e) {
      console.log(`⚠️ Could not delete osha_ita (may not exist): ${e.message}`);
    }
    
    // 2. Delete osha_odi table
    console.log('\n🗑️ Deleting ocean.silver.osha_odi...');
    try {
      await con.run(`DROP TABLE IF EXISTS ocean.silver.osha_odi`);
      console.log('✅ Deleted osha_odi table');
    } catch (e) {
      console.log(`⚠️ Could not delete osha_odi (may not exist): ${e.message}`);
    }
    
    // 3. Rebuild Gold stats tables (to remove duplicates from deleted tables)
    console.log('\n📊 Rebuilding Gold stats tables...');
    
    // Company Stats
    console.log('   Rebuilding company_stats...');
    await con.run(`DROP TABLE IF EXISTS ocean.gold.company_stats`);
    
    // Check if main osha table exists and rebuild from it
    const tables = await con.run("SHOW TABLES FROM ocean.silver");
    const tableRows = await tables.getRows();
    const hasOsha = tableRows.some(row => row[0] === 'osha');
    
    if (hasOsha) {
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
      console.log('✅ Company stats rebuilt from osha table');
    } else {
      console.log('⚠️ No osha table found - company_stats will be empty');
      await con.run(`
        CREATE TABLE ocean.gold.company_stats (
          company_slug VARCHAR,
          company_name VARCHAR,
          violation_count BIGINT,
          total_fines DOUBLE,
          last_violation_date DATE,
          agency VARCHAR[]
        )
      `);
    }
    
    // City Stats
    console.log('   Rebuilding city_stats...');
    await con.run(`DROP TABLE IF EXISTS ocean.gold.city_stats`);
    
    if (hasOsha) {
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
      console.log('✅ City stats rebuilt from osha table');
    } else {
      await con.run(`
        CREATE TABLE ocean.gold.city_stats (
          city VARCHAR,
          state VARCHAR,
          violation_count BIGINT,
          total_fines DOUBLE
        )
      `);
    }
    
    // State Stats
    console.log('   Rebuilding state_stats...');
    await con.run(`DROP TABLE IF EXISTS ocean.gold.state_stats`);
    
    if (hasOsha) {
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
        WHERE state IN ('AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA','HI','ID','IL','IN','IA','KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VT','VA','WA','WV','WI','WY','DC','PR','VI','GU','AS','MP', 'PU', 'AM')
        GROUP BY 1
      `);
      console.log('✅ State stats rebuilt from osha table');
    } else {
      await con.run(`
        CREATE TABLE ocean.gold.state_stats (
          state VARCHAR,
          violation_count BIGINT,
          total_fines DOUBLE
        )
      `);
    }
    
    console.log('\n✨ Cleanup Complete!');
    console.log('📝 You can now reupload your ITA and ODI files from scratch.');
    
  } catch (e) {
    console.error('❌ Cleanup Failed:', e.message);
    throw e;
  }
  
  process.exit(0);
}

deleteOshaTables().catch(console.error);


