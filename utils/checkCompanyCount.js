import { DuckDBInstance } from '@duckdb/node-api';

async function checkUniqueCompanies() {
  const db = await DuckDBInstance.create(':memory:');
  const con = await db.connect();
  
  await con.run("INSTALL aws; LOAD aws;");
  await con.run("INSTALL iceberg; LOAD iceberg;");
  await con.run("CREATE SECRET (TYPE S3, PROVIDER credential_chain);");
  
  const tableBucketArn = 'arn:aws:s3tables:us-east-1:611538926352:bucket/sirsluginston-ventureos-data-ocean';
  await con.run(`ATTACH '${tableBucketArn}' AS ocean (TYPE iceberg, ENDPOINT_TYPE s3_tables)`);
  
  console.log('Checking Silver layer...');
  const silver = await con.run('SELECT COUNT(DISTINCT company_slug) as unique_companies FROM ocean.silver.osha');
  const silverRows = await silver.getRows();
  console.log(`Silver layer unique companies: ${silverRows[0][0].toString()}`);
  
  console.log('\nChecking Gold layer...');
  const gold = await con.run('SELECT COUNT(*) as total_rows FROM ocean.gold.company_stats');
  const goldRows = await gold.getRows();
  console.log(`Gold layer company_stats rows: ${goldRows[0][0].toString()}`);
  
  process.exit(0);
}

checkUniqueCompanies().catch(console.error);


