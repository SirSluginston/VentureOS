/**
 * Inspect 'AM' state rows
 */
import { DuckDBInstance } from '@duckdb/node-api';

async function inspectAM() {
  const db = await DuckDBInstance.create(':memory:');
  const con = await db.connect();
  
  await con.run("INSTALL aws; LOAD aws;");
  await con.run("INSTALL iceberg; LOAD iceberg;");
  await con.run("CREATE SECRET (TYPE S3, PROVIDER credential_chain);");
  
  const tableBucketArn = 'arn:aws:s3tables:us-east-1:611538926352:bucket/sirsluginston-ventureos-data-ocean';
  await con.run(`ATTACH '${tableBucketArn}' AS ocean (TYPE iceberg, ENDPOINT_TYPE s3_tables)`);
  
  const r = await con.run("SELECT city, state, company_name, violation_id FROM ocean.silver.osha WHERE state = 'AM'");
  const rows = await r.getRows();
  console.log(JSON.stringify(rows, null, 2));
}

inspectAM().catch(console.error);


