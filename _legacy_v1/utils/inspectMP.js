/**
 * Inspect 'CN' and 'MP' state rows
 */
import { DuckDBInstance } from '@duckdb/node-api';

async function inspectMP() {
  const db = await DuckDBInstance.create(':memory:');
  const con = await db.connect();
  
  await con.run("INSTALL aws; LOAD aws;");
  await con.run("INSTALL iceberg; LOAD iceberg;");
  await con.run("CREATE SECRET (TYPE S3, PROVIDER credential_chain);");
  
  const tableBucketArn = 'arn:aws:s3tables:us-east-1:611538926352:bucket/sirsluginston-ventureos-data-ocean';
  await con.run(`ATTACH '${tableBucketArn}' AS ocean (TYPE iceberg, ENDPOINT_TYPE s3_tables)`);
  
  console.log("Checking for MP, CN, or Northern Mariana Islands...");
  
  const r = await con.run(`
    SELECT state, COUNT(*) as count 
    FROM ocean.silver.osha 
    WHERE state IN ('MP', 'CN', 'NM') 
    GROUP BY state
  `);
  // Note: NM is New Mexico, but checking just in case of confusion
  
  const rows = await r.getRows();
  
  // Handle BigInt serialization
  const serialized = JSON.stringify(rows, (key, value) =>
    typeof value === 'bigint' ? value.toString() : value
  , 2);
  
  console.log(serialized);
}

inspectMP().catch(console.error);

