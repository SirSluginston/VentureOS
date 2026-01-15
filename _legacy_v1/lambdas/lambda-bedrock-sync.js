import { DuckDBInstance } from '@duckdb/node-api';
import { unmarshall } from '@aws-sdk/util-dynamodb';

let instance;
let connection;

const S3_TABLE_BUCKET_ARN = process.env.S3_TABLE_BUCKET_ARN || 
  'arn:aws:s3tables:us-east-1:611538926352:bucket/sirsluginston-ventureos-data-ocean';

async function getDuckDB() {
  if (connection) return connection;
  instance = await DuckDBInstance.create(':memory:');
  connection = await instance.connect();
  process.env.HOME = '/tmp';
  await connection.run("SET temp_directory='/tmp/duckdb_temp'");
  await connection.run("SET home_directory='/tmp'");
  await connection.run("INSTALL aws; LOAD aws; INSTALL iceberg; LOAD iceberg;");
  await connection.run("CREATE SECRET (TYPE S3, PROVIDER credential_chain);");
  await connection.run(`ATTACH '${S3_TABLE_BUCKET_ARN}' AS ocean (TYPE iceberg, ENDPOINT_TYPE s3_tables)`);
  return connection;
}

export const handler = async (event) => {
  console.log('🔄 Bedrock Sync Started (Inline Method)');
  if (!event.Records || event.Records.length === 0) return { statusCode: 200 };

  const con = await getDuckDB();
  await con.run(`DROP TABLE IF EXISTS stream_batch;`);
  await con.run(`
    CREATE TEMP TABLE stream_batch (
      violation_id VARCHAR,
      bedrock_title VARCHAR,
      bedrock_description VARCHAR,
      bedrock_tags VARCHAR[],
      bedrock_generated_at TIMESTAMP,
      is_verified BOOLEAN,
      verified_at TIMESTAMP
    )
  `);

  const valueRows = [];

  for (const record of event.Records) {
    if (record.eventName === 'REMOVE') continue;
    const data = unmarshall(record.dynamodb.NewImage);
    if (data.SK !== 'BEDROCK_CONTENT' && data.SK !== 'ProcessedContent') continue;

    const pk = data.PK || data.pk;
    const violationId = pk.replace('VIOLATION#', '');
    const bedrockData = data.ProcessedContent || data;

    // Helper to escape single quotes for SQL safety
    const esc = (val) => val ? `'${String(val).replace(/'/g, "''")}'` : 'NULL';
    
    // Format Tags as a DuckDB Array literal: ['tag1', 'tag2']
    let tags = bedrockData.tags || [];
    if (!Array.isArray(tags)) tags = tags ? [tags] : [];
    const tagsSql = tags.length > 0 
        ? `[${tags.map(t => `'${String(t).replace(/'/g, "''")}'`).join(',')}]` 
        : 'NULL';

    const title = esc(bedrockData.title_bedrock || bedrockData.title);
    const desc = esc(bedrockData.description_bedrock || bedrockData.explanation);
    const genAt = esc(bedrockData.generated_at || bedrockData.bedrock_generated_at);
    const isVerified = !!(bedrockData.reviewed_at || bedrockData.attribution?.includes('Reviewed'));
    const verAt = esc(bedrockData.reviewed_at || bedrockData.ProcessedContent?.reviewedAt);

    // Build the row string with explicit casting
    valueRows.push(`(
      ${esc(violationId)}::VARCHAR, 
      ${title}::VARCHAR, 
      ${desc}::VARCHAR, 
      ${tagsSql}::VARCHAR[], 
      ${genAt}::TIMESTAMP, 
      ${isVerified}::BOOLEAN, 
      ${verAt}::TIMESTAMP
    )`);
  }

  if (valueRows.length > 0) {
    try {
      console.log(`📥 Inserting ${valueRows.length} rows into temp table`);
      await con.run(`INSERT INTO stream_batch VALUES ${valueRows.join(',')}`);

      // Extract violation_ids from temp table to find their agencies
      const violationIds = valueRows.map(row => {
        const match = row.match(/\(([^,]+)::VARCHAR/);
        return match ? match[1].replace(/'/g, '') : null;
      }).filter(Boolean);

      console.log(`🔍 Finding agencies for ${violationIds.length} violations...`);
      
      // Try to find which agency table each violation belongs to
      // Common agencies: osha, msha, nhtsa, faa, uscg, fra, epa
      const agencies = ['osha', 'msha', 'nhtsa', 'faa', 'uscg', 'fra', 'epa'];
      const agencyMap = new Map(); // violation_id -> agency

      for (const agency of agencies) {
        try {
          const query = `
            SELECT violation_id FROM ocean.silver.${agency}
            WHERE violation_id IN (${violationIds.map(id => `'${id}'`).join(',')})
          `;
          const reader = await con.run(query);
          const rows = await reader.getRows();
          for (const row of rows) {
            agencyMap.set(row[0], agency);
          }
        } catch (e) {
          // Table might not exist yet, skip
          continue;
        }
      }

      console.log(`📍 Found ${agencyMap.size} violations in agency tables`);

      // MERGE into each agency table that has violations
      const agenciesToUpdate = [...new Set(agencyMap.values())];
      for (const agency of agenciesToUpdate) {
        const violationsInAgency = violationIds.filter(id => agencyMap.get(id) === agency);
        console.log(`🔄 Merging ${violationsInAgency.length} violations into ocean.silver.${agency}...`);
        
        await con.run(`
          MERGE INTO ocean.silver.${agency} AS main
          USING stream_batch AS updates
          ON main.violation_id = updates.violation_id
          WHEN MATCHED THEN 
            UPDATE SET 
              bedrock_title = updates.bedrock_title,
              bedrock_description = updates.bedrock_description,
              bedrock_tags = updates.bedrock_tags,
              bedrock_generated_at = updates.bedrock_generated_at,
              is_verified = updates.is_verified,
              verified_at = updates.verified_at
        `);
      }

      console.log('✅ Merge Success');
    } catch (error) {
      console.error('❌ SQL Execution Error:', error);
      throw error;
    }
  }

  return { statusCode: 200 };
};