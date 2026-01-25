
import { AthenaClient, StartQueryExecutionCommand, GetQueryExecutionCommand } from "@aws-sdk/client-athena";

const REGION = "us-east-1";
const WORKGROUP = "primary";
const DATABASE = "silver";
// UPDATED: Deep Layer (Silver) lives in 'deep/' prefix of the Ocean bucket
const BUCKET = "s3://venture-os-ocean/deep/";
const RESULT_BUCKET = "s3://venture-os-ocean/athena-results/";

const client = new AthenaClient({ region: REGION });

const DDL_SETUP = [
    `CREATE DATABASE IF NOT EXISTS ${DATABASE}`,
    `DROP TABLE IF EXISTS ${DATABASE}.events`,

    // CORRECT SCHEMA matching processor.js (The Fat Table)
    `CREATE TABLE IF NOT EXISTS ${DATABASE}.events (
        event_id string,
        event_source string,
        source_url string,
        ingested_at timestamp,
        event_date timestamp,
        state string,
        city string,
        city_slug string,
        company_name string,
        company_slug string,
        site_id string,
        event_title string,
        event_description string,
        event_details string,
        raw_data string,
        bedrock_event_title string,
        bedrock_event_description string,
        bedrock_verified boolean,
        quarantine_reason string
    )
    PARTITIONED BY (event_source, year(event_date)) 
    LOCATION '${BUCKET}'
    TBLPROPERTIES (
        'table_type'='ICEBERG',
        'format'='parquet',
        'write_compression'='snappy',
        'optimize_rewrite_delete_file_threshold'='10',
        'vacuum_min_snapshots_to_keep'='3',
        'vacuum_max_snapshot_age_seconds'='86400'
    )`
];

async function runConfig() {
    console.log("🛠️ Re-Creating Silver Table with CORRECT Schema...");

    for (const sql of DDL_SETUP) {
        console.log(`\nExecuting: ${sql.substring(0, 50)}...`);
        try {
            const start = await client.send(new StartQueryExecutionCommand({
                QueryString: sql,
                ResultConfiguration: { OutputLocation: RESULT_BUCKET },
                WorkGroup: WORKGROUP
            }));

            const queryId = start.QueryExecutionId;
            console.log(`   Running ID: ${queryId}`);

            while (true) {
                await new Promise(r => setTimeout(r, 1000));
                const status = await client.send(new GetQueryExecutionCommand({ QueryExecutionId: queryId }));
                const state = status.QueryExecution.Status.State;

                if (state === 'SUCCEEDED') {
                    console.log("   ✅ Done.");
                    break;
                } else if (state === 'FAILED') {
                    console.error("   ❌ Failed:", status.QueryExecution.Status.StateChangeReason);
                    process.exit(1);
                }
            }
        } catch (e) {
            console.error("💥 Error:", e);
        }
    }
    console.log("\n🎉 Correct Silver Table Ready.");
}

runConfig();
