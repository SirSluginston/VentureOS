
import { AthenaClient, StartQueryExecutionCommand, GetQueryExecutionCommand } from "@aws-sdk/client-athena";

const client = new AthenaClient({ region: "us-east-1" });
const WORKGROUP = "primary";
const RESULT_BUCKET = "s3://venture-os-ocean/athena-results/";

async function dropReef() {
    console.log("💣 Dropping Stale 'reef' Table...");
    const sql = "DROP TABLE IF EXISTS reef";

    try {
        const start = await client.send(new StartQueryExecutionCommand({
            QueryString: sql,
            ResultConfiguration: { OutputLocation: RESULT_BUCKET },
            WorkGroup: WORKGROUP
        }));

        console.log(`   Running ID: ${start.QueryExecutionId}`);
        // No need to wait, it's fast.
    } catch (e) {
        console.error("Error:", e);
    }
}

dropReef();
