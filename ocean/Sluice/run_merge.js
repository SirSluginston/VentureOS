import { AthenaClient, StartQueryExecutionCommand, GetQueryExecutionCommand } from "@aws-sdk/client-athena";

const athena = new AthenaClient({ region: "us-east-1" });

const QUERY = `
MERGE INTO events AS target
USING "AwsDataCatalog"."bronze"."staging_events" AS source
ON target.event_id = source.event_id

WHEN MATCHED THEN UPDATE SET
    agency = source.agency,
    ingested_at = source.ingested_at,
    event_date = source.event_date,
    state = source.state,
    city = source.city,
    city_slug = source.city_slug,
    company_slug = source.company_slug,
    site_id = source.site_id,
    event_title = source.event_title,
    event_description = source.event_description,
    event_details = source.event_details,
    raw_data = source.raw_data,
    bedrock_event_title = source.bedrock_event_title,
    bedrock_event_description = source.bedrock_event_description

WHEN NOT MATCHED THEN INSERT (
    event_id, agency, ingested_at, event_date,
    state, city, city_slug, company_slug, site_id,
    event_title, event_description, event_details,
    raw_data, bedrock_event_title, bedrock_event_description
) VALUES (
    source.event_id, source.agency, source.ingested_at, source.event_date,
    source.state, source.city, source.city_slug, source.company_slug, source.site_id,
    source.event_title, source.event_description, source.event_details,
    source.raw_data, source.bedrock_event_title, source.bedrock_event_description
);
`;

const CATALOG = "s3tablescatalog/venture-os-the-deep";
const DATABASE = "silver";
const WORKGROUP = "primary";
const RESULT_BUCKET = "s3://venture-os-confluence/athena-results/";

async function runMerge() {
    console.log("🚀 Submitting MERGE Query to Athena...");
    console.log(`   Catalog: ${CATALOG}`);
    console.log(`   Database: ${DATABASE}`);

    try {
        const start = await athena.send(new StartQueryExecutionCommand({
            QueryString: QUERY,
            QueryExecutionContext: {
                Catalog: CATALOG,
                Database: DATABASE
            },
            ResultConfiguration: {
                OutputLocation: RESULT_BUCKET
            },
            WorkGroup: WORKGROUP
        }));

        const queryId = start.QueryExecutionId;
        console.log(`✅ Query Submitted! ID: ${queryId}`);
        console.log("⏳ Waiting for completion...");

        while (true) {
            await new Promise(r => setTimeout(r, 2000));

            const statusCalls = await athena.send(new GetQueryExecutionCommand({ QueryExecutionId: queryId }));
            const status = statusCalls.QueryExecution.Status.State;
            const reason = statusCalls.QueryExecution.Status.StateChangeReason;

            if (status === 'SUCCEEDED') {
                console.log(`✅ MERGE COMPLETE.`);
                console.log(`   Data Scanned: ${statusCalls.QueryExecution.Statistics.DataScannedInBytes} bytes`);
                console.log(`   Execution Time: ${statusCalls.QueryExecution.Statistics.EngineExecutionTimeInMillis} ms`);
                break;
            } else if (status === 'FAILED' || status === 'CANCELLED') {
                console.error(`❌ MERGE FAILED: ${status}`);
                console.error(`   Reason: ${reason}`);
                break;
            } else {
                process.stdout.write(".");
            }
        }

    } catch (e) {
        console.error("❌ Execution Error:", e);
    }
}

runMerge();
