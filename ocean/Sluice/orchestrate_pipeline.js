import { AthenaClient, StartQueryExecutionCommand, GetQueryExecutionCommand } from "@aws-sdk/client-athena";
import { S3Client, ListObjectsV2Command, DeleteObjectsCommand } from "@aws-sdk/client-s3";
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// --- CONFIGURATION ---
const REGION = "us-east-1";
const WORKGROUP = "primary";
const RESULT_BUCKET = "s3://venture-os-ocean/athena-results/";

// Catalog Context
const CATALOG = "AwsDataCatalog"; // Standard Glue Catalog
const DATABASE = "silver"; // Standard Database

const PATH_ROOT = (() => {
    // Lambda Deployment Structure: /var/task/Athena/SQL
    const lambdaPath = path.join(__dirname, "Athena", "SQL");
    if (fs.existsSync(lambdaPath)) return lambdaPath;

    // Local Dev Structure: ./Athena/SQL (Relative to Sluice folder)
    return path.resolve(__dirname, "../Athena/SQL");
})();

const INIT_FILE = path.join(PATH_ROOT, "init_reef.sql");
const MERGE_FILE = path.join(PATH_ROOT, "merge_events.sql");
const QUERIES_DIR = path.join(PATH_ROOT, "queries");

// Ordered Execution
const UNLOAD_ORDER = [
    'unload_nations.sql',
    'unload_states.sql',
    'unload_cities.sql',
    'unload_companies.sql'
];

const athena = new AthenaClient({ region: REGION });
const s3 = new S3Client({ region: REGION });

async function cleanS3Target(fullS3Path) {
    // ... (unchanged)
    // Expected format: s3://bucket-name/prefix/
    const match = fullS3Path.match(/s3:\/\/([^\/]+)\/(.+)/);
    if (!match) {
        console.warn(`⚠️ Could not parse S3 path: ${fullS3Path}. Skipping cleanup.`);
        return;
    }

    const bucket = match[1];
    let prefix = match[2];
    if (!prefix.endsWith('/')) prefix += '/'; // Ensure directory semantic

    console.log(`🧹 Cleaning S3 Target: s3://${bucket}/${prefix}`);

    // List and Delete loop
    try {
        let continuationToken = null;
        do {
            const listCmd = new ListObjectsV2Command({
                Bucket: bucket,
                Prefix: prefix,
                ContinuationToken: continuationToken
            });
            const listRes = await s3.send(listCmd);

            if (listRes.Contents && listRes.Contents.length > 0) {
                const objects = listRes.Contents.map(o => ({ Key: o.Key }));
                await s3.send(new DeleteObjectsCommand({
                    Bucket: bucket,
                    Delete: { Objects: objects }
                }));
                process.stdout.write(`   Deleted ${objects.length} files... `);
            }

            continuationToken = listRes.NextContinuationToken;
        } while (continuationToken);
        console.log("\n   ✅ Target Cleaned.");
    } catch (e) {
        console.error(`❌ Failed to clean S3: ${e.message}`);
        throw e;
    }
}

async function runQuery(name, sql) {
    // ... (unchanged logic)
    console.log(`🚀 [${name}] Submitting...`);

    try {
        const start = await athena.send(new StartQueryExecutionCommand({
            QueryString: sql,
            QueryExecutionContext: {
                Catalog: CATALOG,
                Database: DATABASE // Ensure database is key for short table names
            },
            ResultConfiguration: {
                OutputLocation: RESULT_BUCKET
            },
            WorkGroup: WORKGROUP
        }));

        const queryId = start.QueryExecutionId;
        console.log(`⏳ [${name}] Running... (ID: ${queryId})`);

        while (true) {
            await new Promise(r => setTimeout(r, 2000));
            const statusCalls = await athena.send(new GetQueryExecutionCommand({ QueryExecutionId: queryId }));

            const state = statusCalls.QueryExecution.Status.State;
            const reason = statusCalls.QueryExecution.Status.StateChangeReason;
            const stats = statusCalls.QueryExecution.Statistics;

            if (state === 'SUCCEEDED') {
                const time = stats.EngineExecutionTimeInMillis;
                const bytes = stats.DataScannedInBytes;
                console.log(`✅ [${name}] COMPLETE (${time}ms, ${bytes} bytes)`);
                return true;
            } else if (state === 'FAILED' || state === 'CANCELLED') {
                console.error(`❌ [${name}] FAILED: ${reason}`);
                throw new Error(`Query ${name} failed: ${reason}`);
            }
        }
    } catch (e) {
        console.error(`❌ [${name}] ERROR:`, e.message);
        throw e;
    }
}

function extractCoreQuery(sqlContent, type) {
    const clean = sqlContent.replace(/--.*$/gm, '').trim();
    const statements = clean.split(';').map(s => s.trim()).filter(s => s.length > 0);
    const match = statements.find(s => s.toUpperCase().startsWith(type));
    // Fallback: If no type prefix (like MSCK), just take the first non-empty
    if (!match && type === 'ANY') return statements[0];
    if (!match) throw new Error(`No ${type} statement found.`);
    return match;
}

function extractS3Target(sqlStatement) {
    const match = sqlStatement.match(/TO\s+'(s3:\/\/[^']+)'/i);
    return match ? match[1] : null;
}

async function orchestrate() {
    console.log("🌊 STARTING PIPELINE ORCHESTRATION 🌊");
    const start = Date.now();

    try {
        // 1. READ SQL FILES
        const initSqlRaw = fs.readFileSync(INIT_FILE, 'utf-8');
        const initSql = extractCoreQuery(initSqlRaw, 'CREATE');

        const mergeSqlRaw = fs.readFileSync(MERGE_FILE, 'utf-8');
        let mergeSql = extractCoreQuery(mergeSqlRaw, 'MERGE');

        // FORCE FULLY QUALIFIED TARGET
        mergeSql = mergeSql.replace(
            /(MERGE INTO\s+)(events)(\s+AS)/i,
            `$1"${CATALOG}"."${DATABASE}"."events"$3`
        );

        console.log(`📋 Plan:`);
        console.log(`   1. Init Reef: ${path.basename(INIT_FILE)}`);
        console.log(`   2. Repair Reef (MSCK)`);
        console.log(`   3. Merge (Blocking): ${path.basename(MERGE_FILE)}`);
        console.log(`   4. Unloads: ${UNLOAD_ORDER.join(' -> ')}`);

        // PHASE 1: INIT REEF
        console.log("\n--- PHASE 1: INIT STAGING (Reef) ---");
        await runQuery("INIT REEF", initSql);
        await runQuery("REPAIR REEF", "MSCK REPAIR TABLE reef");

        // PHASE 2: MERGE
        console.log("\n--- PHASE 2: MERGE TO DEEP ---");
        await runQuery("MERGE", mergeSql);

        // PHASE 3: UNLOADS
        console.log("\n--- PHASE 3: UNLOAD TO COAST ---");
        for (const file of UNLOAD_ORDER) {
            console.log(`\n🔹 Processing ${file}...`);
            const raw = fs.readFileSync(path.join(QUERIES_DIR, file), 'utf-8');
            let sql = extractCoreQuery(raw, 'UNLOAD');

            // FORCE FULLY QUALIFIED SOURCE
            sql = sql.replace(
                /(FROM\s+)(events)/i,
                `$1"${CATALOG}"."${DATABASE}"."events"`
            );

            // Safety: Clean S3 Target BEFORE Unload
            const s3Target = extractS3Target(sql);
            if (s3Target) {
                await cleanS3Target(s3Target);
            } else {
                console.warn(`⚠️ No S3 target found in SQL. Skipping cleanup.`);
            }

            // Run Query
            await runQuery(file, sql);
        }

        const duration = ((Date.now() - start) / 1000).toFixed(1);
        console.log(`\n🏁 PIPELINE COMPLETE in ${duration}s`);

    } catch (e) {
        console.error("\n💀 PIPELINE ABORTED due to failure.");
        console.error(e);
        process.exit(1);
    }
}

export { orchestrate };

// Auto-run only if called directly
// Auto-run only if called directly

if (process.argv[1] === fileURLToPath(import.meta.url)) {
    orchestrate();
}
