/**
 * VentureOS Compactor - Full Setup Script
 * 
 * This script orchestrates the complete setup:
 * 1. Creates silver namespace in S3 Tables
 * 2. Creates silver.events Iceberg table
 * 3. Creates bronze database in Glue
 * 4. Creates bronze.staging_events external table
 * 
 * Prerequisites:
 * - AWS credentials configured
 * - S3 Table bucket "venture-os-the-deep" exists
 * - S3 bucket "venture-os-confluence" exists with staging/ data
 * 
 * Usage: npm run setup:all
 */

import { S3TablesClient, CreateNamespaceCommand, ListNamespacesCommand, GetTableBucketCommand } from "@aws-sdk/client-s3tables";
import { GlueClient, CreateDatabaseCommand, CreateTableCommand, GetDatabaseCommand, DeleteTableCommand, GetTableCommand } from "@aws-sdk/client-glue";
import { AthenaClient, StartQueryExecutionCommand, GetQueryExecutionCommand, GetQueryResultsCommand } from "@aws-sdk/client-athena";
import { STSClient, GetCallerIdentityCommand } from "@aws-sdk/client-sts";

const REGION = "us-east-1";
const S3_TABLE_BUCKET = "venture-os-the-deep";
const STAGING_BUCKET = "venture-os-confluence";
const ATHENA_OUTPUT = `s3://${STAGING_BUCKET}/athena-results/`;

const s3tables = new S3TablesClient({ region: REGION });
const glue = new GlueClient({ region: REGION });
const athena = new AthenaClient({ region: REGION });
const sts = new STSClient({ region: REGION });

// Colors for console output
const colors = {
    reset: "\x1b[0m",
    bright: "\x1b[1m",
    green: "\x1b[32m",
    yellow: "\x1b[33m",
    red: "\x1b[31m",
    cyan: "\x1b[36m",
    magenta: "\x1b[35m"
};

function log(emoji, message, color = colors.reset) {
    console.log(`${color}${emoji} ${message}${colors.reset}`);
}

function header(title) {
    console.log(`\n${colors.cyan}${"═".repeat(70)}${colors.reset}`);
    console.log(`${colors.cyan}   ${title}${colors.reset}`);
    console.log(`${colors.cyan}${"═".repeat(70)}${colors.reset}\n`);
}

async function getAccountId() {
    const response = await sts.send(new GetCallerIdentityCommand({}));
    return response.Account;
}

// ═══════════════════════════════════════════════════════════════════════════
// STEP 1: Setup S3 Tables (Silver Layer)
// ═══════════════════════════════════════════════════════════════════════════

async function setupSilverNamespace(tableBucketARN) {
    log("🏗️", "Creating silver namespace in S3 Tables...", colors.yellow);
    
    try {
        await s3tables.send(new CreateNamespaceCommand({
            tableBucketARN,
            namespace: ["silver"]
        }));
        log("✅", "Namespace 'silver' created successfully", colors.green);
        return true;
    } catch (e) {
        if (e.name === 'ConflictException' || e.message?.includes('already exists')) {
            log("⚠️", "Namespace 'silver' already exists (OK)", colors.yellow);
            return true;
        }
        log("❌", `Failed to create namespace: ${e.message}`, colors.red);
        return false;
    }
}

async function verifySilverNamespace(tableBucketARN) {
    log("🔍", "Verifying silver namespace...", colors.yellow);
    
    try {
        const response = await s3tables.send(new ListNamespacesCommand({
            tableBucketARN
        }));
        
        const namespaces = response.namespaces?.map(n => n.namespace?.[0]) || [];
        log("📋", `Namespaces found: ${namespaces.join(', ') || '(none)'}`, colors.cyan);
        
        return namespaces.includes('silver');
    } catch (e) {
        log("❌", `Failed to list namespaces: ${e.message}`, colors.red);
        return false;
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// STEP 2: Setup Glue (Bronze Layer)
// ═══════════════════════════════════════════════════════════════════════════

async function setupBronzeDatabase() {
    log("🏗️", "Creating bronze database in Glue...", colors.yellow);
    
    try {
        // Check if exists
        try {
            await glue.send(new GetDatabaseCommand({ Name: "bronze" }));
            log("⚠️", "Database 'bronze' already exists (OK)", colors.yellow);
            return true;
        } catch (e) {
            // Database doesn't exist, create it
        }
        
        await glue.send(new CreateDatabaseCommand({
            DatabaseInput: {
                Name: "bronze",
                Description: "VentureOS Bronze Layer - Staging data from Sluice pipeline",
                LocationUri: `s3://${STAGING_BUCKET}/staging/`
            }
        }));
        log("✅", "Database 'bronze' created successfully", colors.green);
        return true;
    } catch (e) {
        if (e.name === 'AlreadyExistsException') {
            log("⚠️", "Database 'bronze' already exists (OK)", colors.yellow);
            return true;
        }
        log("❌", `Failed to create database: ${e.message}`, colors.red);
        return false;
    }
}

async function setupStagingEventsTable() {
    log("🏗️", "Creating bronze.staging_events table in Glue...", colors.yellow);
    
    const tableName = "staging_events";
    
    // First, try to delete existing table
    try {
        await glue.send(new DeleteTableCommand({
            DatabaseName: "bronze",
            Name: tableName
        }));
        log("🗑️", "Deleted existing staging_events table", colors.yellow);
    } catch (e) {
        // Table doesn't exist, that's fine
    }
    
    try {
        await glue.send(new CreateTableCommand({
            DatabaseName: "bronze",
            TableInput: {
                Name: tableName,
                Description: "Staged events from Sluice Processor (Parquet files)",
                TableType: "EXTERNAL_TABLE",
                Parameters: {
                    "classification": "parquet",
                    "parquet.compression": "SNAPPY",
                    "EXTERNAL": "TRUE"
                },
                StorageDescriptor: {
                    Columns: [
                        { Name: "event_id", Type: "string", Comment: "SHA256 hash of salted raw row" },
                        { Name: "agency", Type: "string", Comment: "Source agency (osha, fda, nhtsa)" },
                        { Name: "ingested_at", Type: "string", Comment: "ISO timestamp of ingestion" },
                        { Name: "event_date", Type: "string", Comment: "Date of the event" },
                        { Name: "state", Type: "string", Comment: "State code (TN, CA, etc.)" },
                        { Name: "city", Type: "string", Comment: "City name" },
                        { Name: "company_slug", Type: "string", Comment: "Resolved company slug" },
                        { Name: "city_slug", Type: "string", Comment: "Generated city slug" },
                        { Name: "raw_data", Type: "string", Comment: "Original row as JSON string" }
                    ],
                    Location: `s3://${STAGING_BUCKET}/staging/`,
                    InputFormat: "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                    OutputFormat: "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                    SerdeInfo: {
                        SerializationLibrary: "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                        Parameters: {
                            "serialization.format": "1"
                        }
                    },
                    Compressed: true
                }
            }
        }));
        log("✅", "Table 'bronze.staging_events' created successfully", colors.green);
        return true;
    } catch (e) {
        log("❌", `Failed to create table: ${e.message}`, colors.red);
        return false;
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// STEP 3: Run Athena Query Helper
// ═══════════════════════════════════════════════════════════════════════════

async function runAthenaQuery(query, catalog = "AwsDataCatalog", database = "bronze") {
    log("🔄", `Running Athena query (catalog: ${catalog})...`, colors.yellow);
    
    try {
        const startResponse = await athena.send(new StartQueryExecutionCommand({
            QueryString: query,
            QueryExecutionContext: {
                Catalog: catalog,
                Database: database
            },
            ResultConfiguration: {
                OutputLocation: ATHENA_OUTPUT
            }
        }));
        
        const executionId = startResponse.QueryExecutionId;
        log("📝", `Query started: ${executionId}`, colors.cyan);
        
        // Poll for completion
        let status = "RUNNING";
        while (status === "RUNNING" || status === "QUEUED") {
            await new Promise(resolve => setTimeout(resolve, 1000));
            
            const statusResponse = await athena.send(new GetQueryExecutionCommand({
                QueryExecutionId: executionId
            }));
            status = statusResponse.QueryExecution.Status.State;
            
            if (status === "FAILED") {
                const reason = statusResponse.QueryExecution.Status.StateChangeReason;
                throw new Error(`Query failed: ${reason}`);
            }
        }
        
        if (status === "SUCCEEDED") {
            const resultsResponse = await athena.send(new GetQueryResultsCommand({
                QueryExecutionId: executionId
            }));
            log("✅", "Query completed successfully", colors.green);
            return resultsResponse.ResultSet;
        }
        
    } catch (e) {
        log("❌", `Athena query failed: ${e.message}`, colors.red);
        throw e;
    }
}

async function testBronzeQuery() {
    log("🧪", "Testing bronze.staging_events query...", colors.yellow);
    
    try {
        const result = await runAthenaQuery(
            "SELECT COUNT(*) as total FROM staging_events",
            "AwsDataCatalog",
            "bronze"
        );
        
        // Parse result
        const rows = result.Rows || [];
        if (rows.length > 1) {
            const count = rows[1].Data[0].VarCharValue;
            log("📊", `Bronze staging contains ${count} events`, colors.magenta);
        }
        return true;
    } catch (e) {
        log("⚠️", `Could not query bronze (may have no data yet): ${e.message}`, colors.yellow);
        return false;
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// MAIN ORCHESTRATOR
// ═══════════════════════════════════════════════════════════════════════════

async function main() {
    console.clear();
    header("VentureOS Compactor - Full Setup");
    
    // Get account info
    const accountId = await getAccountId();
    const tableBucketARN = `arn:aws:s3tables:${REGION}:${accountId}:bucket/${S3_TABLE_BUCKET}`;
    
    log("🔑", `Account: ${accountId}`, colors.cyan);
    log("📦", `S3 Table Bucket: ${S3_TABLE_BUCKET}`, colors.cyan);
    log("📦", `Staging Bucket: ${STAGING_BUCKET}`, colors.cyan);
    
    // ═══════════════════════════════════════════════════════════════════════
    // Phase 1: Silver Layer (S3 Tables)
    // ═══════════════════════════════════════════════════════════════════════
    header("Phase 1: Silver Layer (S3 Tables)");
    
    const silverNsCreated = await setupSilverNamespace(tableBucketARN);
    if (!silverNsCreated) {
        log("❌", "Silver namespace setup failed. Check S3 Table bucket exists.", colors.red);
        process.exit(1);
    }
    
    await verifySilverNamespace(tableBucketARN);
    
    // Note: Creating the events table in S3 Tables is best done via Athena DDL
    // once the catalog is connected
    log("📋", "Silver namespace ready. Table creation will be done via Athena.", colors.cyan);
    
    // ═══════════════════════════════════════════════════════════════════════
    // Phase 2: Bronze Layer (Glue)
    // ═══════════════════════════════════════════════════════════════════════
    header("Phase 2: Bronze Layer (Glue Catalog)");
    
    const bronzeDbCreated = await setupBronzeDatabase();
    if (!bronzeDbCreated) {
        log("❌", "Bronze database setup failed.", colors.red);
        process.exit(1);
    }
    
    const stagingTableCreated = await setupStagingEventsTable();
    if (!stagingTableCreated) {
        log("❌", "Staging events table setup failed.", colors.red);
        process.exit(1);
    }
    
    // ═══════════════════════════════════════════════════════════════════════
    // Phase 3: Verification
    // ═══════════════════════════════════════════════════════════════════════
    header("Phase 3: Verification");
    
    await testBronzeQuery();
    
    // ═══════════════════════════════════════════════════════════════════════
    // Summary
    // ═══════════════════════════════════════════════════════════════════════
    header("Setup Complete!");
    
    console.log(`${colors.green}✅ Silver namespace created in S3 Tables${colors.reset}`);
    console.log(`${colors.green}✅ Bronze database created in Glue${colors.reset}`);
    console.log(`${colors.green}✅ bronze.staging_events table created${colors.reset}`);
    
    console.log(`\n${colors.bright}📋 Remaining Manual Steps:${colors.reset}`);
    console.log(`
   1. ${colors.yellow}Connect Athena to S3 Tables:${colors.reset}
      - Go to Athena Console → Data sources → Create data source
      - Select "S3 Tables" 
      - Name: venture_os_ocean
      - Select bucket: venture-os-the-deep
   
   2. ${colors.yellow}Create silver.events table via Athena:${colors.reset}
      - See ATHENA_SETUP.md for the CREATE TABLE DDL
   
   3. ${colors.yellow}Run the MERGE query:${colors.reset}
      - See merge-staging.sql
      - Or run: MERGE INTO venture_os_ocean.silver.events USING bronze.staging_events ...

   4. ${colors.yellow}(Optional) Schedule daily merge:${colors.reset}
      - Athena Console → Scheduled queries → Create
      - Schedule: cron(0 6 * * ? *)
`);
}

main().catch(e => {
    console.error(`\n${colors.red}❌ Setup failed: ${e.message}${colors.reset}`);
    process.exit(1);
});

