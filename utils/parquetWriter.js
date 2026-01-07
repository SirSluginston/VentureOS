/**
 * Parquet Writer Utility (VentureOS)
 * 
 * Converts JSON violations to Parquet format using DuckDB
 * Used by: Batch Processor Lambda (Bronze → Silver layer)
 * 
 * CRITICAL REQUIREMENTS:
 * - event_date stored as DATE type (not string) for fast Athena range scans
 * - fine_amount stored as DOUBLE (not string) for accurate math
 * - Uses /tmp for DuckDB temp files (Lambda requirement)
 * - Uses IAM role credentials (no hardcoded keys)
 */

import { Database } from 'duckdb-async';
import { writeFile, unlink } from 'fs/promises';

/**
 * Initialize DuckDB with Lambda-safe configuration
 * @returns {Promise<Database>} Configured DuckDB instance
 */
async function initDuckDB() {
  console.log('🦆 Initializing DuckDB...');
  
  // Create database instance (using async create method)
  const db = await Database.create(':memory:');
  
  // CRITICAL: Set temp directory to /tmp (Lambda requirement)
  await db.run("SET temp_directory='/tmp/duckdb_temp'");
  
  // CRITICAL: Set home directory to /tmp (DuckDB needs a writable home)
  await db.run("SET home_directory='/tmp'");
  
  // Install and load AWS extensions (httpfs + aws for S3 Tables/Iceberg)
  await db.run("INSTALL httpfs; LOAD httpfs; INSTALL aws; LOAD aws;");
  
  // Use IAM role credentials (not hardcoded keys!)
  const isLambda = process.env.AWS_LAMBDA_FUNCTION_NAME !== undefined;
  if (isLambda) {
    await db.run("CALL load_aws_credentials();");
    console.log('✅ Using Lambda IAM role credentials');
  } else {
    console.log('⚠️  Running locally (using default AWS credentials)');
  }
  
  return db;
}

/**
 * Convert violations array to Parquet file in S3 Table Bucket (Iceberg format)
 * @param {Array} violations - Array of violation objects
 * @param {string} outputPath - S3 Tables ARN or table name (e.g., 'violations_silver')
 * @returns {Promise<{rowCount: number, fileSize: number}>} Write stats
 */
export async function convertToParquet(violations, outputPath) {
  console.log(`📝 Converting ${violations.length} violations to Parquet...`);
  console.log(`📂 Output: ${outputPath}`);
  
  const db = await initDuckDB();
  const tempJsonPath = '/tmp/temp_violations.json';
  
  try {
    // Write violations to a temp JSON file (DuckDB reads from file, not string)
    await writeFile(tempJsonPath, JSON.stringify(violations));
    
    // Create temporary table from the temp JSON file
    await db.run(`
      CREATE TEMPORARY TABLE violations AS
      SELECT 
        agency,
        state,
        city,
        company_name,
        company_slug,
        -- CRITICAL: Cast to DATE type (not string!) for fast Athena range scans
        CAST(event_date AS DATE) AS event_date,
        -- CRITICAL: Cast to DOUBLE (not string!) for accurate leaderboard math
        CAST(fine_amount AS DOUBLE) AS fine_amount,
        violation_type,
        violation_id,
        source_url,
        raw_title,
        raw_description,
        bedrock_title,
        bedrock_description,
        tags
      FROM read_json_auto('${tempJsonPath}')
    `);
    
    console.log('✅ Temporary table created');
    
    // Verify data types (critical validation!)
    const dateTypeCheck = await db.all(`SELECT typeof(event_date) as type FROM violations LIMIT 1`);
    const fineTypeCheck = await db.all(`SELECT typeof(fine_amount) as type FROM violations LIMIT 1`);
    
    console.log(`📊 Data type validation:`);
    console.log(`   event_date: ${dateTypeCheck[0]?.type} (expected: DATE)`);
    console.log(`   fine_amount: ${fineTypeCheck[0]?.type} (expected: DOUBLE)`);
    
    if (dateTypeCheck[0]?.type !== 'DATE') {
      throw new Error(`❌ event_date is ${dateTypeCheck[0]?.type}, expected DATE!`);
    }
    
    if (fineTypeCheck[0]?.type !== 'DOUBLE') {
      throw new Error(`❌ fine_amount is ${fineTypeCheck[0]?.type}, expected DOUBLE!`);
    }
    
    // Write to S3 Table (Iceberg) or Local File (Parquet)
    if (outputPath.endsWith('.parquet')) {
      // Local Test Path - use COPY for file output
      await db.run(`COPY violations TO '${outputPath}' (FORMAT PARQUET, COMPRESSION SNAPPY)`);
      console.log(`✅ Wrote to local Parquet file`);
    } else {
      // Production S3 Table Path
      console.log(`➡️ Attempting write to Iceberg table: ${outputPath}`);
      
      try {
        // Try appending first (common case)
        await db.run(`INSERT INTO '${outputPath}' SELECT * FROM violations`);
        console.log(`✅ Appended to existing Iceberg table`);
      } catch (err) {
        // If table doesn't exist, create it (CTAS)
        if (err.message.includes('does not exist')) {
          console.log(`⚠️ Table not found, creating new Iceberg table...`);
          await db.run(`CREATE TABLE '${outputPath}' AS SELECT * FROM violations`);
          console.log(`✅ Created and populated new Iceberg table`);
        } else {
          throw err; // Re-throw other errors
        }
      }
    }
    
    console.log(`✅ Wrote ${violations.length} violations to Parquet`);
    
    // Return stats
    return {
      rowCount: violations.length,
      fileSize: 0 // TODO: Get actual file size if needed
    };
    
  } finally {
    // Cleanup temp file
    try {
      await unlink(tempJsonPath);
    } catch (e) { /* ignore cleanup errors */ }

    await db.close();
    console.log('🦆 DuckDB connection closed');
  }
}

/**
 * Test function (local development only)
 * Usage: node VentureOS/utils/parquetWriter.js
 */
async function testLocal() {
  console.log('🧪 Running local test...');
  
  // Sample violations for testing
  const violations = [
    {
      agency: 'OSHA',
      state: 'TX',
      city: 'Austin',
      company_name: 'Test Company Inc',
      company_slug: 'test-company-inc',
      event_date: '2024-01-15',
      fine_amount: 50000,
      violation_type: 'Serious',
      violation_id: 'OSHA-2024-001',
      source_url: 'https://osha.gov/...',
      raw_title: 'Fall hazard violation',
      raw_description: 'Worker fell from height due to lack of fall protection equipment.',
      bedrock_title: '$50K Fine: Austin Company Cited for Fatal Fall Hazard',
      bedrock_description: 'OSHA issued a $50,000 fine after a worker fell from height. The company failed to provide fall protection equipment.',
      tags: ['fall-protection', 'construction', 'serious']
    },
    {
      agency: 'OSHA',
      state: 'CA',
      city: 'Los Angeles',
      company_name: 'Another Company LLC',
      company_slug: 'another-company-llc',
      event_date: '2024-02-20',
      fine_amount: 125000,
      violation_type: 'Willful',
      violation_id: 'OSHA-2024-002',
      source_url: 'https://osha.gov/...',
      raw_title: 'Chemical exposure violation',
      raw_description: 'Workers exposed to hazardous chemicals without proper PPE.',
      bedrock_title: '$125K Fine: LA Company Exposed Workers to Toxic Chemicals',
      bedrock_description: 'OSHA issued a $125,000 fine after workers were exposed to hazardous chemicals without PPE.',
      tags: ['chemical-exposure', 'ppe', 'willful']
    }
  ];
  
  // For local testing, we'll use a local path (DuckDB can write to local files too)
  // In production, this would be an S3 Tables table name
  const outputPath = './test-output.parquet';
  
  try {
    // Note: Local test writes to file, not S3 Table
    // In production Lambda, outputPath would be S3 Tables table name
    await convertToParquet(violations, outputPath);
    console.log('✅ Local test completed successfully!');
    console.log(`📂 Check file: ${outputPath}`);
    console.log('⚠️  Note: Production will use S3 Tables ARN, not local file path');
  } catch (error) {
    console.error('❌ Local test failed:', error);
    process.exit(1);
  }
}

// Run test if executed directly
if (import.meta.url.startsWith('file:')) {
  const modulePath = new URL(import.meta.url).pathname;
  if (process.argv[1] === modulePath || process.argv[1].endsWith('parquetWriter.js')) {
    testLocal().catch(console.error);
  }
}

