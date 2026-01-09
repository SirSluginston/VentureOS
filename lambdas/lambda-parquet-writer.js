/**
 * Batch Processor Lambda (VentureOS)
 * 
 * Purpose: Bronze -> Silver ETL
 * Trigger: S3 Object Created (s3://bucket/bronze/raw/...)
 * 
 * Process:
 * 1. Parse raw file (CSV/JSON) from S3.
 * 2. Select Normalizer based on path (e.g. /osha-severe/).
 * 3. Convert to Parquet (Unified Schema).
 * 4. Write to Silver (Buffer or Archive).
 */

import { S3Client, GetObjectCommand } from '@aws-sdk/client-s3';
import { convertToParquet } from './utils/parquetWriter.js';
import { parseCSV } from './utils/csvParser.js';
import { normalizeSevereInjuryReport, normalizeEnforcementData } from './utils/normalizers/osha.js';

const s3Client = new S3Client({ region: process.env.AWS_REGION || 'us-east-1' });

// Map path segments to normalizers
const NORMALIZERS = {
  'osha-severe_injury': normalizeSevereInjuryReport,
  'osha-enforcement': normalizeEnforcementData,
  // 'faa-drone_incident': ...
};

export async function handler(event) {
  console.log('🚀 Batch Processor Lambda started');
  
  // Handle S3 Event Notifications (batch size 1 usually)
  if (event.Records && event.Records[0].eventSource === 'aws:s3') {
    return await handleS3Event(event.Records[0]);
  }

  // Handle Manual Invocation (Testing)
  if (event.violations) {
    return await handleManualEvent(event);
  }

  console.error('❌ Unknown event format:', JSON.stringify(event));
  return { statusCode: 400, body: 'Unknown event format' };
}

/**
 * Handle S3 Object Created Event
 */
async function handleS3Event(record) {
  const bucket = record.s3.bucket.name;
  const key = decodeURIComponent(record.s3.object.key.replace(/\+/g, ' '));
  
  console.log(`📂 New File: s3://${bucket}/${key}`);

  // Path: bronze/{frequency}/{AGENCY}/{VIOLATION_TYPE}/{DATE}/{filename}
  // e.g. bronze/historical/osha/severe_injury/2026-01-08/dump.csv
  let type = null;
  
  // Robust Folder-based Detection (Agency + Violation Type)
  if (key.includes('/osha/severe_injury/')) {
      type = 'osha-severe_injury';
  } else if (key.includes('/osha/enforcement/')) {
      type = 'osha-enforcement';
  } else if (key.includes('/faa/drone_incident/')) {
      type = 'faa-drone_incident'; // Placeholder
  } else if (key.includes('/epa/hazardous_spill/')) {
      type = 'epa-hazardous_spill'; // Placeholder
  }
  
  // Strict Fallback: Log warning if structure is violated
  if (!type && (key.includes('/historical/') || key.includes('/daily/'))) {
     console.warn(`⚠️ Unrecognized Folder Structure: ${key}`);
  }

  const normalizer = NORMALIZERS[type];

  if (!normalizer) {
    console.warn(`⚠️ Skipped: No normalizer for type '${type}' (Key: ${key})`);
    return { statusCode: 200, body: 'Skipped (Unknown Type)' };
  }

  try {
    // 1. Read Raw File
    const getCommand = new GetObjectCommand({ Bucket: bucket, Key: key });
    const response = await s3Client.send(getCommand);
    const content = await response.Body.transformToString();

    // 2. Parse (CSV or JSON)
    let rawData = [];
    if (key.endsWith('.csv')) {
      rawData = parseCSV(content);
    } else if (key.endsWith('.json')) {
      rawData = JSON.parse(content);
    } else {
      console.warn('⚠️ Unknown file extension');
      return { statusCode: 200, body: 'Skipped (Unknown Extension)' };
    }

    console.log(`📊 Parsed ${rawData.length} raw rows`);

    // 3. Normalize
    const violations = rawData.map(row => {
      try { return normalizer(row); } 
      catch (e) { return null; }
    }).filter(Boolean);

    console.log(`✅ Normalized ${violations.length} valid violations`);

    // 4. Determine Silver Path
    // silver/violations/buffer/ingest_date=YYYY-MM-DD/uuid.parquet
    // We rely on parquetWriter to handle the "local -> S3" logic?
    // Wait, parquetWriter needs to write to S3 *Tables* or S3 Path directly.
    // In Lambda, we can't write to local file easily and upload manually if using DuckDB S3 support.
    // DuckDB can write directly to S3!
    
    // Construct S3 Output Path (Iceberg/Parquet convention)
    const ingestDate = new Date().toISOString().split('T')[0];
    const uuid = Math.random().toString(36).substring(7);
    const outputPath = `s3://${bucket}/silver/violations/buffer/ingest_date=${ingestDate}/${type}-${uuid}.parquet`;

    // 5. Write Parquet (to S3 Tables)
    // ARN comes from Env Var: S3_TABLE_ARN
    // e.g. arn:aws:s3tables:us-east-1:123:bucket/ocean/table/violations_table
    
    const s3TableArn = process.env.S3_TABLE_ARN;
    
    if (!s3TableArn) {
      throw new Error('❌ Missing Env Var: S3_TABLE_ARN');
    }
    
    // --- GATEKEEPER LOGIC ---
    // We only write to DynamoDB if the file is in a "live" folder (e.g. 'daily', 'api').
    // Historical backfills are skipped to save DynamoDB Write Units.
    // Logic: 
    // - If key includes '/historical/', SKIP DynamoDB.
    // - Else (e.g. '/daily/', '/fresh/'), WRITE to DynamoDB.
    const writeToDynamo = !key.includes('/historical/');
    
    if (writeToDynamo) {
        console.log('⚡ DynamoDB Sync: ENABLED (Live Data)');
    } else {
        console.log('zz DynamoDB Sync: SKIPPED (Historical Backfill)');
    }

    // Dynamic Table Name Logic (Consolidated by Agency)
    // Use the Agency (e.g., 'osha') as the table name (e.g., 'osha')
    // The violation_type column inside the table will differentiate 'severe' vs 'enforcement'
    let targetArn = s3TableArn;
    if (type) {
        // Extract agency from type (e.g. 'osha-severe' -> 'osha')
        const agency = type.split('-')[0]; // 'osha', 'faa', 'epa'
        const tableName = agency; // Simple agency-level table
        
        // Replace the table name in the ARN
        targetArn = s3TableArn.replace(/\/table\/([^\/]+)\/[^\/]+$/, `/table/$1/${tableName}`);
        console.log(`➡️ Dynamic Table ARN: ${targetArn} (Derived from agency: ${agency})`);
    }
    
    console.log(`➡️ Writing to S3 Table: ${targetArn}`);
    await convertToParquet(violations, targetArn, {
        writeToDynamo: writeToDynamo,
        type: type // Passed for Agency detection in Dynamo logic
    });

    return { 
      statusCode: 200, 
      body: JSON.stringify({ 
        message: 'Success', 
        rows: violations.length, 
        output: outputPath 
      }) 
    };

  } catch (error) {
    console.error('❌ Processing Failed:', error);
    throw error; // Trigger Lambda Retry
  }
}

/**
 * Handle Manual Test Payload
 */
async function handleManualEvent(event) {
  const outputPath = event.outputPath || 'manual_test_output.parquet';
  await convertToParquet(event.violations, outputPath);
  return { statusCode: 200, body: 'Manual batch processed' };
}
