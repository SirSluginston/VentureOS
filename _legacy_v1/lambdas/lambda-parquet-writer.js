/**
 * Batch Processor Lambda (VentureOS)
 * 
 * Purpose: Bronze -> Silver ETL
 * Trigger: SQS (Buffering S3 Events) or S3 Direct
 */

import { S3Client, GetObjectCommand } from '@aws-sdk/client-s3';
import { convertToParquet } from './utils/parquetWriter.js';
import { parseCSV } from './utils/csvParser.js';
import { normalizeSevereInjuryReport, normalizeEnforcementData } from './utils/normalizers/osha.js';
import { normalizeOdiReport } from './utils/normalizers/osha_odi.js';
import { normalizeItaReport } from './utils/normalizers/osha_ita.js';

const s3Client = new S3Client({ region: process.env.AWS_REGION || 'us-east-1' });

// Map path segments to normalizers
const NORMALIZERS = {
  'osha-severe_injury': normalizeSevereInjuryReport,
  'osha-enforcement': normalizeEnforcementData,
  'osha-odi': normalizeOdiReport,
  'osha-ita': normalizeItaReport,
};

export async function handler(event) {
  console.log('🚀 Batch Processor Lambda started');
  
  // Handle SQS Event (Buffering S3 Events)
  if (event.Records && event.Records[0].eventSource === 'aws:sqs') {
    console.log(`📨 Received ${event.Records.length} SQS messages`);
    const results = [];
    
    for (const record of event.Records) {
        try {
            const body = JSON.parse(record.body);
            
            // Skip S3 test events (sent when notifications are configured)
            if (body.Service === 'Amazon S3' && body.Event === 's3:TestEvent') {
                console.log('ℹ️ Skipping S3 test event');
                continue;
            }
            
            // S3 Event is inside the SQS body
            if (body.Records && body.Records[0].eventSource === 'aws:s3') {
                const result = await handleS3Event(body.Records[0]);
                results.push(result);
            } else {
                console.warn('⚠️ SQS message body is not a valid S3 event:', JSON.stringify(body).substring(0, 200));
            }
        } catch (e) {
            console.error('❌ Failed to process SQS message:', e);
            throw e; 
        }
    }
    return { statusCode: 200, body: 'SQS Batch Processed', results };
  }

  // Handle S3 Event Notifications (Direct)
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

  // Path Logic - Handle both historical and daily paths
  let type = null;
  const pathParts = key.split('/');
  const historicalIndex = pathParts.indexOf('historical');
  const dailyIndex = pathParts.indexOf('daily');
  
  // Check for historical path: bronze/historical/{agency}/{normalizer}/{date}/file.csv
  if (historicalIndex >= 0 && pathParts.length > historicalIndex + 2) {
    const normalizerFromPath = pathParts[historicalIndex + 2];
    if (NORMALIZERS[normalizerFromPath]) {
      type = normalizerFromPath;
      console.log(`✅ Matched normalizer (historical): ${type}`);
    } else {
      console.warn(`⚠️ Normalizer "${normalizerFromPath}" not found`);
      return { statusCode: 400, body: `Skipped: Unknown normalizer` };
    }
  } 
  // Check for daily path: bronze/daily/{agency}/{normalizer}/{date}/file.json
  else if (dailyIndex >= 0 && pathParts.length > dailyIndex + 2) {
    const normalizerFromPath = pathParts[dailyIndex + 2];
    if (NORMALIZERS[normalizerFromPath]) {
      type = normalizerFromPath;
      console.log(`✅ Matched normalizer (daily): ${type}`);
    } else {
      console.warn(`⚠️ Normalizer "${normalizerFromPath}" not found`);
      return { statusCode: 400, body: `Skipped: Unknown normalizer` };
    }
  } else {
      console.warn(`⚠️ Invalid Path Structure: ${key}`);
      return { statusCode: 400, body: 'Skipped: Invalid Path Structure - must be bronze/historical/... or bronze/daily/...' };
  }
  
  const normalizer = NORMALIZERS[type];

  try {
    // 1. Read Raw File (Load into Memory - 2GB RAM is tight but manageable with splits)
    const getCommand = new GetObjectCommand({ Bucket: bucket, Key: key });
    const response = await s3Client.send(getCommand);
    const content = await response.Body.transformToString(); 
    
    // 2. Parse (CSV or JSON)
    let rawData = [];
    if (key.endsWith('.csv')) {
      rawData = parseCSV(content);
    } else if (key.endsWith('.json')) {
      rawData = JSON.parse(content);
    }

    console.log(`📊 Parsed ${rawData.length} raw rows`);

    // Extract filename for metadata enrichment
    const filename = key.split('/').pop();

    // 3. Normalize
    const violations = rawData.map(row => {
      try { return normalizer(row, filename); } 
      catch (e) { return null; }
    }).filter(Boolean);

    console.log(`✅ Normalized ${violations.length} valid violations`);

    // 4. Determine ARN and Write
    const s3TableArn = process.env.S3_TABLE_ARN;
    const writeToDynamo = !key.includes('/historical/');
    
    let targetArn = s3TableArn;
    if (type) {
        // Use full slug as table name (e.g. osha_severe_injury) for clean schema separation
        const tableName = type.replace(/-/g, '_'); 
        targetArn = s3TableArn.replace(/\/table\/([^\/]+)\/[^\/]+$/, `/table/$1/${tableName}`);
        console.log(`➡️ Dynamic Table ARN: ${targetArn}`);
    }
    
    console.log(`➡️ Writing to S3 Table: ${targetArn}`);
    await convertToParquet(violations, targetArn, {
        writeToDynamo: writeToDynamo,
        type: type
    });

    return { 
      statusCode: 200, 
      body: JSON.stringify({ message: 'Success', rows: violations.length }) 
    };

  } catch (error) {
    console.error('❌ Processing Failed:', error);
    throw error;
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
