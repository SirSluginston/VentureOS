/**
 * Batch Processor Lambda (VentureOS)
 * 
 * Converts violations from Bronze (JSON) to Silver (Parquet/Iceberg)
 * 
 * Trigger: Manual invocation (testing) or S3 event (future)
 * Layer: DuckDB Lambda Layer (provides duckdb binaries)
 * 
 * IAM Policies Required:
 * - parquet-writer-policy.json (read Bronze S3, write Silver S3 Tables, Glue access)
 */

import { S3Client, GetObjectCommand } from '@aws-sdk/client-s3';
import { convertToParquet } from './utils/parquetWriter.js';

const s3Client = new S3Client({ region: process.env.AWS_REGION || 'us-east-1' });

/**
 * Lambda Handler
 * @param {Object} event - Lambda event (manual test or S3 trigger)
 * @returns {Object} Response with processing stats
 */
export async function handler(event) {
  console.log('🚀 Batch Processor Lambda started');
  console.log('📥 Event:', JSON.stringify(event, null, 2));
  
  try {
    // For testing: accept violations array directly in event
    if (event.violations && Array.isArray(event.violations)) {
      console.log(`📝 Processing ${event.violations.length} violations from event payload`);
      
      const outputPath = event.outputPath || 'violations_test';
      const result = await convertToParquet(event.violations, outputPath);
      
      return {
        statusCode: 200,
        body: JSON.stringify({
          message: 'Batch processed successfully',
          rowCount: result.rowCount,
          outputPath: outputPath
        })
      };
    }
    
    // For production: read from S3 Bronze layer
    if (event.s3BronzePath) {
      console.log(`📂 Reading violations from S3: ${event.s3BronzePath}`);
      
      const getCommand = new GetObjectCommand({
        Bucket: event.s3Bucket || 'sirsluginston-ventureos-data',
        Key: event.s3BronzePath
      });
      
      const response = await s3Client.send(getCommand);
      const violationsJson = await response.Body.transformToString();
      const violations = JSON.parse(violationsJson);
      
      console.log(`📝 Processing ${violations.length} violations from S3`);
      
      const outputPath = event.outputPath || 'violations_silver';
      const result = await convertToParquet(violations, outputPath);
      
      return {
        statusCode: 200,
        body: JSON.stringify({
          message: 'Batch processed successfully',
          rowCount: result.rowCount,
          sourcePath: event.s3BronzePath,
          outputPath: outputPath
        })
      };
    }
    
    // No valid input
    throw new Error('❌ No violations provided. Include "violations" array or "s3BronzePath" in event.');
    
  } catch (error) {
    console.error('❌ Batch processor failed:', error);
    
    return {
      statusCode: 500,
      body: JSON.stringify({
        error: error.message,
        stack: error.stack
      })
    };
  }
}

