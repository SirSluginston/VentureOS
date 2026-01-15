/**
 * VentureOS Source Importer
 * 
 * Generic CLI tool to ingest datasets into the Data Lake (Bronze Layer).
 * 
 * Usage:
 * node VentureOS/utils/importSource.js --file <path> --type <type> --bucket <bucket-name>
 * 
 * Flow:
 * 1. Reads local file (CSV/JSON).
 * 2. Uploads RAW file to S3 Bronze Layer.
 * 3. (S3 Event triggers Lambda to normalize -> Silver).
 */

import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';
import { createReadStream } from 'fs';
import path from 'path';

async function main() {
  const args = process.argv.slice(2);
  const getArg = (flag) => {
    const idx = args.indexOf(flag);
    return idx !== -1 ? args[idx + 1] : null;
  };

  const filePath = getArg('--file');
  const type = getArg('--type'); // e.g. "osha-severe"
  const bucketName = getArg('--bucket');

  if (!filePath || !type || !bucketName) {
    console.error('Usage: node importSource.js --file <path> --type <type> --bucket <bucket-name>');
    process.exit(1);
  }

  // Derive Agency from Type (osha-severe -> osha)
  const agency = type.split('-')[0]; // simple heuristic
  
  console.log(`🚀 Starting Ingest: ${type} (Agency: ${agency})`);
  console.log(`📂 Input: ${filePath}`);
  console.log(`☁️ Target: s3://${bucketName}/bronze/historical/${agency}/...`);

  try {
    const s3 = new S3Client({}); // Uses local credentials
    const dateStr = new Date().toISOString().split('T')[0];
    const fileName = path.basename(filePath);
    
    // Bronze/Raw path convention
    // Historical: bronze/historical/{AGENCY}/{DATE}/{file}
    const s3Key = `bronze/historical/${agency}/${dateStr}/${fileName}`;
    
    console.log(`⬆️ Uploading to S3...`);
    const fileStream = createReadStream(filePath);
    
    await s3.send(new PutObjectCommand({
      Bucket: bucketName,
      Key: s3Key,
      Body: fileStream
    }));
    
    console.log(`✅ Uploaded Successfully:`);
    console.log(`   Key: ${s3Key}`);
    console.log(`\nNOTE: This should trigger the 'ventureos-parquet-writer' Lambda automatically.`);

  } catch (error) {
    console.error('❌ Upload Failed:', error);
    process.exit(1);
  }
}

// Run if executed directly
if (import.meta.url.startsWith('file:')) {
  if (process.argv[1] === new URL(import.meta.url).pathname || process.argv[1].endsWith('importSource.js')) {
    main();
  }
}

