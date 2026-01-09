/**
 * Presigned URL Generator for Direct S3 Upload
 * 
 * Purpose: Generate presigned URLs for direct browser → S3 uploads
 * Avoids Lambda costs and timeout issues for large files
 * 
 * Cost Comparison:
 * - Direct S3 Upload: $0.023 per GB (data transfer only)
 * - Lambda Upload: $0.023 per GB (data transfer) + Lambda invocation costs + timeout risk
 * 
 * Result: Direct upload is cheaper AND faster!
 */

import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';
import { getSignedUrl } from '@aws-sdk/s3-request-presigner';

const s3Client = new S3Client({ region: process.env.AWS_REGION || 'us-east-1' });
const DATA_BUCKET = process.env.DATA_BUCKET || 'sirsluginston-ventureos-data';

/**
 * Generate presigned URL for direct S3 upload
 */
export async function generatePresignedUrl(agency, date, fileName, fileSize) {
  // Validate inputs
  if (!agency || !date || !fileName) {
    throw new Error('agency, date, and fileName are required');
  }
  
  // File size validation
  // AWS S3 supports up to 50TB per object
  // Single PUT (presigned URL): Works up to ~5GB, AWS handles multipart automatically for larger
  // Browser uploads: Handle large files fine, but may be slow for very large files
  // 
  // We don't restrict size - AWS handles it. Just warn for very large files.
  const fileSizeMB = fileSize / 1024 / 1024;
  
  if (fileSize > 4 * 1024 * 1024 * 1024) { // > 4GB
    console.warn(`⚠️ Very large file (${fileSizeMB.toFixed(2)}MB). Approaching single PUT limit (~5GB). Consider using AWS CLI with multipart upload or splitting the file.`);
  } else if (fileSize > 500 * 1024 * 1024) { // > 500MB
    console.warn(`⚠️ Large file upload: ${fileSizeMB.toFixed(2)}MB. This may take a while to upload and process.`);
  }
  
  // Generate S3 key
  // Format: bronze/historical/{agency}/{date}/{fileName}
  const dateStr = date instanceof Date ? date.toISOString().split('T')[0] : date;
  const s3Key = `bronze/historical/${agency.toLowerCase()}/${dateStr}/${fileName}`;
  
  // Create PutObject command
  const command = new PutObjectCommand({
    Bucket: DATA_BUCKET,
    Key: s3Key,
    ContentType: getContentType(fileName),
    // Optional: Add metadata
    Metadata: {
      uploadedBy: 'admin-ui',
      uploadedAt: new Date().toISOString(),
      agency: agency.toLowerCase()
    }
  });
  
  // Generate presigned URL (valid for 1 hour)
  const uploadUrl = await getSignedUrl(s3Client, command, { expiresIn: 3600 });
  
  return {
    uploadUrl,
    s3Key,
    expiresAt: new Date(Date.now() + 3600 * 1000).toISOString(),
    bucket: DATA_BUCKET
  };
}

/**
 * Get content type from file extension
 */
function getContentType(fileName) {
  const ext = fileName.split('.').pop()?.toLowerCase();
  const types = {
    'csv': 'text/csv',
    'json': 'application/json',
    'jsonl': 'application/jsonl',
    'txt': 'text/plain'
  };
  return types[ext] || 'application/octet-stream';
}

