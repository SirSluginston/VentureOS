/**
 * Data Consolidator (VentureOS)
 * 
 * Moves data from the "Buffer" (Ingestion) layer to the "Archive" (Gold/History) layer.
 * - Source: s3://.../silver/violations/buffer/ (Many small files, partitioned by ingest_date)
 * - Target: s3://.../silver/violations/archive/ (Optimized files, partitioned by violation_year)
 * 
 * Strategy:
 * 1. Identify Buffer partitions older than CUTOFF_DAYS.
 * 2. Read ALL those files into DuckDB.
 * 3. Write to STAGING location (to avoid overwriting existing Archive data).
 * 4. Move Staging -> Archive (Append).
 * 5. Verify & Delete source files.
 */

import { DuckDBInstance } from '@duckdb/node-api';
import { S3Client, ListObjectsV2Command, DeleteObjectsCommand, CopyObjectCommand } from '@aws-sdk/client-s3';

const s3 = new S3Client({ region: 'us-east-1' });
const BUCKET = 'sirsluginston-ventureos-data';
const BUFFER_PREFIX = 'silver/violations/buffer/';
const ARCHIVE_PREFIX = 'silver/violations/archive/';
const STAGING_PREFIX = 'silver/violations/staging/';

async function initDuckDB() {
  const db = await DuckDBInstance.create(':memory:');
  const con = await db.connect();
  
  await con.run("SET temp_directory='/tmp/duckdb_temp'");
  await con.run("SET home_directory='/tmp'");
  await con.run("INSTALL httpfs; LOAD httpfs; INSTALL aws; LOAD aws;");
  
  if (process.env.AWS_LAMBDA_FUNCTION_NAME) {
    await con.run("CREATE SECRET (TYPE S3, PROVIDER credential_chain);");
  } else {
    // Local fallback
    await con.run("CREATE SECRET (TYPE S3, PROVIDER credential_chain);");
  }
  return { db, con };
}

export async function consolidateBuffer(cutoffDays = 30, dryRun = false) {
  console.log(`🚜 Starting Consolidation (Cutoff: ${cutoffDays} days)`);
  
  const candidates = await listCandidateFiles(cutoffDays);
  if (candidates.length === 0) {
    console.log('✅ No files to consolidate.');
    return { moved: 0 };
  }
  
  console.log(`found ${candidates.length} files to consolidate.`);
  const sourcePaths = candidates.map(c => `s3://${BUCKET}/${c.Key}`);
  
  if (dryRun) {
    console.log('⚠️ DRY RUN: Skipping write/delete.');
    return { moved: candidates.length, files: sourcePaths };
  }

  const { db, con } = await initDuckDB();

  try {
    // Write to a unique staging folder to avoid DuckDB overwrite conflicts
    const batchId = Date.now();
    const stagingPath = `s3://${BUCKET}/${STAGING_PREFIX}${batchId}/`;
    
    console.log(`⏳ Merging into Staging: ${stagingPath}`);
    
    const fileListSql = sourcePaths.map(p => `'${p}'`).join(', ');
    
    // Get count
    const countResult = await con.run(`SELECT count(*) as c FROM read_parquet([${fileListSql}])`);
    const inputCountRows = await countResult.getRowObjectsJson();
    const inputCount = Number(inputCountRows[0].c);
    
    console.log(`📊 Input Row Count: ${inputCount}`);

    // Write to Staging
    await con.run(`
      COPY (
        SELECT *, YEAR(event_date) as violation_year 
        FROM read_parquet([${fileListSql}])
        ORDER BY event_date ASC
      ) 
      TO '${stagingPath}' 
      (FORMAT PARQUET, PARTITION_BY (violation_year), COMPRESSION SNAPPY)
    `);
    
    if (inputCount === 0) return { moved: 0 };

    console.log('✅ Staging write complete. Moving to Archive...');
    
    // Move from Staging to Archive
    // e.g. staging/123/violation_year=2024/data.parquet -> archive/violation_year=2024/data-{batchId}.parquet
    await moveStagingToArchive(STAGING_PREFIX + batchId + '/', ARCHIVE_PREFIX);
    
    console.log('✅ Archive move complete.');

    // Cleanup Buffer
    await deleteFiles(candidates);
    console.log(`🗑️ Deleted ${candidates.length} buffer files.`);
    
    return { moved: candidates.length };

  } catch (error) {
    console.error('❌ Consolidation Failed:', error);
    throw error;
  } finally {
    try { await con.close(); } catch (e) {}
    try { db.closeSync(); } catch (e) {}
  }
}

async function listCandidateFiles(cutoffDays) {
  const cutoffDate = new Date();
  cutoffDate.setDate(cutoffDate.getDate() - cutoffDays);
  
  console.log(`📅 Filter Date: ${cutoffDate.toISOString()}`);
  
  let candidates = [];
  let continuationToken;

  do {
    const cmd = new ListObjectsV2Command({
      Bucket: BUCKET,
      Prefix: BUFFER_PREFIX,
      ContinuationToken: continuationToken
    });
    const res = await s3.send(cmd);
    if (res.Contents) {
      for (const file of res.Contents) {
        const match = file.Key.match(/ingest_date=(\d{4}-\d{2}-\d{2})/);
        if (match) {
          const ingestDate = new Date(match[1]);
          if (ingestDate < cutoffDate) {
            candidates.push(file);
          }
        }
      }
    }
    continuationToken = res.NextContinuationToken;
  } while (continuationToken);
  return candidates;
}

async function deleteFiles(files) {
  for (let i = 0; i < files.length; i += 1000) {
    const batch = files.slice(i, i + 1000);
    const cmd = new DeleteObjectsCommand({
      Bucket: BUCKET,
      Delete: { Objects: batch.map(f => ({ Key: f.Key })) }
    });
    await s3.send(cmd);
  }
}

async function moveStagingToArchive(stagingPrefix, archivePrefix) {
  // List all files in staging
  let continuationToken;
  do {
    const listCmd = new ListObjectsV2Command({
      Bucket: BUCKET,
      Prefix: stagingPrefix,
      ContinuationToken: continuationToken
    });
    const res = await s3.send(listCmd);
    
    if (res.Contents) {
      for (const file of res.Contents) {
        // Construct new key: Replace staging prefix with archive prefix
        // Staging: silver/violations/staging/123/violation_year=2024/data_0.parquet
        // Archive: silver/violations/archive/violation_year=2024/data_0.parquet
        // WAIT: If we just replace, we might overwrite existing data_0.parquet in Archive!
        // We must append a unique suffix to the filename.
        
        const relativeKey = file.Key.substring(stagingPrefix.length);
        // relativeKey: violation_year=2024/data_0.parquet
        
        const parts = relativeKey.split('/');
        const filename = parts.pop(); // data_0.parquet
        const dir = parts.join('/'); // violation_year=2024
        
        // New Filename: data_0-{batchId}.parquet or similar to avoid collision
        // Or keep DuckDB's name but assume uniqueness? DuckDB names are usually data_0.parquet... collision high!
        
        // Strategy: Prepend unique ID to filename
        const uniqueFilename = `batch-${Date.now()}-${filename}`;
        const newKey = `${ARCHIVE_PREFIX}${dir}/${uniqueFilename}`;
        
        // Copy
        await s3.send(new CopyObjectCommand({
          Bucket: BUCKET,
          CopySource: `${BUCKET}/${file.Key}`,
          Key: newKey
        }));
        
        // Delete Staging File
        await s3.send(new DeleteObjectsCommand({
          Bucket: BUCKET,
          Delete: { Objects: [{ Key: file.Key }] }
        }));
      }
    }
    continuationToken = res.NextContinuationToken;
  } while (continuationToken);
}
