import { S3Client, ListObjectsV2Command, GetObjectCommand, PutObjectCommand, DeleteObjectCommand } from "@aws-sdk/client-s3";
import { LambdaClient, InvokeCommand } from "@aws-sdk/client-lambda";
import parquets from '@dsnp/parquetjs';
import fs from 'fs';
import path from 'path';
import crypto from 'crypto';
import { fileURLToPath } from 'url';

// Import Orchestrator Logic (Must be adapted to run in Lambda)
// We will EXECUTE it as a child process or import it if we refactor. 
// For now, let's keep it simple: We call the Athena SDK directly here or import the file.
import { orchestrate } from './orchestrate_pipeline.js';

const s3 = new S3Client({});
const lambda = new LambdaClient({});

const BUCKET = "venture-os-ocean";
const QUARANTINE_PREFIX = "quarantine/";
const STAGING_PREFIX = "staging/";

export const handler = async (event) => {
    console.log("⚡ Sluice-Ops: Received Action", event.action);

    try {
        switch (event.action) {
            case "getQuarantine":
                return await getQuarantineStats(event.limit || 100);
            case "resolveQuarantine":
                return await resolveItems(event.items);
            case "runPipeline":
                return await runPipelineJob();
            default:
                throw new Error(`Unknown action: ${event.action}`);
        }
    } catch (e) {
        console.error("Ops Error:", e);
        return { error: e.message, stack: e.stack };
    }
};

/**
 * 1. Reads all files in quarantine/
 * 2. Aggregates by Similarity (e.g. same Company + same City + same Error)
 * 3. Returns Top N groups.
 */
async function getQuarantineStats(limit) {
    const files = await s3.send(new ListObjectsV2Command({ Bucket: BUCKET, Prefix: QUARANTINE_PREFIX }));
    if (!files.Contents) return { groups: [], totalFiles: 0 };

    const groups = new Map(); // Key -> { count, sample, valid_options? }

    // Optimization: Only read last 20 files to avoid timeout if backlog is huge
    // In a real prod environment, we'd use Athena to query these.
    const recentFiles = files.Contents
        .sort((a, b) => b.LastModified - a.LastModified)
        .slice(0, 20);

    for (const file of recentFiles) {
        try {
            const localPath = `/tmp/${path.basename(file.Key)}`;
            const obj = await s3.send(new GetObjectCommand({ Bucket: BUCKET, Key: file.Key }));

            // Save stream to disk for ParquetJS
            await new Promise((resolve, reject) => {
                const stream = fs.createWriteStream(localPath);
                obj.Body.pipe(stream);
                stream.on('finish', resolve);
                stream.on('error', reject);
            });

            const reader = await parquets.ParquetReader.openFile(localPath);
            const cursor = reader.getCursor();
            let record = null;

            while (record = await cursor.next()) {
                // Group Key: Reason + State + City (to capture the "Overland Park, US" cluster)
                const key = `${record.quarantine_reason}|${record.company_name}|${record.city}|${record.state}`;

                if (!groups.has(key)) {
                    groups.set(key, {
                        id: crypto.randomUUID(),
                        reason: record.quarantine_reason,
                        company: record.company_name || 'Unknown',
                        city: record.city,
                        state: record.state,
                        count: 0,
                        sample_id: record.event_id,
                        file_key: file.Key // Track source file for deletion/resolution
                    });
                }
                groups.get(key).count++;
            }

            await reader.close();
            fs.unlinkSync(localPath);

        } catch (e) {
            console.warn(`Failed to read ${file.Key}:`, e.message);
        }
    }

    // Sort by Popularity
    const sorted = Array.from(groups.values())
        .sort((a, b) => b.count - a.count)
        .slice(0, limit);

    return {
        groups: sorted,
        totalFiles: files.Contents.length,
        scannedFiles: recentFiles.length
    };
}

/**
 * Moves items from Quarantine -> Staging (with fixes)
 * Input: [{ event_id, ...fixedFields, original_file_key }]
 */
async function resolveItems(items) {
    if (!items || items.length === 0) return { success: true, count: 0 };

    const stagingKey = `staging/recovered/${new Date().toISOString()}_${crypto.randomUUID()}.parquet`;
    const localPath = `/tmp/recovered.parquet`;

    // 1. Create Parquet Schema (reusing the standard one)
    // We import schema definition dynamically or define it here
    const schemaDef = {
        event_id: { type: 'UTF8' },
        agency: { type: 'UTF8' },
        ingested_at: { type: 'UTF8' },
        raw_data: { type: 'UTF8' },
        event_date: { type: 'UTF8', optional: true },
        state: { type: 'UTF8', optional: true },
        city: { type: 'UTF8', optional: true },
        city_slug: { type: 'UTF8', optional: true },
        company_slug: { type: 'UTF8', optional: true },
        site_id: { type: 'UTF8', optional: true },
        event_title: { type: 'UTF8', optional: true },
        event_description: { type: 'UTF8', optional: true },
        event_details: { type: 'UTF8', optional: true },
        bedrock_event_title: { type: 'UTF8', optional: true },
        bedrock_event_description: { type: 'UTF8', optional: true }
    };

    const schema = new parquets.ParquetSchema(schemaDef);
    const writer = await parquets.ParquetWriter.openFile(schema, localPath);

    for (const item of items) {
        await writer.appendRow(item); // Item must match schema
    }
    await writer.close();

    // 2. Upload to Staging
    const buffer = fs.readFileSync(localPath);
    await s3.send(new PutObjectCommand({
        Bucket: BUCKET,
        Key: stagingKey,
        Body: buffer,
        ContentType: "application/vnd.apache.parquet"
    }));

    // 3. Delete Original Files? 
    // Complexity: A quarantine file usually has mixed bad rows. usage:
    // If we fix "Overland Park" group, we only fix THOSE rows in the file.
    // We shouldn't delete the whole file unless it's empty. 
    // STRATEGY: For now, we COPY Valid -> Staging. We DO NOT delete from Quarantine automatically.
    // The Admin must click "Purge Fixed" or we implement a complex "Rewrite Quarantine" logic.
    // For V1: Just Copy.

    fs.unlinkSync(localPath);
    return { success: true, count: items.length, stagingKey };
}

async function runPipelineJob() {
    console.log("Triggering Orchestrator...");
    // We invoke the orchestrate function we imported
    await orchestrate(); // This logs to CloudWatch
    return { success: true, message: "Pipeline Execution Completed." };
}
