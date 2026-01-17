import { S3Client, GetObjectCommand } from "@aws-sdk/client-s3";
import { SQSClient, SendMessageBatchCommand } from "@aws-sdk/client-sqs";
import { parse } from "csv-parse";
import { Readable } from "stream";

const s3 = new S3Client({});
const sqs = new SQSClient({});

const PROCESSING_QUEUE_URL = process.env.PROCESSING_QUEUE_URL; // We will need to set this env var

export const handler = async (event) => {
    console.log("🌊 Sluice Splitter: Incoming Wave...");

    for (const record of event.Records) {
        // SQS (Intake) -> Lambda
        // The Intake SQS message body contains the S3 event
        let s3Event;
        try {
            s3Event = JSON.parse(record.body);
        } catch (e) {
            console.error("Failed to parse SQS body", e);
            continue;
        }

        if (s3Event.Records) {
            for (const s3Record of s3Event.Records) {
                await processFile(s3Record.s3.bucket.name, s3Record.s3.object.key);
            }
        }
    }
};

async function processFile(bucket, key) {
    console.log(`Processing Stream: s3://${bucket}/${key}`);

    // Get Stream
    const response = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
    const stream = response.Body;

    // Parse CSV
    const parser = stream.pipe(parse({
        columns: true,
        skip_empty_lines: true,
        trim: true,
        relax_column_count: true // Handle weird rows gracefully
    }));

    let batch = [];
    let batchCount = 0;

    for await (const record of parser) {
        // Metadata to pass to Processor
        const enrichedRecord = {
            raw: record,
            meta: {
                source_bucket: bucket,
                source_key: key,
                ingested_at: new Date().toISOString()
            }
        };

        batch.push({
            Id: `msg_${batchCount}_${Date.now()}`,
            MessageBody: JSON.stringify(enrichedRecord)
        });

        if (batch.length >= 10) { // SQS Batch Limit is 10
            await sendBatch(batch);
            batch = [];
        }
        
        batchCount++;
    }

    // Flush remaining
    if (batch.length > 0) {
        await sendBatch(batch);
    }

    console.log(`✅ Split Complete: ${batchCount} rows sent to Processor.`);
}

async function sendBatch(entries) {
    try {
        await sqs.send(new SendMessageBatchCommand({
            QueueUrl: PROCESSING_QUEUE_URL,
            Entries: entries
        }));
    } catch (e) {
        console.error("SQS Batch Send Failed:", e);
        // In real prod, we might want to retry or DLQ here
        throw e; // Fail the lambda so SQS (Intake) retries the whole file
    }
}

