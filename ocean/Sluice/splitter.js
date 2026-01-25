import { S3Client, GetObjectCommand } from "@aws-sdk/client-s3";
import { SQSClient, SendMessageBatchCommand } from "@aws-sdk/client-sqs";
import { parse } from "csv-parse";
import { Readable } from "stream";

const s3 = new S3Client({});
const sqs = new SQSClient({});

const PROCESSING_QUEUE_URL = process.env.PROCESSING_QUEUE_URL;

export const handler = async (event) => {
    console.log("🌊 Sluice Splitter: Incoming Wave...");

    for (const record of event.Records) {
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

const ROWS_PER_MESSAGE = 100;  // Bundle 100 rows per SQS message
const MESSAGES_PER_BATCH = 10; // SQS max is 10 messages per batch

async function processFile(bucket, key) {
    // 1. Guard against Recursion (Ignore output files)
    if (key.startsWith("reef/") || key.endsWith(".parquet") || key.endsWith(".json")) {
        console.log(`Skipping output file: ${key}`);
        return;
    }

    console.log(`Processing Stream: s3://${bucket}/${key}`);

    // Get Stream
    console.log(`📥 Fetching from S3...`);
    const response = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
    const stream = response.Body;
    console.log(`✅ S3 stream acquired`);

    // Parse CSV
    const parser = stream.pipe(parse({
        columns: true,
        skip_empty_lines: true,
        trim: true,
        relax_column_count: true
    }));

    let rowBuffer = [];      // Accumulate rows for one SQS message
    let sqsBatch = [];       // Accumulate messages for one SQS batch
    let rowCount = 0;
    let messageCount = 0;
    const meta = {
        source_bucket: bucket,
        source_key: key,
        ingested_at: new Date().toISOString()
    };

    for await (const record of parser) {
        if (rowCount === 0) {
            console.log(`🔄 First row parsed, streaming...`);
        }

        rowBuffer.push(record);
        rowCount++;

        // When we have enough rows, create an SQS message
        if (rowBuffer.length >= ROWS_PER_MESSAGE) {
            sqsBatch.push({
                Id: `msg_${messageCount}_${Date.now()}`,
                MessageBody: JSON.stringify({ rows: rowBuffer, meta })
            });
            rowBuffer = [];
            messageCount++;

            // When we have enough messages, send the batch
            if (sqsBatch.length >= MESSAGES_PER_BATCH) {
                await sendBatch(sqsBatch);
                sqsBatch = [];

                // Progress log every 10k rows
                if (rowCount % 10000 < ROWS_PER_MESSAGE * MESSAGES_PER_BATCH) {
                    console.log(`📊 Progress: ${rowCount.toLocaleString()} rows processed`);
                }
            }
        }
    }

    // Flush remaining rows into a message
    if (rowBuffer.length > 0) {
        sqsBatch.push({
            Id: `msg_${messageCount}_${Date.now()}`,
            MessageBody: JSON.stringify({ rows: rowBuffer, meta })
        });
        messageCount++;
    }

    // Flush remaining messages
    if (sqsBatch.length > 0) {
        await sendBatch(sqsBatch);
    }

    console.log(`✅ Split Complete: ${rowCount.toLocaleString()} rows in ${messageCount} messages`);
}

async function sendBatch(entries) {
    try {
        await sqs.send(new SendMessageBatchCommand({
            QueueUrl: PROCESSING_QUEUE_URL,
            Entries: entries
        }));
    } catch (e) {
        console.error("SQS Batch Send Failed:", e);
        throw e;
    }
}
