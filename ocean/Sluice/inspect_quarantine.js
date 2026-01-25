import { S3Client, GetObjectCommand } from "@aws-sdk/client-s3";
import parquets from '@dsnp/parquetjs';
import fs from 'fs';
import path from 'path';

const s3 = new S3Client({ region: "us-east-1" });
const BUCKET = "venture-os-confluence";

const KEYS = [
    "quarantine/2026/2026-01-23T19:27:43.319Z_60f08bcd-5f18-4248-99f5-3e2dafee008b.parquet"
];

async function main() {
    for (const key of KEYS) {
        console.log(`\n🔍 Inspecting Quarantine File: ${key}`);
        const localPath = `temp_${Date.now()}.parquet`;

        try {
            // Download
            const obj = await s3.send(new GetObjectCommand({ Bucket: BUCKET, Key: key }));
            const stream = fs.createWriteStream(localPath);
            await new Promise((resolve, reject) => {
                obj.Body.pipe(stream);
                stream.on('finish', resolve);
                stream.on('error', reject);
            });

            // Read Parquet
            const reader = await parquets.ParquetReader.openFile(localPath);
            const cursor = reader.getCursor();
            let record;
            let count = 0;

            console.log("--- SAMPLE (First 10) ---");
            while (record = await cursor.next()) {
                if (count < 10) {
                    console.log(`[${count + 1}] Reason: ${record.quarantine_reason}`);
                    console.log(`    Parsed: ${record.city}, ${record.state}`);
                    // console.log(`    Company: ${record.company}`);
                    console.log("-------------------");
                }
                count++;
            }
            console.log(`Total Records: ${count}`);
            await reader.close();
        } catch (e) {
            console.error(`Error processing ${key}:`, e.message);
        } finally {
            if (fs.existsSync(localPath)) fs.unlinkSync(localPath);
        }
    }
}

main();
