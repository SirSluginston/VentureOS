import { S3Client, ListObjectsV2Command, GetObjectCommand } from "@aws-sdk/client-s3";
import parquet from '@dsnp/parquetjs';
import fs from 'fs';

const s3 = new S3Client({ region: "us-east-1" });

async function checkReef() {
    console.log("🔍 Checking Reef Parquet files...");

    const list = await s3.send(new ListObjectsV2Command({
        Bucket: "venture-os-ocean",
        Prefix: "reef/",
        MaxKeys: 5
    }));

    for (const obj of list.Contents || []) {
        if (obj.Key.endsWith('.parquet')) {
            console.log(`\n📄 ${obj.Key}`);
            const response = await s3.send(new GetObjectCommand({
                Bucket: "venture-os-ocean",
                Key: obj.Key
            }));

            const buffer = await response.Body.transformToByteArray();
            fs.writeFileSync('./temp.parquet', buffer);

            const reader = await parquet.ParquetReader.openFile('./temp.parquet');
            const cursor = reader.getCursor();
            let record = await cursor.next();
            let count = 0;

            while (record && count < 3) {
                console.log("Row:", JSON.stringify({
                    company_name: record.company_name,
                    company_slug: record.company_slug,
                    city: record.city,
                    state: record.state
                }, null, 2));
                record = await cursor.next();
                count++;
            }

            await reader.close();
            fs.unlinkSync('./temp.parquet');
            break;
        }
    }
}

checkReef();
