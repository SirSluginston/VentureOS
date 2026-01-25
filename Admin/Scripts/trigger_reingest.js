
import { S3Client, ListObjectsV2Command, CopyObjectCommand } from "@aws-sdk/client-s3";

const s3 = new S3Client({ region: "us-east-1" });
const BUCKET = "venture-os-confluence";
const PREFIX = "Historical/"; // Target only historical data

async function reingest() {
    console.log("🌊 STARTING RE-INGESTION WAVE...");
    console.log(`Target: s3://${BUCKET}/${PREFIX}`);

    let continuationToken;
    let count = 0;

    try {
        do {
            const listCmd = new ListObjectsV2Command({
                Bucket: BUCKET,
                Prefix: PREFIX,
                ContinuationToken: continuationToken
            });
            const res = await s3.send(listCmd);

            if (res.Contents) {
                for (const obj of res.Contents) {
                    if (obj.Key.endsWith('.csv')) {
                        console.log(`Processing: ${obj.Key}`);

                        // "Touch" the file by copying it to itself
                        // This triggers the S3 ObjectCreated event -> SQS -> Sluice
                        await s3.send(new CopyObjectCommand({
                            Bucket: BUCKET,
                            CopySource: `${BUCKET}/${obj.Key}`,
                            Key: obj.Key,
                            MetadataDirective: "REPLACE", // Force metadata update to ensure event trigger
                            Metadata: {
                                "reingest-timestamp": new Date().toISOString()
                            }
                        }));
                        count++;
                    }
                }
            }
            continuationToken = res.NextContinuationToken;
        } while (continuationToken);

        console.log(`\n✅ SUCCESSFULLY TRIGGERED PROCESSING FOR ${count} FILES.`);
        console.log("⏳ The Sluice pipeline is now churning. Monitor SQS 'VentureOS-Flume' for activity.");

    } catch (e) {
        console.error("❌ RE-INGESTION FAILED:", e);
    }
}

reingest();
