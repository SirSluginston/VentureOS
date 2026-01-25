import { S3Client, PutBucketNotificationConfigurationCommand, GetBucketNotificationConfigurationCommand } from "@aws-sdk/client-s3";

const s3 = new S3Client({ region: "us-east-1" });
const BUCKET = "venture-os-ocean";
const ACCOUNT_ID = "611538926352";
const REGION = "us-east-1";

async function configureS3Notifications() {
    console.log("🔔 Configuring S3 Notifications...");

    // Get existing configuration
    const existing = await s3.send(new GetBucketNotificationConfigurationCommand({ Bucket: BUCKET }));
    console.log("Existing config:", JSON.stringify(existing, null, 2));

    const newConfig = {
        QueueConfigurations: [
            {
                Id: "estuary-to-intake",
                QueueArn: `arn:aws:sqs:${REGION}:${ACCOUNT_ID}:VentureOS-Intake`,
                Events: ["s3:ObjectCreated:*"],
                Filter: {
                    Key: {
                        FilterRules: [{ Name: "Prefix", Value: "estuary/" }]
                    }
                }
            }
        ],
        LambdaFunctionConfigurations: existing.LambdaFunctionConfigurations || []
    };

    await s3.send(new PutBucketNotificationConfigurationCommand({
        Bucket: BUCKET,
        NotificationConfiguration: newConfig
    }));

    console.log("✅ S3 notifications configured!");
    console.log("   estuary/* → SQS VentureOS-Intake");
}

configureS3Notifications();
