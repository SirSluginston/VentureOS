import { S3Client, PutBucketNotificationConfigurationCommand } from "@aws-sdk/client-s3";
import { SQSClient, GetQueueAttributesCommand, SetQueueAttributesCommand, GetQueueUrlCommand } from "@aws-sdk/client-sqs";

// CONFIGURATION
const REGION = "us-east-1";
const ACCOUNT_ID = "611538926352";
const BUCKET_NAME = "venture-os-ocean";
const QUEUE_NAME = "VentureOS-Intake";

const s3 = new S3Client({ region: REGION });
const sqs = new SQSClient({ region: REGION });

async function wireS3ToSQS() {
    console.log(`🔌 Wiring s3://${BUCKET_NAME} -> SQS/${QUEUE_NAME}...`);

    try {
        // 1. Get Queue URL and ARN
        const qUrlRes = await sqs.send(new GetQueueUrlCommand({ QueueName: QUEUE_NAME }));
        const queueUrl = qUrlRes.QueueUrl;

        const qAttrRes = await sqs.send(new GetQueueAttributesCommand({
            QueueUrl: queueUrl,
            AttributeNames: ["QueueArn", "Policy"]
        }));
        const queueArn = qAttrRes.Attributes.QueueArn;
        let policy = JSON.parse(qAttrRes.Attributes.Policy || "{}");

        console.log(`   Detailed Queue ARN: ${queueArn}`);

        // 2. Update SQS Policy to allow S3 Bucket
        const statementId = `Allow-S3-${BUCKET_NAME}`;
        const newStatement = {
            Sid: statementId,
            Effect: "Allow",
            Principal: { Service: "s3.amazonaws.com" },
            Action: "SQS:SendMessage",
            Resource: queueArn,
            Condition: {
                ArnLike: { "aws:SourceArn": `arn:aws:s3:::${BUCKET_NAME}` }
            }
        };

        if (!policy.Statement) policy.Statement = [];

        // Remove existing statement if it exists (to update it)
        policy.Statement = policy.Statement.filter(s => s.Sid !== statementId);
        policy.Statement.push(newStatement);

        await sqs.send(new SetQueueAttributesCommand({
            QueueUrl: queueUrl,
            Attributes: { Policy: JSON.stringify(policy) }
        }));
        console.log("   ✅ SQS Policy Updated to allow Bucket.");

        // 3. Configure S3 Notification
        await s3.send(new PutBucketNotificationConfigurationCommand({
            Bucket: BUCKET_NAME,
            NotificationConfiguration: {
                QueueConfigurations: [
                    {
                        QueueArn: queueArn,
                        Events: ["s3:ObjectCreated:*"],
                        Filter: {
                            Key: {
                                FilterRules: [
                                    { Name: "prefix", Value: "estuary/" } // Only trigger on Estuary uploads
                                ]
                            }
                        }
                    }
                ]
            }
        }));
        console.log("   ✅ S3 Event Notification Configured.");

    } catch (e) {
        console.error("❌ Wiring Failed:", e);
        process.exit(1);
    }
}

wireS3ToSQS();
