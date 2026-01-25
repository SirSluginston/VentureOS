import { SQSClient, GetQueueAttributesCommand, SetQueueAttributesCommand, GetQueueUrlCommand } from "@aws-sdk/client-sqs";

const sqs = new SQSClient({ region: "us-east-1" });
const QUEUE_NAME = "VentureOS-Intake";
const BUCKET_ARN = "arn:aws:s3:::venture-os-ocean";
const ACCOUNT_ID = "611538926352";

async function fixQueuePolicy() {
    console.log(`🔧 Fixing SQS Policy for ${QUEUE_NAME}...`);

    // Get queue URL
    const urlRes = await sqs.send(new GetQueueUrlCommand({ QueueName: QUEUE_NAME }));
    const queueUrl = urlRes.QueueUrl;
    const queueArn = `arn:aws:sqs:us-east-1:${ACCOUNT_ID}:${QUEUE_NAME}`;

    console.log(`Queue URL: ${queueUrl}`);

    const policy = {
        Version: "2012-10-17",
        Statement: [
            {
                Sid: "AllowS3ToSendMessage",
                Effect: "Allow",
                Principal: {
                    Service: "s3.amazonaws.com"
                },
                Action: "SQS:SendMessage",
                Resource: queueArn,
                Condition: {
                    ArnEquals: {
                        "aws:SourceArn": BUCKET_ARN
                    }
                }
            }
        ]
    };

    await sqs.send(new SetQueueAttributesCommand({
        QueueUrl: queueUrl,
        Attributes: {
            Policy: JSON.stringify(policy)
        }
    }));

    console.log("✅ SQS policy updated! S3 can now send notifications.");
}

fixQueuePolicy();
