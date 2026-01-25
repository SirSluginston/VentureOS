import { SQSClient, GetQueueUrlCommand, SetQueueAttributesCommand, PurgeQueueCommand } from "@aws-sdk/client-sqs";

const sqs = new SQSClient({ region: "us-east-1" });

const QUEUES = [
    "VentureOS-Intake",
    "VentureOS-Flume"
];

async function configureAndPurge() {
    console.log("🚨 STARTING EMERGENCY QUEUE FIX 🚨");

    for (const qName of QUEUES) {
        try {
            console.log(`\nTARGET: ${qName}`);

            // 1. Get URL
            const urlRes = await sqs.send(new GetQueueUrlCommand({ QueueName: qName }));
            const url = urlRes.QueueUrl;
            console.log(`   URL: ${url}`);

            // 2. Update Visibility Timeout to 5 mins (300s)
            console.log(`   ⏳ Updating VisibilityTimeout to 300s...`);
            try {
                await sqs.send(new SetQueueAttributesCommand({
                    QueueUrl: url,
                    Attributes: { VisibilityTimeout: "300" }
                }));
                console.log(`   ✅ Timeout Updated.`);
            } catch (e) {
                console.warn(`   ⚠️ Failed to update timeout: ${e.message}`);
            }

            // 3. Purge Queue
            console.log(`   🔥 PURGING QUEUE...`);
            try {
                await sqs.send(new PurgeQueueCommand({ QueueUrl: url }));
                console.log(`   ✅ PURGE COMMAND SENT.`);
            } catch (e) {
                if (e.name === 'PurgeQueueInProgress') {
                    console.log(`   ⚠️ Purge already in progress (Wait 60s).`);
                } else {
                    throw e;
                }
            }

        } catch (e) {
            console.error(`❌ FAILED to process ${qName}:`, e.message);
        }
    }

    console.log("\n🏁 OPERATION COMPLETE. The loop should stop shortly.");
}

configureAndPurge();
