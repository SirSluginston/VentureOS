import fs from 'fs';
import { LambdaClient, CreateFunctionCommand, UpdateFunctionCodeCommand, UpdateFunctionConfigurationCommand, GetFunctionCommand, AddPermissionCommand } from "@aws-sdk/client-lambda";
import { S3Client, PutBucketNotificationConfigurationCommand } from "@aws-sdk/client-s3";
import { IAMClient, GetRoleCommand, CreateRoleCommand, PutRolePolicyCommand } from "@aws-sdk/client-iam";
import AdmZip from "adm-zip";

const REGION = "us-east-1";
const ACCOUNT_ID = "611538926352";
const ROLE_NAME = "Lighthouse-ExecutionRole";
const ROLE_ARN = `arn:aws:iam::${ACCOUNT_ID}:role/${ROLE_NAME}`;
const FUNCTION_NAME = "Lighthouse-Aggregator";

const lambda = new LambdaClient({ region: REGION });
const iam = new IAMClient({ region: REGION });

async function ensureRole() {
    console.log(`🛡️ Ensuring Role: ${ROLE_NAME}...`);
    try {
        await iam.send(new GetRoleCommand({ RoleName: ROLE_NAME }));
    } catch (e) {
        if (e.name === 'NoSuchEntityException') {
            const trustPolicy = {
                Version: "2012-10-17",
                Statement: [{
                    Effect: "Allow",
                    Principal: { Service: "lambda.amazonaws.com" },
                    Action: "sts:AssumeRole"
                }]
            };
            await iam.send(new CreateRoleCommand({
                RoleName: ROLE_NAME,
                AssumeRolePolicyDocument: JSON.stringify(trustPolicy)
            }));
            console.log("   Created new role.");
            // Wait for propagation
            await new Promise(r => setTimeout(r, 10000));
        } else {
            throw e;
        }
    }

    // Always update policy
    const policy = {
        Version: "2012-10-17",
        Statement: [
            {
                Sid: "OceanRead",
                Effect: "Allow",
                Action: ["s3:GetObject", "s3:ListBucket"],
                Resource: [
                    "arn:aws:s3:::venture-os-ocean",
                    "arn:aws:s3:::venture-os-ocean/coast/*"
                ]
            },
            {
                Sid: "DynamoWrite",
                Effect: "Allow",
                Action: ["dynamodb:PutItem", "dynamodb:UpdateItem", "dynamodb:BatchWriteItem", "dynamodb:GetItem"],
                Resource: "arn:aws:dynamodb:*:*:table/VentureOS-Lighthouse"
            },
            {
                Sid: "Logging",
                Effect: "Allow",
                Action: ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
                Resource: "arn:aws:logs:*:*:*"
            }
        ]
    };

    await iam.send(new PutRolePolicyCommand({
        RoleName: ROLE_NAME,
        PolicyName: "Lighthouse-Access",
        PolicyDocument: JSON.stringify(policy)
    }));
    console.log("   ✅ Policy updated/attached.");

    // Safety wait for IAM propagation
    await new Promise(r => setTimeout(r, 5000));
}

async function deploy() {
    await ensureRole();
    console.log(`🚀 Deploying ${FUNCTION_NAME}...`);

    // Zip - use paths relative to this script's directory
    const zip = new AdmZip();
    zip.addLocalFile("aggregate.js", "", "index.js"); // Rename to index.js for handler
    zip.addLocalFile("package.json");
    if (fs.existsSync("node_modules")) {
        zip.addLocalFolder("node_modules", "node_modules");
    }
    const zipBuffer = zip.toBuffer();

    try {
        await lambda.send(new GetFunctionCommand({ FunctionName: FUNCTION_NAME }));

        // Update Code
        await lambda.send(new UpdateFunctionCodeCommand({
            FunctionName: FUNCTION_NAME,
            ZipFile: zipBuffer
        }));

        // Wait for update
        await new Promise(r => setTimeout(r, 5000));

        // Update Config
        await lambda.send(new UpdateFunctionConfigurationCommand({
            FunctionName: FUNCTION_NAME,
            Role: ROLE_ARN,
            Handler: "index.handler",
            Runtime: "nodejs22.x", // Updated to 22.x as 24 might not be standard yet or causing issues
            Timeout: 300, // 5 mins
            MemorySize: 512
        }));

        console.log("✅ Updated successfully.");

    } catch (e) {
        if (e.name === 'ResourceNotFoundException') {
            await lambda.send(new CreateFunctionCommand({
                FunctionName: FUNCTION_NAME,
                Runtime: "nodejs22.x",
                Role: ROLE_ARN,
                Handler: "index.handler",
                Code: { ZipFile: zipBuffer },
                Timeout: 300,
                MemorySize: 512
            }));
            console.log("✅ Created successfully.");
        } else {
            throw e;
        }
    }

    await wireS3Trigger();
}

async function wireS3Trigger() {
    console.log("🔗 Wiring S3 Trigger (Coast -> Lighthouse)...");
    const s3 = new S3Client({ region: REGION });
    const bucket = "venture-os-ocean";

    // Get Lambda ARN first
    const func = await lambda.send(new GetFunctionCommand({ FunctionName: FUNCTION_NAME }));
    const funcArn = func.Configuration.FunctionArn;

    // Grant Permission for S3 to invoke Lambda
    try {
        await lambda.send(new AddPermissionCommand({
            FunctionName: FUNCTION_NAME,
            StatementId: "s3-coast-trigger-v2", // Changed ID to ensure new permission
            Action: "lambda:InvokeFunction",
            Principal: "s3.amazonaws.com",
            SourceArn: `arn:aws:s3:::${bucket}`
        }));
    } catch (e) {
        if (e.name !== 'ResourceConflictException') console.error("Permission Warning (Trigger):", e.message);
    }

    // Configure Bucket Notification
    await s3.send(new PutBucketNotificationConfigurationCommand({
        Bucket: bucket,
        NotificationConfiguration: {
            LambdaFunctionConfigurations: [
                {
                    LambdaFunctionArn: funcArn,
                    Events: ["s3:ObjectCreated:*"],
                    Filter: {
                        Key: {
                            FilterRules: [
                                { Name: "prefix", Value: "coast/" }
                            ]
                        }
                    }
                }
            ]
        }
    }));
    console.log("✅ Trigger Wired: coast/*.json -> Lighthouse");
}

deploy();
