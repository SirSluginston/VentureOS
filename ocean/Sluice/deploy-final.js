import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { LambdaClient, CreateFunctionCommand, UpdateFunctionCodeCommand, UpdateFunctionConfigurationCommand, GetFunctionCommand, CreateEventSourceMappingCommand } from "@aws-sdk/client-lambda";
import { IAMClient, GetRoleCommand, CreateRoleCommand, PutRolePolicyCommand, GetRolePolicyCommand } from "@aws-sdk/client-iam";
import AdmZip from "adm-zip";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const REGION = "us-east-1";
const ACCOUNT_ID = "611538926352";

// Role Names
const ROLE_SPLITTER = "VentureOS-Role-Splitter";
const ROLE_PROCESSOR = "VentureOS-Role-Processor";
const ROLE_OPS = "VentureOS-Role-Ops";

const lambda = new LambdaClient({ region: REGION });
const iam = new IAMClient({ region: REGION });

// --- POLICY DEFINITIONS ---

const TRUST_POLICY = {
    Version: "2012-10-17",
    Statement: [
        {
            Effect: "Allow",
            Principal: { Service: "lambda.amazonaws.com" },
            Action: "sts:AssumeRole"
        }
    ]
};

const POLICY_SPLITTER = {
    Version: "2012-10-17",
    Statement: [
        {
            Sid: "ReadEstuary",
            Effect: "Allow",
            Action: ["s3:GetObject"],
            Resource: "arn:aws:s3:::venture-os-ocean/estuary/*"
        },
        {
            Sid: "ConsumeIntake",
            Effect: "Allow",
            Action: ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes"],
            Resource: `arn:aws:sqs:${REGION}:${ACCOUNT_ID}:VentureOS-Intake`
        },
        {
            Sid: "SendToFlume",
            Effect: "Allow",
            Action: ["sqs:SendMessage", "sqs:GetQueueUrl", "sqs:GetQueueAttributes"],
            Resource: `arn:aws:sqs:${REGION}:${ACCOUNT_ID}:VentureOS-Flume`
        },
        {
            Sid: "BasicLogging",
            Effect: "Allow",
            Action: ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
            Resource: "arn:aws:logs:*:*:*"
        }
    ]
};

const POLICY_PROCESSOR = {
    Version: "2012-10-17",
    Statement: [
        {
            Sid: "ReceiveFromFlume",
            Effect: "Allow",
            Action: ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes"],
            Resource: `arn:aws:sqs:${REGION}:${ACCOUNT_ID}:VentureOS-Flume`
        },
        {
            Sid: "ReadSextantAndAnchor",
            Effect: "Allow",
            Action: ["dynamodb:GetItem", "dynamodb:Query", "dynamodb:BatchGetItem"],
            Resource: [
                `arn:aws:dynamodb:${REGION}:${ACCOUNT_ID}:table/VentureOS-Sextant`,
                `arn:aws:dynamodb:${REGION}:${ACCOUNT_ID}:table/VentureOS-Anchor`,
                `arn:aws:dynamodb:${REGION}:${ACCOUNT_ID}:table/VentureOS-Anchor/index/*`
            ]
        },
        {
            Sid: "WriteToReefAndQuarantine",
            Effect: "Allow",
            Action: ["s3:PutObject"],
            Resource: [
                "arn:aws:s3:::venture-os-ocean/reef/*",
                "arn:aws:s3:::venture-os-ocean/quarantine/*"
            ]
        },
        {
            Sid: "BasicLogging",
            Effect: "Allow",
            Action: ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
            Resource: "arn:aws:logs:*:*:*"
        }
    ]
};

const POLICY_OPS = {
    Version: "2012-10-17",
    Statement: [
        {
            Sid: "OceanAccess",
            Effect: "Allow",
            Action: ["s3:ListBucket", "s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
            Resource: [
                "arn:aws:s3:::venture-os-ocean",
                "arn:aws:s3:::venture-os-ocean/*"
            ]
        },
        {
            Sid: "AthenaAccess",
            Effect: "Allow",
            Action: ["athena:StartQueryExecution", "athena:GetQueryExecution", "athena:GetQueryResults", "athena:StopQueryExecution"],
            Resource: `arn:aws:athena:${REGION}:${ACCOUNT_ID}:workgroup/primary`
        },
        {
            Sid: "GlueAccess",
            Effect: "Allow",
            Action: ["glue:GetTable", "glue:GetDatabase", "glue:GetPartitions"],
            Resource: "*"
        },
        {
            Sid: "BasicLogging",
            Effect: "Allow",
            Action: ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
            Resource: "arn:aws:logs:*:*:*"
        }
    ]
};

// --- DEPLOYMENT LOGIC ---

async function ensureRole(roleName, policyDoc) {
    console.log(`🛡️ Ensuring Role: ${roleName}...`);
    let roleArn;

    try {
        const res = await iam.send(new GetRoleCommand({ RoleName: roleName }));
        roleArn = res.Role.Arn;
        // console.log("   Exists.");
    } catch (e) {
        if (e.name === 'NoSuchEntity' || e.name === 'NoSuchEntityException') {
            console.log("   Creating...");
            const res = await iam.send(new CreateRoleCommand({
                RoleName: roleName,
                AssumeRolePolicyDocument: JSON.stringify(TRUST_POLICY)
            }));
            roleArn = res.Role.Arn;
            // Wait for propagation
            await new Promise(r => setTimeout(r, 5000));
        } else {
            throw e;
        }
    }

    console.log("   Updating Policy...");
    await iam.send(new PutRolePolicyCommand({
        RoleName: roleName,
        PolicyName: `${roleName}-Policy`,
        PolicyDocument: JSON.stringify(policyDoc)
    }));

    // Safety wait
    await new Promise(r => setTimeout(r, 2000));
    return roleArn;
}

async function deploy() {
    console.log("🚀 Deploying Sluice Lambdas (Least Privilege Mode)...");

    // 1. Ensure Roles
    const arnSplitter = await ensureRole(ROLE_SPLITTER, POLICY_SPLITTER);
    const arnProcessor = await ensureRole(ROLE_PROCESSOR, POLICY_PROCESSOR);
    const arnOps = await ensureRole(ROLE_OPS, POLICY_OPS);

    const zipBuffer = createZip(__dirname);

    // 2. Deploy Splitter
    await deployLambda("Sluice-Splitter", "splitter.handler", zipBuffer, arnSplitter, {
        Environment: {
            Variables: {
                PROCESSING_QUEUE_URL: `https://sqs.${REGION}.amazonaws.com/${ACCOUNT_ID}/VentureOS-Flume`
            }
        }
    });

    // 3. Deploy Processor
    await deployLambda("Sluice-Processor", "processor.handler", zipBuffer, arnProcessor, {
        Timeout: 300,
        MemorySize: 2048
    });

    // 4. Deploy Ops
    await deployLambda("Sluice-Ops", "ops.handler", zipBuffer, arnOps, {
        Timeout: 300,
        MemorySize: 2048
    });

    console.log("🔗 Wiring Triggers...");
    // S3 -> SQS(Intake) -> Splitter -> SQS(Flume) -> Processor
    await wireSQSTrigger("VentureOS-Intake", "Sluice-Splitter");
    await wireSQSTrigger("VentureOS-Flume", "Sluice-Processor");

    console.log("✅ Deployment Complete!");
}

function createZip(folder) {
    console.log("📦 Zipping code + node_modules...");
    const zip = new AdmZip();

    zip.addLocalFile(path.join(folder, "splitter.js"));
    zip.addLocalFile(path.join(folder, "processor.js"));
    zip.addLocalFile(path.join(folder, "ops.js"));
    zip.addLocalFile(path.join(folder, "orchestrate_pipeline.js"));
    zip.addLocalFile(path.join(folder, "package.json"));

    const athenaDir = path.join(folder, "../Athena");
    if (fs.existsSync(athenaDir)) {
        zip.addLocalFolder(athenaDir, "Athena");
    }

    const nodeModulesPath = path.join(folder, "node_modules");
    if (fs.existsSync(nodeModulesPath)) {
        zip.addLocalFolder(nodeModulesPath, "node_modules");
    } else {
        throw new Error("❌ node_modules missing!");
    }

    return zip.toBuffer();
}

async function waitForUpdate(name) {
    process.stdout.write(`Waiting for ${name} update...`);
    await new Promise(r => setTimeout(r, 2000));
    while (true) {
        const res = await lambda.send(new GetFunctionCommand({ FunctionName: name }));
        const status = res.Configuration.LastUpdateStatus;
        if (status === 'Successful') return;
        if (status === 'Failed') throw new Error(`Update failed: ${res.Configuration.LastUpdateStatusReason}`);
        process.stdout.write(".");
        await new Promise(r => setTimeout(r, 2000));
    }
}

async function deployLambda(name, handler, zipFile, roleArn, config = {}) {
    console.log(`Creating/Updating Function: ${name}...`);
    try {
        await lambda.send(new GetFunctionCommand({ FunctionName: name }));
        await lambda.send(new UpdateFunctionCodeCommand({ FunctionName: name, ZipFile: zipFile }));
        await waitForUpdate(name);
        await lambda.send(new UpdateFunctionConfigurationCommand({
            FunctionName: name,
            Role: roleArn,
            Handler: handler,
            Runtime: "nodejs24.x",
            ...config
        }));
        await waitForUpdate(name);
        console.log(`✅ Updated ${name}`);
    } catch (e) {
        if (e.name === 'ResourceNotFoundException') {
            await lambda.send(new CreateFunctionCommand({
                FunctionName: name,
                Runtime: "nodejs24.x",
                Role: roleArn,
                Handler: handler,
                Code: { ZipFile: zipFile },
                Timeout: 30,
                MemorySize: 128,
                ...config
            }));
            await waitForUpdate(name);
            console.log(`✅ Created ${name}`);
        } else {
            throw e;
        }
    }
}

async function wireSQSTrigger(queueName, functionName) {
    const queueArn = `arn:aws:sqs:${REGION}:${ACCOUNT_ID}:${queueName}`;
    try {
        await lambda.send(new CreateEventSourceMappingCommand({
            EventSourceArn: queueArn,
            FunctionName: functionName,
            BatchSize: 10
        }));
        console.log(`✅ Wired ${queueName}`);
    } catch (e) {
        if (e.name === 'ResourceConflictException') console.log(`Trigger for ${queueName} already exists.`);
        else console.error("Trigger Failed:", e.message);
    }
}

deploy();
