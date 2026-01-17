import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { LambdaClient, CreateFunctionCommand, UpdateFunctionCodeCommand, UpdateFunctionConfigurationCommand, GetFunctionCommand, CreateEventSourceMappingCommand } from "@aws-sdk/client-lambda";
import AdmZip from "adm-zip";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const REGION = "us-east-1";
const ACCOUNT_ID = "611538926352"; 
const ROLE_ARN = `arn:aws:iam::${ACCOUNT_ID}:role/VentureOS-LambdaExecutionRole`;

const lambda = new LambdaClient({ region: REGION });

async function deploy() {
    console.log("🚀 Deploying Sluice Lambdas (Node 24 - ParquetJS)...");

    const zipBuffer = createZip(__dirname);

    // Deploy Splitter
    await deployLambda("Sluice-Splitter", "splitter.handler", zipBuffer, {
        Environment: {
            Variables: {
                PROCESSING_QUEUE_URL: `https://sqs.${REGION}.amazonaws.com/${ACCOUNT_ID}/VentureOS-Flume`
            }
        }
    });

    // Deploy Processor
    await deployLambda("Sluice-Processor", "processor.handler", zipBuffer, {
        Timeout: 300, 
        MemorySize: 2048 
    });

    console.log("🔗 Wiring Triggers...");
    await wireSQSTrigger("VentureOS-Intake", "Sluice-Splitter");
    await wireSQSTrigger("VentureOS-Flume", "Sluice-Processor");

    console.log("✅ Deployment Complete!");
}

function createZip(folder) {
    console.log("📦 Zipping code + node_modules...");
    const zip = new AdmZip();
    
    zip.addLocalFile(path.join(folder, "splitter.js"));
    zip.addLocalFile(path.join(folder, "processor.js"));
    zip.addLocalFile(path.join(folder, "package.json"));
    
    const nodeModulesPath = path.join(folder, "node_modules");
    if (fs.existsSync(nodeModulesPath)) {
        zip.addLocalFolder(nodeModulesPath, "node_modules");
    } else {
        throw new Error("❌ node_modules missing! Run npm install first.");
    }
    
    return zip.toBuffer();
}

async function waitForUpdate(name) {
    process.stdout.write(`Waiting for ${name} update...`);
    // INITIAL WAIT to let AWS transition from 'Successful' -> 'InProgress'
    await new Promise(r => setTimeout(r, 3000));
    
    while (true) {
        const res = await lambda.send(new GetFunctionCommand({ FunctionName: name }));
        const status = res.Configuration.LastUpdateStatus;
        if (status === 'Successful') {
            console.log(" Done.");
            return;
        } else if (status === 'Failed') {
            throw new Error(`Update failed for ${name} (${res.Configuration.LastUpdateStatusReason})`);
        }
        process.stdout.write(".");
        await new Promise(r => setTimeout(r, 2000));
    }
}

async function deployLambda(name, handler, zipFile, config = {}) {
    console.log(`Creating/Updating Function: ${name}...`);
    try {
        await lambda.send(new GetFunctionCommand({ FunctionName: name }));
        
        await lambda.send(new UpdateFunctionCodeCommand({
            FunctionName: name,
            ZipFile: zipFile
        }));
        
        await waitForUpdate(name);
        
        await lambda.send(new UpdateFunctionConfigurationCommand({
            FunctionName: name,
            Role: ROLE_ARN,
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
                Role: ROLE_ARN,
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


