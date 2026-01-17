import fs from 'fs';
import { LambdaClient, CreateFunctionCommand, UpdateFunctionCodeCommand, UpdateFunctionConfigurationCommand, GetFunctionCommand, CreateEventSourceMappingCommand } from "@aws-sdk/client-lambda";
import { SQSClient } from "@aws-sdk/client-sqs";
import AdmZip from "adm-zip";

const REGION = "us-east-1";
const ACCOUNT_ID = "611538926352"; 
const ROLE_ARN = `arn:aws:iam::${ACCOUNT_ID}:role/VentureOS-LambdaExecutionRole`;
const DUCKDB_LAYER_ARN = `arn:aws:lambda:${REGION}:${ACCOUNT_ID}:layer:ventureos-duckdb-neo:2`;

const lambda = new LambdaClient({ region: REGION });

async function deploy() {
    console.log("🚀 Deploying Sluice Lambdas...");

    // 1. Zip Code
    const zipBuffer = createZip("VentureOS/ocean/Sluice", ["splitter.js", "processor.js", "package.json"]);

    // 2. Deploy Splitter
    await deployLambda("Sluice-Splitter", "splitter.handler", zipBuffer, {
        Environment: {
            Variables: {
                PROCESSING_QUEUE_URL: `https://sqs.${REGION}.amazonaws.com/${ACCOUNT_ID}/VentureOS-ProcessingQueue`
            }
        }
    });

    // 3. Deploy Processor (With Layer)
    await deployLambda("Sluice-Processor", "processor.handler", zipBuffer, {
        Layers: [DUCKDB_LAYER_ARN],
        Timeout: 300, // 5 mins
        MemorySize: 2048 // 2GB for DuckDB
    });

    // 4. Wire Triggers
    console.log("🔗 Wiring Triggers...");
    await wireSQSTrigger("VentureOS-Intake", "Sluice-Splitter");
    await wireSQSTrigger("VentureOS-ProcessingQueue", "Sluice-Processor");

    console.log("✅ Deployment Complete!");
}

function createZip(folder, files) {
    const zip = new AdmZip();
    files.forEach(f => {
        zip.addLocalFile(`${folder}/${f}`);
    });
    return zip.toBuffer();
}

async function deployLambda(name, handler, zipFile, config = {}) {
    console.log(`Creating/Updating Function: ${name}...`);
    try {
        await lambda.send(new GetFunctionCommand({ FunctionName: name }));
        
        // Update Code
        await lambda.send(new UpdateFunctionCodeCommand({
            FunctionName: name,
            ZipFile: zipFile
        }));
        
        // Update Config (Wait a sec for update to settle)
        await new Promise(r => setTimeout(r, 2000));
        await lambda.send(new UpdateFunctionConfigurationCommand({
            FunctionName: name,
            Role: ROLE_ARN,
            Handler: handler,
            Runtime: "nodejs20.x",
            ...config
        }));
        console.log(`✅ Updated ${name}`);
        
    } catch (e) {
        if (e.name === 'ResourceNotFoundException') {
            await lambda.send(new CreateFunctionCommand({
                FunctionName: name,
                Runtime: "nodejs20.x",
                Role: ROLE_ARN,
                Handler: handler,
                Code: { ZipFile: zipFile },
                Timeout: 30,
                MemorySize: 128,
                ...config
            }));
            console.log(`✅ Created ${name}`);
        } else {
            throw e;
        }
    }
}

async function wireSQSTrigger(queueName, functionName) {
    const queueArn = `arn:aws:sqs:${REGION}:${ACCOUNT_ID}:${queueName}`;
    const functionArn = `arn:aws:lambda:${REGION}:${ACCOUNT_ID}:function:${functionName}`;
    
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
