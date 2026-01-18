import fs from 'fs';
import { LambdaClient, CreateFunctionCommand, UpdateFunctionCodeCommand, UpdateFunctionConfigurationCommand, GetFunctionCommand } from "@aws-sdk/client-lambda";
import AdmZip from "adm-zip";

const REGION = "us-east-1";
const ACCOUNT_ID = "611538926352"; 
const ROLE_ARN = `arn:aws:iam::${ACCOUNT_ID}:role/Lighthouse-ExecutionRole`;
const FUNCTION_NAME = "Lighthouse-Aggregator";

const lambda = new LambdaClient({ region: REGION });

async function deploy() {
    console.log(`🚀 Deploying ${FUNCTION_NAME}...`);

    // Zip
    const zip = new AdmZip();
    zip.addLocalFile("VentureOS/Ocean/Lighthouse/aggregate.js", "", "index.js"); // Rename to index.js for handler
    zip.addLocalFile("VentureOS/Ocean/Lighthouse/package.json");
    if (fs.existsSync("VentureOS/Ocean/Lighthouse/node_modules")) {
        zip.addLocalFolder("VentureOS/Ocean/Lighthouse/node_modules", "node_modules");
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
            Runtime: "nodejs24.x",
            Timeout: 300, // 5 mins
            MemorySize: 512
        }));
        
        console.log("✅ Updated successfully.");
        
    } catch (e) {
        if (e.name === 'ResourceNotFoundException') {
            await lambda.send(new CreateFunctionCommand({
                FunctionName: FUNCTION_NAME,
                Runtime: "nodejs24.x",
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
}

deploy();
