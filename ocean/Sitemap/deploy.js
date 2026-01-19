import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { LambdaClient, CreateFunctionCommand, UpdateFunctionCodeCommand, UpdateFunctionConfigurationCommand, GetFunctionCommand } from "@aws-sdk/client-lambda";
import AdmZip from "adm-zip";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const REGION = "us-east-1";
const ACCOUNT_ID = "611538926352";
const ROLE_ARN = `arn:aws:iam::${ACCOUNT_ID}:role/VentureOS-LambdaExecutionRole`;

const lambda = new LambdaClient({ region: REGION });

/*
 * IAM PERMISSIONS REQUIRED for VentureOS-LambdaExecutionRole:
 * 
 * DynamoDB:
 *   - dynamodb:Scan on arn:aws:dynamodb:us-east-1:611538926352:table/VentureOS-Lighthouse
 * 
 * S3:
 *   - s3:PutObject on arn:aws:s3:::oshatrail-site/*
 * 
 * CloudWatch Logs (should already have):
 *   - logs:CreateLogGroup, logs:CreateLogStream, logs:PutLogEvents
 */

async function deploy() {
    console.log("🗺️ Deploying Sitemap Generator Lambda...");

    const zipBuffer = createZip(__dirname);

    await deployLambda("Sitemap-Generator", "generator.handler", zipBuffer, {
        Timeout: 120, // 2 minutes - streaming should be fast
        MemorySize: 512,
        Environment: {
            Variables: {
                LIGHTHOUSE_TABLE: "VentureOS-Lighthouse",
                SITE_BUCKET: "oshatrail-site",
                BASE_URL: "https://d3hfvmdkfg3f94.cloudfront.net", // CloudFront distribution
                TEST_MODE: "true" // Set to "false" for production
            }
        }
    });

    console.log("✅ Deployment Complete!");
    console.log("\n📋 Next steps:");
    console.log("  1. Test with: aws lambda invoke --function-name Sitemap-Generator out.json && cat out.json");
    console.log("  2. Check S3: aws s3 ls s3://oshatrail-site/sitemap");
    console.log("  3. For production, update TEST_MODE to 'false'");
}

function createZip(folder) {
    console.log("📦 Zipping code + node_modules...");
    const zip = new AdmZip();
    
    zip.addLocalFile(path.join(folder, "generator.js"));
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
    await new Promise(r => setTimeout(r, 3000));
    
    while (true) {
        const res = await lambda.send(new GetFunctionCommand({ FunctionName: name }));
        const status = res.Configuration.LastUpdateStatus;
        if (status === 'Successful') {
            console.log(" Done.");
            return;
        } else if (status === 'Failed') {
            throw new Error(`Update failed for ${name}`);
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
                ...config
            }));
            await waitForUpdate(name);
            console.log(`✅ Created ${name}`);
        } else {
            throw e;
        }
    }
}

deploy();
