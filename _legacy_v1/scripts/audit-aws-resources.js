import { 
    S3Client, ListBucketsCommand 
} from '@aws-sdk/client-s3';
import { 
    LambdaClient, ListFunctionsCommand 
} from '@aws-sdk/client-lambda';
import { 
    DynamoDBClient, ListTablesCommand 
} from '@aws-sdk/client-dynamodb';
import { 
    CloudFormationClient, ListStacksCommand 
} from '@aws-sdk/client-cloudformation';
import fs from 'fs';

const REGION = process.env.AWS_REGION || 'us-east-1';

// Clients
const s3 = new S3Client({ region: REGION });
const lambda = new LambdaClient({ region: REGION });
const dynamo = new DynamoDBClient({ region: REGION });
const cfn = new CloudFormationClient({ region: REGION });

async function audit() {
    console.log(`🔍 Starting AWS Resource Audit [${REGION}]...\n`);
    let report = `# AWS Resource Audit Report (${new Date().toISOString()})\n\n`;

    // 1. CloudFormation Stacks
    console.log('Checking CloudFormation Stacks...');
    const stacks = await cfn.send(new ListStacksCommand({ 
        StackStatusFilter: ['CREATE_COMPLETE', 'UPDATE_COMPLETE', 'ROLLBACK_COMPLETE'] 
    }));
    report += `## CloudFormation Stacks\n`;
    stacks.StackSummaries.forEach(s => {
        if (s.StackName.toLowerCase().includes('venture') || s.StackName.toLowerCase().includes('slug')) {
            report += `- [ ] **${s.StackName}** (Status: ${s.StackStatus})\n`;
        }
    });
    report += `\n`;

    // 2. S3 Buckets
    console.log('Checking S3 Buckets...');
    const buckets = await s3.send(new ListBucketsCommand({}));
    report += `## S3 Buckets\n`;
    buckets.Buckets.forEach(b => {
        if (b.Name.toLowerCase().includes('venture') || b.Name.toLowerCase().includes('slug')) {
            report += `- [ ] **${b.Name}** (Created: ${b.CreationDate})\n`;
        }
    });
    report += `\n`;

    // 3. Lambda Functions
    console.log('Checking Lambda Functions...');
    const funcs = await lambda.send(new ListFunctionsCommand({}));
    report += `## Lambda Functions\n`;
    funcs.Functions.forEach(f => {
        if (f.FunctionName.toLowerCase().includes('venture') || f.FunctionName.toLowerCase().includes('slug')) {
            report += `- [ ] **${f.FunctionName}** (Runtime: ${f.Runtime}, Size: ${(f.CodeSize / 1024 / 1024).toFixed(2)} MB)\n`;
        }
    });
    report += `\n`;

    // 4. DynamoDB Tables
    console.log('Checking DynamoDB Tables...');
    const tables = await dynamo.send(new ListTablesCommand({}));
    report += `## DynamoDB Tables\n`;
    tables.TableNames.forEach(t => {
        if (t.toLowerCase().includes('venture') || t.toLowerCase().includes('slug')) {
            report += `- [ ] **${t}**\n`;
        }
    });

    fs.writeFileSync('AWS_AUDIT_REPORT.md', report);
    console.log('\n✅ Audit Complete! Results saved to AWS_AUDIT_REPORT.md');
    console.log('👉 Open this file and check the boxes for items to DELETE.');
}

audit().catch(console.error);

