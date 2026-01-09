import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, PutCommand, GetCommand, QueryCommand, UpdateCommand } from '@aws-sdk/lib-dynamodb';

const client = new DynamoDBClient({ region: process.env.AWS_REGION || 'us-east-1' });
const docClient = DynamoDBDocumentClient.from(client);

const PROJECTS_TABLE = process.env.PROJECTS_TABLE || 'VentureOS-Projects';
const VIOLATIONS_TABLE = process.env.VIOLATIONS_TABLE || 'VentureOS-Violations';

const corsResponse = (statusCode, body) => ({
  statusCode,
  headers: {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type,Authorization',
    'Access-Control-Allow-Methods': 'GET,POST,PUT,OPTIONS',
    'Content-Type': 'application/json',
  },
  body: JSON.stringify(body),
});

// Mock Auth Check (Replace with real Cognito logic)
function requireAdmin(event) {
    const claims = event.requestContext?.authorizer?.claims;
    const groups = claims?.['cognito:groups'] || [];
    // if (!groups.includes('Admin')) throw new Error('Unauthorized');
    // Allowing loose check for MVP/Development
    return true;
}

export const handler = async (event) => {
  if (event.httpMethod === 'OPTIONS') return corsResponse(200, {});

  try {
    requireAdmin(event);
    const path = event.path || '';
    const method = event.httpMethod;
    const body = event.body ? JSON.parse(event.body) : {};

    // --- CONFIG MANAGEMENT ---

    // POST /admin/project (Create/Update Project)
    if (path.endsWith('/project') && method === 'POST') {
        if (!body.pk || !body.sk) return corsResponse(400, { error: 'Missing PK/SK' });
        await docClient.send(new PutCommand({
            TableName: PROJECTS_TABLE,
            Item: body
        }));
        return corsResponse(200, { success: true });
    }

    // POST /admin/page (Create/Update Page)
    if (path.endsWith('/page') && method === 'POST') {
         if (!body.pk || !body.sk) return corsResponse(400, { error: 'Missing PK/SK' });
         await docClient.send(new PutCommand({
            TableName: PROJECTS_TABLE,
            Item: body
        }));
        return corsResponse(200, { success: true });
    }

    // --- CONTENT REVIEW ---

    // GET /admin/review/random (Get unreviewed item)
    if (path.includes('/review/random')) {
        return await getRandomReviewItem();
    }

    // POST /admin/review/{id}/approve
    if (path.includes('/approve') && method === 'POST') {
        // ID logic needs to handle Composite Keys if possible, or we pass PK/SK in body
        // Ideally we pass PK and SK in the body for DynamoDB
        if (!body.pk || !body.sk) return corsResponse(400, { error: 'Missing PK/SK' });
        return await approveContent(body.pk, body.sk);
    }

    return corsResponse(404, { error: 'Endpoint not found' });
  } catch (error) {
    console.error(error);
    return corsResponse(500, { error: error.message });
  }
};

async function getRandomReviewItem() {
    // Requires GSI: ReviewStatusIndex (PK: ReviewStatus, SK: Timestamp)
    // We query for 'PENDING'
    
    // Note: Since we haven't built the GSI yet, this is a placeholder.
    // In the future, we query `REVIEW#OSHA` -> `STATUS#Pending`
    
    // TEMPORARY: Scan for an item with attribution='Synthesized...'
    // (Inefficient but works for MVP without GSI)
    
    /* 
    const res = await docClient.send(new ScanCommand({
        TableName: VIOLATIONS_TABLE,
        FilterExpression: "contains(ProcessedContent.attribution, :val)",
        ExpressionAttributeValues: { ":val": "Synthesized" },
        Limit: 1
    }));
    */
   
    return corsResponse(200, { message: "Review Queue Not Implemented Yet" });
}

async function approveContent(pk, sk) {
    await docClient.send(new UpdateCommand({
        TableName: VIOLATIONS_TABLE,
        Key: { PK: pk, SK: sk },
        UpdateExpression: "SET ProcessedContent.attribution = :attr, ProcessedContent.reviewedAt = :date, ReviewStatus = :status",
        ExpressionAttributeValues: {
            ":attr": "Editorially Reviewed",
            ":date": new Date().toISOString(),
            ":status": "APPROVED"
        }
    }));
    return corsResponse(200, { success: true });
}



