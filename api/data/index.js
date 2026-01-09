import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, GetCommand, QueryCommand } from '@aws-sdk/lib-dynamodb';

const client = new DynamoDBClient({ region: process.env.AWS_REGION || 'us-east-1' });
const docClient = DynamoDBDocumentClient.from(client);

const ENTITIES_TABLE = process.env.ENTITIES_TABLE || 'VentureOS-Entities';
const VIOLATIONS_TABLE = process.env.VIOLATIONS_TABLE || 'VentureOS-Violations';

const corsResponse = (statusCode, body) => ({
  statusCode,
  headers: {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type,Authorization',
    'Access-Control-Allow-Methods': 'GET,OPTIONS',
    'Content-Type': 'application/json',
  },
  body: JSON.stringify(body),
});

export const handler = async (event) => {
  if (event.httpMethod === 'OPTIONS') return corsResponse(200, {});

  try {
    // API Gateway Path: "data/{proxy+}"
    const proxyPath = event.pathParameters?.proxy || '';
    
    // GET /data/city/{slug}
    if (proxyPath.startsWith('city/')) {
        const slug = proxyPath.split('city/')[1];
        return await getEntityData(`CITY#${slug}`);
    }

    // GET /data/company/{slug}
    if (proxyPath.startsWith('company/')) {
        const slug = proxyPath.split('company/')[1];
        return await getEntityData(`COMPANY#${slug}`);
    }

    // GET /data/state/{slug}
    if (proxyPath.startsWith('state/')) {
        const slug = proxyPath.split('state/')[1];
        return await getEntityData(`STATE#${slug}`);
    }

    return corsResponse(404, { error: `Endpoint not found: ${proxyPath}` });
  } catch (error) {
    console.error(error);
    return corsResponse(500, { error: error.message });
  }
};

async function getEntityData(pk) {
    console.log(`Fetching data for ${pk}`);

    // 1. Fetch Stats (Parallel)
    const statsPromise = docClient.send(new GetCommand({
        TableName: ENTITIES_TABLE,
        Key: { PK: pk, SK: 'STATS#all' }
    }));

    // 2. Fetch Recent Violations (Feed)
    // We query the Violations table for everything under this Entity PK
    const feedPromise = docClient.send(new QueryCommand({
        TableName: VIOLATIONS_TABLE,
        KeyConditionExpression: 'PK = :pk',
        ExpressionAttributeValues: { ':pk': pk },
        ScanIndexForward: false, // Newest first
        Limit: 20 // Fetch a bit more than needed to be safe
    }));

    const [statsRes, feedRes] = await Promise.all([statsPromise, feedPromise]);

    if (!statsRes.Item && (!feedRes.Items || feedRes.Items.length === 0)) {
        return corsResponse(404, { error: 'Entity not found' });
    }

    return corsResponse(200, {
        entity: statsRes.Item || { name: 'Unknown', total_violations: 0 },
        feed: feedRes.Items || []
    });
}
