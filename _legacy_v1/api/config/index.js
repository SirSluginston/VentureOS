import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, GetCommand, QueryCommand } from '@aws-sdk/lib-dynamodb';

const client = new DynamoDBClient({ region: process.env.AWS_REGION || 'us-east-1' });
const docClient = DynamoDBDocumentClient.from(client);

const TABLE_NAME = process.env.PROJECTS_TABLE || 'VentureOS-Projects';

// CORS Helper
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
  console.log('Config API Event:', JSON.stringify(event));

  if (event.httpMethod === 'OPTIONS') return corsResponse(200, {});

  try {
    // API Gateway Proxy Path: "config/{proxy+}"
    // event.pathParameters.proxy contains everything after /config/
    const proxyPath = event.pathParameters?.proxy || '';
    
    // 1. GET /config/brand
    if (proxyPath === 'brand') {
      return await getBrandConfig();
    }

    // 2. GET /config/projects
    if (proxyPath === 'projects') {
      return await listProjects();
    }

    // 3. GET /config/pages/{pk}  (Updated to match service call /config/pages/:pk)
    if (proxyPath.startsWith('pages/')) {
      const pk = decodeURIComponent(proxyPath.split('pages/')[1]);
      return await getProjectPages(pk);
    }

    // 4. GET /config/pk/{pk} (Specific Project) - Updated to match service call /config/pk/:pk
    if (proxyPath.startsWith('pk/')) {
        const pk = decodeURIComponent(proxyPath.split('pk/')[1]);
        return await getProjectConfig(pk);
    }
    
    // 5. GET /config/project/{pk} (Alternative)
    if (proxyPath.startsWith('project/')) {
        const pk = decodeURIComponent(proxyPath.split('project/')[1]);
        return await getProjectConfig(pk);
    }

    return corsResponse(404, { error: `Endpoint not found: ${proxyPath}` });

  } catch (error) {
    console.error('API Error:', error);
    return corsResponse(500, { error: error.message });
  }
};

// --- Handlers ---

async function getBrandConfig() {
  return getProjectConfig('BRAND#SirSluginston');
}

async function getProjectConfig(pk) {
  if (!pk) return corsResponse(400, { error: 'Missing PK' });

  console.log(`Getting Config for PK: ${pk}`);
  const result = await docClient.send(new GetCommand({
    TableName: TABLE_NAME,
    Key: { 
      PK: pk, 
      SK: 'CONFIG' 
    }
  }));

  if (!result.Item) return corsResponse(404, { error: 'Config not found' });
  return corsResponse(200, result.Item);
}

async function listProjects() {
    const knownBrands = [
        'BRAND#SirSluginston',
        'BRAND#OSHAtrail',
        'BRAND#TransportTrail',
        'BRAND#HabiTasks'
    ];

    const projects = [];
    for (const pk of knownBrands) {
        const res = await docClient.send(new GetCommand({
            TableName: TABLE_NAME,
            Key: { PK: pk, SK: 'CONFIG' }
        }));
        if (res.Item) projects.push(res.Item);
    }

    return corsResponse(200, projects);
}

async function getProjectPages(pk) {
  if (!pk) return corsResponse(400, { error: 'Missing PK' });

  const result = await docClient.send(new QueryCommand({
    TableName: TABLE_NAME,
    KeyConditionExpression: 'PK = :pk AND begins_with(SK, :prefix)',
    ExpressionAttributeValues: {
      ':pk': pk,
      ':prefix': 'PAGE#'
    }
  }));

  return corsResponse(200, result.Items || []);
}
