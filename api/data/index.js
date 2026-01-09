import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, GetCommand, QueryCommand, ScanCommand } from '@aws-sdk/lib-dynamodb';
import { LambdaClient, InvokeCommand } from '@aws-sdk/client-lambda';

const client = new DynamoDBClient({ region: process.env.AWS_REGION || 'us-east-1' });
const docClient = DynamoDBDocumentClient.from(client);
const lambdaClient = new LambdaClient({ region: process.env.AWS_REGION || 'us-east-1' });

const ENTITIES_TABLE = process.env.ENTITIES_TABLE || 'VentureOS-Entities';
const VIOLATIONS_TABLE = process.env.VIOLATIONS_TABLE || 'VentureOS-Violations';
const BEDROCK_GENERATOR_FUNCTION = process.env.BEDROCK_GENERATOR_FUNCTION || 'ventureos-bedrock-generator';

const corsResponse = (statusCode, body) => ({
  statusCode,
  headers: {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type,Authorization',
    'Access-Control-Allow-Methods': 'GET,POST,OPTIONS',
    'Content-Type': 'application/json',
  },
  body: JSON.stringify(body),
});

export const handler = async (event) => {
  if (event.httpMethod === 'OPTIONS') return corsResponse(200, {});

  try {
    // API Gateway Path: "data/{proxy+}"
    const proxyPath = event.pathParameters?.proxy || '';
    
    // POST /data/bedrock/generate - Trigger Bedrock content generation
    if (proxyPath === 'bedrock/generate' && event.httpMethod === 'POST') {
      return await generateBedrockContent(event);
    }
    
    // GET /data/city/{slug}
    if (proxyPath.startsWith('city/')) {
        const slug = proxyPath.split('city/')[1];
        const queryParams = event.queryStringParameters || {};
        const state = queryParams.state;
        
        // Construct PK: CITY#slug-state
        // If state is provided, use it. Otherwise, try slug directly (legacy support or if slug contains state)
        let pk = `CITY#${slug}`;
        if (state) {
            // Note: Gold Sync writes PK as CITY#slug-STATE (uppercase state)
            // e.g. CITY#knoxville-TN
            pk = `CITY#${slug}-${state.toUpperCase()}`;
        }
        
        return await getEntityData(pk);
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

/**
 * Invoke Bedrock Generator Lambda asynchronously (fire-and-forget)
 */
async function generateBedrockContent(event) {
  try {
    let body;
    if (typeof event.body === 'string') {
      body = JSON.parse(event.body);
    } else {
      body = event.body || {};
    }
    
    const violationId = body.violation_id;
    
    if (!violationId) {
      return corsResponse(400, { error: 'violation_id is required' });
    }
    
    // Invoke Bedrock Generator Lambda asynchronously
    await lambdaClient.send(new InvokeCommand({
      FunctionName: BEDROCK_GENERATOR_FUNCTION,
      InvocationType: 'Event', // Async invocation (fire-and-forget)
      Payload: JSON.stringify({
        body: JSON.stringify({ violation_id: violationId })
      })
    }));
    
    return corsResponse(202, { 
      message: 'Bedrock content generation started',
      violation_id: violationId
    });
    
  } catch (error) {
    console.error('Error invoking Bedrock Generator:', error);
    return corsResponse(500, { error: error.message });
  }
}

async function getEntityData(pk) {
    try {
        console.log(`[getEntityData] Starting fetch for ${pk}`);

        // 1. Fetch Stats (Parallel)
        console.log(`[getEntityData] Fetching stats from ${ENTITIES_TABLE}...`);
        const statsPromise = docClient.send(new GetCommand({
            TableName: ENTITIES_TABLE,
            Key: { PK: pk, SK: 'STATS#all' }
        }));

        // 2. Fetch Recent Violations (Feed)
        console.log(`[getEntityData] Fetching violations from ${VIOLATIONS_TABLE}...`);
        const feedPromise = docClient.send(new QueryCommand({
            TableName: VIOLATIONS_TABLE,
            KeyConditionExpression: 'PK = :pk',
            ExpressionAttributeValues: { ':pk': pk },
            ScanIndexForward: false, // Newest first
            Limit: 50 // Fetch more violations
        }));

        console.log(`[getEntityData] Waiting for parallel queries...`);
        const [statsRes, feedRes] = await Promise.all([statsPromise, feedPromise]);

        console.log(`[getEntityData] PK: ${pk}`);
        console.log(`[getEntityData] Stats found: ${!!statsRes.Item}`);
        console.log(`[getEntityData] Violations found: ${feedRes.Items?.length || 0}`);
        if (feedRes.Items && feedRes.Items.length > 0) {
            console.log(`[getEntityData] First violation sample:`, JSON.stringify(feedRes.Items[0], null, 2));
        } else {
            console.log(`[getEntityData] No violations found for ${pk}`);
        }

        if (!statsRes.Item && (!feedRes.Items || feedRes.Items.length === 0)) {
            console.log(`[getEntityData] Returning 404 - no stats or violations found`);
            return corsResponse(404, { error: 'Entity not found' });
        }

    const stats = statsRes.Item || {};
    
    // Extract state code if this is a state entity
    const stateCode = pk.startsWith('STATE#') ? pk.replace('STATE#', '') : null;
    
    // Extract cities - query STATE# directory entries
    let cities = [];
    if (pk.startsWith('STATE#')) {
        const stateCode = pk.replace('STATE#', '');
        console.log(`[getEntityData] Querying directory for cities in ${stateCode}...`);
        
        // Query using the Adjacency List pattern
        // PK = STATE#TN, SK begins_with CITY#
        const citiesQuery = await docClient.send(new QueryCommand({
            TableName: ENTITIES_TABLE,
            KeyConditionExpression: 'PK = :pk AND begins_with(SK, :skPrefix)',
            ExpressionAttributeValues: {
                ':pk': `STATE#${stateCode}`,
                ':skPrefix': 'CITY#'
            }
        })).catch(err => {
            console.error(`[getEntityData] City directory query failed:`, err);
            return { Items: [] };
        });
        
        console.log(`[getEntityData] Found ${citiesQuery.Items?.length || 0} cities for ${stateCode}`);
        
        const citySet = new Set();
        (citiesQuery.Items || []).forEach(item => {
            if (item.name) {
                citySet.add(item.name);
            }
        });
        
        // Also get cities from violations as fallback
        (feedRes.Items || []).forEach(item => {
            if (item.city) citySet.add(item.city);
        });
        
        cities = Array.from(citySet).sort();
    }

    // Extract unique companies count if it's a City
    let companyCount = 0;
    if (pk.startsWith('CITY#')) {
        const companyCountQuery = await docClient.send(new QueryCommand({
            TableName: ENTITIES_TABLE,
            KeyConditionExpression: 'PK = :pk AND begins_with(SK, :skPrefix)',
            ExpressionAttributeValues: {
                ':pk': pk,
                ':skPrefix': 'COMPANY#'
            },
            Select: 'COUNT'
        })).catch(err => {
            console.error(`[getEntityData] Company count query failed:`, err);
            return { Count: 0 };
        });
        companyCount = companyCountQuery.Count || 0;
        console.log(`[getEntityData] Company count for ${pk}: ${companyCount}`);
    }

    // Transform violations to match expected format
    const violations = (feedRes.Items || []).map(item => {
        // Extract date from SK if event_date is missing
        // SK format: AGENCY#osha#DATE#2024-01-15#violation_id
        let eventDate = item.event_date;
        if (!eventDate && item.SK) {
            const skParts = item.SK.split('#');
            if (skParts.length >= 4 && skParts[2] === 'DATE') {
                eventDate = skParts[3];
            }
        }
        
        return {
            ViolationData: {
                eventDate: eventDate || item.date || null,
                rawData: item.raw_desc ? { 
                    eventdate: eventDate || item.date, 
                    ...item 
                } : item,
                establishment: { name: item.company_name || item.company || '' },
                penalty: parseFloat(item.fine || item.fine_amount || 0),
                citation: { penalty: parseFloat(item.fine || item.fine_amount || 0) }
            },
            ProcessedContent: item.bedrock_title ? {
                title: item.bedrock_title,
                explanation: item.bedrock_description || item.description
            } : null
        };
    });

    // Return structure matching StatePageData interface
    return corsResponse(200, {
        state: stateCode || stats.name || 'Unknown',
        violations: violations,
        cities: cities,
        stats: {
            totalViolations: parseInt(stats.total_violations || '0'),
            totalFines: parseFloat(stats.total_fines || '0'),
            averageFine: stats.total_violations > 0 ? parseFloat(stats.total_fines || '0') / parseInt(stats.total_violations || '1') : 0,
            totalCities: cities.length || parseInt(stats.total_cities || '0'),
            totalCompanies: companyCount
        },
        meta: {
            totalViolations: parseInt(stats.total_violations || '0'),
            hasMoreViolations: violations.length >= 50
        }
    });
    } catch (error) {
        console.error(`[getEntityData] Error fetching data for ${pk}:`, error);
        console.error(`[getEntityData] Error stack:`, error.stack);
        return corsResponse(500, { error: error.message, stack: error.stack });
    }
}
