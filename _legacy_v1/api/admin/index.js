import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, PutCommand, GetCommand, QueryCommand, UpdateCommand, ScanCommand } from '@aws-sdk/lib-dynamodb';
import { LambdaClient, InvokeCommand } from '@aws-sdk/client-lambda';
import { generatePresignedUrl } from './presigned-upload.js';

const client = new DynamoDBClient({ region: process.env.AWS_REGION || 'us-east-1' });
const docClient = DynamoDBDocumentClient.from(client);
const lambdaClient = new LambdaClient({ region: process.env.AWS_REGION || 'us-east-1' });

const PROJECTS_TABLE = process.env.PROJECTS_TABLE || 'VentureOS-Projects';
const VIOLATIONS_TABLE = process.env.VIOLATIONS_TABLE || 'VentureOS-Violations';
const BATCH_JOBS_TABLE = process.env.BATCH_JOBS_TABLE || 'VentureOS-BedrockBatchJobs';
const STAGING_TABLE = process.env.STAGING_TABLE || 'VentureOS-BedrockStaging';
const USERS_TABLE = process.env.USERS_TABLE || 'VentureOS-Users';
const BATCH_PROCESSOR_FUNCTION = process.env.BATCH_PROCESSOR_FUNCTION || 'ventureos-bedrock-batch-processor';
const GOLD_SYNC_FUNCTION = process.env.GOLD_SYNC_FUNCTION || 'ventureos-gold-sync';

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
    // API Gateway Proxy: path is in event.path or event.pathParameters.proxy
    // For /admin/{proxy+}, the proxy contains everything after /admin/
    const proxyPath = event.pathParameters?.proxy || '';
    const fullPath = event.path || `/admin/${proxyPath}`;
    const path = proxyPath || fullPath.replace('/admin/', '');
    const method = event.httpMethod;
    const body = event.body ? JSON.parse(event.body) : {};
    
    console.log('Admin API Request:', JSON.stringify({ path, proxyPath, fullPath, method, pathParameters: event.pathParameters }));

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

    // GET /admin/review/queue (Get unreviewed items)
    if ((path === 'review/queue' || path.includes('review/queue')) && method === 'GET') {
        const limit = parseInt(event.queryStringParameters?.limit || '10');
        return await getReviewQueue(limit);
    }

    // GET /admin/review/random (Get single unreviewed item)
    if ((path === 'review/random' || path.includes('review/random')) && method === 'GET') {
        return await getRandomReviewItem();
    }

    // POST /admin/review/approve (Approve single item)
    if ((path === 'review/approve' || path.includes('review/approve')) && method === 'POST') {
        if (!body.pk || !body.sk) return corsResponse(400, { error: 'Missing PK/SK' });
        return await approveContent(body.pk, body.sk);
    }

    // POST /admin/review/bulk-approve (Bulk approve)
    if ((path === 'review/bulk-approve' || path.includes('review/bulk-approve')) && method === 'POST') {
        if (!body.items || !Array.isArray(body.items)) return corsResponse(400, { error: 'Missing items array' });
        return await bulkApproveContent(body.items);
    }

    // --- DATA UPLOAD ---

    // POST /admin/upload/presigned-url (Get presigned URL for direct S3 upload)
    if ((path === 'upload/presigned-url' || path.includes('/upload/presigned-url') || path.endsWith('upload/presigned-url')) && method === 'POST') {
        const { agency, date, fileName, fileSize, normalizer } = body;
        if (!agency || !date || !fileName) {
            return corsResponse(400, { error: 'agency, date, and fileName are required' });
        }
        const result = await generatePresignedUrl(agency, date, fileName, fileSize || 0, normalizer || 'osha-severe_injury');
        return corsResponse(200, result);
    }

    // --- BEDROCK BATCH PROCESSING ---

    // GET /admin/bedrock/batch/jobs (List batch jobs) - Check this FIRST before other batch routes
    if ((path === 'bedrock/batch/jobs' || path.includes('bedrock/batch/jobs')) && method === 'GET') {
        const agencies = event.queryStringParameters?.agencies 
            ? event.queryStringParameters.agencies.split(',').filter(a => a)
            : [];
        return await listBatchJobs(agencies);
    }

    // POST /admin/bedrock/batch/estimate (Estimate batch cost)
    if ((path === 'bedrock/batch/estimate' || path.includes('bedrock/batch/estimate')) && method === 'POST') {
        return await estimateBatchCost(body);
    }

    // GET /admin/bedrock/batch/{jobId} (Get job status)
    if (path.startsWith('bedrock/batch/') && method === 'GET' && path !== 'bedrock/batch/jobs') {
        const jobId = path.replace('bedrock/batch/', '');
        return await getBatchJobStatus(jobId);
    }

    // POST /admin/bedrock/batch (Create batch job)
    if ((path === 'bedrock/batch' || path.includes('bedrock/batch')) && method === 'POST') {
        return await createBatchJob(body);
    }

    // --- USER SETTINGS ---

    // GET /admin/user/settings (Get current user's settings)
    if (path === 'user/settings' && method === 'GET') {
        return await getUserSettings(event);
    }

    // PUT /admin/user/settings (Update user settings)
    if (path === 'user/settings' && method === 'PUT') {
        return await updateUserSettings(event, body);
    }

    // --- DATA SYNC ---

    // POST /admin/sync/gold (Manually trigger Gold layer sync to DynamoDB)
    if ((path === 'sync/gold' || path.includes('sync/gold')) && method === 'POST') {
        try {
            // Parse sync options from request body
            // Default: stats-only (fast, safe)
            const syncOptions = body?.mode ? { mode: body.mode } : { mode: 'stats-only' };
            
            // Add optional table/entityType filters for recent5 mode
            if (body?.table) syncOptions.table = body.table;
            if (body?.entityType) syncOptions.entityType = body.entityType;
            
            const invokeResult = await lambdaClient.send(new InvokeCommand({
                FunctionName: GOLD_SYNC_FUNCTION,
                InvocationType: 'Event', // Async invocation
                Payload: JSON.stringify(syncOptions)
            }));
            
            const modeDescription = syncOptions.mode === 'stats-only' 
                ? 'Stats only (fast)' 
                : syncOptions.mode === 'recent5-only' 
                ? 'Recent5 violations only' 
                : 'Full sync (stats + Recent5)';
            
            return corsResponse(200, { 
                success: true, 
                message: `Gold sync triggered: ${modeDescription}`,
                mode: syncOptions.mode,
                requestId: invokeResult.$metadata.requestId
            });
        } catch (error) {
            console.error('Error invoking gold-sync:', error);
            return corsResponse(500, { 
                error: 'Failed to trigger gold sync', 
                message: error.message 
            });
        }
    }

    // POST /admin/user/create (Create user record)
    if (path === 'user/create' && method === 'POST') {
        return await createUserRecord(event, body);
    }

    // Debug logging
    console.log('Path not matched:', { path, proxyPath, fullPath, method, pathParameters: event.pathParameters });

    return corsResponse(404, { error: 'Endpoint not found', path, proxyPath, fullPath });
  } catch (error) {
    console.error(error);
    return corsResponse(500, { error: error.message });
  }
};

async function getReviewQueue(limit = 10) {
    try {
        // Query Staging Table for PENDING items
        const result = await docClient.send(new QueryCommand({
            TableName: STAGING_TABLE,
            IndexName: 'StatusIndex',
            KeyConditionExpression: '#status = :pending',
            ExpressionAttributeNames: {
                '#status': 'status'
            },
            ExpressionAttributeValues: {
                ':pending': 'PENDING'
            },
            Limit: limit,
            ScanIndexForward: true // Oldest first
        }));

        const items = (result.Items || []).map(item => ({
            pk: item.PK, // VIOLATION#id
            sk: item.SK, // METADATA
            violation_id: item.PK.replace('VIOLATION#', ''),
            title_bedrock: item.bedrock_title,
            description_bedrock: item.bedrock_description,
            tags: item.bedrock_tags || [],
            generated_at: item.generated_at,
            attribution: 'Data synthesized by SirSluginston VentureOS'
        }));

        return corsResponse(200, { items, count: items.length });
    } catch (error) {
        console.error('Error getting review queue:', error);
        return corsResponse(500, { error: error.message });
    }
}

async function getRandomReviewItem() {
    try {
        const result = await getReviewQueue(1);
        const parsed = JSON.parse(result.body);
        if (parsed.items && parsed.items.length > 0) {
            return corsResponse(200, parsed.items[0]);
        }
        return corsResponse(200, { message: "No items in review queue" });
    } catch (error) {
        console.error('Error getting random review item:', error);
        return corsResponse(500, { error: error.message });
    }
}

async function approveContent(pk, sk) {
    try {
        const reviewDate = new Date().toISOString();
        
        await docClient.send(new UpdateCommand({
            TableName: STAGING_TABLE,
            Key: { PK: pk, SK: sk },
            UpdateExpression: "SET #status = :approved, reviewed_at = :date",
            ExpressionAttributeNames: {
                "#status": "status"
            },
            ExpressionAttributeValues: {
                ":approved": "APPROVED",
                ":date": reviewDate
            }
        }));
        return corsResponse(200, { success: true, message: 'Content approved' });
    } catch (error) {
        console.error('Error approving content:', error);
        return corsResponse(500, { error: error.message });
    }
}

async function bulkApproveContent(items) {
    try {
        const reviewDate = new Date().toISOString();
        let approved = 0;
        let errors = 0;
        const errorDetails = [];

        // Process items in parallel batches
        const batchSize = 10;
        for (let i = 0; i < items.length; i += batchSize) {
            const batch = items.slice(i, i + batchSize);
            const promises = batch.map(async (item) => {
                try {
                    await docClient.send(new UpdateCommand({
                        TableName: STAGING_TABLE,
                        Key: { PK: item.pk, SK: item.sk },
                        UpdateExpression: "SET #status = :approved, reviewed_at = :date",
                        ExpressionAttributeNames: {
                            "#status": "status"
                        },
                        ExpressionAttributeValues: {
                            ":approved": "APPROVED",
                            ":date": reviewDate
                        }
                    }));
                    approved++;
                } catch (error) {
                    errors++;
                    errorDetails.push({ pk: item.pk, error: error.message });
                }
            });

            await Promise.all(promises);
        }

        return corsResponse(200, { 
            success: true, 
            approved, 
            errors,
            total: items.length,
            errorDetails: errorDetails.length > 0 ? errorDetails : undefined
        });
    } catch (error) {
        console.error('Error in bulk approve:', error);
        return corsResponse(500, { error: error.message });
    }
}

// --- BEDROCK BATCH PROCESSING FUNCTIONS ---

async function createBatchJob(config) {
    try {
        // Invoke batch processor Lambda
        const response = await lambdaClient.send(new InvokeCommand({
            FunctionName: BATCH_PROCESSOR_FUNCTION,
            InvocationType: 'RequestResponse',
            Payload: JSON.stringify({ config })
        }));
        
        const result = JSON.parse(new TextDecoder().decode(response.Payload));
        
        if (result.errorMessage) {
            return corsResponse(500, { error: result.errorMessage });
        }
        
        return corsResponse(200, JSON.parse(result.body || '{}'));
    } catch (error) {
        console.error('Error creating batch job:', error);
        return corsResponse(500, { error: error.message });
    }
}

async function estimateBatchCost(config) {
    try {
        // Invoke batch processor Lambda in dry-run mode
        const response = await lambdaClient.send(new InvokeCommand({
            FunctionName: BATCH_PROCESSOR_FUNCTION,
            InvocationType: 'RequestResponse',
            Payload: JSON.stringify({ 
                config: config || {},
                dryRun: true 
            })
        }));
        
        const result = JSON.parse(new TextDecoder().decode(response.Payload));
        
        if (result.errorMessage) {
            return corsResponse(500, { error: result.errorMessage });
        }
        
        const body = JSON.parse(result.body || '{}');
        return corsResponse(200, body);
    } catch (error) {
        console.error('Error estimating batch cost:', error);
        return corsResponse(500, { error: error.message });
    }
}

async function listBatchJobs(selectedAgencies = []) {
    try {
        let result;
        
        // If specific agencies selected, use Query for efficiency
        if (selectedAgencies && selectedAgencies.length > 0) {
            // Query each agency separately and combine results
            const allJobs = [];
            for (const agency of selectedAgencies) {
                const agencyResult = await docClient.send(new QueryCommand({
                    TableName: BATCH_JOBS_TABLE,
                    KeyConditionExpression: 'PK = :pk AND SK = :sk',
                    ExpressionAttributeValues: {
                        ':pk': `AGENCY#${agency.toLowerCase()}#JOB#`,
                        ':sk': 'METADATA'
                    }
                }));
                // Note: Query with begins_with requires GSI, so we'll scan and filter
                // For now, fall back to scan with filter
            }
            
            // Fall back to scan with filter for multi-agency or if query fails
            result = await docClient.send(new ScanCommand({
                TableName: BATCH_JOBS_TABLE,
                FilterExpression: 'SK = :sk AND (agencies = :agency OR contains(agencies, :agency))',
                ExpressionAttributeValues: {
                    ':sk': 'METADATA',
                    ':agency': selectedAgencies[0] // Simplified - would need OR conditions for multiple
                },
                Limit: 50
            }));
        } else {
            // No filter - get all jobs (scan)
            result = await docClient.send(new ScanCommand({
                TableName: BATCH_JOBS_TABLE,
                FilterExpression: 'SK = :sk',
                ExpressionAttributeValues: {
                    ':sk': 'METADATA'
                },
                Limit: 50
            }));
        }
        
        let jobs = result.Items || [];
        
        // Filter by agencies if specified (client-side filter for now)
        if (selectedAgencies && selectedAgencies.length > 0) {
            jobs = jobs.filter(job => {
                const jobAgencies = job.agencies || [];
                // If job has 'all' or no agencies filter, include it
                if (jobAgencies.includes('all') || jobAgencies.length === 0) {
                    return true;
                }
                // Check if any selected agency matches job agencies
                return selectedAgencies.some(agency => 
                    jobAgencies.includes(agency.toLowerCase())
                );
            });
        }
        
        // Sort by createdAt descending (most recent first)
        jobs.sort((a, b) => {
            const dateA = new Date(a.createdAt || a.CreatedAt || 0).getTime();
            const dateB = new Date(b.createdAt || b.CreatedAt || 0).getTime();
            return dateB - dateA;
        });
        
        return corsResponse(200, {
            jobs
        });
    } catch (error) {
        console.error('Error listing batch jobs:', error);
        return corsResponse(500, { error: error.message });
    }
}

async function getBatchJobStatus(jobId) {
    try {
        const result = await docClient.send(new GetCommand({
            TableName: BATCH_JOBS_TABLE,
            Key: {
                PK: `JOB#${jobId}`,
                SK: 'METADATA'
            }
        }));
        
        if (!result.Item) {
            return corsResponse(404, { error: 'Job not found' });
        }
        
        return corsResponse(200, result.Item);
    } catch (error) {
        console.error('Error getting batch job status:', error);
        return corsResponse(500, { error: error.message });
    }
}

// --- USER SETTINGS FUNCTIONS ---

function getUserIdFromEvent(event) {
    // Extract user ID from Cognito claims
    const claims = event.requestContext?.authorizer?.claims;
    const userId = claims?.sub || claims?.userId;
    if (!userId) {
        throw new Error('User ID not found in request context');
    }
    return userId;
}

async function getUserSettings(event) {
    try {
        const userId = getUserIdFromEvent(event);
        const claims = event.requestContext?.authorizer?.claims;
        
        const result = await docClient.send(new GetCommand({
            TableName: USERS_TABLE,
            Key: {
                PK: `USER#${userId}`,
                SK: 'SETTINGS'
            }
        }));
        
        // If settings don't exist, create default settings automatically
        if (!result.Item) {
            const now = new Date().toISOString();
            const defaultSettings = {
                PK: `USER#${userId}`,
                SK: 'SETTINGS',
                UserId: userId,
                Email: claims?.email || '',
                RealName: claims?.name || '',
                DisplayName: claims?.name || '',
                AvatarURL: '',
                Timezone: '',
                EmailNotifications: true,
                MarketingEmails: false,
                ProjectUpdates: true,
                SystemNotifications: true,
                ThemePreference: 'auto',
                DateFormat: '',
                ShowEmailPublicly: false,
                AnalyticsOptOut: false,
                CreatedAt: now,
                UpdatedAt: now
            };
            
            await docClient.send(new PutCommand({
                TableName: USERS_TABLE,
                Item: defaultSettings
            }));
            
            const { PK, SK, ...settings } = defaultSettings;
            return corsResponse(200, settings);
        }
        
        // Return settings without PK/SK
        const { PK, SK, ...settings } = result.Item;
        return corsResponse(200, settings);
    } catch (error) {
        console.error('Error getting user settings:', error);
        if (error.message.includes('User ID not found')) {
            return corsResponse(401, { error: 'Unauthorized' });
        }
        return corsResponse(500, { error: error.message });
    }
}

async function updateUserSettings(event, body) {
    try {
        const userId = getUserIdFromEvent(event);
        const claims = event.requestContext?.authorizer?.claims;
        
        // Get existing settings or create defaults
        const existing = await docClient.send(new GetCommand({
            TableName: USERS_TABLE,
            Key: {
                PK: `USER#${userId}`,
                SK: 'SETTINGS'
            }
        }));
        
        const now = new Date().toISOString();
        const updatedSettings = {
            PK: `USER#${userId}`,
            SK: 'SETTINGS',
            UserId: userId,
            Email: claims?.email || existing.Item?.Email || '',
            RealName: body.RealName || existing.Item?.RealName || claims?.name || '',
            DisplayName: body.DisplayName || existing.Item?.DisplayName || claims?.name || '',
            AvatarURL: body.AvatarURL !== undefined ? body.AvatarURL : (existing.Item?.AvatarURL || ''),
            Timezone: body.Timezone !== undefined ? body.Timezone : (existing.Item?.Timezone || ''),
            EmailNotifications: body.EmailNotifications !== undefined ? body.EmailNotifications : (existing.Item?.EmailNotifications ?? true),
            MarketingEmails: body.MarketingEmails !== undefined ? body.MarketingEmails : (existing.Item?.MarketingEmails ?? false),
            ProjectUpdates: body.ProjectUpdates !== undefined ? body.ProjectUpdates : (existing.Item?.ProjectUpdates ?? true),
            SystemNotifications: body.SystemNotifications !== undefined ? body.SystemNotifications : (existing.Item?.SystemNotifications ?? true),
            ThemePreference: body.ThemePreference || existing.Item?.ThemePreference || 'auto',
            DateFormat: body.DateFormat !== undefined ? body.DateFormat : (existing.Item?.DateFormat || ''),
            ShowEmailPublicly: body.ShowEmailPublicly !== undefined ? body.ShowEmailPublicly : (existing.Item?.ShowEmailPublicly ?? false),
            AnalyticsOptOut: body.AnalyticsOptOut !== undefined ? body.AnalyticsOptOut : (existing.Item?.AnalyticsOptOut ?? false),
            CreatedAt: existing.Item?.CreatedAt || now,
            UpdatedAt: now
        };
        
        await docClient.send(new PutCommand({
            TableName: USERS_TABLE,
            Item: updatedSettings
        }));
        
        const { PK, SK, ...settings } = updatedSettings;
        return corsResponse(200, settings);
    } catch (error) {
        console.error('Error updating user settings:', error);
        if (error.message.includes('User ID not found')) {
            return corsResponse(401, { error: 'Unauthorized' });
        }
        return corsResponse(500, { error: error.message });
    }
}

async function createUserRecord(event, body) {
    try {
        const userId = getUserIdFromEvent(event);
        const claims = event.requestContext?.authorizer?.claims;
        
        const now = new Date().toISOString();
        const userSettings = {
            PK: `USER#${userId}`,
            SK: 'SETTINGS',
            UserId: userId,
            Email: body.Email || claims?.email || '',
            RealName: body.RealName || claims?.name || '',
            DisplayName: body.DisplayName || body.RealName || claims?.name || '',
            AvatarURL: '',
            Timezone: '',
            EmailNotifications: true,
            MarketingEmails: false,
            ProjectUpdates: true,
            SystemNotifications: true,
            ThemePreference: 'auto',
            DateFormat: '',
            ShowEmailPublicly: false,
            AnalyticsOptOut: false,
            CreatedAt: now,
            UpdatedAt: now
        };
        
        await docClient.send(new PutCommand({
            TableName: USERS_TABLE,
            Item: userSettings
        }));
        
        const { PK, SK, ...settings } = userSettings;
        return corsResponse(200, settings);
    } catch (error) {
        console.error('Error creating user record:', error);
        if (error.message.includes('User ID not found')) {
            return corsResponse(401, { error: 'Unauthorized' });
        }
        return corsResponse(500, { error: error.message });
    }
}
