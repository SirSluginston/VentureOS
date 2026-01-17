import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import { DynamoDBClient, GetItemCommand, QueryCommand } from "@aws-sdk/client-dynamodb";
import { marshall, unmarshall } from "@aws-sdk/util-dynamodb";
import crypto from "node:crypto";
import fs from 'fs';
import parquets from '@dsnp/parquetjs';

const s3 = new S3Client({});
const dynamo = new DynamoDBClient({});

const ANCHOR_TABLE = "VentureOS-Anchor";
const SEXTANT_TABLE = "VentureOS-Sextant";
const STAGING_BUCKET = "venture-os-confluence";
const SALT = "VOS_SALT_v1"; 

const SEXTANT_CACHE = new Map();
const ANCHOR_CACHE = new Map();

// Parquet Schema Definition - Matches silver.events table
const PARQUET_SCHEMA = {
    // Core fields (required)
    event_id: { type: 'UTF8' },
    agency: { type: 'UTF8' },
    ingested_at: { type: 'UTF8' },
    raw_data: { type: 'UTF8' },
    
    // Location fields (optional)
    event_date: { type: 'UTF8', optional: true },
    state: { type: 'UTF8', optional: true },
    city: { type: 'UTF8', optional: true },
    city_slug: { type: 'UTF8', optional: true },
    
    // Entity fields (optional)
    company_slug: { type: 'UTF8', optional: true },
    site_id: { type: 'UTF8', optional: true },
    
    // Display fields (optional)
    event_title: { type: 'UTF8', optional: true },
    event_description: { type: 'UTF8', optional: true },
    event_details: { type: 'UTF8', optional: true }, // JSON blob of all normalized fields
    
    // AI-enhanced fields (NULL until Bedrock processes them)
    bedrock_event_title: { type: 'UTF8', optional: true },
    bedrock_event_description: { type: 'UTF8', optional: true }
};

export const handler = async (event) => {
    console.log(`⚡ Processor: Handling ${event.Records.length} messages...`);

    const outputBatch = [];

    // 1. Process Messages (each message contains multiple rows)
    for (const record of event.Records) {
        try {
            const payload = JSON.parse(record.body);
            
            // Handle both old format (single row) and new format (array of rows)
            const rows = payload.rows || [payload.raw];
            const meta = payload.meta;

            // Identify Agency (from meta path)
            // meta.source_key = "Historical/osha/severe/2024.csv"
            const parts = meta.source_key.split('/');
            
            // Handle root folder (Historical/Daily)
            let agencyIdx = 0;
            if (parts[0] === 'Historical' || parts[0] === 'Daily') {
                agencyIdx = 1;
            }
            
            const agency = parts[agencyIdx] || 'unknown';
            const dataset = parts[agencyIdx + 1] || 'generic';

            const map = await getSextantMap(agency, dataset);
            
            // Process each row in the message
            for (const raw of rows) {
                try {
                    const cleanRecord = await normalizeAndMatch(raw, map, agency);
                    outputBatch.push(cleanRecord);
                } catch (rowErr) {
                    console.error("Row Processing Failed:", rowErr);
                }
            }
        } catch (e) {
            console.error("Message Processing Failed:", e);
        }
    }

    if (outputBatch.length > 0) {
        // 2. ParquetJS: Write to Parquet (In-Memory / Tmp)
        const tmpParquetPath = `/tmp/batch_${crypto.randomUUID()}.parquet`;
        
        try {
            const schema = new parquets.ParquetSchema(PARQUET_SCHEMA);
            const writer = await parquets.ParquetWriter.openFile(schema, tmpParquetPath);
            
            for (const row of outputBatch) {
                // Prepare row for Parquet (ensure types match schema)
                const pRow = {
                    // Core fields
                    event_id: row.event_id,
                    agency: row.agency,
                    ingested_at: row.ingested_at,
                    raw_data: row.raw_data,
                    
                    // Location fields
                    event_date: row.event_date || null,
                    state: row.state || null,
                    city: row.city || null,
                    city_slug: row.city_slug || null,
                    
                    // Entity fields
                    company_slug: row.company_slug || null,
                    site_id: row.site_id || null,
                    
                    // Display fields
                    event_title: row.event_title || null,
                    event_description: row.event_description || null,
                    event_details: row.event_details || null,
                    
                    // AI fields (NULL until Bedrock processes)
                    bedrock_event_title: row.bedrock_event_title || null,
                    bedrock_event_description: row.bedrock_event_description || null
                };
                await writer.appendRow(pRow);
            }
            
            await writer.close();
            
            // Read back
            const buffer = fs.readFileSync(tmpParquetPath);
            fs.unlinkSync(tmpParquetPath);

            // 3. Scribe: Upload Parquet
            const now = new Date();
            const key = `staging/${now.getFullYear()}/${now.toISOString()}_${crypto.randomUUID()}.parquet`;

            await s3.send(new PutObjectCommand({
                Bucket: STAGING_BUCKET,
                Key: key,
                Body: buffer,
                ContentType: "application/vnd.apache.parquet"
            }));

            console.log(`✅ Scribed ${outputBatch.length} rows to s3://${STAGING_BUCKET}/${key}`);

        } catch (e) {
            console.error("Parquet Write Failed:", e);
            throw e;
        }
    }
};

// --- LOGIC ---

async function getSextantMap(agency, dataset) {
    const cacheKey = `${agency}/${dataset}`;
    if (SEXTANT_CACHE.has(cacheKey)) return SEXTANT_CACHE.get(cacheKey);

    try {
        const res = await dynamo.send(new GetItemCommand({
            TableName: SEXTANT_TABLE,
            Key: marshall({ PK: `AGENCY#${agency}`, SK: `SCHEMA#${dataset}` })
        }));
        
        if (res.Item) {
            const map = unmarshall(res.Item).header_map;
            SEXTANT_CACHE.set(cacheKey, map);
            return map;
        }
    } catch (e) { console.warn("Sextant Lookup Failed:", e); }
    return null;
}

async function normalizeAndMatch(rawRow, map, agency) {
    const normalized = {};
    const rawData = { ...rawRow };
    
    // Step 1: Apply Sextant map to normalize field names
    if (map) {
        for (const [coreKey, sourceKeys] of Object.entries(map)) {
            for (const srcKey of sourceKeys) {
                if (rawRow[srcKey] !== undefined) {
                    normalized[coreKey] = rawRow[srcKey];
                    break;
                }
            }
        }
    } else {
        // No map found - lowercase all keys as fallback
        Object.keys(rawRow).forEach(k => normalized[k.toLowerCase()] = rawRow[k]);
    }

    // Step 2: Generate event_id from salted hash of raw row
    const rowString = JSON.stringify(rawRow);
    normalized.event_id = crypto.createHash('sha256').update(SALT + rowString).digest('hex');
    normalized.agency = agency.toUpperCase(); // OSHA, FDA, NHTSA, etc.
    normalized.ingested_at = new Date().toISOString();
    normalized.raw_data = JSON.stringify(rawData);
    
    // Step 3: Extract site_id from company name (e.g., "Walmart #1234" -> site_id: "1234")
    let companyNameForLookup = normalized.company_name;
    if (normalized.company_name) {
        const siteMatch = normalized.company_name.match(/[#\-]\s*(\d+)\s*$/);
        if (siteMatch) {
            normalized.site_id = siteMatch[1];
            // Strip site_id from company name for cleaner lookup
            companyNameForLookup = normalized.company_name.replace(/[#\-]\s*\d+\s*$/, '').trim();
        }
        normalized.company_slug = await resolveEntity(companyNameForLookup);
    }
    
    // Step 4: Generate city_slug
    if (normalized.city && normalized.state) {
        const stateUpper = normalized.state.trim().toUpperCase();
        const cityLower = normalized.city.trim().toLowerCase().replace(/ /g, '_');
        normalized.city_slug = `CITY#${stateUpper}-${cityLower}`;
    }
    
    // Step 5: Generate event_title based on available data
    normalized.event_title = generateEventTitle(normalized, agency);
    
    // Step 6: Extract event_description from normalized fields
    // Uses 'description' field from Sextant map (e.g., "Final Narrative" for OSHA severe)
    normalized.event_description = normalized.description || null;
    
    // Step 7: Store ALL normalized fields as event_details JSON
    // Exclude fields that are already top-level columns
    const excludeFromDetails = [
        'event_id', 'agency', 'ingested_at', 'raw_data', 'event_date',
        'state', 'city', 'city_slug', 'company_slug', 'site_id',
        'event_title', 'event_description'
    ];
    const eventDetails = {};
    for (const [key, value] of Object.entries(normalized)) {
        if (!excludeFromDetails.includes(key) && value !== undefined && value !== null) {
            eventDetails[key] = value;
        }
    }
    normalized.event_details = JSON.stringify(eventDetails);
    
    // Step 8: Bedrock fields are NULL until AI processing
    normalized.bedrock_event_title = null;
    normalized.bedrock_event_description = null;

    return normalized;
}

/**
 * Generate a human-readable event title based on available data
 */
function generateEventTitle(normalized, agency) {
    const company = normalized.company_name || 'Unknown Company';
    const city = normalized.city || '';
    const state = normalized.state || '';
    const year = normalized.event_date ? normalized.event_date.substring(0, 4) : '';
    const location = [city, state].filter(Boolean).join(', ');
    
    // Check if this is an annual summary (ODI/ITA) vs incident report
    const isAnnualSummary = normalized.avg_annual_employees !== undefined || 
                           normalized.total_hours_worked !== undefined;
    
    if (isAnnualSummary) {
        // Annual report style: "2024 Walmart Safety Report"
        return `${year} ${company} Annual Safety Report`.trim();
    }
    
    // Incident style: Use violation_type if available, otherwise generic
    const violationType = normalized.violation_type || 'Incident';
    
    if (location) {
        return `${violationType} at ${company} in ${location}`.trim();
    }
    return `${violationType} at ${company}`.trim();
}

async function resolveEntity(rawName) {
    if (!rawName) return null;
    const cleanName = rawName.trim();
    if (ANCHOR_CACHE.has(cleanName)) return ANCHOR_CACHE.get(cleanName);

    try {
        const res = await dynamo.send(new QueryCommand({
            TableName: ANCHOR_TABLE,
            IndexName: 'GSI1',
            KeyConditionExpression: 'GSI1PK = :alias',
            ExpressionAttributeValues: marshall({ ':alias': `ALIAS#${cleanName.toLowerCase()}` }),
            Limit: 1
        }));

        if (res.Items && res.Items.length > 0) {
            const hit = unmarshall(res.Items[0]);
            const targetSlug = hit.GSI1SK || hit.SK;
            ANCHOR_CACHE.set(cleanName, targetSlug);
            return targetSlug;
        }
    } catch (e) { }
    
    return null;
}
