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

// Parquet Schema Definition
// We use a flexible schema where possible, or stringify complex objects
const PARQUET_SCHEMA = {
    event_id: { type: 'UTF8' },
    agency: { type: 'UTF8' },
    ingested_at: { type: 'UTF8' },
    event_date: { type: 'UTF8', optional: true },
    company_slug: { type: 'UTF8', optional: true },
    city_slug: { type: 'UTF8', optional: true },
    raw_data: { type: 'UTF8' } // Store raw JSON as string
};

export const handler = async (event) => {
    console.log(`⚡ Processor: Handling ${event.Records.length} batches...`);

    const outputBatch = [];

    // 1. Process Batch
    for (const record of event.Records) {
        try {
            const payload = JSON.parse(record.body);
            const { raw, meta } = payload;

            const parts = meta.source_key.split('/');
            const agency = parts[0] || 'unknown';
            const dataset = parts[1] || 'generic';

            const map = await getSextantMap(agency, dataset);
            const cleanRecord = await normalizeAndMatch(raw, map, agency);

            outputBatch.push(cleanRecord);
        } catch (e) {
            console.error("Row Processing Failed:", e);
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
                    event_id: row.event_id,
                    agency: row.agency,
                    ingested_at: row.ingested_at,
                    event_date: row.event_date || null,
                    company_slug: row.company_slug || null,
                    city_slug: row.city_slug || null,
                    raw_data: row.raw_data // Already stringified in normalizeAndMatch
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
        Object.keys(rawRow).forEach(k => normalized[k.toLowerCase()] = rawRow[k]);
    }

    const rowString = JSON.stringify(rawRow);
    normalized.event_id = crypto.createHash('sha256').update(SALT + rowString).digest('hex');
    normalized.agency = agency;
    normalized.ingested_at = new Date().toISOString();
    normalized.raw_data = JSON.stringify(rawData); 

    if (normalized.company_name) {
        normalized.company_slug = await resolveEntity(normalized.company_name);
    }
    if (normalized.city && normalized.state) {
        normalized.city_slug = `CITY#${normalized.state.trim().toUpperCase()}-${normalized.city.trim().toLowerCase().replace(/ /g, '_')}`;
    }

    return normalized;
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
