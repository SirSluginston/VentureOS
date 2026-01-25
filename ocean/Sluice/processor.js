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
// UPDATED: Using the Master Ocean Bucket
const OCEAN_BUCKET = "venture-os-ocean";
const SALT = "VOS_SALT_v1";

const SEXTANT_CACHE = new Map();
const ANCHOR_CACHE = new Map();

// State name to abbreviation mapping
const STATE_ABBREVS = {
    "ALABAMA": "AL", "ALASKA": "AK", "ARIZONA": "AZ", "ARKANSAS": "AR", "CALIFORNIA": "CA",
    "COLORADO": "CO", "CONNECTICUT": "CT", "DELAWARE": "DE", "FLORIDA": "FL", "GEORGIA": "GA",
    "HAWAII": "HI", "IDAHO": "ID", "ILLINOIS": "IL", "INDIANA": "IN", "IOWA": "IA",
    "KANSAS": "KS", "KENTUCKY": "KY", "LOUISIANA": "LA", "MAINE": "ME", "MARYLAND": "MD",
    "MASSACHUSETTS": "MA", "MICHIGAN": "MI", "MINNESOTA": "MN", "MISSISSIPPI": "MS", "MISSOURI": "MO",
    "MONTANA": "MT", "NEBRASKA": "NE", "NEVADA": "NV", "NEW HAMPSHIRE": "NH", "NEW JERSEY": "NJ",
    "NEW MEXICO": "NM", "NEW YORK": "NY", "NORTH CAROLINA": "NC", "NORTH DAKOTA": "ND", "OHIO": "OH",
    "OKLAHOMA": "OK", "OREGON": "OR", "PENNSYLVANIA": "PA", "RHODE ISLAND": "RI", "SOUTH CAROLINA": "SC",
    "SOUTH DAKOTA": "SD", "TENNESSEE": "TN", "TEXAS": "TX", "UTAH": "UT", "VERMONT": "VT",
    "VIRGINIA": "VA", "WASHINGTON": "WA", "WEST VIRGINIA": "WV", "WISCONSIN": "WI", "WYOMING": "WY",
    "DISTRICT OF COLUMBIA": "DC", "PUERTO RICO": "PR", "VIRGIN ISLANDS": "VI", "GUAM": "GU",
    "AMERICAN SAMOA": "AS", "NORTHERN MARIANA ISLANDS": "MP", "US MINOR OUTLYING ISLANDS": "UM"
};

function getStateAbbrev(stateName) {
    if (!stateName) return null;
    const upper = stateName.trim().toUpperCase();
    if (upper.length === 2) return upper;
    return STATE_ABBREVS[upper] || upper;
}

const CITY_NOISE_PREFIXES = /^(STE|SUITE|UNIT|APT|BLDG|BUILDING|FLOOR|FL|RM|ROOM|PO BOX|P\.?O\.?\s*BOX)\b\s*\d*\s*/i;

function cleanCityName(rawCity) {
    if (!rawCity) return rawCity;
    let city = rawCity.trim().toUpperCase();

    if (city.includes(',')) {
        const parts = city.split(',').map(p => p.trim()).filter(Boolean);
        for (let i = parts.length - 1; i >= 0; i--) {
            const part = parts[i].replace(/\d+/g, '').trim();
            if (part.length > 2) {
                city = parts[i];
                break;
            }
        }
    }

    city = city.replace(/\s*\d{5}(-\d{4})?\s*$/, '')
        .replace(/^\d+\s+[A-Z]\.?\s+[A-Z]+\s+(STREET|ST|AVENUE|AVE|ROAD|RD|DRIVE|DR|BLVD|BOULEVARD|WAY|LANE|LN|COURT|CT|CIRCLE|CIR|PLACE|PL)\s*/i, '')
        .replace(CITY_NOISE_PREFIXES, '')
        .replace(/^\d+\s+/, '').trim();

    if (city === 'NYC') city = 'NEW YORK';
    city = city.replace(/ TOWNSHIP$/i, '');

    city = city.replace(/^MT\.?\s+/i, 'MOUNT ')
        .replace(/^FT\.?\s+/i, 'FORT ')
        .replace(/\bST\.?\b/gi, 'SAINT')
        .replace(/\bSTE\.?\b/gi, 'SAINTE');

    city = city.replace(/\bBCH\b/gi, 'BEACH')
        .replace(/\bHTS\b/gi, 'HEIGHTS')
        .replace(/\bSPGS\b/gi, 'SPRINGS')
        .replace(/\bVLY\b/gi, 'VALLEY')
        .replace(/\bCTR\b/gi, 'CENTER')
        .replace(/\bPK\b/gi, 'PARK')
        .replace(/\bPT\b/gi, 'PORT');

    if (/\d/.test(city)) {
        const words = city.split(/\s+/);
        const cityWords = [];
        for (let i = words.length - 1; i >= 0; i--) {
            if (!/\d/.test(words[i]) && words[i].length > 1) {
                cityWords.unshift(words[i]);
            } else if (cityWords.length > 0) {
                break;
            }
        }
        if (cityWords.length > 0) city = cityWords.join(' ');
    }

    return city || rawCity.trim().toUpperCase();
}

function cleanCompanyForAlias(rawName) {
    if (!rawName) return "";
    return rawName.toLowerCase()
        .replace(/[#0-9]/g, '')
        .replace(/\b(inc\.?|corp\.?|llc|co\.?|ltd\.?|store|supercenter|fulfillment|services|motors|coffee)\b/gi, '')
        .replace(/[-]/g, ' ')
        .replace(/\s+/g, ' ')
        .trim();
}

// UPDATED: Parquet Schema Definition (The Fat Table)
const PARQUET_SCHEMA = {
    // Identity
    event_id: { type: 'UTF8' },
    event_source: { type: 'UTF8' },         // Was source (reserved keyword)
    source_url: { type: 'UTF8', optional: true },

    // Time
    ingested_at: { type: 'UTF8' },
    event_date: { type: 'UTF8', optional: true },

    // Anchors
    // Anchors
    state: { type: 'UTF8', optional: true },
    city: { type: 'UTF8', optional: true },
    city_slug: { type: 'UTF8', optional: true },
    company_name: { type: 'UTF8', optional: true }, // Promoted to Top-Level
    company_slug: { type: 'UTF8', optional: true },
    site_id: { type: 'UTF8', optional: true },

    // Content
    event_title: { type: 'UTF8', optional: true },
    event_description: { type: 'UTF8', optional: true },

    // Details (JSON String for Map)
    event_details: { type: 'UTF8', optional: true },

    // Bedrock
    bedrock_event_title: { type: 'UTF8', optional: true },
    bedrock_event_description: { type: 'UTF8', optional: true },
    bedrock_verified: { type: 'BOOLEAN', optional: true }, // Added for Workflow

    // Audit
    raw_data: { type: 'UTF8' },       // Full JSON
    quarantine_reason: { type: 'UTF8', optional: true }
};

const CITY_VALIDATION_CACHE = new Map();

export const handler = async (event) => {
    console.log(`⚡ Processor V3 (Ocean/Reef): Handling ${event.Records.length} messages...`);

    const validBatch = [];
    const quarantineBatch = [];

    for (const record of event.Records) {
        try {
            const payload = JSON.parse(record.body);
            const rows = payload.rows || [payload.raw];
            const meta = payload.meta;

            // UPDATED: Identify Source (was Agency)
            // Estuary Path: estuary/OSHA/severe-incident/file.csv
            // Key Split: [estuary, OSHA, severe-incident, file.csv]
            const parts = meta.source_key.split('/');

            // Logic to find 'Source': It's the folder AFTER 'estuary' (or 'confluence' legacy)
            // If path starts with 'estuary/' -> index 1 is Source.
            let sourceIdx = -1;
            if (parts[0] === 'estuary' || parts[0] === 'confluence') sourceIdx = 1;
            else if (parts[0] === 'Historical' || parts[0] === 'Daily') sourceIdx = 1; // Legacy catch

            const source = (sourceIdx > -1 && parts[sourceIdx]) ? parts[sourceIdx] : 'unknown';
            const dataset = (sourceIdx > -1 && parts[sourceIdx + 1]) ? parts[sourceIdx + 1] : 'generic';

            const map = await getSextantMap(source, dataset);

            for (const raw of rows) {
                try {
                    const cleanRecord = await normalizeAndMatch(raw, map, source, meta.source_key);
                    const validation = await validateRecord(cleanRecord);

                    if (validation.isValid) {
                        validBatch.push(cleanRecord);
                    } else {
                        cleanRecord.quarantine_reason = validation.reason;
                        quarantineBatch.push(cleanRecord);
                    }
                } catch (rowErr) {
                    console.error("Row Processing Failed:", rowErr);
                }
            }
        } catch (e) {
            console.error("Message Processing Failed:", e);
        }
    }

    // UPDATED: Write to 'reef' (valid) and 'quarantine' (invalid)
    await Promise.all([
        writeBatchToS3(validBatch, "reef"),
        writeBatchToS3(quarantineBatch, "quarantine")
    ]);
};

async function writeBatchToS3(batch, prefixType) {
    if (batch.length === 0) return;

    const tmpParquetPath = `/tmp/${prefixType}_${crypto.randomUUID()}.parquet`;

    try {
        const schema = new parquets.ParquetSchema(PARQUET_SCHEMA);
        const writer = await parquets.ParquetWriter.openFile(schema, tmpParquetPath);

        for (const row of batch) {
            await writer.appendRow({
                event_id: row.event_id,
                event_source: row.source || 'unknown',
                source_url: row.source_url || null,
                ingested_at: row.ingested_at,
                raw_data: row.raw_data,
                event_date: row.event_date || null,
                state: row.state || null,
                city: row.city || null,
                city_slug: row.city_slug || null,
                company_name: row.company_name || null,
                company_slug: row.company_slug || null,
                site_id: row.site_id || null,
                event_title: row.event_title || null,
                event_description: row.event_description || null,
                event_details: row.event_details || null,
                bedrock_event_title: row.bedrock_event_title || null,
                bedrock_event_description: row.bedrock_event_description || null,
                bedrock_verified: row.bedrock_verified || false,
                quarantine_reason: row.quarantine_reason || null
            });
        }

        await writer.close();
        const buffer = fs.readFileSync(tmpParquetPath);
        fs.unlinkSync(tmpParquetPath);

        const now = new Date();
        // UPDATED: Path includes Source/Year for organized Reef
        // reef/OSHA/2026/file.parquet
        const sourceFolder = batch[0].source || 'unknown';
        // UPDATED: Hive-style partitioning for Athena MSCK REPAIR support
        // reef/partition_source=OSHA/partition_year=2026/file.parquet
        const key = `${prefixType}/partition_source=${sourceFolder}/partition_year=${now.getFullYear()}/${now.toISOString()}_${crypto.randomUUID()}.parquet`;

        await s3.send(new PutObjectCommand({
            Bucket: OCEAN_BUCKET,
            Key: key,
            Body: buffer,
            ContentType: "application/vnd.apache.parquet"
        }));
        console.log(`✅ [${prefixType.toUpperCase()}] Scribed ${batch.length} rows to s3://${OCEAN_BUCKET}/${key}`);

    } catch (e) {
        console.error(`❌ ${prefixType} Write Failed:`, e);
        throw e;
    }
}

// --- VALIDATION LOGIC ---

async function validateRecord(record) {
    if (!record.state || record.state.length !== 2 || ['US', 'ON', 'XX'].includes(record.state)) {
        return { isValid: false, reason: `Invalid State: ${record.state}` };
    }

    if (record.city_slug) {
        const isValidCity = await lookupCity(record.city_slug, record.city, record.state);
        if (!isValidCity) {
            return { isValid: false, reason: `Unknown City: ${record.city} (${record.city_slug})` };
        }
    } else {
        return { isValid: false, reason: "Missing City/State" };
    }

    return { isValid: true };
}

async function lookupCity(slug, city, state) {
    if (CITY_VALIDATION_CACHE.has(slug)) return CITY_VALIDATION_CACHE.get(slug);

    try {
        const res = await dynamo.send(new GetItemCommand({
            TableName: ANCHOR_TABLE,
            Key: marshall({ PK: `CITY#${slug}`, SK: 'METADATA' })
        }));

        if (res.Item) {
            CITY_VALIDATION_CACHE.set(slug, true);
            return true;
        }

        if (city && state) {
            const aliasKey = `${state.toUpperCase()}-${city.toLowerCase().replace(/ /g, '_')}`;
            const aliasRes = await dynamo.send(new QueryCommand({
                TableName: ANCHOR_TABLE,
                IndexName: 'GSI1',
                KeyConditionExpression: 'GSI1PK = :alias',
                ExpressionAttributeValues: marshall({ ':alias': `ALIAS#${aliasKey}` }),
                Limit: 1
            }));

            if (aliasRes.Items && aliasRes.Items.length > 0) {
                console.log(`✨ City Alias Match: ${slug} -> ${aliasKey}`);
                CITY_VALIDATION_CACHE.set(slug, true);
                return true;
            }
        }
        return false;
    } catch (e) {
        console.warn(`Anchor City Lookup Failed for ${slug}:`, e);
        return false;
    }
}

// --- LOGIC ---

async function getSextantMap(source, dataset) {
    const cacheKey = `${source}/${dataset}`;
    if (SEXTANT_CACHE.has(cacheKey)) return SEXTANT_CACHE.get(cacheKey);
    try {
        // UPDATED KEY: SOURCE# instead of AGENCY#
        const res = await dynamo.send(new GetItemCommand({
            TableName: SEXTANT_TABLE,
            Key: marshall({ PK: `SOURCE#${source}`, SK: `SCHEMA#${dataset}` })
        }));
        if (res.Item) {
            const map = unmarshall(res.Item).header_map;
            SEXTANT_CACHE.set(cacheKey, map);
            return map;
        } else {
            // Fallback to AGENCY# for backwards compatibility during migration? 
            // Turning it off to force clean migration.
        }
    } catch (e) { console.warn("Sextant Lookup Failed:", e); }
    return null;
}

async function normalizeAndMatch(rawRow, map, source, sourceKey) {
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
    normalized.source = source.toUpperCase(); // Was agency
    normalized.source_url = sourceKey; // Added
    normalized.ingested_at = new Date().toISOString();
    normalized.raw_data = JSON.stringify(rawData);

    let companyNameForLookup = normalized.company_name;
    if (normalized.company_name) {
        // ... (existing logic)
        const siteMatch = normalized.company_name.match(/[#\-]\s*(\d+)\s*$/);
        if (siteMatch) {
            normalized.site_id = siteMatch[1];
            companyNameForLookup = normalized.company_name.replace(/[#\-]\s*\d+\s*$/, '').trim();
        }
        normalized.company_slug = await resolveEntitySmart(companyNameForLookup);
    }

    // Default Bedrock Status
    normalized.bedrock_verified = false;

    if (normalized.state) normalized.state = getStateAbbrev(normalized.state);
    if (normalized.city) normalized.city = cleanCityName(normalized.city);
    if (normalized.city && normalized.state) {
        const cityLower = normalized.city.toLowerCase().replace(/ /g, '_');
        normalized.city_slug = `${normalized.state}-${cityLower}`;
    }

    normalized.event_title = generateEventTitle(normalized);
    normalized.event_description = normalized.description || null;

    const excludeFromDetails = new Set(['event_id', 'source', 'ingested_at', 'raw_data',
        'event_date', 'state', 'city', 'city_slug', 'company_slug', 'site_id',
        'event_title', 'event_description', 'source_url']);

    const eventDetails = {};
    for (const [key, value] of Object.entries(normalized)) {
        if (!excludeFromDetails.has(key) && value !== undefined && value !== null) {
            eventDetails[key] = value;
        }
    }
    normalized.event_details = JSON.stringify(eventDetails);

    return normalized;
}

function generateEventTitle(normalized) {
    const company = normalized.company_name || 'Unknown Company';
    const city = normalized.city || '';
    const state = normalized.state || '';
    const year = normalized.event_date ? normalized.event_date.substring(0, 4) : '';
    const location = [city, state].filter(Boolean).join(', ');

    const isAnnualSummary = normalized.avg_annual_employees !== undefined ||
        normalized.total_hours_worked !== undefined;

    if (isAnnualSummary) return `${year} ${company} Annual Safety Report`.trim();

    const violationType = normalized.violation_type || 'Incident';
    if (location) return `${violationType} at ${company} in ${location}`.trim();
    return `${violationType} at ${company}`.trim();
}

async function resolveEntitySmart(rawName) {
    if (!rawName) return null;
    const cleanName = rawName.trim();

    if (ANCHOR_CACHE.has(cleanName)) return ANCHOR_CACHE.get(cleanName);

    let slug = await lookupAlias(`ALIAS#${cleanName.toLowerCase()}`);
    if (slug) {
        ANCHOR_CACHE.set(cleanName, slug);
        return slug;
    }

    const smartAlias = cleanCompanyForAlias(cleanName);
    if (smartAlias && smartAlias !== cleanName.toLowerCase()) {
        slug = await lookupAlias(`ALIAS#${smartAlias}`);
        if (slug) {
            console.log(`✨ SmartMatch: "${cleanName}" -> "${smartAlias}" -> ${slug}`);
            ANCHOR_CACHE.set(cleanName, slug);
            return slug;
        }
    }

    return null;
}

async function lookupAlias(pk) {
    try {
        const res = await dynamo.send(new QueryCommand({
            TableName: ANCHOR_TABLE,
            IndexName: 'GSI1',
            KeyConditionExpression: 'GSI1PK = :alias',
            ExpressionAttributeValues: marshall({ ':alias': pk }),
            Limit: 1
        }));

        if (res.Items && res.Items.length > 0) {
            const hit = unmarshall(res.Items[0]);
            const rawSlug = hit.GSI1SK || hit.SK || '';
            return rawSlug.replace(/^(COMPANY|SLUG|ALIAS)#/i, '');
        }
    } catch (e) {
    }
    return null;
}

