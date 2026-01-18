import { S3Client, GetObjectCommand } from "@aws-sdk/client-s3";
import { DynamoDBClient, BatchWriteItemCommand } from "@aws-sdk/client-dynamodb";
import { marshall } from "@aws-sdk/util-dynamodb";
import { Readable } from "stream";
import readline from "readline";

const s3 = new S3Client({});
const dynamo = new DynamoDBClient({});
const TABLE_NAME = "VentureOS-Lighthouse";

export const handler = async (event) => {
    console.log("🔦 Lighthouse: Processing Result File (JSON)...", JSON.stringify(event));

    for (const record of event.Records) {
        const bucket = record.s3.bucket.name;
        const key = decodeURIComponent(record.s3.object.key.replace(/\+/g, " "));

        const type = detectType(key);
        if (!type) {
            console.log(`Skipping unknown file type: ${key}`);
            continue;
        }

        await processFile(bucket, key, type);
    }
};

function detectType(key) {
    if (key.includes("/nation/")) return "nation";
    if (key.includes("/states/")) return "state";
    if (key.includes("/cities/")) return "city";
    if (key.includes("/companies/")) return "company";
    if (key.includes("/sites/")) return "site";
    return null;
}

async function processFile(bucket, key, type) {
    console.log(`Processing ${type} from s3://${bucket}/${key}`);
    
    const response = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
    const stream = response.Body;
    
    const rl = readline.createInterface({
        input: stream,
        crlfDelay: Infinity
    });

    let batch = [];
    let count = 0;

    for await (const line of rl) {
        if (!line.trim()) continue;

        try {
            const row = JSON.parse(line);
            const item = transformRow(row, type);
            if (!item) continue;

            batch.push({
                PutRequest: { Item: marshall(item, { removeUndefinedValues: true }) }
            });

            if (batch.length >= 25) {
                await writeBatch(batch);
                count += batch.length;
                batch = [];
            }
        } catch (e) {
            console.error("Failed to parse line:", line, e.message);
        }
    }

    if (batch.length > 0) {
        await writeBatch(batch);
        count += batch.length;
    }

    console.log(`✅ Loaded ${count} ${type} items.`);
}

function transformRow(row, type) {
    const now = new Date().toISOString();
    
    // Base object common to all
    const base = {
        updatedAt: now,
        recentEvents: row.recent_events, // Global top 5
        recentEventsOSHA: row.recent_events_osha, // OSHA specific top 5
        // Future: recentEventsMSHA, etc.
    };

    if (type === "nation") {
        return {
            ...base,
            PK: "NATION#usa",
            SK: "SUMMARY",
            name: "United States",
            stats: {
                totalEvents: parseInt(row.total_events || 0),
                totalStates: parseInt(row.total_states || 0), 
                totalCities: parseInt(row.total_cities || 0),
                totalCompanies: parseInt(row.total_companies || 0)
            }
        };
    }
    
    if (type === "state") {
        return {
            ...base,
            PK: `STATE#${row.state}`,
            SK: "SUMMARY",
            name: row.state, 
            stats: {
                totalEvents: parseInt(row.total_events || 0),
                totalCities: parseInt(row.total_cities || 0),
                totalCompanies: parseInt(row.total_companies || 0)
            }
        };
    }
    
    if (type === "city") {
        const pk = row.city_slug.startsWith("CITY#") ? row.city_slug : `CITY#${row.city_slug}`;
        return {
            ...base,
            PK: pk,
            SK: "SUMMARY",
            name: row.city || row.city_slug, 
            state: row.state,
            slug: row.city_slug,
            stats: {
                totalEvents: parseInt(row.total_events || 0),
                totalCompanies: parseInt(row.total_companies || 0)
            }
        };
    }
    
    if (type === "company") {
        const pk = row.company_slug.startsWith("SLUG#") 
            ? row.company_slug.replace("SLUG#", "COMPANY#") 
            : `COMPANY#${row.company_slug}`;
            
        return {
            ...base,
            PK: pk,
            SK: "SUMMARY",
            slug: row.company_slug,
            stats: {
                totalEvents: parseInt(row.total_events || 0),
                lastActive: row.last_active
            }
        };
    }
    
    return null;
}

async function writeBatch(items) {
    try {
        await dynamo.send(new BatchWriteItemCommand({
            RequestItems: {
                [TABLE_NAME]: items
            }
        }));
    } catch (e) {
        console.error("Batch Write Failed:", e.message);
    }
}
