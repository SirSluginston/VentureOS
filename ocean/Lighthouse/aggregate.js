import { S3Client, GetObjectCommand } from "@aws-sdk/client-s3";
import { DynamoDBClient, BatchWriteItemCommand } from "@aws-sdk/client-dynamodb";
import { marshall } from "@aws-sdk/util-dynamodb";
import readline from "readline";
import crypto from "crypto";

const s3 = new S3Client({});
const dynamo = new DynamoDBClient({});
const TABLE_NAME = "VentureOS-Lighthouse";

// State abbreviation to full name mapping
const STATE_NAMES = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas", "CA": "California",
    "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware", "FL": "Florida", "GA": "Georgia",
    "HI": "Hawaii", "ID": "Idaho", "IL": "Illinois", "IN": "Indiana", "IA": "Iowa",
    "KS": "Kansas", "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
    "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi", "MO": "Missouri",
    "MT": "Montana", "NE": "Nebraska", "NV": "Nevada", "NH": "New Hampshire", "NJ": "New Jersey",
    "NM": "New Mexico", "NY": "New York", "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio",
    "OK": "Oklahoma", "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina",
    "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah", "VT": "Vermont",
    "VA": "Virginia", "WA": "Washington", "WV": "West Virginia", "WI": "Wisconsin", "WY": "Wyoming",
    "DC": "District of Columbia", "PR": "Puerto Rico", "VI": "Virgin Islands", "GU": "Guam",
    "AS": "American Samoa", "MP": "Northern Mariana Islands", "UM": "US Minor Outlying Islands",
    "AMERICAN SAMOA": "American Samoa", "NORTHERN MARIANA ISLANDS": "Northern Mariana Islands",
    "US MINOR OUTLYING ISLANDS": "US Minor Outlying Islands",
    "PUERTO RICO": "Puerto Rico", "VIRGIN ISLANDS": "Virgin Islands", "GUAM": "Guam",
    "DISTRICT OF COLUMBIA": "District of Columbia"
};

/**
 * Acronyms that should stay UPPERCASE (not title-cased)
 * Note: Company suffixes (Inc, Ltd, LLC) are fine as title case
 */
const ACRONYMS = new Set([
    // Agencies
    'OSHA', 'MSHA', 'FDA', 'NHTSA', 'FAA', 'EPA', 'DOT', 'CPSC', 'NIOSH',
    // Places
    'USA', 'US', 'NYC', 'LA', 'DC', 'UK',
    // Common abbreviations that look wrong title-cased
    'LLC', 'LP', 'LLP', 'PC', 'PLLC', 'NA', 'NV', 'II', 'III', 'IV'
]);

/**
 * Convert UPPERCASE or slug string to Title Case for display
 * "GREAT FALLS" -> "Great Falls"
 * "great_falls" -> "Great Falls"
 * "walmart-inc" -> "Walmart Inc"
 * Preserves acronyms: "ABC OSHA LLC" -> "Abc Osha Llc" -> "Abc OSHA LLC"
 * Silver stores UPPER, Lighthouse converts to Title Case for frontend
 */
function toTitleCase(str) {
    if (!str) return str;
    return str
        .trim()
        .replace(/[-_]/g, ' ')           // Convert hyphens/underscores to spaces
        .toLowerCase()
        .replace(/\b\w/g, c => c.toUpperCase())
        .split(' ')
        .map(word => ACRONYMS.has(word.toUpperCase()) ? word.toUpperCase() : word)
        .join(' ');
}

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

/**
 * Transform raw Athena event to SharedUI RecentEvent format
 * Frontend expects: { eventId, eventTitle, eventDescription, eventDate, companySlug, companyName, city, state, agency }
 */
function transformEvent(raw, idx, type) {
    if (!raw) return null;

    // Coast JSON uses lowercase field names
    const idSource = `${type}-${raw.eventdate || raw.date || ''}-${raw.eventtitle || raw.title || ''}-${raw.companyslug || ''}-${idx}`;
    const eventId = raw.eventid || crypto.createHash('sha256').update(idSource).digest('hex').substring(0, 16);

    return {
        eventId,
        eventTitle: raw.eventtitle || raw.title || 'Unknown Event',
        eventDescription: raw.eventdescription || raw.description || undefined,
        eventDate: raw.eventdate || raw.date || undefined,
        companySlug: raw.companyslug || undefined,  // URL-friendly slug
        companyName: toTitleCase(raw.companyname) || undefined,  // Display name in Title Case
        citySlug: raw.city_slug || undefined,
        city: toTitleCase(raw.city) || undefined,
        state: raw.state || undefined,  // Keep as abbreviation (GA, TN, etc)
        agency: 'OSHA',  // Source is in the map key, not individual events
    };
}

function transformEvents(rawEvents, type) {
    if (!rawEvents || !Array.isArray(rawEvents)) return [];
    return rawEvents.map((e, i) => transformEvent(e, i, type)).filter(Boolean);
}

/**
 * Transform Athena output row to SharedUI-ready DynamoDB item
 * Goal: Frontend can use this data directly with minimal/no transformation
 */
function transformRow(row, type) {
    const now = new Date().toISOString();

    // === NATION ===
    if (type === "nation") {
        // Transform states_directory to frontend DirectoryItem format
        const directory = (row.states_directory || []).map(s => ({
            slug: s.slug?.toLowerCase() || s.name?.toLowerCase(),  // lowercase for URL
            abbreviation: s.name?.toUpperCase(),                   // The raw code (TX, MP) from S3/Silver
            name: STATE_NAMES[s.name?.toUpperCase()] || toTitleCase(s.name) || s.name,    // Full state name
            count: parseInt(s.count || 0)
        }));

        return {
            PK: "NATION#USA",
            SK: "SUMMARY",
            name: "United States",
            slug: "usa",  // lowercase for URL
            stats: {
                totalEvents: parseInt(row.total_events || 0),
                totalStates: parseInt(row.total_states || 0),
                totalCities: parseInt(row.total_cities || 0),
                totalCompanies: parseInt(row.total_companies || 0)
            },
            directory,  // Renamed from statesDirectory
            updatedAt: now
        };
    }

    // === STATE ===
    if (type === "state") {
        const stateAbbrev = row.state?.toUpperCase();
        const stateName = STATE_NAMES[stateAbbrev] || toTitleCase(row.state) || row.state;

        // Transform cities_directory - slug should NOT have CITY# prefix
        // Convert UPPER city names to Title Case for display
        const directory = (row.cities_directory || []).map(c => ({
            slug: c.slug?.replace(/^CITY#/i, '') || `${stateAbbrev}-${c.name?.toLowerCase().replace(/ /g, '_')}`,
            name: toTitleCase(c.name) || c.slug,
            count: parseInt(c.count || 0)
        }));

        // Build recentByAgency structure from recentByEventSource map
        const recentByAgency = {};
        if (row.recentbyeventsource) {
            for (const [source, events] of Object.entries(row.recentbyeventsource)) {
                if (events && events.length > 0) {
                    recentByAgency[source.toUpperCase()] = transformEvents(events, 'state');
                }
            }
        }

        return {
            PK: `STATE#${stateAbbrev}`,
            SK: "SUMMARY",
            name: stateName,
            slug: stateAbbrev?.toLowerCase(),
            abbreviation: stateAbbrev,
            stats: {
                totalEvents: parseInt(row.total_events || 0),
                totalCities: parseInt(row.total_cities || 0),
                totalCompanies: parseInt(row.total_companies || 0)
            },
            directory,  // Renamed from citiesDirectory
            recentByAgency,
            updatedAt: now
        };
    }

    // === CITY ===
    if (type === "city") {
        const stateAbbrev = row.state?.toUpperCase();
        const stateName = STATE_NAMES[stateAbbrev] || toTitleCase(row.state) || row.state;

        // city_slug from Athena should be like "TN-knoxville" (no CITY# prefix)
        const citySlug = row.city_slug?.replace(/^CITY#/i, '') || '';

        // Transform companies_directory - slug should NOT have COMPANY# prefix
        // Company names stored as slugs for now, convert to readable format
        const directory = (row.companies_directory || []).map(c => ({
            slug: c.slug?.replace(/^(COMPANY|SLUG)#/i, ''),
            name: toTitleCase(c.name) || toTitleCase(c.slug),  // Convert slug to readable
            count: parseInt(c.count || 0)
        }));

        // Build recentByAgency structure from recentByEventSource map
        const recentByAgency = {};
        if (row.recentbyeventsource) {
            for (const [source, events] of Object.entries(row.recentbyeventsource)) {
                if (events && events.length > 0) {
                    recentByAgency[source.toUpperCase()] = transformEvents(events, 'city');
                }
            }
        }

        return {
            PK: `CITY#${citySlug}`,
            SK: "SUMMARY",
            name: toTitleCase(row.city) || toTitleCase(citySlug.split('-').pop()),
            slug: citySlug,
            state: stateAbbrev,
            stateName: stateName,
            stats: {
                totalEvents: parseInt(row.total_events || 0),
                totalCompanies: parseInt(row.total_companies || 0)
            },
            directory,  // Renamed from companiesDirectory
            recentByAgency,
            updatedAt: now
        };
    }

    // === COMPANY ===
    if (type === "company") {
        // Strip any prefix (COMPANY#, SLUG#, etc.)
        const companySlug = row.company_slug?.replace(/^(COMPANY|SLUG)#/i, '') || '';

        // Transform recent events (flat array for company pages)
        const recentEvents = transformEvents(row.recent_events || row.recent_events_osha || [], 'company');

        // TODO: Extract sites directory if we have site_id data
        const sites = []; // Will be populated when we have site-level data

        return {
            PK: `COMPANY#${companySlug}`,
            SK: "SUMMARY",
            name: toTitleCase(row.company_name) || toTitleCase(companySlug),
            slug: companySlug,
            stats: {
                totalEvents: parseInt(row.total_events || 0),
                lastActive: row.last_active || undefined
            },
            sites: sites.length > 0 ? sites : undefined,
            recentEvents,
            updatedAt: now
        };
    }

    // === SITE ===
    if (type === "site") {
        const siteSlug = row.site_slug || '';
        const companySlug = row.company_slug?.replace(/^(COMPANY|SLUG)#/i, '') || '';

        const recentEvents = transformEvents(row.recent_events || [], 'site');

        return {
            PK: `SITE#${companySlug}_${row.site_id}`,
            SK: "SUMMARY",
            name: `${row.company_name || companySlug} #${row.site_id}`,
            siteId: row.site_id,
            companySlug: companySlug,
            companyName: row.company_name || companySlug,
            stats: {
                totalEvents: parseInt(row.total_events || 0),
                lastActive: row.last_active || undefined
            },
            recentEvents,
            updatedAt: now
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
