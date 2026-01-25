import fs from 'fs';
import readline from 'readline';
import { DynamoDBClient, BatchWriteItemCommand } from '@aws-sdk/client-dynamodb';
import { marshall } from '@aws-sdk/util-dynamodb';

const client = new DynamoDBClient({ region: 'us-east-1' });
const TABLE_NAME = 'VentureOS-Anchor';
const BATCH_SIZE = 25;

const FILE_PATH = 'c:\\Code\\SirSluginston Co\\2024_Gaz_place_national.txt';

const clean = (val) => {
    if (val === undefined || val === null) return undefined;
    if (typeof val === 'string' && val.trim() === '') return undefined;
    return val;
};

// Helper: Extract type and clean name
// Helper: Extract type and clean name
const parseName = (rawName) => {
    // 1. Remove " (balance)" and other census artifacts
    let clean = rawName.replace(/ \(balance\)$/i, '');

    // 2. Normalize "St." -> "Saint" (Matches Processor Logic)
    clean = clean.replace(/^St\. /i, 'Saint ');
    clean = clean.replace(/^Ste\. /i, 'Sainte ');

    // 3. Extract Suffix Type
    // Matches common city types for basic splitting
    const match = clean.match(/^(.*?) (city|town|village|CDP|borough|municipality|urban county|comunidad|zona urbana|metropolitan government|unified government|consolidated government|unified.*government)$/i);

    if (match) {
        return {
            name: match[1],
            type: match[2].charAt(0).toUpperCase() + match[2].slice(1).toLowerCase()
        };
    }

    return {
        name: clean,
        type: 'Place'
    };
};

async function importCities() {
    console.log(`🌍 Starting Census Import from ${FILE_PATH}...`);

    if (!fs.existsSync(FILE_PATH)) {
        console.error(`❌ File not found: ${FILE_PATH}`);
        return;
    }

    const fileStream = fs.createReadStream(FILE_PATH);
    const rl = readline.createInterface({
        input: fileStream,
        crlfDelay: Infinity
    });

    let headers = [];
    let buffer = [];
    let seenPKs = new Set();
    let count = 0;
    let collisions = 0;

    for await (const line of rl) {
        let cleanLine = line.trim();
        if (!cleanLine) continue;

        const cols = cleanLine.split('\t').map(c => c.trim());

        if (headers.length === 0) {
            if (cols.includes('USPS') && cols.includes('NAME')) {
                headers = cols;
                console.log("✅ Headers detected:", headers);
            }
            continue;
        }

        const row = {};
        headers.forEach((h, i) => row[h] = cols[i]);

        const state = row['USPS'];
        const rawName = row['NAME'];

        if (!state || !rawName) continue;

        const { name: cleanName, type: placeType } = parseName(rawName);

        // ROBUST NORMALIZATION:
        // 1. Primary Slug (The "Correct" Full Name)
        const slug = cleanName.toLowerCase().replace(/ /g, '_');
        const pk = `CITY#${state.toUpperCase()}-${slug}`;

        if (seenPKs.has(pk)) {
            collisions++;
            continue;
        }
        seenPKs.add(pk);

        // 2. Simple Alias Generation (The "Colloquial" Name)
        // e.g. "Nashville-Davidson..." -> "Nashville"
        // e.g. "Athens-Clarke County..." -> "Athens"
        let simpleAlias = null;
        if (cleanName.includes('-') || cleanName.includes(' government') || cleanName.match(/metropolitan|unified|consolidated/i)) {
            // Aggressive Strip
            const simple = cleanName.split('-')[0]
                .replace(/ (metropolitan|unified|consolidated) government.*/i, '')
                .trim();

            if (simple && simple.length > 2 && simple !== cleanName) {
                simpleAlias = simple.toLowerCase().replace(/ /g, '_');
            }
        }

        const item = {
            PK: pk,
            SK: 'METADATA',
            place_type: placeType,
            name: clean(cleanName),
            state: clean(state),
            location: {
                lat: clean(row['INTPTLAT']),
                lon: clean(row['INTPTLONG'])
            },
            area: {
                land_sqm: clean(row['ALAND']),
                water_sqm: clean(row['AWATER']),
                land_sqmi: clean(row['ALAND_SQMI']),
                water_sqmi: clean(row['AWATER_SQMI'])
            },
            census: {
                geoid: clean(row['GEOID']),
                ansi: clean(row['ANSICODE'])
            },
            GSI1PK: `ALIAS#${slug}-${state.toLowerCase()}`,
            GSI1SK: pk
        };

        buffer.push({
            PutRequest: { Item: marshall(item, { removeUndefinedValues: true }) }
        });

        // 3. Insert Explicit Alias Item if simplified
        // 3. Insert Explicit Alias Item if simplified
        if (simpleAlias) {
            // FIXED: User requested format "TN-nashville" (CAPS STATE - lower city)
            const aliasSlug = `${state.toUpperCase()}-${simpleAlias}`;
            const aliasItem = {
                PK: `ALIAS#${aliasSlug}`, // Global unique alias
                SK: 'POINTER',
                GSI1PK: `ALIAS#${aliasSlug}`,
                GSI1SK: pk, // Points to the REAL key
                target: pk
            };
            buffer.push({
                PutRequest: { Item: marshall(aliasItem) }
            });
        }



        if (buffer.length === BATCH_SIZE) {
            await writeBatch(buffer);
            count += buffer.length;
            process.stdout.write(`\r✅ Imported: ${count} cities (Collisions: ${collisions})...`);
            buffer = [];
        }
    }

    if (buffer.length > 0) {
        await writeBatch(buffer);
        count += buffer.length;
    }

    console.log(`\n🎉 Import Complete! Total Cities: ${count}. Skipped Collisions: ${collisions}`);
}

async function writeBatch(items) {
    try {
        await client.send(new BatchWriteItemCommand({
            RequestItems: {
                [TABLE_NAME]: items
            }
        }));
    } catch (e) {
        console.error("\n❌ Batch Write Failed:", e.message);
    }
}

importCities();
