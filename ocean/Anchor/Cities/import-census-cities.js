import fs from 'fs';
import readline from 'readline';
import { DynamoDBClient, BatchWriteItemCommand } from '@aws-sdk/client-dynamodb';
import { marshall } from '@aws-sdk/util-dynamodb';

const client = new DynamoDBClient({ region: 'us-east-1' });
const TABLE_NAME = 'VentureOS-Anchor';
const BATCH_SIZE = 25; 

const FILE_PATH = 'VentureOS/infrastructure/Census Cities/2024_places.txt';

const clean = (val) => {
    if (val === undefined || val === null) return undefined;
    if (typeof val === 'string' && val.trim() === '') return undefined;
    return val;
};

// Helper: Extract type and clean name
const parseName = (rawName) => {
    const match = rawName.match(/^(.*?) (city|town|village|CDP|borough)$/i);
    
    if (match) {
        return {
            name: match[1], 
            type: match[2].charAt(0).toUpperCase() + match[2].slice(1).toLowerCase() 
        };
    }
    
    return {
        name: rawName,
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
        
        const slug = cleanName.toLowerCase().replace(/ /g, '_');
        const pk = `CITY#${state.toUpperCase()}-${slug}`;
        
        if (seenPKs.has(pk)) {
            collisions++;
            continue;
        }
        seenPKs.add(pk);

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
