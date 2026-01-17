import fs from 'fs';
import readline from 'readline';
import { DynamoDBClient, BatchWriteItemCommand } from '@aws-sdk/client-dynamodb';
import { marshall } from '@aws-sdk/util-dynamodb';

const client = new DynamoDBClient({ region: 'us-east-1' });
const TABLE_NAME = 'VentureOS-Anchor';

const FILE_PATH = 'VentureOS/ocean/Anchor/Companies/nasdaq_screener.csv';

const clean = (val) => {
    if (val === undefined || val === null) return undefined;
    if (typeof val === 'string' && val.trim() === '') return undefined;
    return val.trim();
};

const cleanCompanyName = (rawName) => {
    let name = rawName;
    const suffixes = [
        " Common Stock", " Class A", " Class B", " Class C",
        " Ordinary Shares", " American Depositary Shares", " Depositary Shares",
        " Units", " Rights", " Warrants", " Preferred Stock",
        " Series A", " Series B", " Series C"
    ];
    suffixes.forEach(suffix => {
        const regex = new RegExp(`${suffix}`, 'i');
        name = name.replace(regex, '');
    });
    return name.trim();
};

const extractShareType = (rawName) => {
    const shareTypes = [
        "Common Stock", "Class A", "Class B", "Class C",
        "Ordinary Shares", "American Depositary Shares", "Depositary Shares",
        "Units", "Rights", "Warrants", "Preferred Stock"
    ];
    for (const type of shareTypes) {
        if (rawName.includes(type)) return type;
    }
    return "Common Stock"; 
};

const slugify = (text) => {
    return text.toString().toLowerCase()
        .replace(/\s+/g, '-')           
        .replace(/[^\w\-]+/g, '')       
        .replace(/\-\-+/g, '-')         
        .replace(/^-+/, '')             
        .replace(/-+$/, '');            
};

const parseCSVLine = (line) => {
    const result = [];
    let current = '';
    let inQuote = false;
    
    for (let i = 0; i < line.length; i++) {
        const char = line[i];
        
        if (char === '"') {
            inQuote = !inQuote;
        } else if (char === ',' && !inQuote) {
            result.push(current);
            current = '';
        } else {
            current += char;
        }
    }
    result.push(current); 
    return result.map(c => c.trim().replace(/^"|"$/g, '').replace(/""/g, '"'));
};

async function importCompanies() {
    console.log(`⚓ Starting Company Import from ${FILE_PATH}...`);

    if (!fs.existsSync(FILE_PATH)) {
        console.error(`❌ File not found: ${FILE_PATH}`);
        return;
    }

    const fileStream = fs.createReadStream(FILE_PATH);
    const rl = readline.createInterface({
        input: fileStream,
        crlfDelay: Infinity
    });

    const companyMap = new Map();
    let headers = [];
    let lineCount = 0;
    let skippedCount = 0;

    for await (const line of rl) {
        lineCount++;
        let cleanLine = line;
        if (lineCount === 1) cleanLine = cleanLine.replace(/^\uFEFF/, '');
        
        if (!cleanLine.trim()) continue;

        const values = parseCSVLine(cleanLine);

        if (lineCount === 1) {
            headers = values.map(h => h.trim().toLowerCase().replace(/ /g, ''));
            console.log("Headers detected:", headers);
            continue;
        }

        if (values.length < 2) {
            skippedCount++;
            continue;
        }

        let idxSymbol = headers.indexOf('symbol');
        let idxName = headers.indexOf('name');
        let idxLastSale = headers.indexOf('lastsale');
        let idxNetChange = headers.indexOf('netchange');
        let idxPctChange = headers.indexOf('%change');
        let idxMarketCap = headers.indexOf('marketcap');
        let idxCountry = headers.indexOf('country');
        let idxIpoYear = headers.indexOf('ipoyear');
        let idxVolume = headers.indexOf('volume');
        let idxSector = headers.indexOf('sector');
        let idxIndustry = headers.indexOf('industry');

        const symbol = values[idxSymbol]?.trim();
        const rawName = values[idxName]?.trim();
        
        if (!symbol || !rawName) {
            skippedCount++;
            continue; 
        }

        const cleanName = cleanCompanyName(rawName);
        const slug = slugify(cleanName);
        const shareType = extractShareType(rawName);

        if (!companyMap.has(slug)) {
            companyMap.set(slug, {
                name: cleanName,
                slug: slug,
                sector: clean(values[idxSector]),
                industry: clean(values[idxIndustry]),
                country: clean(values[idxCountry]),
                market_cap: clean(values[idxMarketCap]),
                ipo_year: clean(values[idxIpoYear]),
                tickers: {} 
            });
        }

        const company = companyMap.get(slug);
        
        company.tickers[symbol] = {
            type: shareType,
            raw_name: rawName,
            price: clean(values[idxLastSale]),
            net_change: clean(values[idxNetChange]),
            pct_change: clean(values[idxPctChange]),
            volume: clean(values[idxVolume]),
            market_cap: clean(values[idxMarketCap]) 
        };
        
        if (!company.sector && values[idxSector]) company.sector = clean(values[idxSector]);
        if (!company.market_cap && values[idxMarketCap]) company.market_cap = clean(values[idxMarketCap]);
    }

    console.log(`📊 Aggregated ${companyMap.size} unique companies from ${lineCount} rows. (Skipped: ${skippedCount})`);

    // 2. Build Request List
    const allRequests = [];

    for (const [slug, co] of companyMap) {
        const pk = `SLUG#${slug}`;
        
        const mainItem = {
            PK: pk,
            SK: 'METADATA',
            type: 'Company',
            name: co.name,
            sector: co.sector,
            industry: co.industry,
            country: co.country,
            market_cap: co.market_cap,
            ipo_year: co.ipo_year,
            tickers: co.tickers, 
            primary_ticker: Object.keys(co.tickers)[0] 
        };

        allRequests.push({
            PutRequest: { Item: marshall(mainItem, { removeUndefinedValues: true }) }
        });

        for (const [symbol, info] of Object.entries(co.tickers)) {
            const aliasItem = {
                PK: `ALIAS#${symbol.toLowerCase()}`,
                SK: pk,
                GSI1PK: `ALIAS#${symbol.toLowerCase()}`,
                GSI1SK: pk,
                type: 'Alias',
                target: pk,
                ticker: symbol
            };
            allRequests.push({ PutRequest: { Item: marshall(aliasItem) } });
        }
        
        if (slugify(co.name) !== slug) {
             const aliasName = {
                PK: `ALIAS#${slugify(co.name)}`,
                SK: pk,
                GSI1PK: `ALIAS#${slugify(co.name)}`,
                GSI1SK: pk,
                type: 'Alias',
                target: pk
            };
            allRequests.push({ PutRequest: { Item: marshall(aliasName) } });
        }
    }

    console.log(`📦 Generated ${allRequests.length} writes. Sending to DynamoDB...`);

    // 3. Batch Write (Chunked)
    let processed = 0;
    while (allRequests.length > 0) {
        const batch = allRequests.splice(0, 25); // Exactly 25 or less
        try {
            await client.send(new BatchWriteItemCommand({
                RequestItems: { [TABLE_NAME]: batch }
            }));
            processed += batch.length;
            if (processed % 1000 === 0) process.stdout.write(`\r${processed} items written...`);
        } catch (err) {
            console.error(`\n❌ Batch Failed: ${err.message}`);
            // If failed, we lose these 25 items. In prod, we'd retry unprocessed items.
        }
    }

    console.log(`\n✅ Import Complete! Total Items Written: ${processed}`);
}

importCompanies();
