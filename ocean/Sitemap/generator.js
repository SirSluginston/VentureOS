import { DynamoDBClient, ScanCommand } from "@aws-sdk/client-dynamodb";
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";

const dynamo = new DynamoDBClient({});
const s3 = new S3Client({});

const LIGHTHOUSE_TABLE = process.env.LIGHTHOUSE_TABLE || "VentureOS-Lighthouse";
const SITE_BUCKET = process.env.SITE_BUCKET || "oshatrail-site";
const BASE_URL = process.env.BASE_URL || "https://oshatrail.com";

// Google limits: 50k URLs or 50MB per sitemap file
// Using 25k to stay well under 50MB with long company/city slugs
const MAX_URLS_PER_SITEMAP = 25000;

// Set to small number for testing, null for production
const TEST_LIMIT = process.env.TEST_MODE === "true" ? 100 : null;

export const handler = async () => {
    console.log("🗺️ Sitemap Generator: Starting...");
    console.log(`Mode: ${TEST_LIMIT ? `TEST (limit ${TEST_LIMIT})` : "PRODUCTION"}`);

    // Stream-based sitemap generation
    const sitemapWriters = {
        states: new SitemapWriter("sitemaps/states", "state"),
        cities: new SitemapWriter("sitemaps/cities", "city"),
        companies: new SitemapWriter("sitemaps/companies", "company"),
        sites: new SitemapWriter("sitemaps/sites", "site")
    };

    // Scan Lighthouse table in pages
    let lastKey;
    let scannedCount = 0;
    
    do {
        const scanParams = {
            TableName: LIGHTHOUSE_TABLE,
            ProjectionExpression: "PK",
            FilterExpression: "SK = :sum",
            ExpressionAttributeValues: { ":sum": { S: "SUMMARY" } },
            ExclusiveStartKey: lastKey
        };
        
        // Add limit for testing
        if (TEST_LIMIT) {
            scanParams.Limit = TEST_LIMIT;
        }

        const res = await dynamo.send(new ScanCommand(scanParams));
        
        // Process each item immediately (stream-like)
        for (const item of res.Items) {
            const pk = item.PK.S;
            
            if (pk.startsWith("NATION#")) {
                // Nation is just one URL, handle separately
            } else if (pk.startsWith("STATE#")) {
                await sitemapWriters.states.addUrl(`${BASE_URL}/state/${pk.replace("STATE#", "")}`);
            } else if (pk.startsWith("CITY#")) {
                await sitemapWriters.cities.addUrl(`${BASE_URL}/city/${pk.replace("CITY#", "")}`);
            } else if (pk.startsWith("COMPANY#")) {
                await sitemapWriters.companies.addUrl(`${BASE_URL}/company/${pk.replace("COMPANY#", "")}`);
            } else if (pk.startsWith("SITE#")) {
                await sitemapWriters.sites.addUrl(`${BASE_URL}/site/${pk.replace("SITE#", "")}`);
            }
        }

        scannedCount += res.Items.length;
        lastKey = res.LastEvaluatedKey;
        console.log(`Scanned ${scannedCount} items...`);
        
        // Exit early in test mode
        if (TEST_LIMIT && scannedCount >= TEST_LIMIT) break;
        
    } while (lastKey);

    // Flush any remaining URLs in buffers
    const allSitemapFiles = [];
    for (const [name, writer] of Object.entries(sitemapWriters)) {
        const files = await writer.flush();
        allSitemapFiles.push(...files);
        console.log(`${name}: ${writer.totalUrls} URLs in ${files.length} file(s)`);
    }

    // Generate sitemap index
    await uploadSitemapIndex(allSitemapFiles);

    const summary = {
        scannedItems: scannedCount,
        states: sitemapWriters.states.totalUrls,
        cities: sitemapWriters.cities.totalUrls,
        companies: sitemapWriters.companies.totalUrls,
        sites: sitemapWriters.sites.totalUrls,
        sitemapFiles: allSitemapFiles.length + 1 // +1 for index
    };

    console.log("✅ Sitemap generation complete:", summary);
    return { statusCode: 200, body: JSON.stringify(summary) };
};

/**
 * Stream-based sitemap writer that flushes to S3 every 50k URLs
 */
class SitemapWriter {
    constructor(prefix, type) {
        this.prefix = prefix;
        this.type = type;
        this.buffer = [];
        this.fileCount = 0;
        this.totalUrls = 0;
        this.uploadedFiles = [];
    }

    async addUrl(url) {
        this.buffer.push(url);
        this.totalUrls++;

        // Flush when buffer hits limit
        if (this.buffer.length >= MAX_URLS_PER_SITEMAP) {
            await this.flushBuffer();
        }
    }

    async flushBuffer() {
        if (this.buffer.length === 0) return;

        this.fileCount++;
        const key = this.fileCount === 1 && this.totalUrls <= MAX_URLS_PER_SITEMAP
            ? `${this.prefix}.xml`
            : `${this.prefix}-${this.fileCount}.xml`;

        const xml = generateSitemapXml(this.buffer);
        
        await s3.send(new PutObjectCommand({
            Bucket: SITE_BUCKET,
            Key: key,
            Body: xml,
            ContentType: "application/xml",
            CacheControl: "max-age=86400"
        }));

        console.log(`Uploaded ${key} with ${this.buffer.length} URLs`);
        this.uploadedFiles.push(key);
        this.buffer = []; // Clear buffer
    }

    async flush() {
        // Flush any remaining URLs
        await this.flushBuffer();
        
        // If we had multiple files but named the first one without a number, rename logic
        // Actually, let's just use numbered files if there's more than one flush
        return this.uploadedFiles;
    }
}

function generateSitemapXml(urls) {
    const today = new Date().toISOString().split('T')[0];
    return `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
${urls.map(url => `<url><loc>${url}</loc><lastmod>${today}</lastmod><changefreq>weekly</changefreq></url>`).join("\n")}
</urlset>`;
}

async function uploadSitemapIndex(sitemapFiles) {
    const today = new Date().toISOString().split('T')[0];
    
    // Add static pages to index
    const allFiles = [
        ...sitemapFiles
    ];

    const indexXml = `<?xml version="1.0" encoding="UTF-8"?>
<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
${allFiles.map(file => `<sitemap><loc>${BASE_URL}/${file}</loc><lastmod>${today}</lastmod></sitemap>`).join("\n")}
</sitemapindex>`;

    await s3.send(new PutObjectCommand({
        Bucket: SITE_BUCKET,
        Key: "sitemap.xml",
        Body: indexXml,
        ContentType: "application/xml",
        CacheControl: "max-age=86400"
    }));

    console.log(`Uploaded sitemap.xml index with ${allFiles.length} sitemaps`);
}
