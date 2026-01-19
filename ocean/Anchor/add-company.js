/**
 * Add a company to the Anchor table
 * 
 * Usage: node add-company.js "Walmart Inc" "walmart-inc"
 * 
 * This creates:
 * - Canonical entry: PK=COMPANY#walmart-inc, SK=CANONICAL
 * - Alias entry for lookup: GSI1PK=ALIAS#walmart inc, GSI1SK=COMPANY#walmart-inc
 */

import { DynamoDBClient, PutItemCommand, BatchWriteItemCommand } from "@aws-sdk/client-dynamodb";
import { marshall } from "@aws-sdk/util-dynamodb";

const dynamo = new DynamoDBClient({});
const ANCHOR_TABLE = "VentureOS-Anchor";

async function addCompany(companyName, slug, additionalAliases = []) {
    const canonicalSlug = slug.toLowerCase().trim();
    const primaryAlias = companyName.toLowerCase().trim();
    
    const items = [];
    
    // 1. Canonical entry
    items.push({
        PutRequest: {
            Item: marshall({
                PK: `COMPANY#${canonicalSlug}`,
                SK: "CANONICAL",
                name: companyName,
                slug: canonicalSlug,
                createdAt: new Date().toISOString(),
            })
        }
    });
    
    // 2. Primary alias (the company name itself)
    items.push({
        PutRequest: {
            Item: marshall({
                PK: `ALIAS#${primaryAlias}`,
                SK: `COMPANY#${canonicalSlug}`,
                GSI1PK: `ALIAS#${primaryAlias}`,
                GSI1SK: `COMPANY#${canonicalSlug}`,
            })
        }
    });
    
    // 3. Additional aliases (variations, abbreviations, etc.)
    for (const alias of additionalAliases) {
        const aliasKey = alias.toLowerCase().trim();
        items.push({
            PutRequest: {
                Item: marshall({
                    PK: `ALIAS#${aliasKey}`,
                    SK: `COMPANY#${canonicalSlug}`,
                    GSI1PK: `ALIAS#${aliasKey}`,
                    GSI1SK: `COMPANY#${canonicalSlug}`,
                })
            }
        });
    }
    
    // Batch write (max 25 items per batch)
    for (let i = 0; i < items.length; i += 25) {
        const batch = items.slice(i, i + 25);
        await dynamo.send(new BatchWriteItemCommand({
            RequestItems: {
                [ANCHOR_TABLE]: batch
            }
        }));
    }
    
    console.log(`✅ Added company: "${companyName}" → ${canonicalSlug}`);
    console.log(`   Aliases: ${[primaryAlias, ...additionalAliases].join(', ')}`);
}

// CLI usage
const args = process.argv.slice(2);
if (args.length < 2) {
    console.log('Usage: node add-company.js "Company Name" "company-slug" ["alias1" "alias2" ...]');
    console.log('Example: node add-company.js "Walmart Inc" "walmart-inc" "walmart" "wal-mart"');
    process.exit(1);
}

const [companyName, slug, ...aliases] = args;
addCompany(companyName, slug, aliases);


