import { DynamoDBClient, BatchWriteItemCommand } from '@aws-sdk/client-dynamodb';
import { marshall } from '@aws-sdk/util-dynamodb';

const client = new DynamoDBClient({ region: 'us-east-1' });
const TABLE_NAME = 'VentureOS-Anchor';

// Comprehensive List of US States and Territories
const STATES = [
    { abbr: "AL", name: "Alabama" },
    { abbr: "AK", name: "Alaska" },
    { abbr: "AZ", name: "Arizona" },
    { abbr: "AR", name: "Arkansas" },
    { abbr: "CA", name: "California" },
    { abbr: "CO", name: "Colorado" },
    { abbr: "CT", name: "Connecticut" },
    { abbr: "DE", name: "Delaware" },
    { abbr: "DC", name: "District of Columbia" },
    { abbr: "FL", name: "Florida" },
    { abbr: "GA", name: "Georgia" },
    { abbr: "HI", name: "Hawaii" },
    { abbr: "ID", name: "Idaho" },
    { abbr: "IL", name: "Illinois" },
    { abbr: "IN", name: "Indiana" },
    { abbr: "IA", name: "Iowa" },
    { abbr: "KS", name: "Kansas" },
    { abbr: "KY", name: "Kentucky" },
    { abbr: "LA", name: "Louisiana" },
    { abbr: "ME", name: "Maine" },
    { abbr: "MD", name: "Maryland" },
    { abbr: "MA", name: "Massachusetts" },
    { abbr: "MI", name: "Michigan" },
    { abbr: "MN", name: "Minnesota" },
    { abbr: "MS", name: "Mississippi" },
    { abbr: "MO", name: "Missouri" },
    { abbr: "MT", name: "Montana" },
    { abbr: "NE", name: "Nebraska" },
    { abbr: "NV", name: "Nevada" },
    { abbr: "NH", name: "New Hampshire" },
    { abbr: "NJ", name: "New Jersey" },
    { abbr: "NM", name: "New Mexico" },
    { abbr: "NY", name: "New York" },
    { abbr: "NC", name: "North Carolina" },
    { abbr: "ND", name: "North Dakota" },
    { abbr: "OH", name: "Ohio" },
    { abbr: "OK", name: "Oklahoma" },
    { abbr: "OR", name: "Oregon" },
    { abbr: "PA", name: "Pennsylvania" },
    { abbr: "RI", name: "Rhode Island" },
    { abbr: "SC", name: "South Carolina" },
    { abbr: "SD", name: "South Dakota" },
    { abbr: "TN", name: "Tennessee" },
    { abbr: "TX", name: "Texas" },
    { abbr: "UT", name: "Utah" },
    { abbr: "VT", name: "Vermont" },
    { abbr: "VA", name: "Virginia" },
    { abbr: "WA", name: "Washington" },
    { abbr: "WV", name: "West Virginia" },
    { abbr: "WI", name: "Wisconsin" },
    { abbr: "WY", name: "Wyoming" },
    // Territories
    { abbr: "PR", name: "Puerto Rico" },
    { abbr: "GU", name: "Guam" },
    { abbr: "VI", name: "Virgin Islands" },
    { abbr: "AS", name: "American Samoa" },
    { abbr: "MP", name: "Northern Mariana Islands" }
];

async function seed() {
    console.log(`⚓ Seeding States into Anchor (${TABLE_NAME})...`);
    
    const writeRequests = [];
    
    for (const state of STATES) {
        const pk = `STATE#${state.abbr}`;
        const item = {
            PK: pk,
            SK: 'METADATA',
            name: state.name,
            abbr: state.abbr
        };
        
        writeRequests.push({
            PutRequest: { Item: marshall(item) }
        });

        const aliasPk = `ALIAS#${state.name.toLowerCase().replace(/ /g, '_')}`;
        const aliasItem = {
            PK: aliasPk,
            SK: pk,
            GSI1PK: aliasPk,
            GSI1SK: pk,
            // type: 'Alias', // Keep Type on Alias? Or is PK prefix enough?
            // Let's keep Type on Alias for clarity if we query GSI by Alias Type? 
            // Actually, if we query ALIAS#..., we know it's an alias.
            // Removing type here too for consistency.
            target: pk
        };
        writeRequests.push({ PutRequest: { Item: marshall(aliasItem) } });
    }
    
    // Batch Write
    const chunks = [];
    while (writeRequests.length > 0) {
        chunks.push(writeRequests.splice(0, 25));
    }
    
    console.log(`Writing ${chunks.length} batches...`);
    
    for (const batch of chunks) {
        try {
            await client.send(new BatchWriteItemCommand({
                RequestItems: {
                    [TABLE_NAME]: batch
                }
            }));
            process.stdout.write('.');
        } catch (err) {
            console.error('\n❌ Batch Failed:', err);
        }
    }
    
    console.log('\n✅ Done.');
}

seed();
