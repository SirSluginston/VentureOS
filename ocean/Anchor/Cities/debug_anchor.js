import { DynamoDBClient, QueryCommand, GetItemCommand } from "@aws-sdk/client-dynamodb";
import { marshall, unmarshall } from "@aws-sdk/util-dynamodb";

const client = new DynamoDBClient({ region: "us-east-1" });
const TABLE = "VentureOS-Anchor";

async function check() {
    console.log("🔍 Checking Anchor DB...");

    // 1. Check Nashville Alias
    console.log("\n--- Checking Alias: nashville-tn ---");
    try {
        const res = await client.send(new QueryCommand({
            TableName: TABLE,
            IndexName: 'GSI1',
            KeyConditionExpression: 'GSI1PK = :alias',
            ExpressionAttributeValues: marshall({ ':alias': 'ALIAS#nashville-tn' })
        }));
        console.log("Nashville Alias:", res.Items.map(i => unmarshall(i)));
    } catch (e) {
        console.error("Query Failed:", e.message);
    }

    // 2. Check Castleton Direct
    console.log("\n--- Checking Direct: CITY#NY-castleton ---");
    try {
        const res = await client.send(new GetItemCommand({
            TableName: TABLE,
            Key: marshall({ PK: 'CITY#NY-castleton', SK: 'METADATA' })
        }));
        console.log("Castleton Item:", res.Item ? unmarshall(res.Item) : "NOT FOUND");
    } catch (e) {
        console.error("GetItem Failed:", e.message);
    }
}

check();
