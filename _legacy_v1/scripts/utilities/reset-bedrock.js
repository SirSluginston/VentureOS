import { DynamoDBClient, ScanCommand, UpdateItemCommand } from '@aws-sdk/client-dynamodb';

const client = new DynamoDBClient({ region: 'us-east-1' });
const TABLE_NAME = 'VentureOS-Violations';

async function resetBedrockContent() {
  console.log('🔍 Scanning for items with bedrock content...');
  
  let items = [];
  let lastEvaluatedKey = null;
  
  do {
    const command = new ScanCommand({
      TableName: TABLE_NAME,
      FilterExpression: 'attribute_exists(bedrock_title)',
      ProjectionExpression: 'PK, SK',
      ExclusiveStartKey: lastEvaluatedKey
    });
    
    const response = await client.send(command);
    if (response.Items) {
      items.push(...response.Items);
    }
    lastEvaluatedKey = response.LastEvaluatedKey;
  } while (lastEvaluatedKey);
  
  console.log(`📋 Found ${items.length} items to reset.`);
  
  for (const item of items) {
    console.log(`🗑️ Resetting ${item.PK.S} / ${item.SK.S}...`);
    try {
      await client.send(new UpdateItemCommand({
        TableName: TABLE_NAME,
        Key: {
          PK: item.PK,
          SK: item.SK
        },
        UpdateExpression: 'REMOVE bedrock_title, bedrock_description, attribution, tags, generated_at, title_bedrock, description_bedrock'
      }));
    } catch (e) {
      console.error(`❌ Failed to reset ${item.PK.S}: ${e.message}`);
    }
  }
  
  console.log('✅ Reset complete.');
}

resetBedrockContent().catch(console.error);

