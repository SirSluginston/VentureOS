// State Stats Management
// Handles reading/writing state stats to DynamoDB

import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, GetCommand, PutCommand } from '@aws-sdk/lib-dynamodb';
import { estimateItemSize } from './dynamoSizeHelper.js';

const TABLE_NAME = process.env.DYNAMODB_TABLE || 'SirSluginstonVentureOS';

const dynamoClient = new DynamoDBClient({ region: process.env.AWS_REGION || 'us-east-1' });
const docClient = DynamoDBDocumentClient.from(dynamoClient);

/**
 * Update state stats in DynamoDB
 * State stats only contain aggregates (no companyStats) to avoid 400KB limit
 * @param {string} state - State abbreviation (e.g., 'TX')
 * @param {Object} stats - Stats object (should not include companyStats)
 * @param {string} brandPk - Brand primary key (e.g., 'BRAND#OSHAtrail')
 */
export async function updateStateStats(state, stats, brandPk) {
  const item = {
    pk: brandPk,
    sk: `STATE_STATS#${state}`,
    ...stats,
  };
  
  // Verify size (should be small without companyStats)
  const sizeBytes = estimateItemSize(item);
  const sizeKB = (sizeBytes / 1024).toFixed(1);
  
  if (sizeBytes > 400 * 1024) {
    console.warn(`[WARN] State ${state} stats (${sizeKB}KB) exceeds 400KB limit. This should not happen without companyStats.`);
  }
  
  await docClient.send(new PutCommand({
    TableName: TABLE_NAME,
    Item: item,
  }));
  
  console.log(`[OK] Updated state stats: ${state} for ${brandPk} (${sizeKB}KB)`);
}

/**
 * Get state stats from DynamoDB
 * @param {string} state - State abbreviation (e.g., 'TX')
 * @param {string} brandPk - Brand primary key (e.g., 'BRAND#OSHAtrail')
 */
export async function getStateStats(state, brandPk) {
  const result = await docClient.send(new GetCommand({
    TableName: TABLE_NAME,
    Key: {
      pk: brandPk,
      sk: `STATE_STATS#${state}`,
    },
  }));
  
  return result.Item || null;
}

