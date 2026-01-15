// National Stats Management
// Handles reading/writing national stats to DynamoDB

import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, GetCommand, PutCommand } from '@aws-sdk/lib-dynamodb';
import { estimateItemSize } from './dynamoSizeHelper.js';

const TABLE_NAME = process.env.DYNAMODB_TABLE || 'SirSluginstonVentureOS';

const dynamoClient = new DynamoDBClient({ region: process.env.AWS_REGION || 'us-east-1' });
const docClient = DynamoDBDocumentClient.from(dynamoClient);

/**
 * Update national stats in DynamoDB
 * National stats only contain aggregates (no companyStats) to avoid 400KB limit
 * @param {string} country - Country code (e.g., 'USA')
 * @param {Object} stats - Stats object (should not include companyStats)
 * @param {string} brandPk - Brand primary key (e.g., 'BRAND#OSHAtrail')
 */
export async function updateNationalStats(country, stats, brandPk) {
  const item = {
    pk: brandPk,
    sk: `NATIONAL_STATS#${country}`,
    ...stats,
  };
  
  // Verify size (should be small without companyStats)
  const sizeBytes = estimateItemSize(item);
  const sizeKB = (sizeBytes / 1024).toFixed(1);
  
  if (sizeBytes > 400 * 1024) {
    console.warn(`[WARN] National ${country} stats (${sizeKB}KB) exceeds 400KB limit. This should not happen without companyStats.`);
  }
  
  await docClient.send(new PutCommand({
    TableName: TABLE_NAME,
    Item: item,
  }));
  
  console.log(`[OK] Updated national stats: ${country} for ${brandPk} (${sizeKB}KB)`);
}

/**
 * Get national stats from DynamoDB
 * @param {string} country - Country code (e.g., 'USA')
 * @param {string} brandPk - Brand primary key (e.g., 'BRAND#OSHAtrail')
 */
export async function getNationalStats(country, brandPk) {
  const result = await docClient.send(new GetCommand({
    TableName: TABLE_NAME,
    Key: {
      pk: brandPk,
      sk: `NATIONAL_STATS#${country}`,
    },
  }));
  
  return result.Item || null;
}

