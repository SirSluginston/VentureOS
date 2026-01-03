// City Stats Management
// Handles reading/writing city stats to DynamoDB

import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, GetCommand, PutCommand } from '@aws-sdk/lib-dynamodb';
import { estimateItemSize } from './dynamoSizeHelper.js';

const TABLE_NAME = process.env.DYNAMODB_TABLE || 'SirSluginstonVentureOS';

const dynamoClient = new DynamoDBClient({ region: process.env.AWS_REGION || 'us-east-1' });
const docClient = DynamoDBDocumentClient.from(dynamoClient);

/**
 * Update city stats in DynamoDB
 * Checks size before writing and warns if approaching 400KB limit
 * @param {string} citySlug - City slug (e.g., 'Austin-TX')
 * @param {Object} stats - Stats object
 * @param {string} brandPk - Brand primary key (e.g., 'BRAND#OSHAtrail')
 */
export async function updateCityStats(citySlug, stats, brandPk) {
  const item = {
    pk: brandPk,
    sk: `CITY_STATS#${citySlug}`,
    ...stats,
  };
  
  // Check size before writing
  const sizeBytes = estimateItemSize(item);
  const sizeKB = sizeBytes / 1024;
  
  if (sizeBytes > 400 * 1024) {
    // Exceeded hard limit - this will fail on write
    throw new Error(`City stats for ${citySlug} exceed 400KB DynamoDB limit (${sizeKB.toFixed(1)}KB). Data too large to write.`);
  } else if (sizeKB >= 350) {
    // Approaching limit - warn but still write
    console.warn(`[WARN] City stats for ${citySlug} approaching 400KB limit (${sizeKB.toFixed(1)}KB)`);
  }
  
  await docClient.send(new PutCommand({
    TableName: TABLE_NAME,
    Item: item,
  }));
  
  console.log(`[OK] Updated city stats: ${citySlug} for ${brandPk} (${sizeKB.toFixed(1)}KB)`);
}

/**
 * Get city stats from DynamoDB
 * @param {string} citySlug - City slug (e.g., 'Austin-TX')
 * @param {string} brandPk - Brand primary key (e.g., 'BRAND#OSHAtrail')
 */
export async function getCityStats(citySlug, brandPk) {
  const result = await docClient.send(new GetCommand({
    TableName: TABLE_NAME,
    Key: {
      pk: brandPk,
      sk: `CITY_STATS#${citySlug}`,
    },
  }));
  
  return result.Item || null;
}

