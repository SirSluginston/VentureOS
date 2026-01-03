// Company Stats Management
// Handles reading/writing company stats to DynamoDB

import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, GetCommand, PutCommand } from '@aws-sdk/lib-dynamodb';
import { estimateItemSize } from './dynamoSizeHelper.js';

const TABLE_NAME = process.env.DYNAMODB_TABLE || 'SirSluginstonVentureOS';

const dynamoClient = new DynamoDBClient({ region: process.env.AWS_REGION || 'us-east-1' });
const docClient = DynamoDBDocumentClient.from(dynamoClient);

/**
 * Update company stats in DynamoDB
 * @param {string} companySlug - Company slug (e.g., 'fedex-corporation')
 * @param {Object} stats - Stats object
 * @param {string} brandPk - Brand primary key (e.g., 'BRAND#OSHAtrail')
 */
export async function updateCompanyStats(companySlug, stats, brandPk) {
  const item = {
    pk: brandPk,
    sk: `COMPANY_STATS#${companySlug}`,
    ...stats,
  };
  
  // Check size before writing
  const sizeBytes = estimateItemSize(item);
  const sizeKB = sizeBytes / 1024;
  
  if (sizeBytes > 400 * 1024) {
    throw new Error(`Company stats for ${companySlug} exceed 400KB DynamoDB limit (${sizeKB.toFixed(1)}KB).`);
  }
  
  await docClient.send(new PutCommand({
    TableName: TABLE_NAME,
    Item: item,
  }));
  
  console.log(`[OK] Updated company stats: ${companySlug} for ${brandPk} (${sizeKB.toFixed(1)}KB)`);
}

/**
 * Get company stats from DynamoDB
 * @param {string} companySlug - Company slug (e.g., 'fedex-corporation')
 * @param {string} brandPk - Brand primary key (e.g., 'BRAND#OSHAtrail')
 */
export async function getCompanyStats(companySlug, brandPk) {
  const result = await docClient.send(new GetCommand({
    TableName: TABLE_NAME,
    Key: {
      pk: brandPk,
      sk: `COMPANY_STATS#${companySlug}`,
    },
  }));
  
  return result.Item || null;
}


