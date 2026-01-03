// Daily Log Utility
// Stores sync/rebuild status logs for admin monitoring
// Logs are stored in S3 as JSON files, one per day

import { S3Client, GetObjectCommand, PutObjectCommand } from '@aws-sdk/client-s3';

const S3_BUCKET = process.env.S3_BUCKET || 'sirsluginston-ventureos-data';
const s3Client = new S3Client({ region: process.env.AWS_REGION || 'us-east-1' });

/**
 * Get today's date string (YYYY-MM-DD)
 */
function getTodayString() {
  return new Date().toISOString().split('T')[0];
}

/**
 * Get log key for a specific date
 * @param {string} dateString - Date string (YYYY-MM-DD), defaults to today
 * @returns {string} S3 key for the log file
 */
function getLogKey(dateString = null) {
  const date = dateString || getTodayString();
  return `_logs/daily/${date}.json`;
}

/**
 * Get daily log
 * @param {string} dateString - Date string (YYYY-MM-DD), defaults to today
 * @returns {Promise<Object>} Log object
 */
export async function getDailyLog(dateString = null) {
  const logKey = getLogKey(dateString);
  
  try {
    const response = await s3Client.send(new GetObjectCommand({
      Bucket: S3_BUCKET,
      Key: logKey,
    }));
    
    const body = await response.Body.transformToString();
    return JSON.parse(body);
  } catch (error) {
    if (error.name === 'NoSuchKey' || error.$metadata?.httpStatusCode === 404) {
      // Return empty log if doesn't exist
      return {
        date: dateString || getTodayString(),
        syncs: {},
        rebuilds: {},
        lastUpdated: new Date().toISOString(),
      };
    }
    throw error;
  }
}

/**
 * Append entry to daily log
 * @param {string} type - 'sync' or 'rebuild'
 * @param {string} brandPk - Brand primary key
 * @param {Object} entry - Log entry object
 * @returns {Promise<void>}
 */
export async function appendDailyLog(type, brandPk, entry) {
  const log = await getDailyLog();
  
  // Initialize structure if needed
  if (!log[`${type}s`]) {
    log[`${type}s`] = {};
  }
  
  if (!log[`${type}s`][brandPk]) {
    log[`${type}s`][brandPk] = [];
  }
  
  // Add entry with timestamp
  const logEntry = {
    ...entry,
    timestamp: new Date().toISOString(),
  };
  
  log[`${type}s`][brandPk].push(logEntry);
  log.lastUpdated = new Date().toISOString();
  
  // Write back to S3
  const logKey = getLogKey();
  await s3Client.send(new PutObjectCommand({
    Bucket: S3_BUCKET,
    Key: logKey,
    Body: JSON.stringify(log, null, 2),
    ContentType: 'application/json',
  }));
}

/**
 * Get logs for a date range (for admin panel)
 * @param {string} startDate - Start date (YYYY-MM-DD)
 * @param {string} endDate - End date (YYYY-MM-DD), defaults to today
 * @returns {Promise<Object>} Logs object keyed by date
 */
export async function getLogsForDateRange(startDate, endDate = null) {
  const end = endDate || getTodayString();
  const logs = {};
  
  // Generate date range
  const start = new Date(startDate);
  const endDateObj = new Date(end);
  const current = new Date(start);
  
  while (current <= endDateObj) {
    const dateStr = current.toISOString().split('T')[0];
    try {
      logs[dateStr] = await getDailyLog(dateStr);
    } catch (error) {
      // Skip if log doesn't exist for this date
      console.warn(`No log found for ${dateStr}`);
    }
    current.setDate(current.getDate() + 1);
  }
  
  return logs;
}



