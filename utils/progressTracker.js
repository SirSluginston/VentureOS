// Progress Tracker for Lambda Functions
// Saves/loads progress markers to S3 for resuming long-running processes
//
// When a Lambda function needs to take a break (timeout, graceful exit, etc.),
// this utility helps it remember where it left off. 
//
// Also handles locks to prevent concurrent executions - because two slugs shouldn't
// be working on the same thing at the same time (that's just chaos).

import { S3Client, GetObjectCommand, PutObjectCommand, DeleteObjectCommand } from '@aws-sdk/client-s3';

const S3_BUCKET = process.env.S3_BUCKET || 'sirsluginston-ventureos-data';
const s3Client = new S3Client({ region: process.env.AWS_REGION || 'us-east-1' });

/**
 * Get progress marker for a rebuild operation
 * @param {string} brandPk - Brand primary key
 * @returns {Promise<Object|null>} Progress object or null if none exists
 */
export async function getRebuildProgress(brandPk) {
  const progressKey = `_progress/rebuild-${brandPk.replace('#', '-')}.json`;
  
  try {
    const response = await s3Client.send(new GetObjectCommand({
      Bucket: S3_BUCKET,
      Key: progressKey,
    }));
    
    const body = await response.Body.transformToString();
    return JSON.parse(body);
  } catch (error) {
    if (error.name === 'NoSuchKey' || error.$metadata?.httpStatusCode === 404) {
      return null;
    }
    throw error;
  }
}

/**
 * Save progress marker for a rebuild operation
 * @param {string} brandPk - Brand primary key
 * @param {string} currentState - Current state being processed
 * @param {string} lastCityPath - Last city path processed (optional)
 * @param {number} citiesProcessed - Number of cities processed so far
 * @returns {Promise<void>}
 */
export async function saveRebuildProgress(brandPk, currentState, lastCityPath = null, citiesProcessed = 0) {
  const progressKey = `_progress/rebuild-${brandPk.replace('#', '-')}.json`;
  
  const progress = {
    brandPk,
    currentState,
    lastCityPath,
    citiesProcessed,
    lastUpdated: new Date().toISOString(),
  };
  
  await s3Client.send(new PutObjectCommand({
    Bucket: S3_BUCKET,
    Key: progressKey,
    Body: JSON.stringify(progress, null, 2),
    ContentType: 'application/json',
  }));
}

/**
 * Clear progress marker (when rebuild completes)
 * @param {string} brandPk - Brand primary key
 * @returns {Promise<void>}
 */
export async function clearRebuildProgress(brandPk) {
  const progressKey = `_progress/rebuild-${brandPk.replace('#', '-')}.json`;
  
  try {
    await s3Client.send(new PutObjectCommand({
      Bucket: S3_BUCKET,
      Key: progressKey,
      Body: JSON.stringify({ completed: true, completedAt: new Date().toISOString() }, null, 2),
      ContentType: 'application/json',
    }));
  } catch (error) {
    // Ignore errors when clearing
    console.warn('Error clearing progress:', error.message);
  }
}

/**
 * Get progress marker for daily sync
 * @param {string} brandPk - Brand primary key
 * @returns {Promise<Object|null>} Progress object or null if none exists
 */
export async function getSyncProgress(brandPk) {
  const progressKey = `_progress/sync-${brandPk.replace('#', '-')}.json`;
  
  try {
    const response = await s3Client.send(new GetObjectCommand({
      Bucket: S3_BUCKET,
      Key: progressKey,
    }));
    
    const body = await response.Body.transformToString();
    return JSON.parse(body);
  } catch (error) {
    if (error.name === 'NoSuchKey' || error.$metadata?.httpStatusCode === 404) {
      return null;
    }
    throw error;
  }
}

/**
 * Save progress marker for daily sync
 * @param {string} brandPk - Brand primary key
 * @param {string} currentState - Current state being processed
 * @param {string} lastCityPath - Last city path processed (optional)
 * @param {number} citiesProcessed - Number of cities processed so far
 * @returns {Promise<void>}
 */
export async function saveSyncProgress(brandPk, currentState, lastCityPath = null, citiesProcessed = 0) {
  const progressKey = `_progress/sync-${brandPk.replace('#', '-')}.json`;
  
  const progress = {
    brandPk,
    currentState,
    lastCityPath,
    citiesProcessed,
    lastUpdated: new Date().toISOString(),
  };
  
  await s3Client.send(new PutObjectCommand({
    Bucket: S3_BUCKET,
    Key: progressKey,
    Body: JSON.stringify(progress, null, 2),
    ContentType: 'application/json',
  }));
}

/**
 * Clear sync progress (when sync completes)
 * @param {string} brandPk - Brand primary key
 * @returns {Promise<void>}
 */
export async function clearSyncProgress(brandPk) {
  const progressKey = `_progress/sync-${brandPk.replace('#', '-')}.json`;
  
  try {
    await s3Client.send(new PutObjectCommand({
      Bucket: S3_BUCKET,
      Key: progressKey,
      Body: JSON.stringify({ completed: true, completedAt: new Date().toISOString() }, null, 2),
      ContentType: 'application/json',
    }));
  } catch (error) {
    // Ignore errors when clearing
    console.warn('Error clearing progress:', error.message);
  }
}

/**
 * Acquire a lock for daily sync to prevent concurrent executions
 * @returns {Promise<boolean>} True if lock was acquired, false if already locked
 */
export async function acquireSyncLock() {
  const lockKey = '_progress/sync-lock.json';
  const lockTimeout = 20 * 60 * 1000; // 20 minutes (longer than Lambda timeout)
  
  try {
    // Try to get existing lock
    const response = await s3Client.send(new GetObjectCommand({
      Bucket: S3_BUCKET,
      Key: lockKey,
    }));
    
    const body = await response.Body.transformToString();
    const lock = JSON.parse(body);
    
    // Check if lock is still valid (not expired)
    const lockTime = new Date(lock.lockedAt).getTime();
    const now = Date.now();
    
    if (now - lockTime < lockTimeout) {
      // Lock is still valid, another instance is running
      return false;
    }
    
    // Lock expired, we can take it
  } catch (error) {
    if (error.name !== 'NoSuchKey' && error.$metadata?.httpStatusCode !== 404) {
      throw error;
    }
    // No lock exists, we can acquire it
  }
  
  // Acquire lock
  await s3Client.send(new PutObjectCommand({
    Bucket: S3_BUCKET,
    Key: lockKey,
    Body: JSON.stringify({
      lockedAt: new Date().toISOString(),
      lockedBy: 'daily-sync',
    }),
    ContentType: 'application/json',
  }));
  
  return true;
}

/**
 * Release the daily sync lock
 * @returns {Promise<void>}
 */
export async function releaseSyncLock() {
  const lockKey = '_progress/sync-lock.json';
  
  try {
    await s3Client.send(new DeleteObjectCommand({
      Bucket: S3_BUCKET,
      Key: lockKey,
    }));
  } catch (error) {
    // Ignore errors when releasing lock
    console.warn('Error releasing sync lock:', error.message);
  }
}

/**
 * Acquire a lock for rebuild to prevent concurrent executions
 * @returns {Promise<boolean>} True if lock was acquired, false if already locked
 */
export async function acquireRebuildLock() {
  const lockKey = '_progress/rebuild-lock.json';
  const lockTimeout = 20 * 60 * 1000; // 20 minutes (longer than Lambda timeout)
  
  try {
    // Try to get existing lock
    const response = await s3Client.send(new GetObjectCommand({
      Bucket: S3_BUCKET,
      Key: lockKey,
    }));
    
    const body = await response.Body.transformToString();
    const lock = JSON.parse(body);
    
    // Check if lock is still valid (not expired)
    const lockTime = new Date(lock.lockedAt).getTime();
    const now = Date.now();
    
    if (now - lockTime < lockTimeout) {
      // Lock is still valid, another instance is running
      return false;
    }
    
    // Lock expired, we can take it
  } catch (error) {
    if (error.name !== 'NoSuchKey' && error.$metadata?.httpStatusCode !== 404) {
      throw error;
    }
    // No lock exists, we can acquire it
  }
  
  // Acquire lock
  await s3Client.send(new PutObjectCommand({
    Bucket: S3_BUCKET,
    Key: lockKey,
    Body: JSON.stringify({
      lockedAt: new Date().toISOString(),
      lockedBy: 'stats-rebuild',
    }),
    ContentType: 'application/json',
  }));
  
  return true;
}

/**
 * Release the rebuild lock
 * @returns {Promise<void>}
 */
export async function releaseRebuildLock() {
  const lockKey = '_progress/rebuild-lock.json';
  
  try {
    await s3Client.send(new DeleteObjectCommand({
      Bucket: S3_BUCKET,
      Key: lockKey,
    }));
  } catch (error) {
    // Ignore errors when releasing lock
    console.warn('Error releasing rebuild lock:', error.message);
  }
}

