// Recent5 Manifest Management
// Handles reading/writing/updating manifests for city, state, and national levels

import { S3Client, GetObjectCommand, PutObjectCommand, HeadObjectCommand } from '@aws-sdk/client-s3';
import { getManifestKey } from './s3PathHelper.js';

const S3_BUCKET = process.env.S3_BUCKET || 'sirsluginston-ventureos-data';
const s3Client = new S3Client({ region: process.env.AWS_REGION || 'us-east-1' });

/**
 * Get manifest from S3 (city, state, or national)
 * @param {string} path - Path (city, state, or national)
 * @returns {Promise<Object>} Manifest object
 */
export async function getManifest(path) {
  const manifestKey = getManifestKey(path);
  
  try {
    const response = await s3Client.send(new GetObjectCommand({
      Bucket: S3_BUCKET,
      Key: manifestKey,
    }));
    
    const body = await response.Body.transformToString();
    return JSON.parse(body);
  } catch (error) {
    if (error.name === 'NoSuchKey' || error.$metadata?.httpStatusCode === 404) {
      // Return empty manifest if doesn't exist
      return createEmptyManifest();
    }
    throw error;
  }
}

/**
 * Write manifest to S3
 * @param {string} path - Path (city, state, or national)
 * @param {Object} manifest - Manifest object
 * @returns {Promise<void>}
 */
export async function writeManifest(path, manifest) {
  const manifestKey = getManifestKey(path);
  
  // Update lastUpdated timestamp
  manifest.lastUpdated = new Date().toISOString();
  
  await s3Client.send(new PutObjectCommand({
    Bucket: S3_BUCKET,
    Key: manifestKey,
    Body: JSON.stringify(manifest, null, 2),
    ContentType: 'application/json',
  }));
  
  console.log(`[OK] Wrote manifest to S3: ${manifestKey}`);
}

/**
 * Create empty manifest structure
 * @returns {Object} Empty manifest
 */
export function createEmptyManifest() {
  return {
    lastUpdated: new Date().toISOString(),
    recent5: {}, // Brand-specific: recent5[brandPk][violationType] = [ids]
    inDynamo: {}, // Brand-specific: inDynamo[brandPk][violationType] = [ids]
    brands: [], // List of brand PKs that have data in this location (e.g., ['BRAND#OSHAtrail'])
    stats: null, // Stats will be added by daily sync (brand-specific): stats[brandPk] = {...}
  };
}

/**
 * Update recent5 list for a violation type
 * Keeps only the 5 most recent violations
 * @param {Array<string>} currentList - Current list of violation IDs
 * @param {string} newViolationId - New violation ID to add
 * @param {Object} violation - Violation object (for date sorting)
 * @returns {Array<string>} Updated list (max 5 items, sorted by date descending)
 */
export function updateRecent5List(currentList, newViolationId, violation) {
  const list = currentList || [];
  
  // Add new violation if not already present
  if (!list.includes(newViolationId)) {
    list.push(newViolationId);
  }
  
  // If we have violations with dates, sort by date (newest first)
  // Otherwise, just keep the last 5
  if (violation?.ViolationData?.eventDate || violation?.eventDate) {
    // For now, just keep last 5 (we'll sort properly during daily sync)
    return list.slice(-5);
  }
  
  // Keep last 5
  return list.slice(-5);
}

/**
 * Update manifest with new violation
 * @param {string} path - Path (city, state, or national)
 * @param {string} violationType - Violation type (e.g., 'Severe Injury', 'Enforcement')
 * @param {string} violationId - Violation ID
 * @param {Object} violation - Violation object (must have pk for brand)
 * @returns {Promise<void>}
 */
export async function updateManifestWithViolation(path, violationType, violationId, violation) {
  const manifest = await getManifest(path);
  
  const brandPk = violation.pk;
  if (!brandPk) {
    console.warn('Violation missing pk (brand), cannot update manifest');
    return manifest;
  }
  
  // Track which brands have data
  if (!manifest.brands) {
    manifest.brands = [];
  }
  if (!manifest.brands.includes(brandPk)) {
    manifest.brands.push(brandPk);
  }
  
  // Initialize brand-specific structures
  if (!manifest.recent5[brandPk]) {
    manifest.recent5[brandPk] = {};
  }
  if (!manifest.inDynamo[brandPk]) {
    manifest.inDynamo[brandPk] = {};
  }
  
  // Initialize violation type for this brand
  if (!manifest.recent5[brandPk][violationType]) {
    manifest.recent5[brandPk][violationType] = [];
  }
  if (!manifest.inDynamo[brandPk][violationType]) {
    manifest.inDynamo[brandPk][violationType] = [];
  }
  
  // Update recent5 list for this brand
  manifest.recent5[brandPk][violationType] = updateRecent5List(
    manifest.recent5[brandPk][violationType],
    violationId,
    violation
  );
  
  // Write updated manifest
  await writeManifest(path, manifest);
  
  return manifest;
}

/**
 * Check if manifest exists
 * @param {string} path - Path (city, state, or national)
 * @returns {Promise<boolean>} True if exists
 */
export async function manifestExists(path) {
  const manifestKey = getManifestKey(path);
  
  try {
    await s3Client.send(new HeadObjectCommand({
      Bucket: S3_BUCKET,
      Key: manifestKey,
    }));
    return true;
  } catch (error) {
    if (error.name === 'NotFound' || error.$metadata?.httpStatusCode === 404) {
      return false;
    }
    throw error;
  }
}

