// S3 Violation Writer
// Handles writing violations to S3 with proper path structure
// Also updates manifest stats incrementally (no need to recalculate from scratch)

import { S3Client, PutObjectCommand, HeadObjectCommand } from '@aws-sdk/client-s3';
import { getCityPath, getViolationKey, extractYear, slugifyCity, getStatePath, getNationalPath } from './s3PathHelper.js';
import { getManifest, writeManifest } from './recent5Manifest.js';

const S3_BUCKET = process.env.S3_BUCKET || 'sirsluginston-ventureos-data';
const s3Client = new S3Client({ region: process.env.AWS_REGION || 'us-east-1' });

/**
 * Write a violation to S3
 * @param {Object} violation - Normalized violation object
 * @returns {Promise<Object>} Result with path and key
 */
export async function writeViolationToS3(violation) {
  try {
    // Extract location data
    const country = violation.Country || violation.ViolationData?.location?.country || 'USA';
    const state = violation.State || violation.ViolationData?.location?.state || '';
    const city = violation.City || violation.ViolationData?.location?.city || '';
    
    if (!state || !city) {
      throw new Error(`Missing state or city: state=${state}, city=${city}`);
    }
    
    // Generate paths
    const citySlug = slugifyCity(city, state);
    const cityPath = getCityPath(country, state, citySlug);
    
    // Extract year from violation date
    const violationDate = violation.ViolationData?.eventDate || 
                          violation.ViolationData?.date || 
                          violation.eventDate;
    const year = extractYear(violationDate);
    
    // Generate violation key
    const violationId = violation.sk?.replace('VIOLATION#', '') || 
                       violation.ViolationData?.id || 
                       `unknown-${Date.now()}`;
    const s3Key = getViolationKey(cityPath, year, violationId);
    
    // Prepare violation data for S3 (include all fields)
    const violationData = {
      ...violation,
      // Ensure Country, State, City are top-level for easy access
      Country: country,
      State: state,
      City: city,
      // Include metadata
      _metadata: {
        s3Key,
        cityPath,
        year,
        writtenAt: new Date().toISOString(),
      },
    };
    
    // Write to S3
    await s3Client.send(new PutObjectCommand({
      Bucket: S3_BUCKET,
      Key: s3Key,
      Body: JSON.stringify(violationData, null, 2),
      ContentType: 'application/json',
    }));
    
    console.log(`[OK] Wrote violation to S3: ${s3Key}`);
    
    // Incrementally update stats in manifests (city, state, national)
    // This eliminates the need to recalculate from scratch during daily sync
    try {
      await updateStatsIncrementally(violation, cityPath, state, country);
    } catch (statsError) {
      // Don't fail the write if stats update fails - log and continue
      console.warn(`Warning: Failed to update stats incrementally: ${statsError.message}`);
    }
    
    return {
      success: true,
      s3Key,
      cityPath,
      year,
      violationId,
    };
  } catch (error) {
    console.error(`[ERROR] Error writing violation to S3:`, error);
    throw error;
  }
}

/**
 * Check if a violation already exists in S3
 * @param {string} s3Key - S3 key to check
 * @returns {Promise<boolean>} True if exists
 */
export async function violationExistsInS3(s3Key) {
  try {
    await s3Client.send(new HeadObjectCommand({
      Bucket: S3_BUCKET,
      Key: s3Key,
    }));
    return true;
  } catch (error) {
    if (error.name === 'NotFound' || error.$metadata?.httpStatusCode === 404) {
      return false;
    }
    throw error;
  }
}

/**
 * Write multiple violations to S3 (batch)
 * @param {Array<Object>} violations - Array of normalized violation objects
 * @returns {Promise<Object>} Results with success/error counts
 */
export async function writeViolationsToS3(violations) {
  const results = {
    success: 0,
    errors: 0,
    errorDetails: [],
  };
  
  for (const violation of violations) {
    try {
      await writeViolationToS3(violation);
      results.success++;
    } catch (error) {
      results.errors++;
      results.errorDetails.push({
        violationId: violation.sk || 'unknown',
        error: error.message,
      });
      
      // Log but continue processing
      console.error(`Error writing violation ${violation.sk}:`, error.message);
    }
  }
  
  return results;
}

/**
 * Incrementally update stats in city, state, and national manifests
 * This is called when a violation is written, so we maintain accurate counts
 * without needing to recalculate from scratch
 * @param {Object} violation - Violation object (must have pk for brand)
 * @param {string} cityPath - City path (e.g., 'National/USA/TX/Austin-TX/')
 * @param {string} state - State code (e.g., 'TX')
 * @param {string} country - Country code (e.g., 'USA')
 */
async function updateStatsIncrementally(violation, cityPath, state, country) {
  const brandPk = violation.pk;
  if (!brandPk) {
    console.warn('Violation missing pk (brand), skipping stats update');
    return;
  }
  
  // Extract fine amount
  const fine = violation.ViolationData?.citation?.penalty || 
               violation.ViolationData?.fine || 
               0;
  const fineAmount = parseFloat(fine) || 0;
  
  // Extract violation type
  const violationType = violation.ViolationType || 
                       violation.ViolationData?.violationType || 
                       'Unknown';
  
  // 1. Update city manifest stats
  try {
    const cityManifest = await getManifest(cityPath);
    
    // Track which brands have data (for fast filtering without loading files)
    if (!cityManifest.brands) {
      cityManifest.brands = [];
    }
    if (!cityManifest.brands.includes(brandPk)) {
      cityManifest.brands.push(brandPk);
    }
    
    if (!cityManifest.stats) {
      cityManifest.stats = {};
    }
    if (!cityManifest.stats[brandPk]) {
      cityManifest.stats[brandPk] = {
        totalViolations: 0,
        totalFines: 0,
        violationCounts: {},
        // companyStats removed - companies tracked separately for COMPANY_STATS items
        lastUpdated: new Date().toISOString(),
      };
    }
    
    const cityStats = cityManifest.stats[brandPk];
    cityStats.totalViolations = (cityStats.totalViolations || 0) + 1;
    cityStats.totalFines = (cityStats.totalFines || 0) + fineAmount;
    cityStats.violationCounts[violationType] = (cityStats.violationCounts[violationType] || 0) + 1;
    
    // companyStats removed from city stats - companies tracked separately for COMPANY_STATS items
    
    cityStats.lastUpdated = new Date().toISOString();
    
    await writeManifest(cityPath, cityManifest);
  } catch (error) {
    console.error(`Error updating city stats for ${cityPath}:`, error.message);
  }
  
  // 2. Update state manifest stats
  try {
    const statePath = getStatePath(country, state);
    const stateManifest = await getManifest(statePath);
    if (!stateManifest.stats) {
      stateManifest.stats = {};
    }
    if (!stateManifest.stats[brandPk]) {
      stateManifest.stats[brandPk] = {
        totalViolations: 0,
        totalFines: 0,
        cityCount: 0,
        violationCounts: {},
        lastUpdated: new Date().toISOString(),
      };
    }
    
    const stateStats = stateManifest.stats[brandPk];
    stateStats.totalViolations = (stateStats.totalViolations || 0) + 1;
    stateStats.totalFines = (stateStats.totalFines || 0) + fineAmount;
    stateStats.violationCounts[violationType] = (stateStats.violationCounts[violationType] || 0) + 1;
    stateStats.lastUpdated = new Date().toISOString();
    // Note: cityCount is calculated during daily sync aggregation, not incremented here
    
    await writeManifest(statePath, stateManifest);
  } catch (error) {
    console.error(`Error updating state stats for ${state}:`, error.message);
  }
  
  // 3. Update national manifest stats
  try {
    const nationalPath = getNationalPath(country);
    const nationalManifest = await getManifest(nationalPath);
    if (!nationalManifest.stats) {
      nationalManifest.stats = {};
    }
    if (!nationalManifest.stats[brandPk]) {
      nationalManifest.stats[brandPk] = {
        totalViolations: 0,
        totalFines: 0,
        stateCount: 0,
        cityCount: 0,
        violationCounts: {},
        lastUpdated: new Date().toISOString(),
      };
    }
    
    const nationalStats = nationalManifest.stats[brandPk];
    nationalStats.totalViolations = (nationalStats.totalViolations || 0) + 1;
    nationalStats.totalFines = (nationalStats.totalFines || 0) + fineAmount;
    nationalStats.violationCounts[violationType] = (nationalStats.violationCounts[violationType] || 0) + 1;
    nationalStats.lastUpdated = new Date().toISOString();
    // Note: stateCount and cityCount are calculated during daily sync aggregation
    
    await writeManifest(nationalPath, nationalManifest);
  } catch (error) {
    console.error(`Error updating national stats:`, error.message);
  }
}

