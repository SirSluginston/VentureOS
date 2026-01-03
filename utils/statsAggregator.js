// Stats Aggregator
// Calculates city, state, and national stats from S3 data

import { S3Client, ListObjectsV2Command, GetObjectCommand } from '@aws-sdk/client-s3';
import { getCityPath, getStatePath, getNationalPath } from './s3PathHelper.js';

const S3_BUCKET = process.env.S3_BUCKET || 'sirsluginston-ventureos-data';
const s3Client = new S3Client({ region: process.env.AWS_REGION || 'us-east-1' });

/**
 * Load all violations from S3 for a city, filtered by brand
 */
async function loadAllViolationsFromS3(cityPath, brandPk) {
  const violations = [];
  let continuationToken = undefined;
  
  do {
    const response = await s3Client.send(new ListObjectsV2Command({
      Bucket: S3_BUCKET,
      Prefix: cityPath,
      ContinuationToken: continuationToken,
    }));
    
    // Filter for violation JSON files (exclude manifests and stats)
    const violationKeys = (response.Contents || [])
      .filter(obj => obj.Key.endsWith('.json') && !obj.Key.includes('manifest') && !obj.Key.includes('stats'))
      .map(obj => obj.Key);
    
    // Load violations and filter by brand
    for (const key of violationKeys) {
      try {
        const objResponse = await s3Client.send(new GetObjectCommand({
          Bucket: S3_BUCKET,
          Key: key,
        }));
        const body = await objResponse.Body.transformToString();
        const violation = JSON.parse(body);
        
        // Only include violations for this brand
        if (violation.pk === brandPk) {
          violations.push(violation);
        }
      } catch (error) {
        console.error(`Error loading violation ${key}:`, error.message);
      }
    }
    
    continuationToken = response.NextContinuationToken;
  } while (continuationToken);
  
  return violations;
}

/**
 * Calculate city stats from S3 for a specific brand
 * @param {string} cityPath - S3 path to city data
 * @param {string} brandPk - Brand primary key (e.g., 'BRAND#OSHAtrail')
 */
export async function calculateCityStatsFromS3(cityPath, brandPk) {
  const violations = await loadAllViolationsFromS3(cityPath, brandPk);
  
  // Calculate stats
  const stats = {
    totalViolations: violations.length,
    totalFines: violations.reduce((sum, v) => {
      const fine = v.ViolationData?.citation?.penalty || 
                   v.ViolationData?.fine || 
                   0;
      return sum + (parseFloat(fine) || 0);
    }, 0),
    violationCounts: {},
    lastUpdated: new Date().toISOString(),
    statsNote: 'Stats update daily',
  };
  
  // Group by violation type
  for (const violation of violations) {
    const type = violation.ViolationType || violation.ViolationData?.violationType || 'Unknown';
    stats.violationCounts[type] = (stats.violationCounts[type] || 0) + 1;
  }
  
  // Extract city/state for SEO
  const firstViolation = violations[0];
  const city = firstViolation?.City || firstViolation?.ViolationData?.location?.city || '';
  const state = firstViolation?.State || firstViolation?.ViolationData?.location?.state || '';
  
  // Add SEO and flags (basic for now)
  stats.seo = {
    title: `${city}, ${state} Workplace Safety Violations | OSHA Trail`,
    description: `Comprehensive workplace safety data for ${city}, ${state}. View OSHA violations, safety statistics, and compliance information.`,
    keywords: [`${city} safety`, `${city} OSHA violations`, `${state} workplace safety`, `${city} compliance`],
  };
  
  stats.flags = {
    stateFlagUrl: `https://cdn.example.com/flags/${state.toLowerCase()}.svg`, // TODO: Update with actual CDN
  };
  
  return stats;
}

/**
 * Calculate state stats by aggregating from cities
 */
export async function calculateStateStatsFromCities(state) {
  // TODO: Implement state stats aggregation
  // This would:
  // 1. List all cities in the state
  // 2. Load city stats from S3 (or calculate from violations)
  // 3. Aggregate totals
  // 4. Return state stats with SEO/flags
  
  return {
    totalViolations: 0,
    totalFines: 0,
    cityCount: 0,
    violationCounts: {},
    lastUpdated: new Date().toISOString(),
    statsNote: 'Stats update daily',
    seo: {
      title: `${state} Workplace Safety Violations | OSHA Trail`,
      description: `Comprehensive workplace safety data for ${state}. View OSHA violations by city, safety statistics, and compliance information.`,
      keywords: [`${state} safety`, `${state} OSHA violations`, `${state} workplace safety`],
    },
    flags: {
      stateFlagUrl: `https://cdn.example.com/flags/${state.toLowerCase()}.svg`,
    },
  };
}

/**
 * Calculate national stats by aggregating from states
 */
export async function calculateNationalStatsFromStates() {
  // TODO: Implement national stats aggregation
  // This would:
  // 1. List all states
  // 2. Load state stats from S3
  // 3. Aggregate totals
  // 4. Return national stats with SEO
  
  return {
    totalViolations: 0,
    totalFines: 0,
    stateCount: 0,
    cityCount: 0,
    violationCounts: {},
    lastUpdated: new Date().toISOString(),
    statsNote: 'Stats update daily',
    seo: {
      title: 'US Workplace Safety Violations | OSHA Trail',
      description: 'Comprehensive workplace safety data for the United States. View OSHA violations by state and city, safety statistics, and compliance information.',
      keywords: ['US safety', 'US OSHA violations', 'workplace safety', 'OSHA compliance'],
    },
  };
}

