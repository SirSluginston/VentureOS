// State Stats Aggregator
// Aggregates state stats from city stats

import { S3Client, ListObjectsV2Command, GetObjectCommand } from '@aws-sdk/client-s3';
import { getStatePath, getStatsKey } from './s3PathHelper.js';

const S3_BUCKET = process.env.S3_BUCKET || 'sirsluginston-ventureos-data';
const s3Client = new S3Client({ region: process.env.AWS_REGION || 'us-east-1' });

/**
 * Calculate state stats by aggregating from cities for a specific brand
 * @param {string} state - State abbreviation (e.g., 'TX')
 * @param {string} brandPk - Brand primary key (e.g., 'BRAND#OSHAtrail')
 */
export async function calculateStateStatsFromCities(state, brandPk) {
  const statePath = getStatePath('USA', state);
  
  // List all cities in this state
  const cityPaths = [];
  let continuationToken = undefined;
  
  do {
    const response = await s3Client.send(new ListObjectsV2Command({
      Bucket: S3_BUCKET,
      Prefix: statePath,
      Delimiter: '/',
      ContinuationToken: continuationToken,
    }));
    
    const cityPrefixes = (response.CommonPrefixes || []).map(p => p.Prefix);
    cityPaths.push(...cityPrefixes);
    
    continuationToken = response.NextContinuationToken;
  } while (continuationToken);
  
  // Load city stats from manifests (they're stored per brand) and aggregate
  // Import getManifest to get stats from S3 manifests
  const { getManifest } = await import('./recent5Manifest.js');
  
  let totalViolations = 0;
  let totalFines = 0;
  const violationCounts = {};
  // Note: companyStats is NOT aggregated at state level to avoid 400KB DynamoDB limit
  // Company stats are stored only at city level. To get state-level company stats,
  // query all cities in that state.
  
  for (const cityPath of cityPaths) {
    try {
      const cityManifest = await getManifest(cityPath);
      const cityStat = cityManifest.stats?.[brandPk]; // Read from manifest
      
      if (cityStat) {
        totalViolations += cityStat.totalViolations || 0;
        totalFines += cityStat.totalFines || 0;
        
        // Aggregate violation counts
        for (const [type, count] of Object.entries(cityStat.violationCounts || {})) {
          violationCounts[type] = (violationCounts[type] || 0) + count;
        }
        
        // Company stats are NOT aggregated here - they stay at city level
        // This prevents state/national items from exceeding 400KB DynamoDB limit
      }
    } catch (error) {
      // City manifest stats might not exist yet, skip
      console.warn(`City manifest stats not found for ${cityPath}: ${error.message}`);
    }
  }
  
  // State stats only contains aggregates - no companyStats to avoid size limits
  // Each city has its own companyStats in CITY_STATS items
  const stats = {
    totalViolations,
    totalFines,
    cityCount: cityPaths.length,
    violationCounts,
    // companyStats removed - query cities for company-level data
    lastUpdated: new Date().toISOString(),
    statsNote: 'Stats update daily. Company stats available at city level.',
    seo: {
      title: `${state} Workplace Safety Violations | OSHA Trail`,
      description: `Comprehensive workplace safety data for ${state}. View OSHA violations by city, safety statistics, and compliance information.`,
      keywords: [`${state} safety`, `${state} OSHA violations`, `${state} workplace safety`],
    },
    flags: {
      stateFlagUrl: `https://cdn.example.com/flags/${state.toLowerCase()}.svg`,
    },
  };
  
  return stats;
}

