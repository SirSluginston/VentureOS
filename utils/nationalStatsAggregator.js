// National Stats Aggregator
// Aggregates national stats from state stats

import { S3Client, ListObjectsV2Command, GetObjectCommand } from '@aws-sdk/client-s3';
import { getNationalPath, getStatsKey } from './s3PathHelper.js';

const S3_BUCKET = process.env.S3_BUCKET || 'sirsluginston-ventureos-data';
const s3Client = new S3Client({ region: process.env.AWS_REGION || 'us-east-1' });

/**
 * Calculate national stats by aggregating from states for a specific brand
 * @param {string} brandPk - Brand primary key (e.g., 'BRAND#OSHAtrail')
 */
export async function calculateNationalStatsFromStates(brandPk) {
  const nationalPath = getNationalPath('USA');
  
  // List all states
  const statePaths = [];
  let continuationToken = undefined;
  
  do {
    const response = await s3Client.send(new ListObjectsV2Command({
      Bucket: S3_BUCKET,
      Prefix: nationalPath,
      Delimiter: '/',
      ContinuationToken: continuationToken,
    }));
    
    const statePrefixes = (response.CommonPrefixes || []).map(p => p.Prefix);
    statePaths.push(...statePrefixes);
    
    continuationToken = response.NextContinuationToken;
  } while (continuationToken);
  
  // Load state stats from DynamoDB (they're stored per brand) and aggregate
  let totalViolations = 0;
  let totalFines = 0;
  let totalCities = 0;
  const violationCounts = {};
  const stateStats = [];
  
  // Import stateStats to get stats from DynamoDB
  const { getStateStats } = await import('./stateStats.js');
  
  // List of all 50 US states + DC + 5 territories (56 jurisdictions total)
  // Territories: PR (Puerto Rico), VI (U.S. Virgin Islands), GU (Guam), AS (American Samoa), MP (Northern Mariana Islands)
  const allStates = ['AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 
                     'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
                     'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
                     'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
                     'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY', 'DC',
                     'PR', 'VI', 'GU', 'AS', 'MP'];
  
  // Load state stats from manifests (they're stored per brand) and aggregate
  // Import getManifest to get stats from S3 manifests
  const { getManifest } = await import('./recent5Manifest.js');
  
  for (const state of allStates) {
    try {
      const statePath = getStatePath('USA', state);
      const stateManifest = await getManifest(statePath);
      const stateStat = stateManifest.stats?.[brandPk]; // Read from manifest
      
      if (stateStat) {
        stateStats.push(stateStat);
        totalViolations += stateStat.totalViolations || 0;
        totalFines += stateStat.totalFines || 0;
        totalCities += stateStat.cityCount || 0;
        
        // Aggregate violation counts
        for (const [type, count] of Object.entries(stateStat.violationCounts || {})) {
          violationCounts[type] = (violationCounts[type] || 0) + count;
        }
      }
    } catch (error) {
      // State manifest stats might not exist yet, skip
      console.warn(`State manifest stats not found for ${state}: ${error.message}`);
    }
  }
  
  return {
    totalViolations,
    totalFines,
    stateCount: statePaths.length,
    cityCount: totalCities,
    violationCounts,
    lastUpdated: new Date().toISOString(),
    statsNote: 'Stats update daily',
    seo: {
      title: 'US Workplace Safety Violations | OSHA Trail',
      description: 'Comprehensive workplace safety data for the United States. View OSHA violations by state and city, safety statistics, and compliance information.',
      keywords: ['US safety', 'US OSHA violations', 'workplace safety', 'OSHA compliance'],
    },
  };
}

