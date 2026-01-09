// VentureOS Daily Sync Lambda Handler
// Version: 2.1.0 - Auto-Resume, Optimized Stats Reading
//
// The daily sync handler - the fast one that takes shortcuts (unlike its thorough sibling).
// Runs daily via EventBridge to keep DynamoDB in sync with S3 manifests for all brands.
// This one is optimized: it reads stats from manifests instead of recalculating everything,
// which makes it much faster for daily updates. Think of it as the express slug route.
//
// Features:
// - Graceful exit at 14:45 to avoid timeouts (because even slugs need breaks)
// - Auto-resume: self-invokes if graceful exit occurred (persistent little thing)
// - Progress tracking with resume capability
// - Daily logging for admin monitoring
// - Brand-specific manifest handling
// - Optimized: reads stats from manifests (no recalculation unless missing)
//
// This is a SHARED resource (VentureOS) that handles all pSEO brands:
// - OSHATrail
// - TransportTrail
// - Future brands...

import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient, QueryCommand, DeleteCommand, PutCommand, BatchWriteCommand } from '@aws-sdk/lib-dynamodb';
import { S3Client, ListObjectsV2Command, GetObjectCommand, PutObjectCommand } from '@aws-sdk/client-s3';
import { SQSClient, SendMessageCommand } from '@aws-sdk/client-sqs';

const dynamoClient = new DynamoDBClient({ region: process.env.AWS_REGION || 'us-east-1' });
const docClient = DynamoDBDocumentClient.from(dynamoClient);
const s3Client = new S3Client({ region: process.env.AWS_REGION || 'us-east-1' });
const sqsClient = new SQSClient({ region: process.env.AWS_REGION || 'us-east-1' });

// SQS queue URL for sequential state processing
const SYNC_QUEUE_URL = process.env.SYNC_QUEUE_URL || 'https://sqs.us-east-1.amazonaws.com/611538926352/ventureos-sync-queue';

const TABLE_NAME = process.env.DYNAMODB_TABLE || 'SirSluginstonVentureOS';
const S3_BUCKET = process.env.S3_BUCKET || 'sirsluginston-ventureos-data';

// Brands that use S3-first architecture (add new brands here)
const BRANDS = [
  { pk: 'BRAND#OSHAtrail', name: 'OSHATrail' },
  { pk: 'BRAND#TransportTrail', name: 'TransportTrail' },
  // Add more brands as they're created
];

// Import utilities (ES modules)
import { getManifest, writeManifest } from './utils/recent5Manifest.js';
import { getCityPath, getStatePath, getNationalPath } from './utils/s3PathHelper.js';
import { calculateCityStatsFromS3 } from './utils/statsAggregator.js';
import { calculateStateStatsFromCities } from './utils/stateStatsAggregator.js';
import { calculateNationalStatsFromStates } from './utils/nationalStatsAggregator.js';
import { updateCityStats } from './utils/cityStats.js';
import { updateStateStats } from './utils/stateStats.js';
import { updateNationalStats } from './utils/nationalStats.js';
import { getSyncProgress, saveSyncProgress, clearSyncProgress, acquireSyncLock, releaseSyncLock } from './utils/progressTracker.js';
import { appendDailyLog } from './utils/dailyLog.js';


/**
 * Load violation from S3
 */
async function loadViolationFromS3(s3Key) {
  try {
    const response = await s3Client.send(new GetObjectCommand({
      Bucket: S3_BUCKET,
      Key: s3Key,
    }));
    const body = await response.Body.transformToString();
    return JSON.parse(body);
  } catch (error) {
    console.error(`Error loading violation from S3 (${s3Key}):`, error);
    return null;
  }
}

/**
 * Sync city manifest to DynamoDB
 * Full replace strategy: Delete all current, write all new
 */
async function syncCityManifest(cityPath, citySlug, state, brandPk) {
  const manifest = await getManifest(cityPath);
  
  let totalUpdates = 0;
  
  // Get brand-specific recent5 and inDynamo
  const brandRecent5 = manifest.recent5?.[brandPk] || {};
  const brandInDynamo = manifest.inDynamo?.[brandPk] || {};
  
  // For each violation type for this brand
  for (const [violationType, recent5Ids] of Object.entries(brandRecent5)) {
    const inDynamoIds = brandInDynamo[violationType] || [];
    
    // Compare arrays (order matters for recent5)
    const needsUpdate = JSON.stringify(recent5Ids) !== JSON.stringify(inDynamoIds);
    
    if (needsUpdate) {
      console.log(`  Updating ${citySlug} - ${violationType}: ${inDynamoIds.length} → ${recent5Ids.length}`);
      
      // 1. Delete all current violations for this city/type
      if (inDynamoIds.length > 0) {
        const deleteRequests = inDynamoIds.map(violationId => ({
          DeleteRequest: {
            Key: {
              pk: brandPk,
              sk: `VIOLATION#${violationId}`,
            },
          },
        }));
        
        // Batch delete (max 25 per batch)
        for (let i = 0; i < deleteRequests.length; i += 25) {
          const batch = deleteRequests.slice(i, i + 25);
          await docClient.send(new BatchWriteCommand({
            RequestItems: {
              [TABLE_NAME]: batch,
            },
          }));
        }
      }
      
      // 2. Load violations from S3 and write to DynamoDB
      if (recent5Ids.length > 0) {
        const violations = [];
        
        // Try to extract year from violation ID (format: YYYYMMDD-XXXXX or similar)
        // Or try common years (current year, last 2 years)
        const currentYear = new Date().getFullYear();
        const yearsToTry = [currentYear.toString(), (currentYear - 1).toString(), (currentYear - 2).toString()];
        
        for (const violationId of recent5Ids) {
          let violation = null;
          
          // Try to extract year from violation ID (if it starts with YYYY)
          let yearFromId = null;
          if (violationId.match(/^(\d{4})/)) {
            const yearMatch = violationId.match(/^(\d{4})/);
            if (yearMatch) {
              const year = parseInt(yearMatch[1]);
              // Sanity check: year should be between 2000 and current year + 1
              if (year >= 2000 && year <= currentYear + 1) {
                yearFromId = year.toString();
              }
            }
          }
          
          // Try years: from ID first, then common years
          const years = yearFromId ? [yearFromId, ...yearsToTry] : yearsToTry;
          
          for (const year of years) {
            try {
              const violationKey = `${cityPath}${year}/${violationId}.json`;
              violation = await loadViolationFromS3(violationKey);
              if (violation) {
                break; // Found it, stop trying other years
              }
            } catch (error) {
              // File doesn't exist at this path, try next year
              continue;
            }
          }
          
          if (violation) {
            violations.push(violation);
          } else {
            console.warn(`  [WARN] Could not find violation ${violationId} in S3 for ${citySlug} (tried years: ${years.join(', ')})`);
          }
        }
        
        // Write violations to DynamoDB
        if (violations.length > 0) {
          const putRequests = violations.map(violation => ({
            PutRequest: {
              Item: violation,
            },
          }));
          
          // Batch write (max 25 per batch)
          for (let i = 0; i < putRequests.length; i += 25) {
            const batch = putRequests.slice(i, i + 25);
            await docClient.send(new BatchWriteCommand({
              RequestItems: {
                [TABLE_NAME]: batch,
              },
            }));
          }
        }
      }
      
      // 3. Update manifest.inDynamo for this brand
      if (!manifest.inDynamo) {
        manifest.inDynamo = {};
      }
      if (!manifest.inDynamo[brandPk]) {
        manifest.inDynamo[brandPk] = {};
      }
      manifest.inDynamo[brandPk][violationType] = [...recent5Ids];
      totalUpdates++;
    }
  }
  
  // Save updated manifest if changes were made
  if (totalUpdates > 0) {
    await writeManifest(cityPath, manifest);
  }
  
  return totalUpdates;
}

/**
 * Get all city paths from S3 for a specific brand
 * S3 structure: National/Country/State/City/
 * We need to filter by brand by checking violation files' pk field
 */
async function getAllCityPathsForBrand(brandPk) {
  const cityPaths = [];
  let continuationToken = undefined;
  
  do {
    const response = await s3Client.send(new ListObjectsV2Command({
      Bucket: S3_BUCKET,
      Prefix: 'National/USA/',
      Delimiter: '/',
      ContinuationToken: continuationToken,
    }));
    
    // Get state prefixes
    const statePrefixes = (response.CommonPrefixes || []).map(p => p.Prefix);
    
    for (const statePrefix of statePrefixes) {
      // List cities in this state
      let cityContinuationToken = undefined;
      do {
        const cityResponse = await s3Client.send(new ListObjectsV2Command({
          Bucket: S3_BUCKET,
          Prefix: statePrefix,
          Delimiter: '/',
          ContinuationToken: cityContinuationToken,
        }));
        
        const cityPrefixes = (cityResponse.CommonPrefixes || []).map(p => p.Prefix);
        
        // Check if this city has violations for this brand
        // (by checking manifest's brands field - no need to load violation files!)
        for (const cityPrefix of cityPrefixes) {
          try {
            const manifest = await getManifest(cityPrefix);
            
            // Fast check: if manifest has brands array and it includes this brand, add it
            if (manifest.brands && Array.isArray(manifest.brands) && manifest.brands.includes(brandPk)) {
              cityPaths.push(cityPrefix);
            } else if (manifest.recent5 && Object.keys(manifest.recent5).length > 0) {
              // Fallback: if brands array doesn't exist (old data), check recent5
              // This is slower but handles backwards compatibility
              // Sample a few violations to check brand
              const violationIdsToCheck = [];
              for (const [type, ids] of Object.entries(manifest.recent5)) {
                if (Array.isArray(ids) && ids.length > 0) {
                  violationIdsToCheck.push(ids[0]); // Just check first one from each type
                  if (violationIdsToCheck.length >= 3) break; // Limit to 3 for speed
                }
              }
              
              if (violationIdsToCheck.length > 0) {
                const listResponse = await s3Client.send(new ListObjectsV2Command({
                  Bucket: S3_BUCKET,
                  Prefix: cityPrefix,
                }));
                
                const allViolationKeys = (listResponse.Contents || [])
                  .filter(obj => obj.Key.endsWith('.json') && 
                                !obj.Key.includes('manifest') && 
                                !obj.Key.includes('stats'))
                  .map(obj => obj.Key);
                
                // Check if any sampled violation belongs to this brand
                for (const violationId of violationIdsToCheck) {
                  const violationKey = allViolationKeys.find(key => 
                    key.includes(`/${violationId}.json`)
                  );
                  
                  if (violationKey) {
                    const violation = await loadViolationFromS3(violationKey);
                    if (violation && violation.pk === brandPk) {
                      cityPaths.push(cityPrefix);
                      // Update manifest with brands field for future fast lookups
                      if (!manifest.brands) {
                        manifest.brands = [];
                      }
                      if (!manifest.brands.includes(brandPk)) {
                        manifest.brands.push(brandPk);
                        await writeManifest(cityPrefix, manifest);
                      }
                      break;
                    }
                  }
                }
              }
            }
          } catch (error) {
            // Skip if we can't check (manifest might not exist yet)
            // This is fine - cities without manifests will be skipped
          }
        }
        
        cityContinuationToken = cityResponse.NextContinuationToken;
      } while (cityContinuationToken);
    }
    
    continuationToken = response.NextContinuationToken;
  } while (continuationToken);
  
  return cityPaths;
}

/**
 * Process a single state - called from SQS message
 * Simple: process state, sync stats, exit. SQS handles sequencing.
 */
async function processSingleState(state) {
  console.log(`[SQS] Processing state: ${state}`);
  console.log(`Table: ${TABLE_NAME}`);
  console.log(`Bucket: ${S3_BUCKET}`);
  console.log(`Brands: ${BRANDS.map(b => b.name).join(', ')}\n`);
  
  const startTime = Date.now();
  const allResults = {};
  
  // Initialize results for each brand
  for (const brand of BRANDS) {
    allResults[brand.name] = {
      brand: brand.name,
      citiesSynced: 0,
      citiesUpdated: 0,
      statesSynced: 0,
      errors: [],
    };
  }
  
  try {
    // No lock needed for SQS processing - each message is for a unique state
    // Multiple Lambdas can process different states in parallel without conflict
    // SQS handles sequencing and ensures each state is only processed once
    
    try {
      // Find state path
      const nationalPath = getNationalPath('USA');
      const statePath = `${nationalPath}${state}/`;
      
      console.log(`Processing state ${state}\n`);
      
      // List cities in this state
      const cityPaths = [];
      let cityContinuationToken = undefined;
      
      do {
        const cityResponse = await s3Client.send(new ListObjectsV2Command({
          Bucket: S3_BUCKET,
          Prefix: statePath,
          Delimiter: '/',
          ContinuationToken: cityContinuationToken,
        }));
        
        const cityPrefixes = (cityResponse.CommonPrefixes || []).map(p => p.Prefix);
        cityPaths.push(...cityPrefixes);
        
        cityContinuationToken = cityResponse.NextContinuationToken;
      } while (cityContinuationToken);
      
      console.log(`  Found ${cityPaths.length} cities in ${state}`);
      
      // Process each city - sync all brands that have data there
      for (const cityPath of cityPaths) {
        try {
          const parts = cityPath.replace('National/USA/', '').split('/');
          if (parts.length >= 2) {
            const citySlug = parts[1];
            
            // Load manifest once for this city
            const cityManifest = await getManifest(cityPath);
            const brandsInCity = cityManifest.brands || [];
            
            // Sync each brand that has data in this city
            for (const brand of BRANDS) {
              if (brandsInCity.includes(brand.pk)) {
                try {
                  const updates = await syncCityManifest(cityPath, citySlug, state, brand.pk);
                  allResults[brand.name].citiesSynced++;
                  
                  if (updates > 0) {
                    allResults[brand.name].citiesUpdated++;
                  }
                  
                  // Sync city stats for this brand
                  try {
                    let cityStats = cityManifest.stats?.[brand.pk];
                    
                    if (!cityStats || !cityStats.totalViolations) {
                      console.log(`    [WARN] Stats missing for ${citySlug} (${brand.name}), recalculating...`);
                      cityStats = await calculateCityStatsFromS3(cityPath, brand.pk);
                      
                      if (!cityManifest.stats) {
                        cityManifest.stats = {};
                      }
                      cityManifest.stats[brand.pk] = cityStats;
                      await writeManifest(cityPath, cityManifest);
                    }
                    
                    await updateCityStats(citySlug, cityStats, brand.pk);
                  } catch (statsError) {
                    console.error(`    [ERROR] Error processing stats for ${citySlug} (${brand.name}):`, statsError.message);
                  }
                } catch (error) {
                  allResults[brand.name].errors.push({ cityPath, error: error.message });
                  console.error(`    [ERROR] Error syncing ${cityPath} (${brand.name}):`, error.message);
                }
              }
            }
          }
        } catch (error) {
          console.error(`  [ERROR] Error processing city ${cityPath}:`, error.message);
        }
      }
      
      // After processing all cities, sync state stats for all brands
      console.log(`  Syncing state stats for ${state}...`);
      for (const brand of BRANDS) {
        try {
          const stateManifest = await getManifest(statePath);
          let stateStats = stateManifest.stats?.[brand.pk];
          
          if (!stateStats || !stateStats.totalViolations) {
            stateStats = await calculateStateStatsFromCities(state, brand.pk);
            if (!stateManifest.stats) {
              stateManifest.stats = {};
            }
            stateManifest.stats[brand.pk] = stateStats;
            await writeManifest(statePath, stateManifest);
          }
          
          await updateStateStats(state, stateStats, brand.pk);
          allResults[brand.name].statesSynced++;
        } catch (error) {
          console.error(`    [ERROR] Error syncing state stats for ${state} (${brand.name}):`, error.message);
        }
      }
      
      const elapsedSeconds = (Date.now() - startTime) / 1000;
      console.log(`  [OK] Completed state ${state} in ${elapsedSeconds.toFixed(1)}s`);
      
      // Log completion for all brands
      for (const brand of BRANDS) {
        await appendDailyLog('sync', brand.pk, {
          status: 'completed',
          message: `${brand.name} - ${state} synced`,
          state,
          citiesSynced: allResults[brand.name].citiesSynced,
          citiesUpdated: allResults[brand.name].citiesUpdated,
          elapsedSeconds: elapsedSeconds.toFixed(1),
        });
      }
      
      return {
        brands: allResults,
        summary: {
          message: `State ${state} processed successfully`,
          state,
          totalDuration: elapsedSeconds.toFixed(2),
        },
      };
    } catch (error) {
      console.error(`[ERROR] Error processing state ${state}:`, error.message);
      throw error;
    }
  } catch (error) {
    console.error('[ERROR] Daily sync failed:', error);
    throw error;
  }
}

// Lambda handler
export const handler = async (event) => {
  console.log('Event:', JSON.stringify(event, null, 2));
  
  // Handle SQS events (one state per message, or special "USA" message for national stats)
  if (event.Records && Array.isArray(event.Records)) {
    // SQS event - process one state per message, or "USA" for national stats
    const record = event.Records[0];
    let messageBody;
    const bodyStr = record.body || '';
    
    // Try to parse as JSON first
    try {
      messageBody = JSON.parse(bodyStr);
    } catch (parseError) {
      // JSON parse failed - try to extract state from malformed JSON
      // This handles cases like {state:RI} from earlier PowerShell commands
      // No need to log error - we're handling it gracefully
      let stateMatch = bodyStr.match(/"state"\s*:\s*"([A-Z]{2,3})"/) || 
                       bodyStr.match(/state\s*:\s*"([A-Z]{2,3})"/) ||
                       bodyStr.match(/state\s*:\s*([A-Z]{2,3})/) ||
                       bodyStr.match(/state[:\s]+([A-Z]{2,3})/);
      
      if (stateMatch && stateMatch[1]) {
        const state = stateMatch[1];
        console.log(`[INFO] Extracted state from malformed JSON: ${state}`);
        messageBody = { state };
      } else {
        // Last resort: try to find any 2-3 letter uppercase code
        const anyStateMatch = bodyStr.match(/([A-Z]{2,3})/);
        if (anyStateMatch) {
          const state = anyStateMatch[1];
          console.log(`[INFO] Extracted state code (fallback): ${state}`);
          messageBody = { state };
        } else {
          // Only log error if we truly can't extract the state
          console.error('[ERROR] Could not extract state from message:', bodyStr);
          return {
            statusCode: 200, // Return 200 so SQS doesn't retry
            body: JSON.stringify({ error: 'Invalid JSON in SQS message', body: bodyStr }),
          };
        }
      }
    }
    
    const state = messageBody.state;
    
    if (!state) {
      console.error('[ERROR] SQS message missing state');
      return { statusCode: 200, body: JSON.stringify({ error: 'Missing state in SQS message' }) };
    }
    
    // Special handling for "USA" message - update national stats only
    if (state === 'USA') {
      console.log('[SQS] Processing national stats aggregation (USA)');
      
      try {
        for (const brand of BRANDS) {
          try {
            const nationalStats = await calculateNationalStatsFromStates(brand.pk);
            if (nationalStats && nationalStats.totalViolations > 0) {
              await updateNationalStats('USA', nationalStats, brand.pk);
              console.log(`  [OK] Updated national stats for ${brand.name}: ${nationalStats.totalViolations} violations`);
            }
          } catch (error) {
            console.error(`  [ERROR] Error updating national stats for ${brand.name}:`, error.message);
          }
        }
        
        return {
          statusCode: 200,
          body: JSON.stringify({
            message: 'National stats updated successfully',
          }),
        };
      } catch (error) {
        console.error('[ERROR] Unexpected error updating national stats:', error);
        return {
          statusCode: 200,
          body: JSON.stringify({ 
            error: 'Unexpected error updating national stats', 
            message: error.message,
            failed: true,
          }),
        };
      }
    }
    
    // Regular state processing
    console.log(`[SQS] Processing state from queue: ${state}`);
    
    try {
      const results = await processSingleState(state);
      
      // Always return 200 so SQS doesn't retry
      if (results.summary?.skipped) {
        return {
          statusCode: 200,
          body: JSON.stringify({
            message: `State ${state} skipped`,
            results,
          }),
        };
      }
      
      return {
        statusCode: 200,
        body: JSON.stringify({
          message: `State ${state} processed`,
          results,
        }),
      };
    } catch (error) {
      // Unexpected error - log but don't retry
      console.error(`[ERROR] Unexpected error processing state ${state}:`, error);
      return {
        statusCode: 200, // Return 200 so SQS doesn't retry
        body: JSON.stringify({ 
          error: `Unexpected error processing state ${state}`, 
          message: error.message,
          failed: true,
        }),
      };
    }
  }
  
  // Handle EventBridge trigger - populate SQS queue with all states
  if (event.source === 'aws.events' || event['detail-type'] === 'Scheduled Event') {
    console.log('[INFO] EventBridge trigger detected, populating SQS queue with all states');
    
    const allStates = ['AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 
                       'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
                       'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
                       'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
                       'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY', 'DC',
                       'PR', 'VI', 'GU', 'AS', 'MP'];
    
    let queued = 0;
    for (const state of allStates) {
      try {
        await sendToSQS({ state });
        queued++;
      } catch (error) {
        console.error(`[ERROR] Failed to queue state ${state}:`, error.message);
      }
    }
    
    // Queue special "USA" message to update national stats after all states complete
    try {
      await sendToSQS({ state: 'USA' });
      queued++;
      console.log('[OK] Queued national stats update (USA)');
    } catch (error) {
      console.error('[ERROR] Failed to queue national stats update:', error.message);
    }
    
    console.log(`[OK] Queued ${queued} messages to SQS (${allStates.length} states + 1 national stats update)`);
    
    return {
      statusCode: 200,
      body: JSON.stringify({
        message: `Daily sync started - ${queued} states queued`,
        statesQueued: queued,
      }),
    };
  }
  
  // Manual invocation (for testing)
  console.log('[WARN] Manual invocation - use EventBridge or SQS');
  return {
    statusCode: 400,
    body: JSON.stringify({ error: 'Use EventBridge trigger or SQS message' }),
  };
};

/**
 * Check if any brand has incomplete progress (needs resume)
 */
async function hasIncompleteProgress() {
  try {
    for (const brand of BRANDS) {
      const progress = await getSyncProgress(brand.pk);
      if (progress && !progress.completed) {
        return true;
      }
    }
    return false;
  } catch (error) {
    console.error('Error checking progress:', error);
    return false;
  }
}

/**
 * Send message to SQS to trigger next state processing
 * @param {Object} message - Message payload { state: "AL" }
 */
async function sendToSQS(message) {
  try {
    await sqsClient.send(new SendMessageCommand({
      QueueUrl: SYNC_QUEUE_URL,
      MessageBody: JSON.stringify(message),
    }));
    console.log(`[OK] Sent SQS message: ${JSON.stringify(message)}`);
  } catch (error) {
    console.error('[ERROR] Failed to send SQS message:', error.message);
    throw error; // Fail fast - we need this to work
  }
}

/**
 * Self-invoke Lambda to continue sync (auto-resume for EventBridge mode)
 */
async function selfInvoke() {
  const functionName = process.env.AWS_LAMBDA_FUNCTION_NAME;
  if (!functionName) {
    console.warn('[WARN] AWS_LAMBDA_FUNCTION_NAME not set, cannot self-invoke');
    return;
  }
  
  try {
    const { LambdaClient, InvokeCommand } = await import('@aws-sdk/client-lambda');
    const lambdaClient = new LambdaClient({ region: process.env.AWS_REGION || 'us-east-1' });
    
    await lambdaClient.send(new InvokeCommand({
      FunctionName: functionName,
      InvocationType: 'Event', // Async invocation
      Payload: JSON.stringify({ autoResume: true }),
    }));
    console.log('[OK] Self-invoked Lambda to continue sync');
  } catch (error) {
    console.error('[ERROR] Failed to self-invoke:', error.message);
  }
}

