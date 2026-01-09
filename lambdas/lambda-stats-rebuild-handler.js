// VentureOS Stats Rebuild Lambda Handler
// Version: 2.0.0 - Graceful Exit with Progress Tracking
//
// This handler slugs through all violations in S3 and rebuilds stats from scratch.
// Unlike the daily sync (which takes shortcuts via manifests), this one does the full
// trail - reading every violation file to ensure accuracy. It's thorough, not fast.
//
// Features:
// - Processes violations state-by-state, city-by-city (no shortcuts!)
// - Graceful exits: time-based (885s) and size-based (350KB warning, 400KB hard limit)
// - Auto-resume: saves progress and can pick up where it left off
// - One-state-at-a-time mode: processes a state, then auto-invokes the next
// - Lock mechanism: prevents concurrent rebuilds (because two slugs shouldn't cross paths)

import { S3Client, ListObjectsV2Command, GetObjectCommand, PutObjectCommand } from '@aws-sdk/client-s3';
import { getManifest, writeManifest } from './utils/recent5Manifest.js';
import { acquireRebuildLock, releaseRebuildLock } from './utils/progressTracker.js';
import { updateCompanyStats } from './utils/companyStats.js';

const s3Client = new S3Client({ region: process.env.AWS_REGION || 'us-east-1' });
const S3_BUCKET = process.env.S3_BUCKET || 'sirsluginston-ventureos-data';

const BRANDS = [
  { pk: 'BRAND#OSHAtrail', name: 'OSHATrail' },
  { pk: 'BRAND#TransportTrail', name: 'TransportTrail' },
];

/**
 * Process a single city - called from SQS message
 * Simple: process city violations, update city manifest and company stats, exit.
 */
async function processSingleCity(cityPath, filterBrandPk = null) {
  console.log(`[SQS] Processing city: ${cityPath}`);
  console.log(`Bucket: ${S3_BUCKET}`);
  if (filterBrandPk) {
    console.log(`Filtering by brand: ${filterBrandPk}`);
  }
  
  try {
    // Acquire lock to prevent concurrent processing
    const lockAcquired = await acquireRebuildLock();
    if (!lockAcquired) {
      console.log('[SKIP] Another rebuild instance is already running. Exiting.');
      return {
        success: false,
        message: 'Rebuild already in progress, skipping',
        skipped: true,
      };
    }
    
    try {
      // Process city violations
      const result = await rebuildCityStats(cityPath, filterBrandPk);
      if (!result) {
        console.warn(`[WARN] rebuildCityStats returned null for ${cityPath}`);
        return {
          success: false,
          message: 'No data found for city',
          cityPath,
        };
      }
      
      const { statsByBrand = {}, brands = [], companyDataByBrand: rawCompanyData = {} } = result;
      const companyDataByBrand = rawCompanyData && typeof rawCompanyData === 'object' ? rawCompanyData : {};
      
      if (Object.keys(statsByBrand).length === 0) {
        console.log(`[SKIP] No violations found for ${cityPath}`);
        return {
          success: true,
          message: 'City processed (no violations)',
          cityPath,
        };
      }
      
      // Update city manifest
      const cityManifest = await getManifest(cityPath);
      if (!cityManifest.stats) {
        cityManifest.stats = {};
      }
      cityManifest.brands = brands;
      
      for (const [brandPk, cityStats] of Object.entries(statsByBrand)) {
        cityManifest.stats[brandPk] = cityStats;
      }
      
      await writeManifest(cityPath, cityManifest);
      console.log(`[OK] Updated city manifest for ${cityPath}`);
      
      // Update company stats for each brand
      for (const [brandPk, companies] of Object.entries(companyDataByBrand)) {
        if (!companies || !Array.isArray(companies)) continue;
        
        for (const company of companies) {
          try {
            const companyStats = {
              totalViolations: company.count,
              totalFines: company.totalFines,
              averageFine: company.count > 0 ? company.totalFines / company.count : 0,
              citiesCount: 1, // Will be aggregated later if needed
              lastUpdated: new Date().toISOString(),
            };
            
            await updateCompanyStats(company.slug, companyStats, brandPk);
          } catch (error) {
            console.error(`[ERROR] Failed to update company stats for ${company.slug} (${brandPk}):`, error.message);
          }
        }
      }
      
      console.log(`[OK] Completed city ${cityPath}`);
      
      return {
        success: true,
        message: `City processed successfully`,
        cityPath,
        brandsProcessed: Object.keys(statsByBrand).length,
        violationsProcessed: Object.values(statsByBrand).reduce((sum, s) => sum + (s.totalViolations || 0), 0),
      };
    } catch (error) {
      console.error(`[ERROR] Error processing city ${cityPath}:`, error.message);
      console.error(`[ERROR] Stack:`, error.stack);
      
      // Return success (not error) so SQS doesn't retry forever
      // Log the failure for manual review if needed
      return {
        success: false,
        message: `City processing failed: ${error.message}`,
        cityPath,
        error: error.message,
        failed: true,
      };
    } finally {
      // Always release lock when done
      await releaseRebuildLock();
    }
  } catch (error) {
    console.error('[ERROR] Rebuild failed:', error);
    // Release lock even on error
    await releaseRebuildLock().catch(() => {});
    
    // Return success (not throw) so SQS doesn't retry
    return {
      success: false,
      message: `Rebuild failed: ${error.message}`,
      error: error.message,
      failed: true,
    };
  }
}

/**
 * Rebuild stats for a single city by reading all violations from S3
 * This is the heavy lifting - iterates through every violation file in the city path
 * and aggregates stats by brand. No manifest shortcuts here.
 */
async function rebuildCityStats(cityPath, filterBrandPk = null) {
  const statsByBrand = {};
  const brands = new Set();
  let continuationToken = undefined;
  
  do {
    const response = await s3Client.send(new ListObjectsV2Command({
      Bucket: S3_BUCKET,
      Prefix: cityPath,
      ContinuationToken: continuationToken,
    }));
    
    const violationKeys = (response.Contents || [])
      .filter(obj => obj.Key.endsWith('.json') && 
                    !obj.Key.includes('manifest') && 
                    !obj.Key.includes('stats'))
      .map(obj => obj.Key);
    
    for (const key of violationKeys) {
      try {
        const objResponse = await s3Client.send(new GetObjectCommand({
          Bucket: S3_BUCKET,
          Key: key,
        }));
        const body = await objResponse.Body.transformToString();
        const violation = JSON.parse(body);
        
        const violationBrandPk = violation.pk;
        if (!violationBrandPk) continue;
        
        brands.add(violationBrandPk);
        
        if (filterBrandPk && violationBrandPk !== filterBrandPk) {
          continue;
        }
        
        if (!statsByBrand[violationBrandPk]) {
          statsByBrand[violationBrandPk] = {
            totalViolations: 0,
            totalFines: 0,
            violationCounts: {},
            lastUpdated: new Date().toISOString(),
          };
        }
        
        const stats = statsByBrand[violationBrandPk];
        stats.totalViolations++;
        
        const fine = violation.ViolationData?.citation?.penalty || 
                    violation.ViolationData?.fine || 
                    0;
        stats.totalFines += parseFloat(fine) || 0;
        
        const violationType = violation.ViolationType || 
                             violation.ViolationData?.violationType || 
                             'Unknown';
        stats.violationCounts[violationType] = (stats.violationCounts[violationType] || 0) + 1;
        
        const companyName = violation.ViolationData?.company || 
                           violation.Company || 
                           violation.ViolationData?.establishmentName || 
                           'Unknown';
        const companySlug = violation.ViolationData?.establishment?.slug || 
                           violation.CompanySlug || 
                           companyName.toLowerCase().replace(/[^a-z0-9]+/g, '-');
        
        if (!statsByBrand[violationBrandPk]._companyMap) {
          statsByBrand[violationBrandPk]._companyMap = new Map();
        }
        if (!statsByBrand[violationBrandPk]._companyMap.has(companySlug)) {
          statsByBrand[violationBrandPk]._companyMap.set(companySlug, {
            name: companyName,
            slug: companySlug,
            count: 0,
            totalFines: 0,
          });
        }
        const companyData = statsByBrand[violationBrandPk]._companyMap.get(companySlug);
        companyData.count++;
        companyData.totalFines += parseFloat(fine) || 0;
      } catch (error) {
        console.error(`Error processing violation ${key}:`, error.message);
      }
    }
    
    continuationToken = response.NextContinuationToken;
  } while (continuationToken);
  
  const companyDataByBrand = {};
  for (const [brandPk, stats] of Object.entries(statsByBrand)) {
    if (stats._companyMap) {
      companyDataByBrand[brandPk] = Array.from(stats._companyMap.values());
      delete stats._companyMap;
    }
  }
  
  return { statsByBrand, brands: Array.from(brands), companyDataByBrand };
}

/**
 * Check if the rebuild lock has been acquired (indicating next state started)
 * OR if the next state already completed (for last state scenario)
 * @param {string} nextState - The state we're waiting for
 * @param {number} maxWaitMs - Maximum time to wait in milliseconds
 * @param {number} checkIntervalMs - How often to check in milliseconds
 * @returns {Promise<boolean>} True if lock was acquired or state completed, false if timeout
 */
async function verifyNextStateStarted(nextState, maxWaitMs = 10000, checkIntervalMs = 1000) {
  const lockKey = '_progress/rebuild-lock.json';
  const progressKey = '_progress/rebuild-ALL.json';
  const startTime = Date.now();
  
  while (Date.now() - startTime < maxWaitMs) {
    try {
      // Check if lock exists (next state started)
      const lockResponse = await s3Client.send(new GetObjectCommand({
        Bucket: S3_BUCKET,
        Key: lockKey,
      }));
      
      const lockBody = await lockResponse.Body.transformToString();
      const lock = JSON.parse(lockBody);
      const lockTime = new Date(lock.lockedAt).getTime();
      const now = Date.now();
      
      // If lock was created recently (within last 30 seconds), next state started!
      if (now - lockTime < 30000) {
        return true;
      }
    } catch (error) {
      // Lock doesn't exist - check if next state already completed (last state scenario)
      if (error.name === 'NoSuchKey' || error.$metadata?.httpStatusCode === 404) {
        try {
          // Check progress file - if next state is past the current state, it completed
          const progressResponse = await s3Client.send(new GetObjectCommand({
            Bucket: S3_BUCKET,
            Key: progressKey,
          }));
          
          const progressBody = await progressResponse.Body.transformToString();
          const progress = JSON.parse(progressBody);
          
          // If progress shows a state after nextState, or if nextState completed and moved on
          // This handles the case where the last state completes quickly
          const allStates = ['AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 
                           'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
                           'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
                           'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
                           'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY', 'DC',
                           'PR', 'VI', 'GU', 'AS', 'MP'];
          
          const nextStateIndex = allStates.indexOf(nextState);
          const currentStateIndex = allStates.indexOf(progress.currentState);
          
          // If current state is past next state, next state completed
          if (currentStateIndex > nextStateIndex) {
            console.log(`  [VERIFY] Next state ${nextState} appears to have completed (current: ${progress.currentState})`);
            return true;
          }
          
          // If we're at the last state and lock doesn't exist, it might have completed
          if (nextStateIndex === allStates.length - 1 && !progress.lastCityPath) {
            // Last state completed quickly - give it a moment
            await new Promise(resolve => setTimeout(resolve, checkIntervalMs));
            continue;
          }
        } catch (progressError) {
          // Progress file doesn't exist or error - keep waiting for lock
        }
        
        await new Promise(resolve => setTimeout(resolve, checkIntervalMs));
        continue;
      }
      throw error;
    }
    
    await new Promise(resolve => setTimeout(resolve, checkIntervalMs));
  }
  
  return false; // Timeout - next state didn't start
}

/**
 * Invoke the next state's rebuild asynchronously
 * When oneStateAtATime is enabled, this keeps the rebuild chain moving automatically.
 * Each state gets its own Lambda invocation, so we don't hit timeout limits.
 * Timeout limits are still protected by Graceful Exits.
 * 
 * Includes retry logic AND verification to ensure the next state actually starts.
 * This handles the case where async invocations are accepted but don't execute.
 */
async function invokeNextState(progressKey, nextState, options) {
  const nextOptions = {
    ...options,
    startState: nextState,
  };
  
  console.log(`  Invoking next state: ${nextState}`);
  
  const maxRetries = 3;
  let lastError = null;
  let invocationSucceeded = false;
  
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      const response = await lambdaClient.send(new InvokeCommand({
        FunctionName: REBUILD_FUNCTION_NAME,
        InvocationType: 'Event',
        Payload: JSON.stringify({ options: nextOptions }),
      }));
      
      // Check if invocation was accepted
      const statusCode = response.StatusCode;
      if (statusCode === 202 || statusCode === 200) {
        console.log(`  [OK] Invocation accepted for next state ${nextState} (attempt ${attempt}/${maxRetries})`);
        invocationSucceeded = true;
        
        // Verify the next state actually started by checking if lock was acquired
        console.log(`  [VERIFY] Waiting up to 10 seconds to verify ${nextState} started...`);
        const started = await verifyNextStateStarted(nextState, 10000, 1000);
        
        if (started) {
          console.log(`  [OK] Verified next state ${nextState} has started (lock acquired)`);
          return; // Success!
        } else {
          console.warn(`  [WARN] Next state ${nextState} invocation accepted but didn't start within 10 seconds`);
          // Continue to retry
        }
      } else {
        throw new Error(`Unexpected status code: ${statusCode}`);
      }
    } catch (error) {
      lastError = error;
      console.error(`  [ERROR] Failed to invoke next state ${nextState} (attempt ${attempt}/${maxRetries}):`, error.message);
    }
    
    if (attempt < maxRetries) {
      // Exponential backoff: wait 2s, 4s, 8s
      const waitTime = Math.pow(2, attempt) * 1000;
      console.log(`  [RETRY] Waiting ${waitTime}ms before retry...`);
      await new Promise(resolve => setTimeout(resolve, waitTime));
    }
  }
  
  // All retries failed - log critical error
  if (invocationSucceeded) {
    console.error(`  [CRITICAL] Next state ${nextState} invocation was accepted but never started after ${maxRetries} attempts.`);
  } else {
    console.error(`  [CRITICAL] Failed to invoke next state ${nextState} after ${maxRetries} attempts. Last error:`, lastError?.message);
  }
  console.error(`  [CRITICAL] Rebuild chain may be broken. Manual intervention may be required to continue from state ${nextState}.`);
  
  // Don't throw - we've already logged the error. The current state completed successfully,
  // so we don't want to fail the entire rebuild. But this is a problem that needs attention.
}

/**
 * Main rebuild function - orchestrates the entire stats rebuild process
 * Processes states sequentially, aggregating stats as it goes. Handles graceful exits,
 * size limits, and can auto-invoke the next state if oneStateAtATime is enabled.
 * 
 * This is where the magic happens (or the slow, methodical slug-crawl, depending on
 * your perspective). Either way, it gets the job done.
 */
async function rebuildStats(options = {}) {
  console.log('[REBUILD] Starting Stats Rebuild');
  console.log(`[REBUILD] Bucket: ${S3_BUCKET}`);
  console.log(`[REBUILD] Options:`, JSON.stringify(options, null, 2));
  
  // Acquire lock to prevent concurrent rebuilds (we're territorial slugs)
  const lockAcquired = await acquireRebuildLock();
  if (!lockAcquired) {
    console.log('[SKIP] Another rebuild instance is already running.');
    return {
      success: false,
      message: 'Rebuild already in progress',
      skipped: true,
    };
  }
  
  let lockReleased = false;
  try {
    const startTime = Date.now();
    const brandsToProcess = options.brandPk 
      ? BRANDS.filter(b => b.pk === options.brandPk)
      : BRANDS;
    
    const allStates = ['AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 
                       'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
                       'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
                       'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
                       'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY', 'DC',
                       'PR', 'VI', 'GU', 'AS', 'MP'];
    
    const progressKey = options.brandPk || 'ALL';
    let startStateIndex = 0;
    let lastCityPath = null;
    let citiesProcessed = 0;
    
    if (options.startState) {
      startStateIndex = allStates.indexOf(options.startState);
      if (startStateIndex === -1) startStateIndex = 0;
      console.log(`  [CLAIM] Starting at state: ${options.startState}`);
    } else {
      const existingProgress = await getRebuildProgress(progressKey);
      if (existingProgress && !existingProgress.completed) {
        startStateIndex = allStates.indexOf(existingProgress.currentState);
        if (startStateIndex === -1) startStateIndex = 0;
        lastCityPath = existingProgress.lastCityPath;
        citiesProcessed = existingProgress.citiesProcessed || 0;
        console.log(`  [RESUME] Resuming from: ${existingProgress.currentState}`);
      }
    }
    
    let totalCitiesWithData = 0;
    let resumeFromCity = lastCityPath !== null;
    
    const nationalStatsByBrand = {};
    for (const brand of brandsToProcess) {
      nationalStatsByBrand[brand.pk] = {
        totalViolations: 0,
        totalFines: 0,
        stateCount: 0,
        cityCount: 0,
        violationCounts: {},
        lastUpdated: new Date().toISOString(),
      };
    }
    
    for (let i = startStateIndex; i < allStates.length; i++) {
      const state = allStates[i];
      const statePath = getStatePath('USA', state);
      
      await getManifest(statePath);
      
      console.log(`\n  Processing state: ${state} (${i + 1}/${allStates.length})`);
      
      await saveRebuildProgress(progressKey, state, null, citiesProcessed);
      
      const stateStatsByBrand = {};
      const companyStatsByBrand = {};
      
      for (const brand of brandsToProcess) {
        stateStatsByBrand[brand.pk] = {
          totalViolations: 0,
          totalFines: 0,
          cityCount: 0,
          violationCounts: {},
          lastUpdated: new Date().toISOString(),
        };
        companyStatsByBrand[brand.pk] = {};
      }
      
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
      
      console.log(`    Found ${cityPaths.length} cities`);
      
      let cityStartIndex = 0;
      if (resumeFromCity && lastCityPath) {
        const resumeIndex = cityPaths.findIndex(p => p === lastCityPath);
        if (resumeIndex >= 0) {
          cityStartIndex = resumeIndex + 1;
          console.log(`    Resuming from city ${cityStartIndex + 1}/${cityPaths.length}`);
        }
        resumeFromCity = false;
      }
      
      for (let j = cityStartIndex; j < cityPaths.length; j++) {
        const cityPath = cityPaths[j];
        
        try {
          const result = await rebuildCityStats(cityPath, options.brandPk || null);
          if (!result) {
            console.warn(`    [WARN] rebuildCityStats returned null for ${cityPath}`);
            continue;
          }
          const { statsByBrand = {}, brands = [], companyDataByBrand: rawCompanyData = {} } = result;
          const companyDataByBrand = rawCompanyData && typeof rawCompanyData === 'object' ? rawCompanyData : {};
          
          if (Object.keys(statsByBrand).length > 0) {
            totalCitiesWithData++;
            
            const cityManifest = await getManifest(cityPath);
            if (!cityManifest.stats) {
              cityManifest.stats = {};
            }
            cityManifest.brands = brands;
            
            for (const [brandPk, cityStats] of Object.entries(statsByBrand)) {
              cityManifest.stats[brandPk] = cityStats;
              
              if (stateStatsByBrand[brandPk]) {
                const stateStats = stateStatsByBrand[brandPk];
                stateStats.totalViolations += cityStats.totalViolations || 0;
                stateStats.totalFines += cityStats.totalFines || 0;
                stateStats.cityCount++;
                
                for (const [type, count] of Object.entries(cityStats.violationCounts || {})) {
                  stateStats.violationCounts[type] = (stateStats.violationCounts[type] || 0) + count;
                }
                
                const testStateItem = {
                  pk: brandPk,
                  sk: `STATE_STATS#${state}`,
                  ...stateStats,
                };
                const sizeBytes = estimateItemSize(testStateItem);
                const sizeKB = sizeBytes / 1024;
                const warningSizeKB = 350;  // Start getting nervous at 350KB
                const maxSizeKB = 400;      // Hard DynamoDB limit - can't exceed this
                
                // Size check: if we've hit the limit, revert the last city's aggregation
                // This ensures clean data and prevents slimy DynamoDB errors
                if (sizeKB >= maxSizeKB) {
                  const citySlug = cityPath.split('/').pop() || cityPath;
                  console.log(`    [ERROR] State stats exceeded 400KB (${sizeKB.toFixed(1)}KB) after ${citySlug}. Reverting...`);
                  
                  stateStats.totalViolations -= cityStats.totalViolations || 0;
                  stateStats.totalFines -= cityStats.totalFines || 0;
                  stateStats.cityCount--;
                  
                  for (const [type, count] of Object.entries(cityStats.violationCounts || {})) {
                    stateStats.violationCounts[type] = (stateStats.violationCounts[type] || 0) - count;
                    if (stateStats.violationCounts[type] <= 0) {
                      delete stateStats.violationCounts[type];
                    }
                  }
                  
                  const revertedItem = {
                    pk: brandPk,
                    sk: `STATE_STATS#${state}`,
                    ...stateStats,
                  };
                  const revertedSizeKB = estimateItemSize(revertedItem) / 1024;
                  
                  console.log(`    [OK] Reverted to ${revertedSizeKB.toFixed(1)}KB`);
                  
                  await saveRebuildProgress(progressKey, state, cityPath, citiesProcessed);
                  
                  const brand = BRANDS.find(b => b.pk === brandPk);
                  await appendDailyLog('rebuild', brandPk, {
                    status: 'graceful_exit_size_revert',
                    message: `${brand?.name || brandPk} - ${state} reverted from ${sizeKB.toFixed(1)}KB to ${revertedSizeKB.toFixed(1)}KB`,
                    state,
                    city: citySlug,
                    sizeKB: sizeKB.toFixed(1),
                    revertedSizeKB: revertedSizeKB.toFixed(1),
                    citiesProcessed,
                    timestamp: new Date().toISOString(),
                  });
                  
                  return {
                    success: true,
                    message: `State ${state} stats exceeded 400KB limit. Reverted to ${revertedSizeKB.toFixed(1)}KB.`,
                    state,
                    city: citySlug,
                    sizeKB: sizeKB.toFixed(1),
                    revertedSizeKB: revertedSizeKB.toFixed(1),
                    citiesProcessed,
                    gracefulExit: true,
                    reverted: true,
                  };
                } else if (sizeKB >= warningSizeKB) {
                  const citySlug = cityPath.split('/').pop() || cityPath;
                  console.log(`    [GRACEFUL EXIT] ${sizeKB.toFixed(1)}KB. ${citySlug}`);
                  
                  const nextCityIndex = j + 1;
                  const nextCityPath = nextCityIndex < cityPaths.length ? cityPaths[nextCityIndex] : null;
                  await saveRebuildProgress(progressKey, state, nextCityPath, citiesProcessed);
                  
                  const brand = BRANDS.find(b => b.pk === brandPk);
                  await appendDailyLog('rebuild', brandPk, {
                    status: 'graceful_exit_size',
                    message: `${brand?.name || brandPk} - ${state} at ${sizeKB.toFixed(1)}KB`,
                    state,
                    city: citySlug,
                    sizeKB: sizeKB.toFixed(1),
                    citiesProcessed,
                    timestamp: new Date().toISOString(),
                  });
                  
                  return {
                    success: true,
                    message: `State ${state} stats reached ${sizeKB.toFixed(1)}KB limit. Graceful exit.`,
                    state,
                    city: citySlug,
                    sizeKB: sizeKB.toFixed(1),
                    citiesProcessed,
                    gracefulExit: true,
                  };
                }
              }
            }
            
            await writeManifest(cityPath, cityManifest);
            
            if (companyDataByBrand) {
              for (const [brandPk, companyData] of Object.entries(companyDataByBrand)) {
                if (companyStatsByBrand[brandPk]) {
                  for (const company of companyData) {
                    const { slug, name, count, totalFines } = company;
                    if (!companyStatsByBrand[brandPk][slug]) {
                      companyStatsByBrand[brandPk][slug] = {
                        name,
                        slug,
                        totalViolations: 0,
                        totalFines: 0,
                        cities: new Set(),
                      };
                    }
                    companyStatsByBrand[brandPk][slug].totalViolations += count;
                    companyStatsByBrand[brandPk][slug].totalFines += totalFines;
                    companyStatsByBrand[brandPk][slug].cities.add(cityPath);
                  }
                }
              }
            }
            
            const brandSummary = Object.entries(statsByBrand)
              .map(([pk, s]) => {
                const brandName = BRANDS.find(b => b.pk === pk)?.name || pk;
                return `${brandName}: ${s.totalViolations}`;
              })
              .join(', ');
            console.log(`    [OK] ${cityPath.split('/').pop()}: ${brandSummary}`);
          }
          
          citiesProcessed++;
          
          const elapsedSeconds = (Date.now() - startTime) / 1000;
          if (elapsedSeconds >= 885) {
            const pathParts = cityPath.split('/').filter(p => p);
            const cityName = pathParts[pathParts.length - 1] || cityPath.split('/').slice(-2)[0] || 'unknown';
            const timestamp = new Date().toISOString();
            const citySlug = cityPath.split('/').pop() || cityPath;
            const message = `${cityName} at ${timestamp} (${citiesProcessed} cities processed). Resuming...`;
            console.log(`  [GRACEFUL EXIT] Saved progress at ${elapsedSeconds.toFixed(1)}s. ${citySlug}`);
            await saveRebuildProgress(progressKey, state, cityPath, citiesProcessed);
            
            await appendDailyLog('rebuild', progressKey, {
              status: 'graceful_exit',
              message,
              state,
              city: cityName,
              citiesProcessed,
              elapsedSeconds: elapsedSeconds.toFixed(1),
              timestamp,
            });
            
            return {
              success: true,
              message,
              currentState: state,
              lastCityPath: cityPath,
              citiesProcessed,
              timestamp,
              resume: true,
            };
          }
          
          if (citiesProcessed % 50 === 0) {
            const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
            console.log(`    Progress: ${citiesProcessed} cities processed, ${totalCitiesWithData} with data (${elapsed}s elapsed)`);
          }
        } catch (error) {
          console.error(`    [ERROR] Error processing city ${cityPath}:`, error.message);
        }
      }
      
      if (!options.citiesOnly) {
        const stateManifest = await getManifest(statePath);
        if (!stateManifest.stats) {
          stateManifest.stats = {};
        }
        
        for (const brand of brandsToProcess) {
          const stateStats = stateStatsByBrand[brand.pk];
          if (stateStats && stateStats.totalViolations > 0) {
            stateManifest.stats[brand.pk] = stateStats;
            
            const nationalStats = nationalStatsByBrand[brand.pk];
            nationalStats.totalViolations += stateStats.totalViolations || 0;
            nationalStats.totalFines += stateStats.totalFines || 0;
            nationalStats.stateCount++;
            nationalStats.cityCount += stateStats.cityCount || 0;
            
            for (const [type, count] of Object.entries(stateStats.violationCounts || {})) {
              nationalStats.violationCounts[type] = (nationalStats.violationCounts[type] || 0) + count;
            }
            
            console.log(`    [OK] State ${state} (${brand.name}): ${stateStats.totalViolations} violations, ${stateStats.cityCount} cities`);
          }
        }
        
        await writeManifest(statePath, stateManifest);
        
        for (const brand of brandsToProcess) {
          const companies = companyStatsByBrand[brand.pk];
          if (companies && Object.keys(companies).length > 0) {
            console.log(`    Writing ${Object.keys(companies).length} company stats for ${brand.name}...`);
            
            for (const [companySlug, companyData] of Object.entries(companies)) {
              try {
                const companyStats = {
                  totalViolations: companyData.totalViolations,
                  totalFines: companyData.totalFines,
                  averageFine: companyData.totalViolations > 0 
                    ? companyData.totalFines / companyData.totalViolations 
                    : 0,
                  citiesCount: companyData.cities.size,
                  lastUpdated: new Date().toISOString(),
                };
                
                await updateCompanyStats(companySlug, companyStats, brand.pk);
              } catch (error) {
                console.error(`    [ERROR] Failed to write company stats for ${companySlug} (${brand.name}):`, error.message);
              }
            }
          }
        }
      }
      
      await saveRebuildProgress(progressKey, state, null, citiesProcessed);
      
      if (options.oneStateAtATime && i < allStates.length - 1) {
        const nextState = allStates[i + 1];
        // Release lock FIRST so next state can acquire it immediately
        await releaseRebuildLock().catch((error) => {
          console.warn('Error releasing rebuild lock:', error.message);
        });
        lockReleased = true; // Mark as released so finally block doesn't try again
        
        // Now invoke next state - it should be able to acquire the lock immediately
        await invokeNextState(progressKey, nextState, options);
        console.log(`  [OK] Completed state ${state}, invoked next state ${nextState}`);
        return {
          success: true,
          message: `Completed state ${state}, invoked next state ${nextState}`,
          currentState: state,
          nextState,
        };
      }
    }
    
    if (!options.citiesOnly) {
      const nationalPath = getNationalPath('USA');
      const nationalManifest = await getManifest(nationalPath);
      if (!nationalManifest.stats) {
        nationalManifest.stats = {};
      }
      
      for (const brand of brandsToProcess) {
        const nationalStats = nationalStatsByBrand[brand.pk];
        if (nationalStats && nationalStats.totalViolations > 0) {
          nationalManifest.stats[brand.pk] = nationalStats;
          console.log(`  [OK] National (${brand.name}): ${nationalStats.totalViolations} violations, ${nationalStats.cityCount} cities, ${nationalStats.stateCount} states`);
        }
      }
      
      await writeManifest(nationalPath, nationalManifest);
    }
    
    await clearRebuildProgress(progressKey);
    
    const duration = ((Date.now() - startTime) / 1000 / 60).toFixed(2);
    const successMessage = `Rebuild Completed Successfully`;
    await appendDailyLog('rebuild', progressKey, {
      status: 'completed',
      message: successMessage,
      duration: `${duration} minutes`,
      citiesProcessed,
      citiesWithData: totalCitiesWithData,
    });
    console.log(`\n[SUCCESS] ${successMessage} in ${duration} minutes`);
    
    return {
      success: true,
      message: 'All rebuilds completed successfully',
      duration: `${duration} minutes`,
    };
  } finally {
    if (!lockReleased) {
      await releaseRebuildLock().catch((error) => {
        console.warn('Error releasing rebuild lock:', error.message);
      });
      lockReleased = true;
    }
  }
}

export const handler = async (event) => {
  console.log('[HANDLER] Event:', JSON.stringify(event, null, 2));
  
  // Handle SQS events (one city per message - simple!)
  if (event.Records && Array.isArray(event.Records)) {
    // SQS event - process one city per message
    const record = event.Records[0];
    const messageBody = JSON.parse(record.body);
    const cityPath = messageBody.cityPath;
    const brandPk = messageBody.brandPk || null;
    
    if (!cityPath) {
      console.error('[ERROR] SQS message missing cityPath');
      return { statusCode: 400, body: JSON.stringify({ error: 'Missing cityPath in SQS message' }) };
    }
    
    console.log(`[SQS] Processing city from queue: ${cityPath}`);
    
    try {
      const results = await processSingleCity(cityPath, brandPk);
      
      // Always return 200 so SQS doesn't retry
      // Check results.success to see if it actually succeeded
      if (results.success) {
        return {
          statusCode: 200,
          body: JSON.stringify({
            message: `City ${cityPath} processed successfully`,
            results,
          }),
        };
      } else {
        // Processing failed but we handled it gracefully
        console.warn(`[WARN] City ${cityPath} failed but handled gracefully:`, results.message);
        return {
          statusCode: 200, // Still 200 so SQS doesn't retry
          body: JSON.stringify({
            message: `City ${cityPath} failed (logged for review)`,
            results,
            failed: true,
          }),
        };
      }
    } catch (error) {
      // Unexpected error - log but don't retry
      console.error(`[ERROR] Unexpected error processing city ${cityPath}:`, error);
      return {
        statusCode: 200, // Return 200 so SQS doesn't retry
        body: JSON.stringify({ 
          error: `Unexpected error processing city ${cityPath}`, 
          message: error.message,
          failed: true,
        }),
      };
    }
  }
  
  // Legacy handler for direct invocation (for backwards compatibility)
  console.log('[WARN] Direct invocation - use SQS for rebuild');
  return {
    statusCode: 400,
    body: JSON.stringify({ error: 'Use SQS queue for rebuild' }),
  };
};
