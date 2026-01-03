// Size Tracking Helper
// Tracks item size during aggregation and provides save/resume points

import { estimateItemSize } from './dynamoSizeHelper.js';

/**
 * Track size during aggregation loop
 * Checks if item is approaching size limit and returns save point info
 * @param {Object} currentItem - Item being built during aggregation
 * @param {number} currentIndex - Current index in aggregation loop
 * @param {number} totalItems - Total items to aggregate
 * @param {number} safetyMarginKB - Safety margin in KB (default: 50KB before 400KB limit)
 * @returns {Object} { shouldSave: boolean, sizeKB: number, progress: { index, total, percent } }
 */
export function checkSizeLimit(currentItem, currentIndex, totalItems, safetyMarginKB = 50) {
  const sizeBytes = estimateItemSize(currentItem);
  const sizeKB = sizeBytes / 1024;
  const maxSizeKB = 400 - safetyMarginKB; // 350KB default safety margin
  
  const shouldSave = sizeKB >= maxSizeKB;
  
  return {
    shouldSave,
    sizeBytes,
    sizeKB: sizeKB.toFixed(1),
    maxSizeKB,
    progress: {
      index: currentIndex,
      total: totalItems,
      percent: ((currentIndex / totalItems) * 100).toFixed(1),
    },
  };
}

/**
 * Create a save point for aggregation progress
 * Returns object that can be saved to S3 progress tracker
 * @param {string} aggregationType - 'state', 'national', 'city'
 * @param {string} location - State/city identifier
 * @param {string} brandPk - Brand primary key
 * @param {number} currentIndex - Current index in loop
 * @param {number} totalItems - Total items to process
 * @param {Object} partialItem - Partially built item (what we have so far)
 * @returns {Object} Save point object
 */
export function createSavePoint(aggregationType, location, brandPk, currentIndex, totalItems, partialItem) {
  return {
    aggregationType,
    location,
    brandPk,
    progress: {
      currentIndex,
      totalItems,
      percent: ((currentIndex / totalItems) * 100).toFixed(1),
    },
    partialItem,
    sizeKB: (estimateItemSize(partialItem) / 1024).toFixed(1),
    timestamp: new Date().toISOString(),
  };
}


