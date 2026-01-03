// DynamoDB Item Size Helper
// Generic utility for handling DynamoDB 400KB item size limit
//
// DynamoDB has a hard limit of 400KB per item. When your data gets too chunky,
// this utility helps you split it up intelligently. No data loss, just smart chunking.
// Because sometimes even slugs need to watch their size (especially when dealing with
// massive violation datasets).

/**
 * Estimate DynamoDB item size in bytes
 * DynamoDB calculates size as sum of attribute names + values (UTF-8 bytes).
 * This gives us an accurate estimate so we know when we're getting too big for our britches.
 * 
 * @param {Object} item - DynamoDB item
 * @returns {number} Estimated size in bytes
 */
export function estimateItemSize(item) {
  const jsonString = JSON.stringify(item);
  // Convert to UTF-8 byte length (accurate for DynamoDB size calculation)
  return new TextEncoder().encode(jsonString).length;
}

/**
 * Split a large object into chunks that fit within size limit
 * Generic function that can split any object field (companyStats, violationCounts, etc.)
 * @param {Object} largeObject - Object to split (e.g., companyStats)
 * @param {Object} baseItem - Base item without the large object
 * @param {string} fieldName - Name of the field being split (e.g., 'companyStats')
 * @param {string} chunkSuffix - Suffix pattern for chunk items (e.g., '#COMPANIES')
 * @param {number} maxSizeBytes - Maximum size per item (default: 350KB safety margin)
 * @param {Function} sortFn - Optional function to sort entries before chunking (for priority ordering)
 * @returns {Array} Array of items to write, with pagination metadata
 */
export function splitLargeField(
  largeObject,
  baseItem,
  fieldName,
  chunkSuffix,
  maxSizeBytes = 350 * 1024,
  sortFn = null
) {
  if (!largeObject || Object.keys(largeObject).length === 0) {
    return [{ item: baseItem, isChunk: false }];
  }

  const entries = Object.entries(largeObject);
  
  // Sort if sort function provided (e.g., by violation count for companies)
  const sortedEntries = sortFn ? entries.sort(sortFn) : entries;
  
  const items = [];
  let currentChunk = {};
  let chunkIndex = 1;
  let currentSize = estimateItemSize(baseItem);
  
  // Calculate size of base item + metadata
  const baseWithMetadata = {
    ...baseItem,
    [`${fieldName}Chunked`]: true,
    [`${fieldName}TotalChunks`]: 0, // Will be updated at end
    [`${fieldName}Total`]: sortedEntries.length,
  };
  const baseSize = estimateItemSize(baseWithMetadata);
  
  // Reserve space for chunk metadata in each chunk item
  const chunkMetadataSize = estimateItemSize({
    pk: baseItem.pk,
    sk: `${baseItem.sk}${chunkSuffix}#1`,
    chunkIndex: 1,
    totalChunks: 1,
    [fieldName]: {},
  });
  
  const availableSizePerChunk = maxSizeBytes - chunkMetadataSize;
  
  // First, try to fit everything in base item
  const testBaseItem = {
    ...baseItem,
    [fieldName]: Object.fromEntries(sortedEntries),
  };
  
  if (estimateItemSize(testBaseItem) <= maxSizeBytes) {
    // Everything fits in one item - no chunking needed
    return [{ item: testBaseItem, isChunk: false }];
  }
  
  // Need to split into chunks
  // Base item gets summary stats only (no large field)
  const baseItemFinal = {
    ...baseItem,
    [`${fieldName}Chunked`]: true,
    [`${fieldName}Total`]: sortedEntries.length,
  };
  
  // Calculate how many chunks we'll need
  let totalChunks = 1;
  let currentChunkSize = chunkMetadataSize;
  
  for (const [key, value] of sortedEntries) {
    const testEntry = { [key]: value };
    const entrySize = estimateItemSize(testEntry);
    
    if (currentChunkSize + entrySize > availableSizePerChunk) {
      // Current chunk is full, start new chunk
      totalChunks++;
      currentChunkSize = chunkMetadataSize + entrySize;
      currentChunk = { [key]: value };
    } else {
      currentChunk[key] = value;
      currentChunkSize += entrySize;
    }
  }
  
  // Update base item with total chunks
  baseItemFinal[`${fieldName}TotalChunks`] = totalChunks;
  items.push({ item: baseItemFinal, isChunk: false });
  
  // Now create actual chunk items
  currentChunk = {};
  chunkIndex = 1;
  currentChunkSize = chunkMetadataSize;
  
  for (const [key, value] of sortedEntries) {
    const testEntry = { [key]: value };
    const entrySize = estimateItemSize(testEntry);
    
    if (currentChunkSize + entrySize > availableSizePerChunk && Object.keys(currentChunk).length > 0) {
      // Current chunk is full, save it and start new chunk
      items.push({
        item: {
          pk: baseItem.pk,
          sk: `${baseItem.sk}${chunkSuffix}#${chunkIndex}`,
          chunkIndex,
          totalChunks,
          [fieldName]: currentChunk,
        },
        isChunk: true,
        chunkIndex,
      });
      
      chunkIndex++;
      currentChunk = { [key]: value };
      currentChunkSize = chunkMetadataSize + entrySize;
    } else {
      currentChunk[key] = value;
      currentChunkSize += entrySize;
    }
  }
  
  // Add final chunk
  if (Object.keys(currentChunk).length > 0) {
    items.push({
      item: {
        pk: baseItem.pk,
        sk: `${baseItem.sk}${chunkSuffix}#${chunkIndex}`,
        chunkIndex,
        totalChunks,
        [fieldName]: currentChunk,
      },
      isChunk: true,
      chunkIndex,
    });
  }
  
  return items;
}

/**
 * Split companyStats into multiple DynamoDB items if needed
 * Preserves all company data across chunks
 * @param {Object} item - Full DynamoDB item with companyStats
 * @param {number} maxSizeBytes - Maximum size per item (default: 350KB)
 * @returns {Array} Array of items to write: [{ item, isChunk, chunkIndex? }, ...]
 */
export function splitCompanyStats(item, maxSizeBytes = 350 * 1024) {
  const { companyStats, ...baseItem } = item;
  
  // Sort companies by violation count (descending) - most important first
  const sortFn = (a, b) => b[1].count - a[1].count;
  
  return splitLargeField(
    companyStats,
    baseItem,
    'companyStats',
    '#COMPANIES',
    maxSizeBytes,
    sortFn
  );
}

/**
 * Ensure item fits within DynamoDB size limit
 * Automatically splits large fields (like companyStats) across multiple items
 * @param {Object} item - Full DynamoDB item
 * @param {number} maxSizeBytes - Maximum size per item (default: 350KB safety margin)
 * @returns {Array} Array of items to write, preserving all data
 */
export function ensureItemSize(item, maxSizeBytes = 350 * 1024) {
  const currentSize = estimateItemSize(item);
  
  if (currentSize <= maxSizeBytes) {
    return [{ item, isChunk: false, sizeBytes: currentSize }];
  }
  
  // Item is too large - check if companyStats is the culprit
  if (item.companyStats && Object.keys(item.companyStats).length > 0) {
    const splitItems = splitCompanyStats(item, maxSizeBytes);
    
    // Add size info to each item
    return splitItems.map(splitItem => ({
      ...splitItem,
      sizeBytes: estimateItemSize(splitItem.item),
      originalSizeBytes: currentSize,
    }));
  }
  
  // If no companyStats or other large field, return as-is (will fail on write, but that's better than silent truncation)
  console.warn(`[WARN] Item exceeds size limit but no splittable field found. Size: ${(currentSize / 1024).toFixed(1)}KB`);
  return [{ item, isChunk: false, sizeBytes: currentSize, exceedsLimit: true }];
}


