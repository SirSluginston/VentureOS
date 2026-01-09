/**
 * Bedrock Batch Processing Configuration
 * 
 * Defines priority tiers, filters, and limits for batch processing
 */

export const PRIORITY_TIERS = {
  HIGH: {
    name: 'High Priority',
    filters: {
      minFineAmount: 100000,
      hasFatalities: true,
      maxDaysOld: 30
    },
    processing: 'lazy', // Use lazy loading (instant)
    description: 'Fatalities, fines >$100k, recent violations'
  },
  
  MEDIUM: {
    name: 'Medium Priority',
    filters: {
      minFineAmount: 50000,
      maxFineAmount: 100000,
      maxDaysOld: 90
    },
    processing: 'batch', // Use batch processing
    description: 'Fines $50k-$100k, serious violations, 30-90 days old'
  },
  
  LOW: {
    name: 'Low Priority',
    filters: {
      maxFineAmount: 50000,
      minDaysOld: 90
    },
    processing: 'batch', // Use batch processing
    description: 'Fines <$50k, minor violations, >90 days old'
  }
};

/**
 * Default batch processing configuration
 */
export const DEFAULT_BATCH_CONFIG = {
  // Filter Criteria
  filters: {
    minFineAmount: 0,
    maxFineAmount: null,
    violationTypes: [],
    excludeTypes: [],
    dateRange: {
      start: null,
      end: null
    },
    agencies: [],
    hasFatalities: null,
    minDaysOld: null,
    maxDaysOld: null
  },
  
  // Processing Limits
  limits: {
    maxViolations: 10000,
    maxSpendUSD: 50,
    maxPercent: 100,
    priority: 'medium' // 'high' | 'medium' | 'low' | 'all'
  },
  
  // Batch Job Settings
  batch: {
    outputFormat: 'jsonl',
    s3InputPrefix: 'bedrock-batches/input/',
    s3OutputPrefix: 'bedrock-batches/output/',
    modelId: 'mistral.mistral-large-2402-v1:0'
  }
};

/**
 * Estimate cost for batch processing
 */
export function estimateBatchCost(violationCount, avgTokensPerViolation = 300) {
  const tokensPerViolation = avgTokensPerViolation;
  const totalTokens = violationCount * tokensPerViolation;
  const costPer1KTokens = 0.001; // Batch pricing (50% discount)
  const totalCost = (totalTokens / 1000) * costPer1KTokens;
  
  return {
    violationCount,
    totalTokens,
    costUSD: totalCost,
    costPerViolation: totalCost / violationCount
  };
}

/**
 * Apply priority tier filters to violations
 */
export function filterByPriority(violations, tier = 'MEDIUM') {
  const tierConfig = PRIORITY_TIERS[tier];
  if (!tierConfig) return violations;
  
  const { filters } = tierConfig;
  const now = new Date();
  
  return violations.filter(v => {
    // Fine amount filter
    if (filters.minFineAmount && (v.fine_amount || 0) < filters.minFineAmount) {
      return false;
    }
    if (filters.maxFineAmount && (v.fine_amount || 0) > filters.maxFineAmount) {
      return false;
    }
    
    // Fatalities filter
    if (filters.hasFatalities !== null && filters.hasFatalities !== undefined) {
      const hasFatality = v.violation_details?.fatality || 
                         v.violation_type?.toLowerCase().includes('fatal') ||
                         v.raw_description?.toLowerCase().includes('fatal');
      if (filters.hasFatalities !== hasFatality) {
        return false;
      }
    }
    
    // Days old filter
    if (v.event_date) {
      const eventDate = new Date(v.event_date);
      const daysOld = Math.floor((now - eventDate) / (1000 * 60 * 60 * 24));
      
      if (filters.maxDaysOld && daysOld > filters.maxDaysOld) {
        return false;
      }
      if (filters.minDaysOld && daysOld < filters.minDaysOld) {
        return false;
      }
    }
    
    return true;
  });
}

/**
 * Apply custom filters to violations
 */
export function applyFilters(violations, config = DEFAULT_BATCH_CONFIG) {
  const { filters } = config;
  let filtered = [...violations];
  
  // Fine amount
  if (filters.minFineAmount) {
    filtered = filtered.filter(v => (v.fine_amount || 0) >= filters.minFineAmount);
  }
  if (filters.maxFineAmount) {
    filtered = filtered.filter(v => (v.fine_amount || 0) <= filters.maxFineAmount);
  }
  
  // Violation types
  if (filters.violationTypes && filters.violationTypes.length > 0) {
    filtered = filtered.filter(v => filters.violationTypes.includes(v.violation_type));
  }
  if (filters.excludeTypes && filters.excludeTypes.length > 0) {
    filtered = filtered.filter(v => !filters.excludeTypes.includes(v.violation_type));
  }
  
  // Agencies
  if (filters.agencies && filters.agencies.length > 0) {
    filtered = filtered.filter(v => filters.agencies.includes(v.agency?.toLowerCase()));
  }
  
  // Date range
  if (filters.dateRange?.start) {
    const startDate = new Date(filters.dateRange.start);
    filtered = filtered.filter(v => v.event_date && new Date(v.event_date) >= startDate);
  }
  if (filters.dateRange?.end) {
    const endDate = new Date(filters.dateRange.end);
    filtered = filtered.filter(v => v.event_date && new Date(v.event_date) <= endDate);
  }
  
  // Fatalities
  if (filters.hasFatalities !== null && filters.hasFatalities !== undefined) {
    filtered = filtered.filter(v => {
      const hasFatality = v.violation_details?.fatality || 
                         v.violation_type?.toLowerCase().includes('fatal') ||
                         v.raw_description?.toLowerCase().includes('fatal');
      return filters.hasFatalities === hasFatality;
    });
  }
  
  return filtered;
}

/**
 * Apply limits to violations
 */
export function applyLimits(violations, config = DEFAULT_BATCH_CONFIG) {
  const { limits } = config;
  let limited = [...violations];
  
  // Priority tier filter
  if (limits.priority && limits.priority !== 'all') {
    limited = filterByPriority(limited, limits.priority.toUpperCase());
  }
  
  // Max violations
  if (limits.maxViolations) {
    limited = limited.slice(0, limits.maxViolations);
  }
  
  // Max percent
  if (limits.maxPercent && limits.maxPercent < 100) {
    const maxCount = Math.floor((violations.length * limits.maxPercent) / 100);
    limited = limited.slice(0, maxCount);
  }
  
  // Max spend (estimate and trim)
  if (limits.maxSpendUSD) {
    const estimate = estimateBatchCost(limited.length);
    if (estimate.costUSD > limits.maxSpendUSD) {
      // Trim to fit budget
      const maxCount = Math.floor((limits.maxSpendUSD / estimate.costPerViolation));
      limited = limited.slice(0, maxCount);
    }
  }
  
  return limited;
}

