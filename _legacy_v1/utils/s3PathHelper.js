// S3 Path Helper Utilities
// Provides consistent path generation for S3 bucket structure

/**
 * Generate S3 path for a city's violation data
 * @param {string} country - Country code (e.g., 'USA')
 * @param {string} state - State abbreviation (e.g., 'TX')
 * @param {string} citySlug - Slugified city name (e.g., 'Austin-TX')
 * @returns {string} S3 path (e.g., 'National/USA/Texas/Austin-TX/')
 */
export function getCityPath(country, state, citySlug) {
  return `National/${country}/${state}/${citySlug}/`;
}

/**
 * Generate S3 path for a state's data
 * @param {string} country - Country code (e.g., 'USA')
 * @param {string} state - State abbreviation (e.g., 'TX')
 * @returns {string} S3 path (e.g., 'National/USA/Texas/')
 */
export function getStatePath(country, state) {
  return `National/${country}/${state}/`;
}

/**
 * Generate S3 path for national data
 * @param {string} country - Country code (e.g., 'USA')
 * @returns {string} S3 path (e.g., 'National/USA/')
 */
export function getNationalPath(country) {
  return `National/${country}/`;
}

/**
 * Generate S3 key for a violation file
 * @param {string} cityPath - City path (from getCityPath)
 * @param {string} year - Year (e.g., '2024')
 * @param {string} violationId - Violation ID
 * @returns {string} Full S3 key (e.g., 'National/USA/Texas/Austin-TX/2024/20241215-12345.json')
 */
export function getViolationKey(cityPath, year, violationId) {
  return `${cityPath}${year}/${violationId}.json`;
}

/**
 * Generate S3 key for a manifest file
 * @param {string} path - Path (city, state, or national)
 * @returns {string} Manifest key (e.g., 'National/USA/Texas/Austin-TX/manifest.json')
 */
export function getManifestKey(path) {
  return `${path}manifest.json`;
}

/**
 * Generate S3 key for a stats file
 * @param {string} path - Path (city, state, or national)
 * @returns {string} Stats key (e.g., 'National/USA/Texas/Austin-TX/stats.json')
 */
export function getStatsKey(path) {
  return `${path}stats.json`;
}

/**
 * Extract year from date string or Date object
 * @param {string|Date} date - Date to extract year from
 * @returns {string} Year (e.g., '2024')
 */
export function extractYear(date) {
  if (!date) return new Date().getFullYear().toString();
  
  if (date instanceof Date) {
    return date.getFullYear().toString();
  }
  
  // Try to parse date string
  const dateObj = new Date(date);
  if (!isNaN(dateObj.getTime())) {
    return dateObj.getFullYear().toString();
  }
  
  // Fallback to current year
  return new Date().getFullYear().toString();
}

/**
 * Slugify city name for use in paths
 * @param {string} city - City name
 * @param {string} state - State abbreviation
 * @returns {string} Slugified city name (e.g., 'Austin-TX')
 */
export function slugifyCity(city, state) {
  if (!city) return 'Unknown';
  
  const slug = city
    .toLowerCase()
    .trim()
    .replace(/[^\w\s-]/g, '') // Remove special chars
    .replace(/\s+/g, '-')      // Replace spaces with hyphens
    .replace(/-+/g, '-');      // Replace multiple hyphens with single
  
  // Append state for uniqueness
  return state ? `${slug}-${state.toUpperCase()}` : slug;
}

