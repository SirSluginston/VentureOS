// CSV Parser for OSHA Data
// Handles CSV downloads from OSHA.gov and DOL Enforcement Data

/**
 * Parse CSV string into array of objects
 * @param {string} csvText - Raw CSV text content
 * @param {Object} options - Parser options
 * @returns {Array<Object>} Array of parsed row objects
 */
export function parseCSV(csvText, options = {}) {
  const {
    skipHeader = true,
    delimiter = ',',
    quoteChar = '"',
  } = options;

  const lines = csvText.split('\n').filter(line => line.trim());
  
  if (lines.length === 0) {
    return [];
  }

  // Extract header row
  const headerLine = lines[0];
  const headers = parseCSVLine(headerLine, delimiter, quoteChar);
  
  // Skip header if requested
  const dataLines = skipHeader ? lines.slice(1) : lines;
  
  // Parse each data row
  const rows = [];
  for (const line of dataLines) {
    if (!line.trim()) continue;
    
    const values = parseCSVLine(line, delimiter, quoteChar);
    
    // Create object from headers and values
    const row = {};
    headers.forEach((header, index) => {
      // Normalize header names (remove spaces, special chars)
      const normalizedHeader = normalizeHeader(header);
      row[normalizedHeader] = values[index] || '';
    });
    
    rows.push(row);
  }
  
  return rows;
}

/**
 * Parse a single CSV line, handling quoted fields
 */
function parseCSVLine(line, delimiter, quoteChar) {
  const values = [];
  let currentValue = '';
  let inQuotes = false;
  
  for (let i = 0; i < line.length; i++) {
    const char = line[i];
    const nextChar = line[i + 1];
    
    if (char === quoteChar) {
      if (inQuotes && nextChar === quoteChar) {
        // Escaped quote (double quote)
        currentValue += quoteChar;
        i++; // Skip next quote
      } else {
        // Toggle quote state
        inQuotes = !inQuotes;
      }
    } else if (char === delimiter && !inQuotes) {
      // End of field
      values.push(currentValue.trim());
      currentValue = '';
    } else {
      currentValue += char;
    }
  }
  
  // Add last value
  values.push(currentValue.trim());
  
  return values;
}

/**
 * Normalize header names to valid JavaScript property names
 */
function normalizeHeader(header) {
  return header
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '_') // Replace non-alphanumeric with underscore
    .replace(/^_+|_+$/g, ''); // Remove leading/trailing underscores
}

/**
 * Download CSV from URL and parse it
 * @param {string} url - URL to CSV file
 * @returns {Promise<Array<Object>>} Parsed CSV data
 */
export async function downloadAndParseCSV(url) {
  try {
    const response = await fetch(url);
    if (!response.ok) {
      throw new Error(`Failed to download CSV: ${response.status} ${response.statusText}`);
    }
    
    const csvText = await response.text();
    return parseCSV(csvText);
  } catch (error) {
    console.error('Error downloading/parsing CSV:', error);
    throw error;
  }
}

/**
 * Parse CSV file from S3 (for Lambda)
 * @param {string} bucket - S3 bucket name
 * @param {string} key - S3 object key
 * @param {Object} s3Client - AWS S3 client instance
 * @returns {Promise<Array<Object>>} Parsed CSV data
 */
export async function parseCSVFromS3(bucket, key, s3Client) {
  try {
    const { GetObjectCommand } = await import('@aws-sdk/client-s3');
    
    const command = new GetObjectCommand({
      Bucket: bucket,
      Key: key,
    });
    
    const response = await s3Client.send(command);
    const csvText = await response.Body.transformToString();
    
    return parseCSV(csvText);
  } catch (error) {
    console.error('Error reading CSV from S3:', error);
    throw error;
  }
}


