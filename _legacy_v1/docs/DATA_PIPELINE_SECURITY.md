# Data Pipeline Security & Validation

## Current Pipeline Flow

```
Raw File (CSV/JSON)
    ↓
[1] Generic Parser (csvParser.js)
    - Splits on delimiter (default: comma)
    - Handles quoted fields
    - Normalizes headers (lowercase, underscores)
    ↓
[2] Normalizer (normalizers/{agency}.js)
    - Maps raw fields → Unified Schema
    - Validates required fields
    - Generates IDs, slugs
    ↓
[3] Schema Validation (schema.js)
    - Validates unified schema
    - Type checking (dates, numbers)
    ↓
[4] Parquet Writer
    - Writes to Silver (S3 Tables)
    - Type enforcement (DATE, DOUBLE)
    ↓
[5] Gold Aggregation
    - Stats calculation
    ↓
[6] DynamoDB Sync (if live data)
    - Recent5 updates
```

## Security Concerns & Solutions

### 1. **Parser-Level Security**

**Current State**: Generic parser assumes standard CSV format
**Risks**:
- Malformed CSV could break parsing
- Injection attacks in field values
- Encoding issues (UTF-8 vs Windows-1252)

**Solutions**:
- ✅ Parser already handles quoted fields safely
- ✅ Header normalization prevents injection via field names
- ⚠️ **Missing**: Size limits, encoding detection, malformed row handling

### 2. **Normalizer-Level Security**

**Current State**: Each normalizer validates its own data
**Risks**:
- Missing required fields
- Invalid data types
- Malicious content in text fields

**Solutions**:
- ✅ Normalizers validate required fields (return `null` if invalid)
- ✅ Type coercion (parseInt, parseFloat, parseDate)
- ⚠️ **Missing**: Content sanitization, length limits

### 3. **Schema Validation**

**Current State**: `validateRow()` in schema.js checks unified schema
**Risks**:
- Invalid dates, numbers
- Missing required fields
- Type mismatches

**Solutions**:
- ✅ Schema validation exists
- ✅ Type enforcement at Parquet write
- ✅ Invalid rows filtered out (return `null`)

## Recommendation: Combined Parser+Normalizer Pattern

For data sources with unique formats, combine parsing and normalization:

### Benefits

1. **Format-Specific Parsing**: Handle delimiters, encoding, structure
2. **Early Validation**: Catch issues before normalization
3. **Better Error Handling**: Know exactly where parsing failed
4. **Security**: Validate and sanitize at the source

### Implementation Pattern

```javascript
/**
 * Combined Parser + Normalizer for OSHA Severe Injury Reports
 * Handles format-specific parsing AND normalization in one step
 */
export function parseAndNormalizeSevereInjuryReport(csvText) {
  // Step 1: Format-Specific Parsing
  const rawRows = parseOSHASevereInjuryCSV(csvText, {
    delimiter: ',',
    encoding: 'utf-8',
    skipEmptyRows: true,
    validateHeaders: ['eventdate', 'city', 'state', 'employer']
  });
  
  // Step 2: Normalize Each Row
  const violations = rawRows
    .map(row => {
      try {
        // Validate early
        if (!row.eventdate || !row.city || !row.state) {
          console.warn('Missing required fields, skipping row');
          return null;
        }
        
        // Normalize
        return normalizeSevereInjuryReport(row);
      } catch (e) {
        console.error('Normalization error:', e);
        return null;
      }
    })
    .filter(Boolean);
  
  return violations;
}
```

## When to Use Combined vs Separate

### Use Combined Parser+Normalizer When:
- ✅ Data source has non-standard format (semicolon delimiter, fixed-width)
- ✅ Requires pre-processing (encoding conversion, header manipulation)
- ✅ Format-specific validation needed before normalization
- ✅ Security-critical data source

### Use Separate Parser+Normalizer When:
- ✅ Standard CSV/JSON format
- ✅ Generic parser handles it fine
- ✅ Multiple normalizers share same parser
- ✅ Simpler maintenance

## Proposed Architecture

### Option A: Hybrid Approach (Recommended)

Keep generic parser for standard formats, add combined parsers for special cases:

```javascript
// lambda-parquet-writer.js

const PARSERS_AND_NORMALIZERS = {
  // Combined (format-specific)
  'osha-severe_injury': {
    type: 'combined',
    handler: parseAndNormalizeSevereInjuryReport
  },
  'fda-recall-xml': {
    type: 'combined',
    handler: parseAndNormalizeFDARecallXML
  },
  
  // Separate (standard format)
  'osha-enforcement': {
    type: 'separate',
    parser: parseCSV,
    normalizer: normalizeEnforcementData
  },
  'nhtsa-recall': {
    type: 'separate',
    parser: parseCSV,
    normalizer: normalizeNHTSARecall
  }
};
```

### Option B: Always Combined

Every data source gets its own parser+normalizer function:

```javascript
const PROCESSORS = {
  'osha-severe_injury': processOSHASevereInjury,
  'osha-enforcement': processOSHAEnforcement,
  'fda-recall': processFDARecall,
  // Each function handles parsing AND normalization
};
```

## Security Best Practices

### 1. Input Validation
```javascript
function parseAndNormalize(data, options) {
  // Size limit
  if (data.length > MAX_FILE_SIZE) {
    throw new Error('File too large');
  }
  
  // Encoding detection
  const encoding = detectEncoding(data);
  if (encoding !== 'utf-8') {
    data = convertEncoding(data, encoding, 'utf-8');
  }
  
  // Parse with validation
  const rows = parseWithValidation(data, options);
  
  // Normalize with validation
  return rows.map(normalizeWithValidation);
}
```

### 2. Content Sanitization
```javascript
function sanitizeTextField(value, maxLength = 10000) {
  if (!value) return '';
  
  // Remove null bytes
  value = value.replace(/\0/g, '');
  
  // Truncate if too long
  if (value.length > maxLength) {
    value = value.substring(0, maxLength);
  }
  
  return value.trim();
}
```

### 3. Type Validation
```javascript
function validateAndCoerceTypes(row) {
  return {
    event_date: validateDate(row.eventdate),
    fine_amount: validateNumber(row.fine, { min: 0, max: 10000000 }),
    city: validateString(row.city, { maxLength: 100 }),
    // ...
  };
}
```

### 4. Error Handling
```javascript
function safeParseAndNormalize(data, processor) {
  try {
    const violations = processor(data);
    
    // Log stats
    console.log(`Processed ${violations.length} violations`);
    
    return violations;
  } catch (error) {
    // Don't expose internal errors
    console.error('Processing error:', error);
    throw new Error('Failed to process data file');
  }
}
```

## Migration Path

### Phase 1: Keep Current System
- Generic parser + separate normalizers
- Add validation layers

### Phase 2: Add Combined Processors for Special Cases
- Create combined parser+normalizer for non-standard formats
- Keep generic parser for standard formats

### Phase 3: Evaluate
- If most sources need custom parsing → move to always-combined
- If most sources are standard → keep hybrid approach

## Current Status

✅ **Working**: Generic CSV parser handles standard formats
✅ **Working**: Normalizers validate and filter invalid rows
⚠️ **Needs**: Format-specific parsers for non-standard sources
⚠️ **Needs**: Enhanced validation and sanitization
⚠️ **Needs**: Better error handling and logging

## Next Steps

1. **Immediate**: Add validation to existing normalizers
2. **Short-term**: Create combined parser+normalizer for FDA XML format
3. **Long-term**: Evaluate if all sources need combined approach

