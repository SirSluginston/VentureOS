# Normalizer Architecture & Naming Convention

## How Normalizers Work

### Flow Overview

```
Admin Panel Upload
    ↓
User selects: Agency + Normalizer + Date + File
    ↓
File uploaded to: bronze/historical/{agency}/{normalizer}/{date}/{filename}
    ↓
S3 Event triggers Parquet Writer Lambda
    ↓
Parquet Writer extracts normalizer name from path
    ↓
Looks up normalizer function in NORMALIZERS map
    ↓
[1] Generic Parser: Parses CSV/JSON → Raw Objects
    ↓
[2] Normalizer: Maps raw fields → Unified Schema (validates, filters invalid rows)
    ↓
[3] Schema Validation: Ensures unified schema compliance
    ↓
Writes to Silver (S3 Tables)
```

### Parser vs Normalizer

**Parser** (`utils/csvParser.js`):
- **Generic**: Handles standard CSV format (comma-delimited, quoted fields)
- **Format-agnostic**: Works for any CSV with standard structure
- **Current**: Single generic parser for all CSV files

**Normalizer** (`utils/normalizers/{agency}.js`):
- **Data-specific**: Maps agency-specific fields to unified schema
- **Validates**: Checks required fields, data types
- **Filters**: Returns `null` for invalid rows (filtered out)

**Combined Parser+Normalizer** (for special cases):
- Use when data source has non-standard format
- Example: FDA XML files, fixed-width files, semicolon-delimited CSVs
- See [DATA_PIPELINE_SECURITY.md](DATA_PIPELINE_SECURITY.md) for details

### Key Points

1. **Path-Based Detection**: The parquet writer extracts the normalizer name from the S3 path (4th segment after `historical`)
2. **Admin Panel Selection**: The normalizer you select in the admin panel becomes part of the S3 path
3. **Exact Match Required**: The normalizer name in the path must exactly match a key in the `NORMALIZERS` map in `lambda-parquet-writer.js`

## Naming Convention

### Format: `{agency}-{data_type}`

**Examples:**
- `osha-severe_injury` - OSHA Severe Injury Reports
- `osha-enforcement` - OSHA Enforcement Actions
- `fda-recall` - FDA Product Recalls
- `fda-warning_letter` - FDA Warning Letters
- `nhtsa-recall` - NHTSA Vehicle Recalls
- `faa-drone_incident` - FAA Drone Incidents
- `epa-violation` - EPA Environmental Violations

### Rules

1. **Lowercase only** - No uppercase letters
2. **Hyphens separate agency and type** - `agency-type`
3. **Underscores for multi-word types** - `severe_injury`, `warning_letter`
4. **Be descriptive** - Name should clearly indicate the data source/type
5. **Consistent agency prefix** - Always use the same agency name (e.g., always `osha`, never `OSHA` or `Osha`)

## Adding a New Normalizer

### Step 1: Create Normalizer Function

Create file: `VentureOS/utils/normalizers/{agency}.js`

```javascript
/**
 * Normalize {Agency} {Data Type} data
 */
export function normalize{DataType}(rawRow) {
  // Map raw data to unified schema
  return {
    violation_id: generateId(rawRow),
    agency: '{AGENCY}',
    state: normalizeState(rawRow.state),
    city: normalizeCity(rawRow.city),
    company_name: rawRow.company || '',
    company_slug: slugify(rawRow.company || ''),
    event_date: parseDate(rawRow.date),
    fine_amount: parseFloat(rawRow.fine || 0),
    violation_type: '{TYPE}',
    raw_title: rawRow.title || '',
    raw_description: rawRow.description || '',
    source_url: rawRow.url || null,
    tags: [],
    // ... other unified schema fields
  };
}
```

### Step 2: Register in Parquet Writer

Edit `VentureOS/lambdas/lambda-parquet-writer.js`:

```javascript
import { normalize{DataType} } from './utils/normalizers/{agency}.js';

const NORMALIZERS = {
  'osha-severe_injury': normalizeSevereInjuryReport,
  'osha-enforcement': normalizeEnforcementData,
  '{agency}-{data_type}': normalize{DataType}, // Add your new normalizer
};
```

### Step 3: Add to Admin Panel

Edit `osha-trail/src/components/DataUpload.tsx` and `sirsluginston-site/src/components/DataUpload.tsx`:

```typescript
const normalizers = [
  { value: 'osha-severe_injury', label: 'OSHA - Severe Injury Reports' },
  { value: 'osha-enforcement', label: 'OSHA - Enforcement Data' },
  { value: '{agency}-{data_type}', label: '{Agency} - {Human Readable Type}' }, // Add here
];
```

### Step 4: Deploy

1. Deploy updated Lambda: `lambda-parquet-writer.js`
2. Rebuild frontend: Admin panel will show new normalizer option
3. Test: Upload a test file with the new normalizer

## Current Normalizers

| Normalizer Name | Agency | Data Type | File | Function |
|----------------|--------|-----------|------|----------|
| `osha-severe_injury` | OSHA | Severe Injury Reports | `utils/normalizers/osha.js` | `normalizeSevereInjuryReport` |
| `osha-enforcement` | OSHA | Enforcement Actions | `utils/normalizers/osha.js` | `normalizeEnforcementData` |

## Future Normalizers (Planned)

| Normalizer Name | Agency | Data Type | Status |
|----------------|--------|-----------|--------|
| `fda-recall` | FDA | Product Recalls | To Create |
| `fda-warning_letter` | FDA | Warning Letters | To Create |
| `nhtsa-recall` | NHTSA | Vehicle Recalls | To Create |
| `faa-drone_incident` | FAA | Drone Incidents | To Create |
| `epa-violation` | EPA | Environmental Violations | To Create |

## Best Practices

### 1. One Normalizer Per Data Structure

If an agency has multiple data types with different structures, create separate normalizers:
- ✅ `osha-severe_injury` (different structure)
- ✅ `osha-enforcement` (different structure)
- ❌ Don't try to handle both in one normalizer

### 2. Validate Early

Add validation in your normalizer function:
```javascript
export function normalizeMyData(rawRow) {
  if (!rawRow.required_field) {
    console.warn('Missing required field, skipping row');
    return null; // Will be filtered out
  }
  // ... rest of normalization
}
```

### 3. Handle Missing Data Gracefully

```javascript
const city = normalizeCity(rawRow.city || 'Unknown');
const fineAmount = parseFloat(rawRow.fine || 0);
```

### 4. Test Before Production

1. Create a small test file (10-20 rows)
2. Upload via admin panel
3. Check CloudWatch logs for errors
4. Verify data in Silver table (S3 Tables)
5. Check frontend displays correctly

## Troubleshooting

### "Normalizer not found" Warning

**Cause**: Normalizer name in path doesn't match NORMALIZERS map

**Fix**: 
1. Check exact spelling (case-sensitive, hyphens vs underscores)
2. Verify normalizer is registered in `lambda-parquet-writer.js`
3. Check CloudWatch logs for extracted normalizer name

### Data Not Appearing in Silver

**Check**:
1. CloudWatch logs for normalization errors
2. Rows might be filtered out (returning `null` from normalizer)
3. Check S3 Tables directly (via Athena or DuckDB)

### Wrong Data Structure

**Cause**: Normalizer not handling the actual CSV/JSON structure

**Fix**:
1. Inspect raw data structure first
2. Update normalizer to match actual fields
3. Test with sample data

## Parser vs Normalizer: When to Combine

### Current Approach: Separate Parser + Normalizer

**Generic Parser** (`utils/csvParser.js`):
- Handles standard CSV format
- Works for any CSV with standard structure
- Used by all normalizers

**Normalizer** (`utils/normalizers/{agency}.js`):
- Maps agency-specific fields → unified schema
- Validates and filters invalid rows

### When to Use Combined Parser+Normalizer

Use combined approach when:
- ✅ Non-standard format (semicolon delimiter, fixed-width, XML)
- ✅ Format-specific validation needed
- ✅ Security-critical data processing
- ✅ Pre-processing required (encoding conversion, header manipulation)

**Example**: `utils/normalizers/osha-combined.js` demonstrates combined parser+normalizer pattern

See [DATA_PIPELINE_SECURITY.md](DATA_PIPELINE_SECURITY.md) for detailed security considerations.

## Related Files

- `VentureOS/lambdas/lambda-parquet-writer.js` - Normalizer registry and path detection
- `VentureOS/utils/normalizers/osha.js` - Example normalizer implementation (separate)
- `VentureOS/utils/normalizers/osha-combined.js` - Example combined parser+normalizer
- `VentureOS/utils/csvParser.js` - Generic CSV parser
- `VentureOS/utils/schema.js` - Unified schema definition
- `osha-trail/src/components/DataUpload.tsx` - Admin panel normalizer selection
- `VentureOS/api/admin/presigned-upload.js` - Path generation logic
- `VentureOS/docs/DATA_PIPELINE_SECURITY.md` - Security and validation guide

