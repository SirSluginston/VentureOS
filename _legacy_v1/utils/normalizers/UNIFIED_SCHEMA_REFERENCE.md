# Unified Schema Reference

**Purpose**: This document lists all fields expected by the Unified Schema. Use this as a reference when creating new normalizers.

## ⚠️ Important Notes

- **Field names are case-sensitive** - Use exact names shown below
- **All fields should be present** - Even if NULL/empty, include them in your normalized object
- **Domain-specific fields** - Any fields NOT in this list will automatically go into `violation_details` (JSON)
- **Common mappings** - See "Common Field Mappings" section below for typical source → unified mappings

---

## Required Unified Schema Fields

### Core Identity
| Field | Type | Required | Description | Example |
|-------|------|----------|-------------|---------|
| `violation_id` | VARCHAR | ✅ Yes | Unique identifier for this violation | `"OSHA-2024-001"`, `"ITA-2016-acme-corp-abc123"` |
| `agency` | VARCHAR | ✅ Yes | Agency code (uppercase) | `"OSHA"`, `"FDA"`, `"NHTSA"`, `"FAA"` |

### Location
| Field | Type | Required | Description | Example |
|-------|------|----------|-------------|---------|
| `state` | VARCHAR | ✅ Yes | 2-letter US state code | `"CA"`, `"TX"`, `"NY"` |
| `city` | VARCHAR | ✅ Yes | City name (normalized) | `"Los Angeles"`, `"New York"` |

### Company/Entity
| Field | Type | Required | Description | Example |
|-------|------|----------|-------------|---------|
| `company_name` | VARCHAR | ✅ Yes | Company/establishment name | `"Acme Corporation"` |
| `company_slug` | VARCHAR | ✅ Yes | URL-friendly slug | `"acme-corporation"` |

### Event Stats
| Field | Type | Required | Description | Example |
|-------|------|----------|-------------|---------|
| `event_date` | DATE | ✅ Yes | Date of violation/event (YYYY-MM-DD) | `"2024-12-31"` |
| `fine_amount` | DOUBLE | ✅ Yes | Financial penalty (0 if none) | `12500.00`, `0` |
| `violation_type` | VARCHAR | ✅ Yes | Type/severity classification | `"Serious"`, `"Willful"`, `"Annual Summary"` |

### Content
| Field | Type | Required | Description | Example |
|-------|------|----------|-------------|---------|
| `raw_title` | VARCHAR | ✅ Yes | Original headline/title | `"OSHA Violation at Acme Corp"` |
| `raw_description` | VARCHAR | ✅ Yes | Full description/text | `"Worker injured due to..."` |
| `bedrock_title` | VARCHAR | ❌ Optional | AI-generated summary title | `null` or generated |
| `bedrock_description` | VARCHAR | ❌ Optional | AI-generated summary | `null` or generated |
| `bedrock_tags` | VARCHAR[] | ❌ Optional | AI-generated tags array | `null` or `["safety", "injury"]` |
| `bedrock_generated_at` | TIMESTAMP | ❌ Optional | When AI content was generated | `null` or timestamp |
| `is_verified` | BOOLEAN | ❌ Optional | Editorially reviewed flag | `false` |
| `verified_at` | TIMESTAMP | ❌ Optional | When verified | `null` |
| `verified_by` | VARCHAR | ❌ Optional | Admin email who verified | `null` |
| `source_url` | VARCHAR | ❌ Optional | Link to original source | `null` or URL |

### Metadata
| Field | Type | Required | Description | Example |
|-------|------|----------|-------------|---------|
| `tags` | VARCHAR[] | ❌ Optional | Universal tagging system | `[]` or `["safety", "construction"]` |

### Variant (Domain-Specific)
| Field | Type | Required | Description | Example |
|-------|------|----------|-------------|---------|
| `violation_details` | JSON | ❌ Optional | Domain-specific fields (auto-packed) | `{"naics_code": "1234", "inspection_id": "456"}` |

---

## Common Field Mappings

When creating normalizers, you'll often need to map source data fields to unified schema fields. Here are common mappings:

### Company Name Mapping
```javascript
// ❌ WRONG - Don't use these field names:
employer: "..."
establishment_name: "..."
company: "..."

// ✅ CORRECT - Use unified schema field:
company_name: rawRow.employer || rawRow.establishment_name || rawRow.company_name || ""
```

### Company Slug Generation
```javascript
// Generate from company_name (not employer!)
company_slug: company_name
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
```

### Date Mapping
```javascript
// Parse various date formats to YYYY-MM-DD
event_date: parseDate(rawRow.date) || parseDate(rawRow.event_date) || parseDate(rawRow.inspection_date)
// Result: "2024-12-31"
```

### Fine Amount Mapping
```javascript
// Handle various field names, default to 0
fine_amount: parseFloat(rawRow.fine || rawRow.penalty || rawRow.fine_amount || 0)
```

### Violation Type Mapping
```javascript
// Map to standard types
violation_type: rawRow.violation_type || rawRow.severity || "Unknown"
// Examples: "Serious", "Willful", "Other", "Annual Summary"
```

---

## Example: Complete Normalizer Structure

```javascript
export function normalizeExampleReport(rawRow, filename) {
    const companyName = (rawRow.employer || rawRow.company_name || '').trim();
    
    return {
        // ✅ REQUIRED Unified Schema Fields
        violation_id: generateViolationId(rawRow),
        agency: 'OSHA',
        
        state: normalizeState(rawRow.state),
        city: normalizeCity(rawRow.city),
        
        company_name: companyName,  // ← Map from employer/establishment_name
        company_slug: slugify(companyName),
        
        event_date: parseDate(rawRow.date),  // YYYY-MM-DD format
        fine_amount: parseFloat(rawRow.fine || 0),
        violation_type: rawRow.type || 'Unknown',
        
        raw_title: rawRow.title || '',
        raw_description: rawRow.description || '',
        
        // ❌ Optional fields (can be null/undefined)
        bedrock_title: null,
        bedrock_description: null,
        bedrock_tags: null,
        bedrock_generated_at: null,
        is_verified: false,
        verified_at: null,
        verified_by: null,
        source_url: rawRow.url || null,
        tags: [],
        
        // ✅ Domain-specific fields (will auto-go to violation_details)
        naics_code: rawRow.naics,
        inspection_id: rawRow.inspection,
        // ... any other fields not in unified schema
    };
}
```

---

## What Goes Where?

### ✅ Unified Schema Fields (Top-Level)
These fields are indexed and queryable:
- `violation_id`, `agency`, `state`, `city`
- `company_name`, `company_slug`
- `event_date`, `fine_amount`, `violation_type`
- `raw_title`, `raw_description`
- `bedrock_*` fields (if using AI)
- `tags`

### ✅ Domain-Specific Fields (violation_details JSON)
These fields are stored in JSON but still queryable:
- `naics_code`, `sic_code`
- `inspection_id`, `upa_number`
- `total_deaths`, `total_injuries`
- `annual_average_employees`
- Any other agency-specific fields

**Note**: You don't need to manually pack into `violation_details` - the `validateRow()` function does this automatically!

---

## Common Mistakes to Avoid

1. ❌ **Using `employer` instead of `company_name`**
   ```javascript
   // WRONG
   employer: rawRow.company_name
   
   // CORRECT
   company_name: rawRow.employer || rawRow.company_name
   ```

2. ❌ **Missing required fields**
   ```javascript
   // WRONG - Missing agency, violation_type, etc.
   return { violation_id: "...", company_name: "..." }
   
   // CORRECT - Include all required fields
   return {
       violation_id: "...",
       agency: "OSHA",
       company_name: "...",
       violation_type: "...",
       // ... etc
   }
   ```

3. ❌ **Wrong date format**
   ```javascript
   // WRONG
   event_date: "12/31/2024"
   
   // CORRECT
   event_date: "2024-12-31"  // YYYY-MM-DD
   ```

4. ❌ **Not generating company_slug**
   ```javascript
   // WRONG - Missing slug
   company_name: "Acme Corp"
   
   // CORRECT - Include slug
   company_name: "Acme Corp",
   company_slug: "acme-corp"
   ```

---

## Quick Reference Checklist

When creating a new normalizer, ensure you include:

- [ ] `violation_id` - Unique identifier
- [ ] `agency` - Agency code (uppercase)
- [ ] `state` - 2-letter code
- [ ] `city` - City name
- [ ] `company_name` - Company name (NOT `employer`)
- [ ] `company_slug` - Generated from `company_name`
- [ ] `event_date` - YYYY-MM-DD format
- [ ] `fine_amount` - Number (0 if none)
- [ ] `violation_type` - Type classification
- [ ] `raw_title` - Title/headline
- [ ] `raw_description` - Full description
- [ ] `tags` - Array (can be empty `[]`)

---

## See Also

- `../schema.js` - Full schema definition and validation logic
- `osha.js` - Example normalizer (Severe Injury & Enforcement)
- `osha_ita.js` - Example normalizer (Annual Summary)
- `osha_odi.js` - Example normalizer (Historical Annual Summary)

