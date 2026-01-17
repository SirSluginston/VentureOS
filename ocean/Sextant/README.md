# The Sextant 🧭

**Schema Registry & Header Normalization**

> *"Every voyage needs a navigator. Every dataset needs a map."*

## Overview

Government datasets are notoriously inconsistent. One OSHA dataset uses `C7A` for skin disorders. Another calls it `total_skin_disorders`. The Sextant maps these cryptic headers to a unified canonical schema.

## How It Works

```
Raw CSV Header          Sextant Map           Canonical Key
─────────────────────────────────────────────────────────────
"C7A"            ──►    header_map     ──►    "illness_skin"
"total_skin_disorders"  ─────────────────►    "illness_skin"
"SKIN_M2"        ─────────────────────────►   "illness_skin"
```

One canonical key. Multiple source aliases. Query once.

## Storage

- **Table:** `VentureOS-Sextant` (DynamoDB)
- **PK:** `AGENCY#{agency}` (e.g., `AGENCY#osha`)
- **SK:** `SCHEMA#{dataset}` (e.g., `SCHEMA#severe-incident`)
- **Attribute:** `header_map` - Object mapping canonical keys to source header arrays

## Current Maps

| Agency | Dataset | Coverage |
|--------|---------|----------|
| OSHA | `severe-incident` | Injury reports with narratives |
| OSHA | `odi-96-01` | Annual surveys (legacy format) |
| OSHA | `odi-02-11` | Annual surveys (modern format) |
| OSHA | `ita` | Injury Tracking Application |

## Canonical Schema (Unified Keys)

The Sextant normalizes to these core fields:

**Identity & Time:**
- `semantic_id`, `event_date`, `inspection_id`

**Location:**
- `street`, `city`, `state`, `zip`, `location_lat`, `location_lon`

**Entity:**
- `company_name`, `ein`, `naics_code`, `sic_code`

**Incident Data:**
- `description`, `violation_type`, `violation_code`
- `injuries_*`, `illness_*`, `total_*`

## Adding New Maps

1. Analyze the source CSV headers
2. Add a new entry to `seed-sextant.js`
3. Run the seed script:

```bash
node VentureOS/ocean/Sextant/seed-sextant.js
```

## Runtime Usage

The Processor Lambda loads maps at runtime:

```javascript
// Fetches header_map from DynamoDB
const map = await getSextantMap('osha', 'severe-incident');

// Apply to raw row
for (const [canonicalKey, sourceAliases] of Object.entries(map)) {
    // First matching alias wins
}
```

---

*"Without the Sextant, one is merely adrift in a sea of columns."*

