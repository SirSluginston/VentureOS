# The Anchor ⚓

**Entity Registry & Resolution Engine**

> *"In turbulent data, one needs a fixed point."*

## Overview

Raw datasets contain messy entity references: "Walmart Inc", "WAL-MART STORES INC", "Walmart Store #4521". The Anchor resolves these to canonical slugs, enabling cross-dataset entity aggregation.

## Entity Types

| Type | Count | Source |
|------|-------|--------|
| **Cities** | ~32,000 | US Census Bureau Gazetteer |
| **Companies** | ~6,800 | NASDAQ/NYSE/AMEX Listings |
| **States** | 56 | US States & Territories |

## Storage

- **Table:** `VentureOS-Anchor` (DynamoDB)
- **Primary Key Pattern:** `{TYPE}#{slug}`
- **GSI1:** Reverse lookup by alias

## Schema

### Cities
```
PK: CITY#TN-knoxville
SK: METADATA
─────────────────────────
name: "Knoxville"
state: "TN"
place_type: "City"
location: { lat, lon }
```

### Companies
```
PK: SLUG#walmart-inc
SK: METADATA
─────────────────────────
name: "Walmart Inc."
sector: "Consumer Defensive"
industry: "Discount Stores"
tickers: { WMT: {...} }
```

## Resolution Flow

```
Raw Input                    Anchor Lookup              Result
───────────────────────────────────────────────────────────────
"Walmart #1234"        ──►   GSI1: ALIAS#walmart   ──►  SLUG#walmart-inc
                             site_id extracted: "1234"
```

The Processor:
1. Extracts site identifiers (e.g., `#1234`)
2. Queries Anchor GSI1 for alias match
3. Returns canonical slug
4. Stores both `company_slug` and `site_id`

## Seeding

```bash
# Cities (32k Census places)
node VentureOS/ocean/Anchor/Cities/import-census-cities.js

# Companies (NASDAQ screener)
node VentureOS/ocean/Anchor/Companies/import-companies.js

# States
node VentureOS/ocean/Anchor/States/seed-states.js
```

## GSI1 (Alias Index)

Enables fuzzy matching via pre-computed aliases:

```
GSI1PK: ALIAS#knoxville-tn    →  CITY#TN-knoxville
GSI1PK: ALIAS#aapl            →  SLUG#apple-inc
GSI1PK: ALIAS#walmart         →  SLUG#walmart-inc
```

## Future: Fuzzy Matching

The current implementation uses exact alias matching. Future enhancements may include:
- Levenshtein distance for typo tolerance
- Company name normalization (strip "LLC", "Inc", etc.)
- Bedrock-powered entity resolution for ambiguous cases

---

*"Drop anchor. Know where you stand."*

