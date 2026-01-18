# The Lighthouse 🔦

**Silver → Gold Aggregation Engine**

> *"From the depths, the beacon rises."*

## Overview

The Lighthouse transforms raw event data from the Silver layer (Iceberg) into pre-aggregated views in DynamoDB for instant frontend consumption. No query-time aggregation. Just fast reads.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          The Lighthouse                                  │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│   Silver (Athena)          Aggregator            Gold (DynamoDB)         │
│   ┌──────────────┐        ┌──────────┐         ┌──────────────────┐     │
│   │ silver.events│ ──►    │  Lambda  │  ──►    │ VentureOS-       │     │
│   │              │        │          │         │   Lighthouse     │     │
│   └──────────────┘        └──────────┘         └──────────────────┘     │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

## Trigger Options

| Trigger | When | Use Case |
|---------|------|----------|
| **Post-MERGE** | After Compactor runs | Daily batch updates |
| **Scheduled** | EventBridge cron | Nightly refresh |
| **Manual** | Admin button | Testing / force refresh |

## Gold Layer Schema

**Table:** `VentureOS-Lighthouse`

### Entity Summaries

| PK | SK | Contains |
|----|----|---------| 
| `NATION#usa` | `SUMMARY` | National totals, top states/companies |
| `STATE#{code}` | `SUMMARY` | State totals, top cities/companies |
| `CITY#{state}-{slug}` | `SUMMARY` | City totals, top companies, recent events |
| `COMPANY#{slug}` | `SUMMARY` | Company totals, sites, states present |
| `SITE#{company}#{id}` | `SUMMARY` | Site-specific event list |

### Summary Item Structure

```json
{
  "PK": "STATE#TN",
  "SK": "SUMMARY",
  "name": "Tennessee",
  "stats": {
    "totalEvents": 2847,
    "violations": 2103,
    "injuries": 489,
    "fatalities": 12,
    "fines": 4250000
  },
  "safetyScore": {
    "value": 72,
    "trend": "improving",
    "disclaimer": "Based on reported OSHA incidents..."
  },
  "topCities": [
    { "slug": "TN-nashville", "name": "Nashville", "count": 847 },
    { "slug": "TN-memphis", "name": "Memphis", "count": 632 }
  ],
  "topCompanies": [
    { "slug": "walmart-inc", "name": "Walmart Inc.", "count": 156 }
  ],
  "recentEvents": [
    { "eventId": "abc123...", "title": "...", "date": "2025-01-15" }
  ],
  "updatedAt": "2026-01-17T12:00:00Z"
}
```

## Aggregation Queries

The Lighthouse runs these queries against Silver:

### State Summary
```sql
SELECT 
    state,
    COUNT(*) as total_events,
    COUNT(DISTINCT company_slug) as unique_companies,
    COUNT(DISTINCT city_slug) as unique_cities
FROM silver.events
WHERE state = ?
GROUP BY state;
```

### Top Companies by State
```sql
SELECT 
    company_slug,
    COUNT(*) as event_count
FROM silver.events
WHERE state = ?
GROUP BY company_slug
ORDER BY event_count DESC
LIMIT 10;
```

### Recent Events
```sql
SELECT 
    event_id, event_title, event_date, 
    company_slug, city, state
FROM silver.events
WHERE state = ?
ORDER BY event_date DESC
LIMIT 20;
```

## Safety Score Calculation

```
SafetyScore = 100 - (weighted_incident_rate * 10)

Where:
- Base: 100 (perfect)
- Deductions for:
  - Fatalities: -5 per incident
  - Hospitalizations: -2 per incident
  - Violations: -0.5 per incident
- Normalized to 0-100 scale
- Trend: Compare last 12mo vs previous 12mo
```

**Important:** SafetyScore includes disclaimer about data limitations.

## Files

| File | Purpose |
|------|---------|
| `aggregate.js` | Lambda handler - queries Athena, writes DynamoDB |
| `queries.sql` | SQL templates for aggregation |
| `deploy.js` | Package and deploy to AWS |

## Deployment

```bash
# From workspace root
node VentureOS/ocean/Lighthouse/deploy.js
```

## Usage

### Manual Trigger
```bash
aws lambda invoke --function-name Lighthouse-Aggregator \
    --payload '{"scope": "all"}' \
    response.json
```

### Scoped Refresh
```bash
# Just one state
aws lambda invoke --function-name Lighthouse-Aggregator \
    --payload '{"scope": "state", "key": "TN"}' \
    response.json
```

---

*"The Lighthouse does not seek the ships. The ships seek the Lighthouse."*

