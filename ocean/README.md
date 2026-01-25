# The Event Ocean 🌊

**A Data Lakehouse Architecture by SirSluginston Co.**

> *"One does not simply store data. One cultivates an ocean."*

## Overview

The Event Ocean is a serverless data lakehouse built on AWS, designed to ingest, normalize, and serve regulatory event data at scale. Raw government datasets flow in; structured, queryable intelligence flows out.

**What We Ingest:**
- OSHA Severe Incident Reports
- OSHA ITA Annual Summaries
- OSHA ODI Survey Data
- FDA Recalls & Warning Letters *(planned)*
- NHTSA Safety Complaints *(planned)*

## Architecture

```
                    ┌─────────────────────────────────────────────────────┐
                    │                  THE CONFLUENCE                     │
                    │                (S3 - Landing Zone)                  │
                    │                                                     │
  Raw CSVs ───────► │   Historical/osha/severe-incident/                  │
                    │   Historical/osha/ita/                              │
                    │   Historical/osha/odi/                              │
                    └──────────────────────┬──────────────────────────────┘
                                           │ S3 Event Trigger
                                           ▼
                    ┌─────────────────────────────────────────────────────┐
                    │                   THE SLUICE                        │
                    │               (Ingestion Pipeline)                  │
                    │                                                     │
                    │   ┌──────────┐    SQS    ┌───────────┐              │
                    │   │ Splitter │ ────────► │ Processor │              │
                    │   └──────────┘  Batches  └─────┬─────┘              │
                    │                                │                    │
                    │   ┌──────────┐          ┌──────┴──────┐             │
                    │   │ SEXTANT  │◄─────────┤ Normalize & │             │
                    │   │ (Schema) │  Lookup  │   Resolve   │             │
                    │   └──────────┘          └──────┬──────┘             │
                    │                                │                    │
                    │   ┌──────────┐                 │                    │
                    │   │  ANCHOR  │◄────────────────┘                    │
                    │   │(Entities)│  Entity Resolution                   │
                    │   └──────────┘                                      │
                    └──────────────────────┬──────────────────────────────┘
                                           │ Parquet Files
                                           ▼
                    ┌─────────────────────────────────────────────────────┐
                    │              CONFLUENCE staging/                    │
                    │                (Bronze Layer)                       │
                    └──────────────────────┬──────────────────────────────┘
                                           │ Athena MERGE
                                           ▼
                    ┌─────────────────────────────────────────────────────┐
                    │                   THE DEEP                          │
                    │            (S3 Tables - Silver Layer)               │
                    │                                                     │
                    │              silver.events (Iceberg)                │
                    │    Queryable, Time-Travel, Schema Evolution         │
                    └──────────────────────┬──────────────────────────────┘
                                           │ Athena UNLOAD
                                           ▼
                    ┌─────────────────────────────────────────────────────┐
                    │                  GOLD LAYER                         │
                    │              (S3 - Aggregated JSON)                 │
                    │                                                     │
                    │   nation/usa.json    states/*.json                  │
                    │   cities/*.json      companies/*.json               │
                    └──────────────────────┬──────────────────────────────┘
                                           │ S3 Event Trigger
                                           ▼
                    ┌─────────────────────────────────────────────────────┐
                    │                 THE LIGHTHOUSE                      │
                    │              (DynamoDB - Serving Layer)             │
                    │                                                     │
                    │   NATION#usa          STATE#CO                      │
                    │   CITY#CO-denver      COMPANY#walmart-inc           │
                    │                                                     │
                    │         Optimized for Frontend Reads                │
                    └──────────────────────┬──────────────────────────────┘
                                           │
                                           ▼
                    ┌─────────────────────────────────────────────────────┐
                    │                   FRONTEND                          │
                    │                  (SharedUI)                         │
                    │                                                     │
                    │   NationPage → StatePage → CityPage → CompanyPage   │
                    └─────────────────────────────────────────────────────┘
```

## The Fleet

| Component | Purpose | Technology |
|-----------|---------|------------|
| **Confluence** | Raw data landing & staging buffer | S3 |
| **Sluice** | High-throughput ingestion pipeline | Lambda, SQS |
| **Sextant** | Schema registry & header normalization | DynamoDB |
| **Anchor** | Entity registry (Companies, Cities, States) | DynamoDB |
| **The Deep** | Queryable Silver layer with time-travel | S3 Tables (Iceberg) |
| **Lighthouse** | Pre-aggregated views for frontend | Lambda, DynamoDB |

## Data Flow

### 1. Ingestion (Sluice)
Raw CSV files dropped into Confluence trigger the **Splitter** Lambda, which chunks large files into batched SQS messages. The **Processor** Lambda consumes these batches, normalizes headers via **Sextant** mappings, resolves entities via **Anchor**, and writes Parquet files to the staging area.

### 2. Merge (Bronze → Silver)
Athena MERGE queries deduplicate and merge staged data into the **silver.events** Iceberg table in The Deep. Deterministic `event_id` hashes ensure idempotent ingestion.

### 3. Aggregation (Silver → Gold)
Athena UNLOAD queries aggregate Silver data into pre-computed JSON summaries: nation-level stats, state directories, city breakdowns, and company profiles. These land in S3 Gold.

### 4. Serving (Lighthouse)
S3 triggers invoke the **Lighthouse** Lambda, which transforms Gold JSON into DynamoDB items optimized for single-query frontend reads.

## Data Model

Events are the atomic unit. An **Event** is an immutable record of something that happened at a specific time and place.

```
┌─────────────────────────────────────────────────────────────┐
│                         EVENT                               │
├─────────────────────────────────────────────────────────────┤
│  event_id           │  Deterministic hash (agency-salted)   │
│  agency             │  OSHA, FDA, NHTSA...                  │
│  event_date         │  When it happened                     │
│  state              │  2-letter abbreviation (CO, TX, NY)   │
│  city               │  City name                            │
│  city_slug          │  URL-safe identifier (CO-denver)      │
│  company_slug       │  Resolved company (walmart-inc)       │
│  site_id            │  Specific location identifier         │
│  event_title        │  Human-readable summary               │
│  event_description  │  Detailed narrative                   │
│  event_details      │  Structured metadata (JSON)           │
│  raw_data           │  Original source record (preserved)   │
└─────────────────────────────────────────────────────────────┘
```

## Design Principles

1. **Immutability First** - Raw data is preserved. Always.
2. **Schema on Read** - Flexible ingestion, structured queries.
3. **Deterministic IDs** - Same input = same `event_id`. No duplicates.
4. **Entity Resolution** - "Walmart Store #1234" → `walmart-inc` + `site_id: 1234`
5. **Batch Everything** - SQS batching, Lambda batching, DynamoDB BatchWriteItem.
6. **Frontend-Ready** - Lighthouse outputs match SharedUI types exactly.

## URL Structure

Clean, hierarchical navigation:
```
/usa                    → Nation overview
/state/co               → State page (Colorado)
/city/CO-denver         → City page
/company/walmart-inc    → Company profile
/site/walmart-inc_1234  → Specific site
```

## Tech Stack

- **Compute:** AWS Lambda (Node.js 22.x)
- **Queuing:** Amazon SQS (batch processing)
- **Storage:** S3, S3 Tables (Apache Iceberg)
- **Query:** Amazon Athena
- **Database:** DynamoDB (single-table design)
- **Frontend:** React + SharedUI component library

---

## Operational Runbooks 🛠️

### 1. Admin Dashboard & Sluice-Ops
The **Admin Page** (`/admin`) provides visibility into the ingestion pipeline and quarantine queue.
- **Backend:** Powered by `Sluice-Ops` Lambda (Source: `VentureOS/Ocean/Sluice/ops.js`).
- **Access:** Restricted to Admin users via Cognito.
- **Refresh:** Manual refresh only (Auto-polling disabled to save costs).

### 2. Anchor Database Maintenance
The **Anchor** table (`VentureOS-Anchor`) validates city/state/company names. 
If validated cities appear as "Unknown City" in Quarantine:
1. **Source:** `VentureOS/Ocean/Anchor/Cities/import-census-cities.js`
2. **Logic:** Ensure `parseName()` handles suffixes like "municipality" or "(balance)".
3. **Action:** Run `node import-census-cities.js` to re-seed/upsert corrected metadata.
4. **Validation:** Re-run ingestion to clear quarantine.

### 3. Historical Re-Ingestion
To re-process historical files (e.g., after a schema fix):
1. **Script:** `node reingest_history.js` (Root Directory)
2. **Mechanism:** Copies S3 objects to themselves in place, updating metadata `reingest-timestamp`.
3. **Trigger:** S3 Event -> SQS -> Sluice Pipeline.
4. **Monitoring:** Watch `VentureOS-Flume` SQS queue depth or use Admin Page.

### 4. Quarantine Management
- **Quarantine Bucket:** `s3://venture-os-confluence/quarantine/` (Parquet files)
- **Resolution:**
    - Fix the data source or Anchor validation logic.
    - Delete affected quarantine files (or clear bucket).
    - Re-trigger ingestion for the original source files.

---

*Designed, Dredged, and Deployed by the Debonair Gastropod himself, SirSluginston* 🐌

