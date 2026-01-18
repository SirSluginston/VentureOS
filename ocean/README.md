# The Event Ocean 🌊

**A Data Lakehouse Architecture by SirSluginston Co.**

> *"One does not simply store data. One cultivates an ocean."*

## Overview

The Event Ocean is a serverless data lakehouse built on AWS, designed to ingest, normalize, and serve regulatory event data at scale. Raw government datasets flow in; structured, queryable intelligence flows out.

**What We Ingest:**
- OSHA Violations & Incident Reports
- FDA Recalls & Warning Letters
- NHTSA Safety Complaints
- *(And more to come...)*

## Architecture

```
                            ┌─────────────────────────────────────────┐
                            │           THE CONFLUENCE                │
                            │         (S3 - Bronze Layer)             │
                            │                                         │
  Raw CSVs ──────────────►  │  Historical/  │  Daily/  │  staging/    │
                            └───────┬───────────────────────┬─────────┘
                                    │                       │
                                    ▼                       │
                            ┌───────────────┐               │
                            │   THE SLUICE  │               │
                            │  (Ingestion)  │               │
                            │               │               │
                            │  Splitter ─►  │               │
                            │  Processor ─► │ ──────────────┘
                            └───────┬───────┘      Parquet
                                    │
                    ┌───────────────┼───────────────┐
                    ▼               ▼               ▼
            ┌───────────┐   ┌───────────┐   ┌───────────┐
            │  SEXTANT  │   │  ANCHOR   │   │ COMPACTOR │
            │  (Schema) │   │ (Entities)│   │  (Merge)  │
            └───────────┘   └───────────┘   └─────┬─────┘
                                                  │
                                                  ▼
                            ┌─────────────────────────────────────────┐
                            │             THE DEEP                    │
                            │       (S3 Tables - Silver Layer)        │
                            │                                         │
                            │         silver.events (Iceberg)         │
                            └───────────────────┬─────────────────────┘
                                                │
                                                ▼
                            ┌─────────────────────────────────────────┐
                            │           THE LIGHTHOUSE                │
                            │       (DynamoDB - Gold Layer)           │
                            │                                         │
                            │    Pre-aggregated views for frontend    │
                            └───────────────────┬─────────────────────┘
                                                │
                                                ▼
                                        ┌───────────────┐
                                        │   FRONTEND    │
                                        │  (SharedUI)   │
                                        └───────────────┘
```

## The Fleet

| Component | Purpose | Technology |
|-----------|---------|------------|
| **Confluence** | Raw data landing zone & staging buffer | S3 |
| **Sluice** | High-throughput ingestion pipeline | Lambda, SQS |
| **Sextant** | Schema registry & header normalization | DynamoDB |
| **Anchor** | Entity registry (Companies, Cities) | DynamoDB |
| **Compactor** | Bronze → Silver merge orchestration | Athena |
| **The Deep** | Queryable Silver layer | S3 Tables (Iceberg) |
| **Lighthouse** | Silver → Gold aggregation for frontend | Lambda, DynamoDB |

## Data Model

Events are the atomic unit. An **Event** is an immutable record of something happening at a specific time and place.

```
┌─────────────────────────────────────────────────────────────┐
│                         EVENT                               │
├─────────────────────────────────────────────────────────────┤
│  event_id         │  Deterministic hash (salted)            │
│  agency           │  OSHA, FDA, NHTSA...                    │
│  event_date       │  When it happened                       │
│  state / city     │  Where it happened                      │
│  company_slug     │  Who was involved (resolved)            │
│  event_title      │  Human-readable summary                 │
│  event_details    │  Structured metadata (JSON)             │
│  raw_data         │  Original source record                 │
└─────────────────────────────────────────────────────────────┘
```

## Design Principles

1. **Immutability First** - Raw data is preserved. Always.
2. **Schema on Read** - Flexible ingestion, structured queries.
3. **Deterministic IDs** - Same input = same `event_id`. No duplicates.
4. **Entity Resolution** - "Walmart Store #1234" resolves to a canonical entity.

## Getting Started

Each component has its own README with setup instructions:

- [Sluice](./Sluice/README.md) - Ingestion Pipeline
- [Sextant](./Sextant/README.md) - Schema Maps
- [Anchor](./Anchor/README.md) - Entity Registry
- [Compactor](./Compactor/README.md) - Merge Operations
- [Lighthouse](./Lighthouse/README.md) - Aggregation Engine

## Tech Stack

- **Compute:** AWS Lambda (Node.js 24)
- **Queuing:** Amazon SQS
- **Storage:** S3, S3 Tables (Iceberg)
- **Query:** Amazon Athena
- **Registry:** DynamoDB

---

*Designed, Dredged, and Deployed by the Debonair Gastropod himself, SirSluginston*

