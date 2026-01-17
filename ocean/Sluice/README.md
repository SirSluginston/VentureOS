# The Sluice ⚙️

**High-Throughput Ingestion Pipeline**

> *"Raise the gate. Let the data flow."*

## Overview

The Sluice is an SQS fan-out ingestion engine designed to process massive CSV files (1GB+) without Lambda timeouts. Raw government data enters; normalized Parquet exits.

## Architecture

```
S3 Upload ──► SQS Intake ──► Splitter ──► SQS Flume ──► Processor ──► Parquet
   │              │              │             │             │           │
   │              │              │             │             │           │
Historical/   VentureOS      Streams &    VentureOS      Normalize    staging/
  Daily/       -Intake        Batches      -Flume       & Scribe     (Bronze)
```

**Why Fan-Out?**

A single 1GB CSV with 2 million rows would timeout any Lambda. By splitting into batches of 10 rows and processing in parallel, we achieve:
- No timeouts
- Horizontal scale
- Fault isolation (one bad row doesn't kill the batch)

## Components

### Splitter (`splitter.js`)
The gatekeeper. Streams incoming CSVs line-by-line, chunks them into batches, and dispatches to the processing queue.

- **Trigger:** S3 ObjectCreated → SQS Intake
- **Output:** Batches of 10 rows → SQS Flume
- **Memory:** ~128MB (streaming, not loading)

### Processor (`processor.js`)
The worker. Receives batches, normalizes via Sextant, resolves entities via Anchor, and scribes Parquet to staging.

- **Trigger:** SQS Flume
- **Output:** Parquet files → `s3://venture-os-confluence/staging/`
- **Memory:** 2048MB (Parquet serialization)

## Schema Output

The processor outputs 15-column Parquet files:

| Column | Description |
|--------|-------------|
| `event_id` | Deterministic hash of the source row |
| `agency` | Source agency (OSHA, FDA, etc.) |
| `ingested_at` | Processing timestamp |
| `event_date` | Date of the event |
| `state` / `city` | Location fields |
| `city_slug` | Canonical city reference |
| `company_slug` | Resolved entity reference |
| `site_id` | Facility identifier (if applicable) |
| `event_title` | Generated human-readable title |
| `event_description` | Extracted narrative |
| `event_details` | All normalized fields (JSON) |
| `raw_data` | Original row (JSON) |
| `bedrock_*` | AI-enhanced fields (populated later) |

## Deployment

```bash
# From workspace root
node VentureOS/ocean/Sluice/deploy-final.js
```

This zips the code with `node_modules` and deploys both Lambdas.

## File Path Convention

The S3 path determines which Sextant schema map is applied:

```
Historical/{agency}/{dataset}/{file}.csv
    │          │         │
    │          │         └── Determines SK: SCHEMA#{dataset}
    │          └──────────── Determines PK: AGENCY#{agency}
    └─────────────────────── Root folder (Historical or Daily)
```

**Examples:**
- `Historical/osha/severe-incident/2024.csv` → `AGENCY#osha` + `SCHEMA#severe-incident`
- `Daily/fda/recalls/latest.csv` → `AGENCY#fda` + `SCHEMA#recalls`

---

*"Thanks to the Sluice, our ocean is trash-free."*

