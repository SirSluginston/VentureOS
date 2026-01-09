# VentureOS Master Plan: The Modern Data Lakehouse

## 🎯 **Goal:** 
Ingest, normalize, and display millions of corporate violations (OSHA, FAA, EPA) to identify "Bad Actor" companies using a scalable, serverless architecture.

---

## 🏗️ **Architecture Overview**

The system follows the **Medallion Architecture** (Bronze → Silver → Gold), powered by **AWS S3 Tables (Iceberg)** for analytics and **DynamoDB** for instant user-facing dashboards.

### **1. Data Pipeline (The "Backend")**
*   **Bronze Layer (Raw Archive):**
    *   **Input:** Manual Uploads / Scrapers.
    *   **Format:** Raw CSV/JSON files.
    *   **Storage:** Standard S3 (`s3://.../bronze/daily/...` or `.../historical/...`).
    *   **Gatekeeper:** 
        *   `daily/*` -> Process Silver + **Update DynamoDB (Live Feed)**.
        *   `historical/*` -> Process Silver ONLY (Skip DynamoDB to save costs).
    *   **Cost:** Intelligent Tiering moves old files to Archive automatically.
*   **Silver Layer (Cleaned Data):**
    *   **Trigger:** S3 Event -> Lambda (`ventureos-parquet-writer`).
    *   **Action:** Normalizes raw data into a Unified Schema.
    *   **Storage:** S3 Tables (`silver.osha`, `silver.faa`).
    *   **Format:** Iceberg Parquet (Partitioned by Agency).
    *   **Schema:** "Core" fields (Date, Company) + "Variant" JSON (`violation_details`).
*   **Gold Layer (Aggregated Stats):**
    *   **Trigger:** Same Lambda (immediately after Silver write).
    *   **Action:** Calculates "Running Totals" (Append-Only Delta).
    *   **Storage:** S3 Tables (`gold.company_stats`, `gold.city_stats`, `gold.state_stats`).
    *   **Purpose:** Fast lookups for "Top 10" lists and dashboards.

### **2. Serving Layer (DynamoDB Strategy)**
We use a **Split-Table Strategy** to optimize for speed and cost.

#### **Table A: `VentureOS-Entities` (Profiles & Stats)**
*   **Purpose:** The "Yellow Pages." Fast lookup for metadata and aggregated stats.
*   **Partition Key (PK):** `ENTITY#<slug>` (e.g., `COMPANY#tesla`, `CITY#austin-tx`, `STATE#tx`).
*   **Sort Key (SK):** `METADATA` (Profile info) or `STATS#<agency>` (Aggregates).
*   **Content:** 
    *   `SK: METADATA` -> { Name: "Tesla", Industry: "Auto", Logo: "..." }
    *   `SK: STATS#all` -> { TotalViolations: 125, TotalFines: $500k, LastUpdated: ... }
*   **Update Mechanism:** `ventureos-gold-sync` Lambda (Gold -> DynamoDB).

#### **Table B: `VentureOS-Violations` (The Feed)**
*   **Purpose:** Instant "Recent Activity" feeds for dashboards.
*   **Strategy:** **"Top-N per Agency" (No TTL).** We keep the *latest 5* items per agency, per entity.
*   **Partition Key (PK):** `ENTITY#<slug>` (e.g., `COMPANY#tesla`).
*   **Sort Key (SK):** `AGENCY#<agency>#DATE#<date>#ID`.
*   **Why?** Allows precise queries: "Give me Tesla's recent OSHA violations."
*   **Update Mechanism:** `ventureos-parquet-writer` (Ingestion Lambda) -> Puts new item -> Checks count -> Deletes oldest if > N.

#### **Table C: `VentureOS-Projects` (Config)**
*   **Purpose:** Branding and UI configs for multi-tenant support (OSHATrail vs. TransportTrail).
*   **Partition Key:** `BRAND#<id>` (e.g., `BRAND#oshatrail`).
*   **Content:** Colors, Layouts, Navigation logic.

---

## 🧠 **The "Bedrock Loop" (AI Summaries)**
*   **Trigger:** User scrolls to a violation on the frontend.
*   **Check:** Frontend checks DynamoDB/API if `summary` exists.
*   **If Missing:**
    1.  Frontend triggers "Generate Summary" endpoint (Fire-and-Forget).
    2.  Frontend displays Raw Description immediately.
    3.  Backend calls Bedrock (Claude/Titan).
    4.  Backend updates DynamoDB with the summary.
    5.  User sees summary on next visit/refresh.

---

## ✅ **Completed Milestones**

### **Phase 1: Foundation (Infrastructure)**
- [x] **IAM Roles:** Configured for S3, Lambda, and S3 Tables access.
- [x] **S3 Buckets:** Created `data` (Bronze) and `data-ocean` (Silver/Gold Tables).
- [x] **DuckDB Integration:** Custom Lambda Layer (`duckdb-neo`) deployed.

### **Phase 2: The Pipeline (Ingestion)**
- [x] **Unified Schema:** Designed "Core + Variant" schema.
- [x] **Normalizers:** Built OSHA Normalizers.
- [x] **Gatekeeper Logic:** Folder-based routing (`daily` vs `historical`) to control DynamoDB costs.
- [x] **Silver Layer:** Lambda writes Iceberg Parquet to S3 Tables.
- [x] **Gold Layer:** Lambda aggregates stats to S3 Tables.

### **Phase 3: The Serving Layer (DynamoDB)**
- [x] **Schema Design:** Split-Table Architecture (`Entities`, `Violations`, `Projects`).
- [x] **Top-N Logic:** Implemented in Ingestion Lambda (Trim old violations automatically).
- [x] **Sync Lambda:** Created `ventureos-gold-sync` to push Gold Stats to DynamoDB `Entities`.
- [x] **Migration:** Migrated configs from Legacy Table (`SirSluginstonVentureOS`) to `VentureOS-Projects`.
- [x] **Cleanup:** Deleted Legacy Table and unused artifacts.

---

## 🚀 **Next Steps (The Roadmap)**

### **Phase 4: API Rebuild (The Modern Interface)**
*   **Objective:** Replace the legacy monolithic API with a modular, split-table aware API.
*   **Status:** **PENDING**
*   **Tasks:**
    *   [ ] **Design V2 API:** Define endpoints (`GET /api/city/:id`, `GET /api/company/:id`).
    *   [ ] **Create Handlers:**
        *   `api-city-handler.js` (Reads from `Entities` + `Violations`).
        *   `api-company-handler.js` (Reads from `Entities` + `Violations`).
        *   `api-config-handler.js` (Reads from `Projects`).
    *   [ ] **Deploy:** Deploy as separate Lambdas or a routed Monolith (ES Modules).

### **Phase 5: Frontend Integration**
*   **Objective:** Connect the UI to the new V2 API.
*   **Tasks:**
    *   [ ] **Update API Client:** Point to new endpoints.
    *   [ ] **Implement "Lazy Load" AI:** Add frontend logic to trigger summary generation.
    *   [ ] **Test Dashboards:** Verify City/Company pages load correctly.

### **Phase 6: Expansion**
*   **Objective:** Add more agencies.
*   *   [ ] **FAA Data:** Build FAA Normalizer & Import Script.
*   *   [ ] **EPA Data:** Build EPA Normalizer.
