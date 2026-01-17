-- ═══════════════════════════════════════════════════════════════════════════
-- VentureOS - Create Silver Events Table
-- ═══════════════════════════════════════════════════════════════════════════
--
-- Prerequisites:
-- 1. S3 Tables bucket "venture-os-the-deep" exists
-- 2. Athena data source connected to S3 Tables catalog
-- 3. Namespace "silver" created (via CLI or setup-all.js)
--
-- Run in Athena with:
--   Catalog: s3tablescatalog/venture-os-the-deep
--   Database: silver
-- ═══════════════════════════════════════════════════════════════════════════

-- Verify namespace exists
SHOW DATABASES;

-- Drop if recreating (WARNING: deletes all data!)
-- DROP TABLE IF EXISTS events;

-- ═══════════════════════════════════════════════════════════════════════════
-- THE TABLE
-- ═══════════════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS events (
    -- Core Fields (Required)
    event_id STRING,                    -- Deterministic hash (salted)
    agency STRING,                      -- OSHA, FDA, NHTSA, etc.
    ingested_at STRING,                 -- ISO8601 processing timestamp
    
    -- Temporal
    event_date STRING,                  -- When the event occurred
    
    -- Location
    state STRING,                       -- State code (TN, CA, etc.)
    city STRING,                        -- City name
    city_slug STRING,                   -- Canonical ref (CITY#TN-knoxville)
    
    -- Entity
    company_slug STRING,                -- Resolved company ref
    site_id STRING,                     -- Facility ID (e.g., "1234" from "Walmart #1234")
    
    -- Display
    event_title STRING,                 -- Generated human-readable title
    event_description STRING,           -- Extracted narrative (if available)
    event_details STRING,               -- All normalized fields (JSON)
    
    -- Preservation
    raw_data STRING,                    -- Original source row (JSON)
    
    -- AI Enhancement (NULL until processed)
    bedrock_event_title STRING,
    bedrock_event_description STRING
);

-- ═══════════════════════════════════════════════════════════════════════════
-- VERIFICATION
-- ═══════════════════════════════════════════════════════════════════════════

DESCRIBE events;

SELECT COUNT(*) as total_events FROM events;
