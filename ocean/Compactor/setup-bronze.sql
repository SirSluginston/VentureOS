-- ═══════════════════════════════════════════════════════════════════════════
-- VentureOS - Bronze Layer Setup (Glue External Table)
-- ═══════════════════════════════════════════════════════════════════════════
--
-- Creates an external table pointing to Parquet files from the Sluice.
--
-- Run in Athena with:
--   Data source: AwsDataCatalog
--   Database: (any, or create bronze)
-- ═══════════════════════════════════════════════════════════════════════════

-- Create the bronze database
CREATE DATABASE IF NOT EXISTS bronze
COMMENT 'VentureOS Bronze Layer - Staging data from Sluice';

-- Drop if recreating
-- DROP TABLE IF EXISTS bronze.staging_events;

-- ═══════════════════════════════════════════════════════════════════════════
-- THE TABLE
-- ═══════════════════════════════════════════════════════════════════════════

CREATE EXTERNAL TABLE bronze.staging_events (
    -- Core Fields
    event_id STRING,
    agency STRING,
    ingested_at STRING,
    
    -- Temporal
    event_date STRING,
    
    -- Location
    state STRING,
    city STRING,
    city_slug STRING,
    
    -- Entity
    company_slug STRING,
    site_id STRING,
    
    -- Display
    event_title STRING,
    event_description STRING,
    event_details STRING,
    
    -- Preservation
    raw_data STRING,
    
    -- AI Enhancement
    bedrock_event_title STRING,
    bedrock_event_description STRING
)
STORED AS PARQUET
LOCATION 's3://venture-os-confluence/staging/'
TBLPROPERTIES (
    'classification' = 'parquet',
    'parquet.compression' = 'SNAPPY'
);

-- ═══════════════════════════════════════════════════════════════════════════
-- VERIFICATION
-- ═══════════════════════════════════════════════════════════════════════════

SELECT COUNT(*) as staged_events FROM bronze.staging_events;

-- Preview
SELECT event_id, agency, state, city, event_title, site_id
FROM bronze.staging_events
LIMIT 10;

-- ═══════════════════════════════════════════════════════════════════════════
-- MONITORING QUERIES
-- ═══════════════════════════════════════════════════════════════════════════

-- By agency
SELECT agency, COUNT(*) as events
FROM bronze.staging_events
GROUP BY agency;

-- By ingest date
SELECT DATE(ingested_at) as date, COUNT(*) as events
FROM bronze.staging_events
GROUP BY DATE(ingested_at)
ORDER BY date DESC;
