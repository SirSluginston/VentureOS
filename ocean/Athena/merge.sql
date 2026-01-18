-- ═══════════════════════════════════════════════════════════════════════════
-- VentureOS - The Conflux (Bronze → Silver MERGE)
-- ═══════════════════════════════════════════════════════════════════════════
--
-- Merges staged Parquet data into the Silver Iceberg table.
-- Uses event_id for idempotent upserts (same input = same result).
--
-- Run in Athena with:
--   Catalog: s3tablescatalog/venture-os-the-deep
--   Database: silver
-- ═══════════════════════════════════════════════════════════════════════════

-- ═══════════════════════════════════════════════════════════════════════════
-- PRE-MERGE CHECKS
-- ═══════════════════════════════════════════════════════════════════════════

-- Bronze count
SELECT COUNT(*) as bronze FROM "AwsDataCatalog"."bronze"."staging_events";

-- Silver count (before)
SELECT COUNT(*) as silver FROM events;

-- ═══════════════════════════════════════════════════════════════════════════
-- THE MERGE
-- ═══════════════════════════════════════════════════════════════════════════

MERGE INTO events AS target
USING "AwsDataCatalog"."bronze"."staging_events" AS source
ON target.event_id = source.event_id

WHEN MATCHED THEN UPDATE SET
    agency = source.agency,
    ingested_at = source.ingested_at,
    event_date = source.event_date,
    state = source.state,
    city = source.city,
    city_slug = source.city_slug,
    company_slug = source.company_slug,
    site_id = source.site_id,
    event_title = source.event_title,
    event_description = source.event_description,
    event_details = source.event_details,
    raw_data = source.raw_data,
    bedrock_event_title = source.bedrock_event_title,
    bedrock_event_description = source.bedrock_event_description

WHEN NOT MATCHED THEN INSERT (
    event_id, agency, ingested_at, event_date,
    state, city, city_slug, company_slug, site_id,
    event_title, event_description, event_details,
    raw_data, bedrock_event_title, bedrock_event_description
) VALUES (
    source.event_id, source.agency, source.ingested_at, source.event_date,
    source.state, source.city, source.city_slug, source.company_slug, source.site_id,
    source.event_title, source.event_description, source.event_details,
    source.raw_data, source.bedrock_event_title, source.bedrock_event_description
);

-- ═══════════════════════════════════════════════════════════════════════════
-- POST-MERGE VERIFICATION
-- ═══════════════════════════════════════════════════════════════════════════

-- Silver count (after)
SELECT COUNT(*) as silver FROM events;

-- By agency
SELECT agency, COUNT(*) as events
FROM events
GROUP BY agency
ORDER BY events DESC;

-- Recent
SELECT event_id, agency, state, city, event_title
FROM events
ORDER BY ingested_at DESC
LIMIT 10;
