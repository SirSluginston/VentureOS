-- FIX: Normalize Silver layer to ALL CAPS for consistency
-- Silver = source of truth (UPPER), Lighthouse = display-ready (Title Case)
-- Run each UPDATE separately in Athena

-- STEP 1: Trim whitespace from city names (already done)
-- UPDATE "s3tablescatalog/venture-os-the-deep"."silver"."events"
-- SET city = TRIM(city)
-- WHERE city IS NOT NULL AND city != TRIM(city);

-- STEP 2: Normalize city names to UPPERCASE
UPDATE "s3tablescatalog/venture-os-the-deep"."silver"."events"
SET city = UPPER(TRIM(city))
WHERE city IS NOT NULL 
  AND city != UPPER(TRIM(city));

-- STEP 3: Regenerate city_slug to ensure consistency
UPDATE "s3tablescatalog/venture-os-the-deep"."silver"."events"
SET city_slug = CONCAT(state, '-', REPLACE(LOWER(TRIM(city)), ' ', '_'))
WHERE city IS NOT NULL AND state IS NOT NULL;

-- STEP 4: Trim event_title whitespace (optional cleanup)
UPDATE "s3tablescatalog/venture-os-the-deep"."silver"."events"
SET event_title = TRIM(event_title)
WHERE event_title IS NOT NULL 
  AND event_title != TRIM(event_title);
