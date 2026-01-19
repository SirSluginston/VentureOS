-- FIX: Clean city names with bad data (addresses, zips, etc.)
-- Run each UPDATE separately in Athena
-- Check row counts after each to verify changes

-- STEP 1: Remove trailing ZIP codes (5 or 9 digit)
-- "ORLANDO 32801" → "ORLANDO"
-- "MIAMI 33101-1234" → "MIAMI"
UPDATE "s3tablescatalog/venture-os-the-deep"."silver"."events"
SET city = UPPER(TRIM(REGEXP_REPLACE(city, '\s+\d{5}(-\d{4})?$', '')))
WHERE city IS NOT NULL 
  AND REGEXP_LIKE(city, '\d{5}(-\d{4})?$');

-- STEP 2: For cities with commas, take text AFTER the LAST comma
-- "123 MAIN ST, MIAMI" → "MIAMI"
-- Uses REVERSE trick since Athena doesn't have negative SPLIT_PART index
UPDATE "s3tablescatalog/venture-os-the-deep"."silver"."events"
SET city = UPPER(TRIM(
    REVERSE(SPLIT_PART(REVERSE(city), ',', 1))
))
WHERE city LIKE '%,%'
  AND LENGTH(TRIM(REVERSE(SPLIT_PART(REVERSE(city), ',', 1)))) > 2;

-- STEP 3: Remove leading numbers + word patterns (addresses)
-- "333 E ASHLEY STREET JACKSONVILLE" → "JACKSONVILLE"  
-- "400 TAMPA" → "TAMPA"
UPDATE "s3tablescatalog/venture-os-the-deep"."silver"."events"
SET city = UPPER(TRIM(REGEXP_REPLACE(city, '^\d+\s+', '')))
WHERE city IS NOT NULL
  AND REGEXP_LIKE(city, '^\d+\s');

-- STEP 4: Remove suite/unit prefixes
-- "STE 400 TAMPA" → "TAMPA"
-- "UNIT 5 ORLANDO" → "ORLANDO"
UPDATE "s3tablescatalog/venture-os-the-deep"."silver"."events"
SET city = UPPER(TRIM(REGEXP_REPLACE(city, '^(STE|SUITE|UNIT|APT|BLDG|FLOOR|RM)\s*\d*\s+', '')))
WHERE city IS NOT NULL
  AND REGEXP_LIKE(city, '^(STE|SUITE|UNIT|APT|BLDG|FLOOR|RM)');

-- STEP 5: Remove street suffixes that got left behind
-- "ASHLEY STREET" → remove if it looks like a street name
UPDATE "s3tablescatalog/venture-os-the-deep"."silver"."events"
SET city = UPPER(TRIM(REGEXP_REPLACE(city, '\s+(STREET|ST|AVENUE|AVE|ROAD|RD|DRIVE|DR|BLVD|WAY|LANE|LN)$', '')))
WHERE REGEXP_LIKE(city, '\s+(STREET|ST|AVENUE|AVE|ROAD|RD|DRIVE|DR|BLVD|WAY|LANE|LN)$');

-- STEP 6: Final pass - ensure uppercase and trimmed
UPDATE "s3tablescatalog/venture-os-the-deep"."silver"."events"
SET city = UPPER(TRIM(city))
WHERE city IS NOT NULL AND city != UPPER(TRIM(city));

-- STEP 7: Regenerate city_slug after all cleanups
UPDATE "s3tablescatalog/venture-os-the-deep"."silver"."events"
SET city_slug = CONCAT(state, '-', REPLACE(LOWER(TRIM(city)), ' ', '_'))
WHERE city IS NOT NULL AND state IS NOT NULL;
