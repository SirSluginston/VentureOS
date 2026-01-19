-- COMPANY REVIEW QUEUE
-- Shows unmatched company names with suggested slugs, ordered by event count
-- Run this periodically to update the admin review queue
-- Output: s3://venture-os-confluence/admin/company-review-queue/

UNLOAD (
    SELECT 
        company_name as "companyName",
        -- Generate suggested slug from company_name
        LOWER(REGEXP_REPLACE(
            REGEXP_REPLACE(TRIM(company_name), '[^a-zA-Z0-9 ]', ''),
            ' +', '-'
        )) as "suggestedSlug",
        event_count as "eventCount",
        -- Check if has site ID pattern (like "Walmart #1234")
        REGEXP_LIKE(company_name, '#\d+|#\s*\d+') as "hasSiteId",
        cities_count as "citiesCount",
        states_count as "statesCount",
        sample_locations as "sampleLocations"
    FROM (
        -- Inner query: group by raw company name from OSHA headers
        SELECT 
            COALESCE(
                JSON_EXTRACT_SCALAR(raw_data, '$.estab_name'),
                JSON_EXTRACT_SCALAR(raw_data, '$["Estab Name"]'),
                JSON_EXTRACT_SCALAR(raw_data, '$.Employer'),
                JSON_EXTRACT_SCALAR(raw_data, '$.employer'),
                JSON_EXTRACT_SCALAR(raw_data, '$.Company'),
                JSON_EXTRACT_SCALAR(raw_data, '$.company')
            ) as company_name,
            COUNT(*) as event_count,
            COUNT(DISTINCT city) as cities_count,
            COUNT(DISTINCT state) as states_count,
            SLICE(ARRAY_AGG(DISTINCT CONCAT(city, ', ', state)), 1, 3) as sample_locations
        FROM "silver"."events"
        WHERE company_slug IS NULL
          AND COALESCE(
                JSON_EXTRACT_SCALAR(raw_data, '$.estab_name'),
                JSON_EXTRACT_SCALAR(raw_data, '$["Estab Name"]'),
                JSON_EXTRACT_SCALAR(raw_data, '$.Employer'),
                JSON_EXTRACT_SCALAR(raw_data, '$.employer'),
                JSON_EXTRACT_SCALAR(raw_data, '$.Company'),
                JSON_EXTRACT_SCALAR(raw_data, '$.company')
              ) IS NOT NULL
        GROUP BY COALESCE(
                JSON_EXTRACT_SCALAR(raw_data, '$.estab_name'),
                JSON_EXTRACT_SCALAR(raw_data, '$["Estab Name"]'),
                JSON_EXTRACT_SCALAR(raw_data, '$.Employer'),
                JSON_EXTRACT_SCALAR(raw_data, '$.employer'),
                JSON_EXTRACT_SCALAR(raw_data, '$.Company'),
                JSON_EXTRACT_SCALAR(raw_data, '$.company')
              )
        HAVING COUNT(*) >= 5
    )
    ORDER BY event_count DESC
    LIMIT 500
)
TO 's3://venture-os-confluence/admin/company-review-queue/'
WITH (format = 'JSON', compression = 'NONE')
