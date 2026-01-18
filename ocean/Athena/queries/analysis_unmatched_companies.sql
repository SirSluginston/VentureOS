-- Find the most frequent companies that were NOT matched to the Anchor registry.
-- Use this list to prioritize seeding new companies in VentureOS-Anchor.

SELECT 
    -- Extract company name from raw_data if not directly available, 
    -- or use the normalized name if the processor extracted it but failed to match.
    json_extract_scalar(raw_data, '$.company_name') as raw_company_name,
    COUNT(*) as violation_count,
    MIN(event_date) as first_seen,
    MAX(event_date) as last_seen
FROM "silver"."events"
WHERE company_slug IS NULL
GROUP BY json_extract_scalar(raw_data, '$.company_name')
ORDER BY count(*) DESC
LIMIT 500;
