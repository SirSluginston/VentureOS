UNLOAD (
    SELECT 
        COUNT(*) as total_events,
        COUNT(DISTINCT company_slug) as total_companies,
        COUNT(DISTINCT city_slug) as total_cities,
        COUNT(DISTINCT state) as total_states,
        MIN(event_date) as first_seen,
        MAX(event_date) as last_seen,
        current_date as aggregation_date
    FROM "silver"."events"
) 
TO 's3://venture-os-confluence/gold/nation/' 
WITH (format = 'JSON', compression = 'NONE')