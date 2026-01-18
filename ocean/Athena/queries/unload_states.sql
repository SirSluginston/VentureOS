UNLOAD (
    WITH ranked_events AS (
        SELECT 
            state,
            event_date,
            event_title,
            city,
            company_slug,
            agency,
            ROW_NUMBER() OVER (PARTITION BY state ORDER BY event_date DESC) as rn_global,
            ROW_NUMBER() OVER (PARTITION BY state, agency ORDER BY event_date DESC) as rn_agency
        FROM "silver"."events"
        WHERE state IS NOT NULL
    )
    SELECT 
        r.state,
        COUNT(*) as total_events,
        COUNT(DISTINCT r.company_slug) as total_companies,
        COUNT(DISTINCT r.city) as total_cities,
        current_date as aggregation_date,

        ARRAY_AGG(
            CAST(
                ROW(event_date, event_title, city, company_slug, agency) 
                AS ROW(date VARCHAR, title VARCHAR, city VARCHAR, company VARCHAR, agency VARCHAR)
            )
        ) FILTER (WHERE rn_global <= 5) as recent_events,

        ARRAY_AGG(
            CAST(
                ROW(event_date, event_title, city, company_slug) 
                AS ROW(date VARCHAR, title VARCHAR, city VARCHAR, company VARCHAR)
            )
        ) FILTER (WHERE agency = 'OSHA' AND rn_agency <= 5) as recent_events_osha

    FROM ranked_events r
    GROUP BY r.state
) 
TO 's3://venture-os-confluence/gold/states/' 
WITH (format = 'JSON', compression = 'NONE')