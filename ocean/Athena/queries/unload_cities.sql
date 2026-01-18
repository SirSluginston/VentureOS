UNLOAD (
    WITH ranked_events AS (
        SELECT 
            city_slug,
            city,
            state,
            event_date,
            event_title,
            company_slug,
            agency,
            ROW_NUMBER() OVER (PARTITION BY city_slug ORDER BY event_date DESC) as rn_global,
            ROW_NUMBER() OVER (PARTITION BY city_slug, agency ORDER BY event_date DESC) as rn_agency
        FROM "silver"."events"
        WHERE city_slug IS NOT NULL
    )
    SELECT 
        r.city_slug,
        r.city,
        r.state,
        COUNT(*) as total_events,
        COUNT(DISTINCT r.company_slug) as total_companies,
        current_date as aggregation_date,

        ARRAY_AGG(
            CAST(
                ROW(event_date, event_title, company_slug, agency) 
                AS ROW(date VARCHAR, title VARCHAR, company VARCHAR, agency VARCHAR)
            )
        ) FILTER (WHERE rn_global <= 5) as recent_events,

        ARRAY_AGG(
            CAST(
                ROW(event_date, event_title, company_slug) 
                AS ROW(date VARCHAR, title VARCHAR, company VARCHAR)
            )
        ) FILTER (WHERE agency = 'OSHA' AND rn_agency <= 5) as recent_events_osha

    FROM ranked_events r
    GROUP BY r.city_slug, r.city, r.state
) 
TO 's3://venture-os-confluence/gold/cities/' 
WITH (format = 'JSON', compression = 'NONE')