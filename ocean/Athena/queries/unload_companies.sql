UNLOAD (
    WITH ranked_events AS (
        SELECT 
            company_slug,
            event_date,
            event_title,
            event_description,
            agency,
            ROW_NUMBER() OVER (PARTITION BY company_slug ORDER BY event_date DESC) as rn_global,
            ROW_NUMBER() OVER (PARTITION BY company_slug, agency ORDER BY event_date DESC) as rn_agency
        FROM "silver"."events"
        WHERE company_slug IS NOT NULL
    )
    SELECT 
        r.company_slug,
        COUNT(*) as total_events,
        MAX(r.event_date) as last_active,
        current_date as aggregation_date,
        
        ARRAY_AGG(
            CAST(
                ROW(event_date, event_title, agency) 
                AS ROW(date VARCHAR, title VARCHAR, agency VARCHAR)
            )
        ) FILTER (WHERE rn_global <= 5) as recent_events,

        ARRAY_AGG(
            CAST(
                ROW(event_date, event_title, event_description) 
                AS ROW(date VARCHAR, title VARCHAR, description VARCHAR)
            )
        ) FILTER (WHERE agency = 'OSHA' AND rn_agency <= 5) as recent_events_osha

    FROM ranked_events r
    GROUP BY r.company_slug
) 
TO 's3://venture-os-confluence/gold/companies/' 
WITH (format = 'JSON', compression = 'NONE')