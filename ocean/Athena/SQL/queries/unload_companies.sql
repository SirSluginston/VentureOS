UNLOAD (
    WITH company_events AS (
        SELECT 
            company_slug,
            company_name,
            city,
            state,
            event_date,
            COALESCE(bedrock_event_title, event_title) as event_title,
            COALESCE(bedrock_event_description, event_description) as event_description,
            event_source,
            event_id,
            ROW_NUMBER() OVER (PARTITION BY company_slug, event_source ORDER BY event_date DESC) as rn
        FROM "silver"."events"
        WHERE company_slug IS NOT NULL
    ),
    company_stats AS (
        SELECT 
            company_slug,
            MAX(company_name) as company_name,
            COUNT(*) as total_events,
            CAST(MAX(event_date) AS VARCHAR) as last_active
        FROM "silver"."events"
        WHERE company_slug IS NOT NULL
        GROUP BY company_slug
    ),
    recent_by_source AS (
        SELECT 
            company_slug,
            event_source,
            ARRAY_AGG(
                CAST(
                    ROW(event_id, event_title, event_description, event_date, city, state) 
                    AS ROW(eventId VARCHAR, eventTitle VARCHAR, eventDescription VARCHAR, eventDate VARCHAR, city VARCHAR, state VARCHAR)
                )
                ORDER BY event_date DESC
            ) as events
        FROM company_events
        WHERE rn <= 5
        GROUP BY company_slug, event_source
    )
    SELECT 
        cs.company_slug,
        cs.company_name,
        cs.total_events,
        cs.last_active,
        MAP_AGG(rbs.event_source, rbs.events) as recentByEventSource
    FROM company_stats cs
    LEFT JOIN recent_by_source rbs ON cs.company_slug = rbs.company_slug
    GROUP BY cs.company_slug, cs.company_name, cs.total_events, cs.last_active
) 
TO 's3://venture-os-ocean/coast/companies/' 
WITH (format = 'JSON', compression = 'NONE')
