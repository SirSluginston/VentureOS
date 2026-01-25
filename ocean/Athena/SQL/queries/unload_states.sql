UNLOAD (
    WITH state_events AS (
        SELECT 
            state,
            city,
            city_slug,
            company_slug,
            company_name,
            event_date,
            COALESCE(bedrock_event_title, event_title) as event_title,
            COALESCE(bedrock_event_description, event_description) as event_description,
            event_source,
            event_id,
            ROW_NUMBER() OVER (PARTITION BY state, event_source ORDER BY event_date DESC) as rn
        FROM "silver"."events"
        WHERE state IS NOT NULL
    ),
    state_stats AS (
        SELECT 
            state,
            COUNT(*) as total_events,
            COUNT(DISTINCT company_slug) as total_companies,
            COUNT(DISTINCT city_slug) as total_cities
        FROM "silver"."events"
        WHERE state IS NOT NULL
        GROUP BY state
    ),
    recent_by_source AS (
        SELECT 
            state,
            event_source,
            ARRAY_AGG(
                CAST(
                    ROW(event_id, event_title, event_description, event_date, company_slug, company_name, city, state) 
                    AS ROW(eventId VARCHAR, eventTitle VARCHAR, eventDescription VARCHAR, eventDate VARCHAR, companySlug VARCHAR, companyName VARCHAR, city VARCHAR, state VARCHAR)
                )
                ORDER BY event_date DESC
            ) as events
        FROM state_events
        WHERE rn <= 5 AND event_source IS NOT NULL
        GROUP BY state, event_source
    ),
    cities_directory AS (
        SELECT 
            state,
            ARRAY_AGG(
                CAST(
                    ROW(city_slug, city, cnt) 
                    AS ROW(slug VARCHAR, name VARCHAR, count BIGINT)
                )
                ORDER BY cnt DESC
            ) as cities
        FROM (
            SELECT state, city_slug, MAX(city) as city, COUNT(*) as cnt
            FROM "silver"."events"
            WHERE state IS NOT NULL AND city_slug IS NOT NULL
            GROUP BY state, city_slug
        ) sub
        GROUP BY state
    )
    SELECT 
        ss.state,
        ss.total_events,
        ss.total_companies,
        ss.total_cities,
        COALESCE(cd.cities, ARRAY[]) as cities_directory,
        MAP_AGG(rbs.event_source, rbs.events) as recentByEventSource
    FROM state_stats ss
    LEFT JOIN cities_directory cd ON ss.state = cd.state
    LEFT JOIN recent_by_source rbs ON ss.state = rbs.state
    GROUP BY ss.state, ss.total_events, ss.total_companies, ss.total_cities, cd.cities
) 
TO 's3://venture-os-ocean/coast/states/' 
WITH (format = 'JSON', compression = 'NONE')
