UNLOAD (
    WITH city_events AS (
        SELECT 
            city_slug,
            city,
            state,
            company_slug,
            company_name,
            event_date,
            COALESCE(bedrock_event_title, event_title) as event_title,
            COALESCE(bedrock_event_description, event_description) as event_description,
            event_source,
            event_id,
            ROW_NUMBER() OVER (PARTITION BY city_slug, event_source ORDER BY event_date DESC) as rn
        FROM "silver"."events"
        WHERE city_slug IS NOT NULL
    ),
    city_stats AS (
        SELECT 
            city_slug,
            MAX(city) as city,
            MAX(state) as state,
            COUNT(*) as total_events,
            COUNT(DISTINCT company_slug) as total_companies
        FROM "silver"."events"
        WHERE city_slug IS NOT NULL
        GROUP BY city_slug
    ),
    recent_by_source AS (
        SELECT 
            city_slug,
            event_source,
            ARRAY_AGG(
                CAST(
                    ROW(event_id, event_title, event_description, event_date, company_slug, company_name, city, state) 
                    AS ROW(eventId VARCHAR, eventTitle VARCHAR, eventDescription VARCHAR, eventDate VARCHAR, companySlug VARCHAR, companyName VARCHAR, city VARCHAR, state VARCHAR)
                )
                ORDER BY event_date DESC
            ) as events
        FROM city_events
        WHERE rn <= 5
        GROUP BY city_slug, event_source
    ),
    companies_directory AS (
        SELECT 
            city_slug,
            ARRAY_AGG(
                CAST(
                    ROW(company_slug, company_name, cnt) 
                    AS ROW(slug VARCHAR, name VARCHAR, count BIGINT)
                )
                ORDER BY cnt DESC
            ) as companies
        FROM (
            SELECT city_slug, company_slug, MAX(company_name) as company_name, COUNT(*) as cnt
            FROM "silver"."events"
            WHERE city_slug IS NOT NULL AND company_slug IS NOT NULL
            GROUP BY city_slug, company_slug
        ) sub
        GROUP BY city_slug
    )
    SELECT 
        cs.city_slug,
        cs.city,
        cs.total_events,
        cs.total_companies,
        COALESCE(cd.companies, ARRAY[]) as companies_directory,
        MAP_AGG(rbs.event_source, rbs.events) as recentByEventSource,
        cs.state
    FROM city_stats cs
    LEFT JOIN companies_directory cd ON cs.city_slug = cd.city_slug
    LEFT JOIN recent_by_source rbs ON cs.city_slug = rbs.city_slug
    GROUP BY cs.city_slug, cs.city, cs.state, cs.total_events, cs.total_companies, cd.companies
) 
TO 's3://venture-os-ocean/coast/cities/' 
WITH (format = 'JSON', compression = 'NONE', partitioned_by = ARRAY['state'])
