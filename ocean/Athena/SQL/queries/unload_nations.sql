UNLOAD (
    WITH state_counts AS (
        SELECT 
            state,
            COUNT(*) as count
        FROM "silver"."events"
        WHERE state IS NOT NULL
        GROUP BY state
    ),
    totals AS (
        SELECT 
            COUNT(*) as total_events,
            COUNT(DISTINCT company_slug) as total_companies,
            COUNT(DISTINCT city_slug) as total_cities,
            COUNT(DISTINCT state) as total_states,
            CAST(MIN(event_date) AS VARCHAR) as first_seen,
            CAST(MAX(event_date) AS VARCHAR) as last_seen,
            current_date as aggregation_date
        FROM "silver"."events"
    ),
    recent_events AS (
        SELECT 
            event_source,
            event_id,
            COALESCE(bedrock_event_title, event_title) as event_title,
            COALESCE(bedrock_event_description, event_description) as event_description,
            event_date,
            company_name,
            city,
            state,
            ROW_NUMBER() OVER (PARTITION BY event_source ORDER BY event_date DESC) as rn
        FROM "silver"."events"
    ),
    events_by_source AS (
        SELECT 
            event_source,
            ARRAY_AGG(
                CAST(
                    ROW(event_id, event_title, event_description, event_date, company_name, city, state)
                    AS ROW(eventId VARCHAR, eventTitle VARCHAR, eventDescription VARCHAR, eventDate VARCHAR, company VARCHAR, city VARCHAR, state VARCHAR)
                )
                ORDER BY event_date DESC
            ) FILTER (WHERE rn <= 5) as events
        FROM recent_events
        GROUP BY event_source
    )
    SELECT 
        t.*,
        ARRAY_AGG(
            CAST(
                ROW(LOWER(s.state), s.state, s.count) 
                AS ROW(slug VARCHAR, name VARCHAR, count BIGINT)
            )
            ORDER BY s.count DESC
        ) as states_directory,
        MAP_AGG(e.event_source, e.events) as recentByEventSource
    FROM totals t
    CROSS JOIN state_counts s
    LEFT JOIN events_by_source e ON 1=1
    GROUP BY t.total_events, t.total_companies, t.total_cities, t.total_states, 
             t.first_seen, t.last_seen, t.aggregation_date
) 
TO 's3://venture-os-ocean/coast/nation/' 
WITH (format = 'JSON', compression = 'NONE')
