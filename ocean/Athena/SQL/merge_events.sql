MERGE INTO events AS target
USING (
    SELECT * FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY ingested_at DESC) as rn
        FROM reef
    ) WHERE rn = 1
) AS incoming
ON target.event_id = incoming.event_id

WHEN MATCHED THEN UPDATE SET
    event_source = incoming.partition_source,
    source_url = incoming.source_url,
    ingested_at = from_iso8601_timestamp(incoming.ingested_at),
    event_date = COALESCE(TRY(from_iso8601_timestamp(incoming.event_date)), TRY(date_parse(substr(incoming.event_date, 1, 10), '%Y-%m-%d'))),
    state = incoming.state,
    city = incoming.city,
    city_slug = incoming.city_slug,
    company_name = incoming.company_name,
    company_slug = incoming.company_slug,
    site_id = incoming.site_id,
    event_title = incoming.event_title,
    event_description = incoming.event_description,
    event_details = incoming.event_details,
    raw_data = incoming.raw_data,
    bedrock_event_title = incoming.bedrock_event_title,
    bedrock_event_description = incoming.bedrock_event_description,
    bedrock_verified = incoming.bedrock_verified

WHEN NOT MATCHED THEN INSERT (
    event_id, event_source, source_url, ingested_at, event_date,
    state, city, city_slug, company_name, company_slug, site_id,
    event_title, event_description, event_details,
    raw_data, bedrock_event_title, bedrock_event_description, bedrock_verified,
    quarantine_reason
) VALUES (
    incoming.event_id, incoming.partition_source, incoming.source_url,
    from_iso8601_timestamp(incoming.ingested_at),
    COALESCE(TRY(from_iso8601_timestamp(incoming.event_date)), TRY(date_parse(substr(incoming.event_date, 1, 10), '%Y-%m-%d'))),
    incoming.state, incoming.city, incoming.city_slug, incoming.company_name, incoming.company_slug, incoming.site_id,
    incoming.event_title, incoming.event_description, incoming.event_details,
    incoming.raw_data, incoming.bedrock_event_title, incoming.bedrock_event_description, incoming.bedrock_verified,
    incoming.quarantine_reason);

);
