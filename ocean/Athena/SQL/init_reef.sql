CREATE EXTERNAL TABLE IF NOT EXISTS reef (
    event_id string,
    event_source string,
    source_url string,
    ingested_at string,
    event_date string,
    state string,
    city string,
    city_slug string,
    company_name string,
    company_slug string,
    site_id string,
    event_title string,
    event_description string,
    event_details string,
    raw_data string,
    bedrock_event_title string,
    bedrock_event_description string,
    bedrock_verified boolean,
    quarantine_reason string
)
PARTITIONED BY (partition_source string, partition_year string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://venture-os-ocean/reef/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');
