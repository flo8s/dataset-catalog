SELECT
    datasource,
    unique_id,
    source_name,
    name,
    schema_name,
    database,
    description,
    identifier,
    loader,
    meta_json,
    tags_json
FROM {{ ref('stg_sources') }}
