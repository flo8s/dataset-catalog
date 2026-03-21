SELECT
    datasource,
    node_id,
    node_index,
    name,
    schema_name,
    description,
    materialized,
    title,
    license,
    license_url,
    source_url,
    is_published,
    tags_json,
    sql,
    file_path
FROM {{ ref('stg_models') }}
