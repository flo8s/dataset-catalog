SELECT
    datasource,
    unique_id,
    name,
    table_unique_id,
    alias,
    schema_name,
    database,
    description,
    entities_json,
    dimensions_json,
    measures_json
FROM {{ ref('stg_semantic_models') }}
