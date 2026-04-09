{{ config(materialized='view') }}

WITH schemas_expanded AS (
    SELECT
        r.datasource,
        s.schema_name,
        json_extract(r.schemas, '$.' || s.schema_name || '.tables') AS tables_json
    FROM {{ ref('stg_catalog') }} r,
    LATERAL (
        SELECT UNNEST(json_keys(r.schemas)) AS schema_name
    ) s
    WHERE json_type(json_extract(r.schemas, '$.' || s.schema_name || '.tables')) = 'ARRAY'
),
tables_expanded AS (
    SELECT
        se.datasource,
        se.schema_name,
        json_extract(se.tables_json, '$[' || t.table_index || ']') AS tbl
    FROM schemas_expanded se,
    LATERAL (
        SELECT UNNEST(generate_series(0::BIGINT, json_array_length(se.tables_json)::BIGINT - 1)) AS table_index
    ) t
),
entities_expanded AS (
    SELECT
        te.datasource,
        te.schema_name,
        te.tbl->>'$.name' AS table_name,
        'model.' || te.datasource || '.' || (te.tbl->>'$.name') AS node_id,
        json_extract(te.tbl, '$.semantic.entities[' || e.entity_index || ']') AS entity
    FROM tables_expanded te,
    LATERAL (
        SELECT UNNEST(generate_series(
            0::BIGINT,
            json_array_length(json_extract(te.tbl, '$.semantic.entities'))::BIGINT - 1
        )) AS entity_index
    ) e
    WHERE json_type(json_extract(te.tbl, '$.semantic.entities')) = 'ARRAY'
      AND json_array_length(json_extract(te.tbl, '$.semantic.entities')) > 0
)

SELECT
    datasource,
    schema_name,
    table_name,
    node_id,
    entity->>'$.name' AS entity_name,
    entity->>'$.type' AS entity_type,
    entity->>'$.expr' AS expr,
    COALESCE(entity->>'$.description', '') AS description
FROM entities_expanded
