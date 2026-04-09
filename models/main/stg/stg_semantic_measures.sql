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
measures_expanded AS (
    SELECT
        te.datasource,
        te.schema_name,
        te.tbl->>'$.name' AS table_name,
        'model.' || te.datasource || '.' || (te.tbl->>'$.name') AS node_id,
        json_extract(te.tbl, '$.semantic.measures[' || m.measure_index || ']') AS measure
    FROM tables_expanded te,
    LATERAL (
        SELECT UNNEST(generate_series(
            0::BIGINT,
            json_array_length(json_extract(te.tbl, '$.semantic.measures'))::BIGINT - 1
        )) AS measure_index
    ) m
    WHERE json_type(json_extract(te.tbl, '$.semantic.measures')) = 'ARRAY'
      AND json_array_length(json_extract(te.tbl, '$.semantic.measures')) > 0
)

SELECT
    datasource,
    schema_name,
    table_name,
    node_id,
    measure->>'$.name' AS measure_name,
    measure->>'$.agg' AS agg,
    measure->>'$.expr' AS expr,
    COALESCE(measure->>'$.description', '') AS description
FROM measures_expanded
