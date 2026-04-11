{{ config(materialized='view') }}

WITH expanded AS (
    SELECT
        sm.datasource,
        sm.unique_id AS semantic_model_unique_id,
        sm.table_unique_id,
        json_extract(sm.dimensions_json, '$[' || d.idx || ']') AS dim
    FROM {{ ref('stg_semantic_models') }} sm,
    LATERAL (
        SELECT UNNEST(generate_series(
            0::BIGINT,
            json_array_length(sm.dimensions_json)::BIGINT - 1
        )) AS idx
    ) d
    WHERE json_type(sm.dimensions_json) = 'ARRAY'
      AND json_array_length(sm.dimensions_json) > 0
)

SELECT
    datasource,
    semantic_model_unique_id,
    table_unique_id,
    dim->>'name' AS dimension_name,
    dim->>'type' AS dimension_type,
    COALESCE(dim->>'expr', dim->>'name') AS expr,
    COALESCE(dim->>'description', '') AS description
FROM expanded
