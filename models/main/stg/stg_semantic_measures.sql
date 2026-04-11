{{ config(materialized='view') }}

WITH expanded AS (
    SELECT
        sm.datasource,
        sm.unique_id AS semantic_model_unique_id,
        sm.table_unique_id,
        json_extract(sm.measures_json, '$[' || ms.idx || ']') AS measure
    FROM {{ ref('stg_semantic_models') }} sm,
    LATERAL (
        SELECT UNNEST(generate_series(
            0::BIGINT,
            json_array_length(sm.measures_json)::BIGINT - 1
        )) AS idx
    ) ms
    WHERE json_type(sm.measures_json) = 'ARRAY'
      AND json_array_length(sm.measures_json) > 0
)

SELECT
    datasource,
    semantic_model_unique_id,
    table_unique_id,
    measure->>'name' AS measure_name,
    measure->>'agg' AS agg,
    COALESCE(measure->>'expr', measure->>'name') AS expr,
    COALESCE(measure->>'description', '') AS description
FROM expanded
