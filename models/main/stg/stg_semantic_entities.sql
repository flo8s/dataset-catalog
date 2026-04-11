{{ config(materialized='view') }}

WITH expanded AS (
    SELECT
        sm.datasource,
        sm.unique_id AS semantic_model_unique_id,
        sm.table_unique_id,
        json_extract(sm.entities_json, '$[' || e.idx || ']') AS entity
    FROM {{ ref('stg_semantic_models') }} sm,
    LATERAL (
        SELECT UNNEST(generate_series(
            0::BIGINT,
            json_array_length(sm.entities_json)::BIGINT - 1
        )) AS idx
    ) e
    WHERE json_type(sm.entities_json) = 'ARRAY'
      AND json_array_length(sm.entities_json) > 0
)

SELECT
    datasource,
    semantic_model_unique_id,
    table_unique_id,
    entity->>'name' AS entity_name,
    entity->>'type' AS entity_type,
    COALESCE(entity->>'expr', entity->>'name') AS expr,
    COALESCE(entity->>'description', '') AS description
FROM expanded
