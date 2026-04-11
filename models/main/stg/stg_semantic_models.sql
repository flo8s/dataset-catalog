{{ config(materialized='view') }}

-- manifest.semantic_models を1行1semantic model に展開
-- raw JSON (entities/dimensions/measures) を保持

SELECT
    m.datasource,
    k.unique_id,
    (m.semantic_models->k.unique_id)->>'name' AS name,
    (m.semantic_models->k.unique_id)->'node_relation'->>'schema_name' AS schema_name,
    (m.semantic_models->k.unique_id)->'node_relation'->>'database' AS database,
    (m.semantic_models->k.unique_id)->'node_relation'->>'alias' AS alias,
    'model.' || m.datasource || '.' || ((m.semantic_models->k.unique_id)->'node_relation'->>'alias') AS table_unique_id,
    (m.semantic_models->k.unique_id)->>'description' AS description,
    (m.semantic_models->k.unique_id)->'entities' AS entities_json,
    (m.semantic_models->k.unique_id)->'dimensions' AS dimensions_json,
    (m.semantic_models->k.unique_id)->'measures' AS measures_json,
    m.semantic_models->k.unique_id AS sm
FROM {{ ref('stg_all_manifests') }} m,
LATERAL (
    SELECT UNNEST(json_keys(m.semantic_models)) AS unique_id
) k
WHERE json_type(m.semantic_models) = 'OBJECT'
  AND json_array_length(json_keys(m.semantic_models)::JSON) > 0
