{{ config(materialized='view') }}

-- manifest.sources を展開。dbt の source ノード。

SELECT
    m.datasource,
    k.unique_id,
    m.sources->k.unique_id->>'source_name' AS source_name,
    m.sources->k.unique_id->>'name' AS name,
    m.sources->k.unique_id->>'schema' AS schema_name,
    m.sources->k.unique_id->>'database' AS database,
    m.sources->k.unique_id->>'description' AS description,
    m.sources->k.unique_id->'meta' AS meta_json,
    m.sources->k.unique_id->'tags' AS tags_json,
    m.sources->k.unique_id->>'identifier' AS identifier,
    m.sources->k.unique_id->>'loader' AS loader
FROM {{ ref('stg_all_manifests') }} m,
LATERAL (
    SELECT UNNEST(json_keys(m.sources)) AS unique_id
) k
WHERE json_type(m.sources) = 'OBJECT'
  AND json_array_length(json_keys(m.sources)::JSON) > 0
