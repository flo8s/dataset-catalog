{{ config(materialized='view') }}

-- manifest.parent_map を edge リストに展開
-- フルの unique_id (例: "model.e_stat.small_area") で保持

WITH parent_map_entries AS (
    SELECT
        m.datasource,
        k.child_unique_id,
        m.parent_map->k.child_unique_id AS parents_json
    FROM {{ ref('stg_all_manifests') }} m,
    LATERAL (
        SELECT UNNEST(json_keys(m.parent_map)) AS child_unique_id
    ) k
)

SELECT
    pe.datasource,
    pe.child_unique_id,
    p.parent_unique_id::VARCHAR AS parent_unique_id
FROM parent_map_entries pe,
LATERAL (
    SELECT UNNEST(
        from_json(pe.parents_json, '["VARCHAR"]')
    ) AS parent_unique_id
) p
