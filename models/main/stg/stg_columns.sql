{{ config(materialized='view') }}

-- manifest.nodes.*.columns を展開 + catalog.json の実行時型情報で補強

WITH node_columns AS (
    SELECT
        n.datasource,
        n.unique_id,
        n.name AS table_name,
        c.col_name,
        n.columns_json->c.col_name AS col
    FROM {{ ref('stg_nodes') }} n,
    LATERAL (
        SELECT UNNEST(json_keys(n.columns_json)) AS col_name
    ) c
    WHERE n.columns_json IS NOT NULL
      AND json_type(n.columns_json) = 'OBJECT'
      AND json_array_length(json_keys(n.columns_json)::JSON) > 0
),
-- catalog.json のカラム情報
catalog_types AS (
    SELECT
        cat.datasource,
        k.unique_id,
        col.col_name,
        (cat.nodes->k.unique_id->'columns'->col.col_name)->>'type' AS catalog_type,
        CAST((cat.nodes->k.unique_id->'columns'->col.col_name)->>'index' AS INTEGER) AS catalog_index
    FROM {{ ref('stg_all_catalogs') }} cat,
    LATERAL (
        SELECT UNNEST(json_keys(cat.nodes)) AS unique_id
    ) k,
    LATERAL (
        SELECT UNNEST(json_keys(
            cat.nodes->k.unique_id->'columns'
        )) AS col_name
    ) col
    WHERE json_type(cat.nodes->k.unique_id->'columns') = 'OBJECT'
)

SELECT
    nc.datasource,
    nc.unique_id,
    nc.table_name,
    nc.col_name AS column_name,
    COALESCE(ct.catalog_index, ROW_NUMBER() OVER (PARTITION BY nc.unique_id ORDER BY nc.col_name))::INTEGER AS column_index,
    COALESCE(nc.col->'meta'->>'title', '') AS title,
    nc.col->>'description' AS description,
    COALESCE(ct.catalog_type, nc.col->>'data_type', '') AS data_type,
    nc.col->'meta' AS meta_json,
    nc.col->'constraints' AS constraints_json,
    true AS nullable  -- manifest/catalog does not expose nullable; default to true
FROM node_columns nc
LEFT JOIN catalog_types ct
    ON nc.datasource = ct.datasource
    AND nc.unique_id = ct.unique_id
    AND nc.col_name = ct.col_name
