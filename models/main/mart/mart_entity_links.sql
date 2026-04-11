{{ config(materialized='table') }}

-- semantic entities の self-join によるクロスデータセット結合グラフ

SELECT
    a.datasource AS from_datasource,
    a.table_unique_id AS from_unique_id,
    a.entity_name,
    a.entity_type AS from_type,
    a.expr AS from_column,
    b.datasource AS to_datasource,
    b.table_unique_id AS to_unique_id,
    b.entity_type AS to_type,
    b.expr AS to_column
FROM {{ ref('stg_semantic_entities') }} a
JOIN {{ ref('stg_semantic_entities') }} b
    ON a.entity_name = b.entity_name
WHERE (a.datasource, a.table_unique_id) != (b.datasource, b.table_unique_id)
