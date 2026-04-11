{{ config(materialized='table') }}

-- entities/dimensions/measures を UNION した「カラム視点」のビュー。
-- column_name は expr が単純な識別子の場合のみ採用する。

{% set ident_regex = "^[A-Za-z_][A-Za-z0-9_]*$" %}

SELECT
    datasource,
    table_unique_id AS unique_id,
    CASE WHEN regexp_matches(expr, '{{ ident_regex }}') THEN expr ELSE NULL END AS column_name,
    'entity' AS semantic_role,
    entity_name AS semantic_name,
    entity_type AS semantic_type,
    NULL AS agg,
    description
FROM {{ ref('stg_semantic_entities') }}

UNION ALL

SELECT
    datasource,
    table_unique_id AS unique_id,
    CASE WHEN regexp_matches(expr, '{{ ident_regex }}') THEN expr ELSE NULL END AS column_name,
    'dimension' AS semantic_role,
    dimension_name AS semantic_name,
    dimension_type AS semantic_type,
    NULL AS agg,
    description
FROM {{ ref('stg_semantic_dimensions') }}

UNION ALL

SELECT
    datasource,
    table_unique_id AS unique_id,
    CASE WHEN regexp_matches(expr, '{{ ident_regex }}') THEN expr ELSE NULL END AS column_name,
    'measure' AS semantic_role,
    measure_name AS semantic_name,
    NULL AS semantic_type,
    agg,
    description
FROM {{ ref('stg_semantic_measures') }}
