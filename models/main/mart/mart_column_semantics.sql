{{ config(materialized='table') }}

SELECT
    datasource,
    node_id,
    expr AS column_name,
    'entity' AS semantic_role,
    entity_name AS semantic_name,
    entity_type AS semantic_type,
    NULL AS agg,
    description
FROM {{ ref('stg_semantic_entities') }}

UNION ALL

SELECT
    datasource,
    node_id,
    expr AS column_name,
    'dimension' AS semantic_role,
    dimension_name AS semantic_name,
    dimension_type AS semantic_type,
    NULL AS agg,
    description
FROM {{ ref('stg_semantic_dimensions') }}

UNION ALL

SELECT
    datasource,
    node_id,
    expr AS column_name,
    'measure' AS semantic_role,
    measure_name AS semantic_name,
    NULL AS semantic_type,
    agg,
    description
FROM {{ ref('stg_semantic_measures') }}
