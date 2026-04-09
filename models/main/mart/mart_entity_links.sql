{{ config(materialized='table') }}

SELECT
    a.datasource AS from_datasource,
    a.schema_name AS from_schema,
    a.table_name AS from_table,
    a.entity_name,
    a.entity_type AS from_type,
    a.expr AS from_column,
    b.datasource AS to_datasource,
    b.schema_name AS to_schema,
    b.table_name AS to_table,
    b.entity_type AS to_type,
    b.expr AS to_column
FROM {{ ref('stg_semantic_entities') }} a
JOIN {{ ref('stg_semantic_entities') }} b
    ON a.entity_name = b.entity_name
WHERE (a.datasource, a.table_name) != (b.datasource, b.table_name)
