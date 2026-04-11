SELECT
    datasource,
    child_unique_id,
    parent_unique_id
FROM {{ ref('stg_lineage_edges') }}
