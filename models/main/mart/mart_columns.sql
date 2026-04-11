SELECT
    datasource,
    unique_id,
    table_name,
    column_name,
    column_index,
    title,
    description,
    data_type,
    meta_json,
    constraints_json,
    nullable
FROM {{ ref('stg_columns') }}
