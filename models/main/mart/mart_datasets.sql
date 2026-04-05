SELECT
    datasource,
    title,
    description,
    cover,
    ducklake_url,
    repository_url,
    schedule,
    tags_json,
    readme
FROM {{ ref('stg_datasets') }}
