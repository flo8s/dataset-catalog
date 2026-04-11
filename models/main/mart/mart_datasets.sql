SELECT
    datasource,
    title,
    description,
    cover,
    ducklake_url,
    repository_url,
    schedule,
    tags_json,
    license,
    license_url,
    source_url,
    schemas_json,
    dbt_version,
    dbt_generated_at,
    dbt_invocation_id,
    readme
FROM {{ ref('stg_datasets') }}
