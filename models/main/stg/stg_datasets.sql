{{ config(materialized='view') }}

-- fdl.toml メタデータ + manifest.metadata を統合

SELECT
    m.datasource,
    m.title,
    m.description,
    m.cover,
    m.ducklake_url,
    m.repository_url,
    m.schedule,
    m.tags::JSON AS tags_json,
    m.license,
    m.license_url,
    m.source_url,
    m.schemas AS schemas_json,
    -- manifest.metadata
    mf.metadata->>'dbt_version' AS dbt_version,
    mf.metadata->>'generated_at' AS dbt_generated_at,
    mf.metadata->>'invocation_id' AS dbt_invocation_id,
    NULL::VARCHAR AS readme
FROM {{ ref('stg_all_metas') }} m
LEFT JOIN {{ ref('stg_all_manifests') }} mf ON m.datasource = mf.datasource
