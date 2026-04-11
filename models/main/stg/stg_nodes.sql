{{ config(materialized='view') }}

-- manifest.nodes を展開。全 resource_type (model/test/seed/snapshot/operation) を含む。
-- dbt ネイティブ名（unique_id, resource_type, materialized 等）を使用。

WITH nodes_expanded AS (
    SELECT
        m.datasource,
        k.unique_id,
        m.nodes->k.unique_id AS node
    FROM {{ ref('stg_all_manifests') }} m,
    LATERAL (
        SELECT UNNEST(json_keys(m.nodes)) AS unique_id
    ) k
),
-- fdl.toml のライセンスデフォルト
dataset_defaults AS (
    SELECT
        datasource,
        license AS default_license,
        license_url AS default_license_url,
        source_url AS default_source_url,
        schemas AS schemas_json
    FROM {{ ref('stg_all_metas') }}
)

SELECT
    n.datasource,
    n.unique_id,
    n.node->>'resource_type' AS resource_type,
    n.node->>'name' AS name,
    n.node->>'schema' AS schema_name,
    n.node->>'database' AS database,
    n.node->>'alias' AS alias,
    n.node->>'description' AS description,
    n.node->'config'->>'materialized' AS materialized,
    n.node->>'access' AS access,
    n.node->>'original_file_path' AS file_path,
    -- 派生カラム（よく使うもの）
    COALESCE(n.node->'meta'->>'title', '') AS title,
    COALESCE(CAST(n.node->'meta'->>'published' AS BOOLEAN), false) AS is_published,
    -- 3段階ライセンスフォールバック
    COALESCE(
        NULLIF(n.node->'meta'->>'license', ''),
        json_extract_string(d.schemas_json, '$.' || (n.node->>'schema') || '.license'),
        d.default_license
    ) AS license,
    COALESCE(
        NULLIF(n.node->'meta'->>'license_url', ''),
        json_extract_string(d.schemas_json, '$.' || (n.node->>'schema') || '.license_url'),
        d.default_license_url
    ) AS license_url,
    COALESCE(
        NULLIF(n.node->'meta'->>'source_url', ''),
        json_extract_string(d.schemas_json, '$.' || (n.node->>'schema') || '.source_url'),
        d.default_source_url
    ) AS source_url,
    n.node->'meta'->'tags' AS tags_json,
    -- raw 保持（情報ロスゼロ）
    n.node->'meta' AS meta_json,
    n.node->'config' AS config_json,
    n.node->'depends_on' AS depends_on_json,
    n.node->'refs' AS refs_json,
    n.node->'sources' AS sources_json,
    n.node->'fqn' AS fqn_json,
    NULLIF(TRIM(n.node->>'compiled_code'), '') AS compiled_code,
    -- carry columns dict for stg_columns
    n.node->'columns' AS columns_json
FROM nodes_expanded n
LEFT JOIN dataset_defaults d ON n.datasource = d.datasource
