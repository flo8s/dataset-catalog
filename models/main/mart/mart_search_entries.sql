WITH published_models AS (
    SELECT *
    FROM {{ ref('mart_nodes') }}
    WHERE resource_type = 'model' AND is_published = true
),

tags_agg AS (
    SELECT
        n.datasource,
        n.unique_id,
        STRING_AGG(tag.value::VARCHAR, ' ') AS tags_text
    FROM published_models n,
        LATERAL UNNEST(
            CASE WHEN n.tags_json IS NOT NULL
                 THEN CAST(n.tags_json AS VARCHAR[])
                 ELSE ARRAY[]::VARCHAR[]
            END
        ) AS tag(value)
    GROUP BY n.datasource, n.unique_id
),

tables AS (
    SELECT
        'table' AS entry_type,
        n.datasource,
        n.schema_name,
        n.name AS table_name,
        n.title AS table_title,
        NULL::VARCHAR AS column_name,
        n.description,
        COALESCE(REPLACE(n.name, '_', ' '), '') || ' ' ||
            COALESCE(n.title, '') || ' ' ||
            COALESCE(n.description, '') || ' ' ||
            COALESCE(ta.tags_text, '') AS search_text,
        '/datasets/' || n.datasource || '/' || n.schema_name || '/' || n.name AS href
    FROM published_models n
    LEFT JOIN tags_agg ta ON n.datasource = ta.datasource AND n.unique_id = ta.unique_id
),

columns AS (
    SELECT
        'column' AS entry_type,
        c.datasource,
        n.schema_name,
        n.name AS table_name,
        n.title AS table_title,
        c.column_name,
        c.description,
        COALESCE(REPLACE(c.column_name, '_', ' '), '') || ' ' ||
            COALESCE(c.title, '') || ' ' ||
            COALESCE(c.description, '') || ' ' ||
            COALESCE(REPLACE(n.name, '_', ' '), '') AS search_text,
        '/datasets/' || c.datasource || '/' || n.schema_name || '/' || n.name AS href
    FROM {{ ref('mart_columns') }} c
    JOIN published_models n
        ON c.datasource = n.datasource AND c.unique_id = n.unique_id
)

SELECT * FROM tables
UNION ALL
SELECT * FROM columns
