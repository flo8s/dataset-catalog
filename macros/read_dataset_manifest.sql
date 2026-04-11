{% macro read_dataset_manifest(datasource) %}

SELECT '{{ datasource }}' AS datasource, *
FROM read_json(
    '{{ env_var("FDL_STORAGE_BASE") }}/{{ datasource }}/dbt/manifest.json',
    format='auto',
    columns={
        nodes: 'JSON',
        sources: 'JSON',
        parent_map: 'JSON',
        metadata: 'JSON',
        semantic_models: 'JSON'
    }
)

{% endmacro %}
