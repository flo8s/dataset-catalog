{% macro read_dataset_catalog(datasource) %}

SELECT '{{ datasource }}' AS datasource, *
FROM read_json(
    '{{ env_var("FDL_STORAGE_BASE") }}/{{ datasource }}/dbt/catalog.json',
    format='auto',
    columns={
        nodes: 'JSON',
        sources: 'JSON'
    }
)

{% endmacro %}
