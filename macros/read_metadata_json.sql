{% macro read_metadata_json(datasource) %}

SELECT '{{ datasource }}' AS datasource, *
FROM read_json(
    '.fdl/artifacts/{{ datasource }}.json',
    columns={
        title: 'VARCHAR',
        description: 'VARCHAR',
        cover: 'VARCHAR',
        tags: 'JSON',
        ducklake_url: 'VARCHAR',
        repository_url: 'VARCHAR',
        schemas: 'JSON',
        dependencies: 'JSON',
        lineage: 'JSON',
        readme: 'VARCHAR'
    }
)

{% endmacro %}
