{% macro read_dataset_meta(datasource) %}

SELECT *
FROM read_json(
    '.fdl/artifacts/{{ datasource }}_meta.json',
    format='auto',
    columns={
        datasource: 'VARCHAR',
        title: 'VARCHAR',
        description: 'VARCHAR',
        cover: 'VARCHAR',
        tags: 'JSON',
        repository_url: 'VARCHAR',
        schedule: 'VARCHAR',
        license: 'VARCHAR',
        license_url: 'VARCHAR',
        source_url: 'VARCHAR',
        ducklake_url: 'VARCHAR',
        schemas: 'JSON'
    }
)

{% endmacro %}
