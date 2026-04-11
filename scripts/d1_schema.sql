-- D1 schema for dataset-catalog sync.
-- Source of truth: the INSERT column lists in scripts/sync_catalog_d1.py.
-- Applied idempotently at the start of every sync run.

-- Drop tables removed in the mart refactor (ad854b6).
DROP TABLE IF EXISTS catalog_tables;
DROP TABLE IF EXISTS catalog_dependencies;

-- Drop tables whose column shape changed in ad854b6 so they get recreated.
DROP TABLE IF EXISTS catalog_datasets;
DROP TABLE IF EXISTS catalog_columns;

CREATE TABLE IF NOT EXISTS catalog_datasets (
  datasource TEXT PRIMARY KEY,
  title TEXT,
  description TEXT,
  cover TEXT,
  ducklake_url TEXT,
  repository_url TEXT,
  schedule TEXT,
  tags TEXT,
  dbt_version TEXT,
  dbt_generated_at TEXT
);

CREATE TABLE IF NOT EXISTS catalog_schemas (
  id TEXT PRIMARY KEY,
  datasource TEXT,
  schema_name TEXT,
  title TEXT
);

CREATE TABLE IF NOT EXISTS catalog_nodes (
  unique_id TEXT PRIMARY KEY,
  datasource TEXT,
  resource_type TEXT,
  name TEXT,
  schema_name TEXT,
  database TEXT,
  description TEXT,
  materialized TEXT,
  title TEXT,
  license TEXT,
  license_url TEXT,
  source_url TEXT,
  is_published INTEGER,
  tags TEXT,
  compiled_code TEXT,
  file_path TEXT,
  node_index INTEGER
);

CREATE TABLE IF NOT EXISTS catalog_columns (
  id TEXT PRIMARY KEY,
  datasource TEXT,
  unique_id TEXT,
  table_name TEXT,
  column_name TEXT,
  title TEXT,
  description TEXT,
  data_type TEXT,
  column_index INTEGER
);

CREATE TABLE IF NOT EXISTS catalog_column_semantics (
  id TEXT PRIMARY KEY,
  datasource TEXT,
  unique_id TEXT,
  column_name TEXT,
  semantic_role TEXT,
  semantic_name TEXT,
  semantic_type TEXT,
  agg TEXT
);
